import copy
from pathlib import Path
import tempfile
import time
import unittest

from cdispatch.common import validate,enqueue
from cdispatch.daemon import Daemon
from cdispatch.planning import forecast
from cdispatch.scheduler import choose,queue_plan,reserve,memory
from test_dispatcher import config,node,snapshot,spec,Fake


class ForecastTests(unittest.TestCase):
    def setUp(self):
        self.cfg=config();self.snap=snapshot();self.cfg['node_cost']={'old':1,'fast':40}
        self.snap['nodes']=[node('old',cpus=36,part='old'),node('fast',cpus=128,part='cpu')]
        self.old=dict(name='serial-old',partition='old',nodes=1,ranks_per_node=1,threads=2,mem_mib=512,wall_minutes=10,slowdown=1.5)
        self.fast=dict(self.old,name='serial-fast',partition='cpu',slowdown=1)
        self.task=spec(profiles=[self.fast,self.old],max_slowdown=2,users=['alice'])

    def data(self,tasks=None):return dict(heartbeat=time.time(),snapshot=self.snap,tasks=tasks or {})

    def test_small_job_prefers_old_then_falls_back_when_full(self):
        result=forecast(self.task,self.data(),self.cfg)
        self.assertEqual(result['selection']['nodes'],['old']);self.assertEqual(result['resources']['cpus'],2)
        self.snap['nodes'][0]['CPUAlloc']='36'
        self.assertEqual(forecast(self.task,self.data(),self.cfg)['selection']['nodes'],['fast'])

    def test_explicit_speed_preference_and_memory_per_layout(self):
        self.task['placement']={'policy':'fast'}
        self.fast.update(ranks_per_node=8,threads=4,mem_mib=100, memory_model=dict(base_mib=100,per_rank_mib=200,per_thread_mib=50,safety_factor=1.2))
        self.task=validate(dict(self.task,profiles=[self.fast,self.old]))
        result=forecast(self.task,self.data(),self.cfg)
        self.assertEqual(result['resources']['cpus'],32);self.assertEqual(result['layout']['mem_mib_per_node'],3960)
        self.assertEqual(result['selection']['nodes'],['fast'])

    def test_preferences_are_soft_or_strict_as_requested(self):
        self.task['placement']={'preferred_nodes':['fast']}
        self.assertEqual(forecast(self.task,self.data(),self.cfg)['selection']['nodes'],['fast'])
        self.snap['nodes'][1]['CPUAlloc']='128'
        self.assertEqual(forecast(self.task,self.data(),self.cfg)['selection']['nodes'],['old'])
        self.task['placement']['allow_fallback']=False
        self.assertIsNone(forecast(self.task,self.data(),self.cfg)['selection'])

    def test_cpu_job_avoids_gpu_and_explicit_opt_in_works(self):
        self.snap['nodes'][0]['Gres']='gpu:a100:4';self.snap['nodes'][1]['CPUAlloc']='128'
        self.assertIsNone(forecast(self.task,self.data(),self.cfg)['selection'])
        self.task.setdefault('placement',{})['avoid_gpu_nodes']=False
        self.assertEqual(forecast(self.task,self.data(),self.cfg)['selection']['nodes'],['old'])

    def test_experimental_has_no_profile_fallback_or_retry(self):
        with self.assertRaises(ValueError):validate(dict(self.task,experimental=True))
        fixed=spec(experimental=True,profiles=[dict(self.fast,slowdown=1)])
        self.assertEqual(fixed['max_attempts'],1)
        self.snap['nodes'][1]['CPUAlloc']='128'
        self.assertIsNone(forecast(fixed,self.data(),self.cfg)['selection'])
        with self.assertRaises(ValueError):validate(dict(fixed,restart_from_scratch_safe=True))
        with self.assertRaises(ValueError):validate(dict(fixed,max_attempts=2))

    def test_stale_dependencies_and_pause_prevent_forecast_admission(self):
        data=self.data();data['heartbeat']=0
        self.assertFalse(forecast(self.task,data,self.cfg)['would_submit'])
        data=self.data();data['paused']=True
        self.assertIn('paused',forecast(self.task,data,self.cfg)['reason'])
        self.assertEqual(forecast(dict(self.task,depends_on=['absent']),self.data(),self.cfg)['reason'],'unknown dependency')

    def test_forecast_is_pure_and_accounts_for_competing_priority(self):
        self.snap['nodes']=[node('fast',cpus=2,part='cpu')]
        high=copy.deepcopy(self.task);high.update(priority=900,workdir='/tmp/other')
        tasks={'high':dict(id='high',spec=high,state='QUEUED',created=1,attempts=[])}
        data=self.data(tasks);before=copy.deepcopy(data)
        result=forecast(self.task,data,self.cfg)
        self.assertEqual(data,before);self.assertFalse(result['would_submit']);self.assertIsNone(result['selection']);self.assertFalse(result['pause'])

    def test_checkpoint_prediction_matches_daemon_and_protects_other_jobs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);fake=Fake();daemon=Daemon(root,self.cfg,fake)
            victim_spec=spec(workdir=tmp,priority=10,preemptible=True,checkpoint_command=['/bin/true'],checkpoint_validate_command=['/bin/true'],checkpoint_files=[tmp+'/check'],resume_command=['/bin/true'])
            enqueue(root,victim_spec,'victim');daemon.tick();victim=daemon.state['tasks']['victim'];victim['state']='RUNNING'
            fake.snap=snapshot();reserve(fake.snap,victim['selection'],victim['job_id'],victim['job_name']);fake.snap['jobs'][0]['JobState']='RUNNING';fake.calls.clear()
            wanted=spec(workdir=tmp+'/next',urgent=True,profiles=[dict(spec()['profiles'][0],ranks_per_node=32)])
            data=dict(heartbeat=time.time(),snapshot=fake.snap,tasks=daemon.state['tasks'])
            before=copy.deepcopy(data);result=forecast(wanted,data,self.cfg)
            self.assertEqual(result['pause'],['victim']);self.assertEqual(data,before);self.assertFalse(fake.calls)
            enqueue(root,wanted,'wanted');daemon.tick()
            self.assertIn(('pause',victim['job_id']),fake.calls);self.assertFalse(any(isinstance(call,dict) for call in fake.calls))
            victim['state']='RUNNING';victim['spec']['preemptible']=False
            result=forecast(wanted,dict(data,tasks={'victim':victim}),self.cfg)
            self.assertFalse(result['pause']);self.assertIsNone(result['selection'])

    def test_preempt_for_speed_waits_instead_of_starting_slow_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);fake=Fake();daemon=Daemon(root,self.cfg,fake)
            victim=spec(workdir=tmp,preemptible=True,checkpoint_command=['/bin/true'],checkpoint_validate_command=['/bin/true'],checkpoint_files=[tmp+'/cp'],resume_command=['/bin/true'])
            enqueue(root,victim,'victim');daemon.tick();task=daemon.state['tasks']['victim'];task['state']='RUNNING'
            reserve(fake.snap,task['selection'],task['job_id'],task['job_name']);fake.snap['jobs'][0]['JobState']='RUNNING';fake.calls.clear()
            slow=dict(spec()['profiles'][0],name='slow',slowdown=2,expected_seconds=2000)
            fast=dict(slow,name='fast',ranks_per_node=32,slowdown=1,expected_seconds=1000)
            wanted=spec(workdir=tmp+'/wanted',urgent=True,priority=200,max_slowdown=2,placement=dict(policy='fast',preempt_for_speed=True),profiles=[fast,slow])
            result=forecast(wanted,dict(heartbeat=time.time(),snapshot=fake.snap,tasks=daemon.state['tasks']),self.cfg)
            self.assertEqual(result['selection']['profile']['name'],'fast');self.assertEqual(result['pause'],['victim'])
            enqueue(root,wanted,'wanted');daemon.tick();daemon.tick()
            self.assertEqual(task['state'],'PAUSING');self.assertEqual(daemon.state['tasks']['wanted']['state'],'QUEUED')
            self.assertEqual(fake.calls,[('pause',task['job_id'])])


if __name__=='__main__':unittest.main()
