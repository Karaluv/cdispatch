import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from cdispatch.common import atomic,read,validate,enqueue,fields,lock
from cdispatch.scheduler import choose,reserve,memory,usage
from cdispatch.daemon import Daemon
from cdispatch.cli import report

BASE=Path(__file__).resolve().parents[1]

def config():
    c=read(BASE/'tests/fixtures/config.json');c['owner']='alice';c['users']={'alice':dict(enabled=True,authorized=True,account='lab',limits=dict(jobs=5,cpus=256,gpus=4,mem_mib=3000000,nodes=8),partitions=['cpu','old','gpu'])}
    c['submit_interval']=0;c['interactive_reserve_jobs']=0
    return c

def spec(**kw):
    s=dict(name='test',workdir='/tmp/test-work',priority=100,labels=dict(project='glass',purpose='test'),
           command=['/bin/true'],validate_command=['/bin/true'],profiles=[dict(name='cpu64',partition='cpu',ranks_per_node=16,threads=4,mem_mib=100000,wall_minutes=10)])
    s.update(kw);return validate(s)

def node(name='n1',part='cpu',cpus=128,mem=1000000,state='IDLE',**kw):
    n=dict(NodeName=name,Partitions=part,CPUTot=str(cpus),CPUEfctv=str(cpus),CPUAlloc='0',RealMemory=str(mem),AllocMem='0',State=state,Gres='(null)',AllocTRES='')
    n.update(kw);return n

def snapshot():
    return dict(nodes=[node()],partitions={k:dict(State='UP',MaxTime='1-00:00:00') for k in ['cpu','gpu','old']},jobs=[],time=time.time())

class Fake:
    def __init__(self):self.snap=snapshot();self.acct=[];self.calls=[];self.error=False
    def snapshot(self):
        if self.error:raise RuntimeError('controller unreachable')
        return copy.deepcopy(self.snap)
    def accounting(self,since,ids=None):return self.acct
    def submit(self,user,sel,a):self.calls.append(sel);return '123'
    def cancel(self,u,j):self.calls.append(('cancel',j))
    def pause(self,u,j):self.calls.append(('pause',j))

class SchedulerTests(unittest.TestCase):
    def test_two_64_fill_128(self):
        snap=snapshot();c=config();s=spec()
        sel,_=choose(s,snap,c);self.assertIsNotNone(sel);reserve(snap,sel,'1','a')
        sel,_=choose(s,snap,c);self.assertIsNotNone(sel);reserve(snap,sel,'2','b')
        self.assertIsNone(choose(s,snap,c)[0])
    def test_memory_allocation_not_os_freemem(self):
        snap=snapshot();snap['nodes'][0].update(AllocMem='950001',FreeMem='99999999')
        self.assertIsNone(choose(spec(),snap,config())[0])
    def test_bad_nodes(self):
        for state in ['MIXED+DRAIN','UNKNOWN+INVALID_REG','DOWN','IDLE+PLANNED','FAIL','IDLE+NOT_RESPONDING']:
            snap=snapshot();snap['nodes'][0]['State']=state
            self.assertIsNone(choose(spec(),snap,config())[0])
    def test_typed_gpu(self):
        snap=snapshot();snap['nodes']=[node(part='gpu',Gres='gpu:a100:4',AllocTRES='gres/gpu=3')]
        s=spec();s['profiles'][0].update(partition='gpu',gpu_type='h200nvl',gpus_per_node=1)
        self.assertIsNone(choose(s,snap,config())[0]);s['profiles'][0]['gpu_type']='a100'
        self.assertIsNotNone(choose(s,snap,config())[0]);s['profiles'][0]['gpus_per_node']=2
        self.assertIsNone(choose(s,snap,config())[0])
    def test_multinode_needs_all_nodes(self):
        snap=snapshot();s=spec();s['profiles'][0]['nodes']=2
        self.assertIsNone(choose(s,snap,config())[0]);snap['nodes'].append(node('n2'))
        self.assertEqual(len(choose(s,snap,config())[0]['nodes']),2)
    def test_slowdown_gate(self):
        s=spec();s['profiles'][0]['slowdown']=2
        self.assertIsNone(choose(s,snapshot(),config())[0]);s['max_slowdown']=2
        self.assertIsNotNone(choose(s,snapshot(),config())[0])
    def test_user_limit_separate_from_pool(self):
        c=config();c['users']['alice']['limits']['cpus']=32
        self.assertIsNone(choose(spec(),snapshot(),c)[0])
    def test_live_qos_limit(self):
        snap=snapshot();snap['limits']={'associations':[dict(account='lab',user='alice',qos='stud')], 'qos':[dict(name='stud',user_tres='cpu=32,gres/gpu=2,node=2')]}
        self.assertIsNone(choose(spec(),snap,config())[0])
    def test_parent_account_limit(self):
        snap=snapshot();snap['limits']={'associations':[dict(account='lab',user='',group_tres='cpu=80')], 'qos':[]}
        snap['account_jobs']=[dict(JobId='other',Account='lab',UserId='bob',JobState='RUNNING',AllocTRES='cpu=32,mem=1G,node=1')]
        self.assertIsNone(choose(spec(),snap,config())[0])
    def test_interactive_reserve(self):
        c=config();c['pool_limits']['jobs']=1;c['interactive_reserve_jobs']=1
        self.assertIsNone(choose(spec(),snapshot(),c)[0]);s=spec();s['kind']='interactive';s['profiles'][0]['ranks_per_node']=1
        self.assertIsNotNone(choose(s,snapshot(),c)[0])
    def test_memory_model(self):
        p=spec()['profiles'][0];p['memory_model']=dict(base_mib=100000,per_rank_mib=2000,safety_factor=1.2)
        self.assertEqual(memory(p),158400)
    def test_parser_keeps_cancellation_actor(self):
        d=fields('JobId=1 JobState=CANCELLED Reason=Nodes required are DOWN, DRAINED UserId=alice(1) CPUs/Task=2')
        self.assertEqual(d['Reason'],'Nodes required are DOWN, DRAINED');self.assertEqual(d['CPUs/Task'],'2')
    def test_checkpoint_contract_rejected(self):
        with self.assertRaises(ValueError):spec(preemptible=True)

class DaemonTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.fake=Fake();self.d=Daemon(self.root,config(),self.fake)
    def tearDown(self):self.tmp.cleanup()
    def submit(self,**kw):
        kw.setdefault('workdir',str(self.root));tid=enqueue(self.root,spec(**kw));self.d.tick();return tid,self.d.state['tasks'][tid]
    def terminal(self,t,state='COMPLETED'):
        self.fake.acct=[dict(id=t['job_id'],name=t['job_name'],user='alice',state=state,exit_code='0:0',elapsed='60',cpus='64',cpu_seconds='3840',total_cpu='00:00:01',tres='cpu=64,mem=100G,node=1',nodes='n1',max_rss='')]
    def test_duplicate_submission_key(self):
        tid,t=self.submit();enqueue(self.root,spec(workdir=str(self.root)),tid);self.d.tick();self.assertEqual(len(self.fake.calls),1)
    def test_no_submit_on_snapshot_failure(self):
        self.fake.error=True;tid,t=self.submit();self.assertEqual(t['state'],'QUEUED');self.assertFalse(self.fake.calls)
    def test_ambiguous_submit_never_retried(self):
        self.fake.submit=lambda *a:(_ for _ in ()).throw(TimeoutError('network lost'))
        tid,t=self.submit();self.assertEqual(t['state'],'SUBMIT_UNKNOWN')
        self.d.tick();self.assertEqual(len(t['attempts']),1);self.assertTrue(self.d.state['circuit'])
    def test_recover_intent_after_crash(self):
        tid,t=self.submit();t.pop('job_id');t['state']='SUBMITTING'
        self.fake.snap['jobs']=[dict(JobId='123',JobName=t['job_name'],JobState='RUNNING',UserId='alice',AllocTRES='cpu=64,mem=100G,node=1')]
        self.d.tick();self.assertEqual(t['state'],'RUNNING');self.assertEqual(len(self.fake.calls),1)
    def test_external_cancel_actor_and_no_retry(self):
        tid,t=self.submit();self.terminal(t,'CANCELLED by 9001');self.d.tick()
        self.assertEqual(t['state'],'FAILED');e=self.d.state['events'][-1]
        self.assertTrue(e['external']);self.assertEqual(e['cancellation_actor'],'9001');self.d.tick();self.assertEqual(len(self.fake.calls),1)
    def test_accounting_lag_does_not_finish(self):
        tid,t=self.submit();atomic(Path(t['attempt'])/'result.json',dict(state='SUCCEEDED'))
        self.d.tick();self.assertEqual(t['state'],'PENDING')
    def test_slurm_zero_without_worker_result_is_failure(self):
        tid,t=self.submit();self.terminal(t);self.d.tick();self.assertEqual(t['state'],'FAILED')
    def test_dependencies_need_application_validation(self):
        tid,t=self.submit();self.terminal(t);atomic(Path(t['attempt'])/'result.json',dict(state='FINISHED_UNVALIDATED'))
        child=enqueue(self.root,spec(depends_on=[tid],workdir='/tmp/child'));self.d.tick()
        self.assertEqual(self.d.state['tasks'][child]['state'],'BLOCKED')
    def test_workdir_lock(self):
        tid,t=self.submit();second=enqueue(self.root,spec(workdir=str(self.root)));self.d.tick()
        self.assertEqual(self.d.state['tasks'][second]['state'],'QUEUED');self.assertEqual(len(self.fake.calls),1)
    def test_single_writer(self):
        with lock(self.root/'controller.lock'):
            with self.assertRaises(BlockingIOError):
                with lock(self.root/'controller.lock'): pass
    def test_allocation_hours_not_double_count_steps(self):
        tid,t=self.submit();self.terminal(t);r=self.fake.acct[0]
        step=dict(r,id='123.batch');out=report(dict(accounting=[r,step],tasks=self.d.state['tasks']))
        self.assertAlmostEqual(out['alice / glass']['allocated_cpu_hours'],3840/3600)

class WorkerTests(unittest.TestCase):
    def test_completion_and_checkpoint_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            a=Path(tmp);checkpoint=a/'checkpoint-data';output=a/'resumed'
            s=spec(workdir=tmp,command=[sys.executable,'-c','import time; time.sleep(120)'],preemptible=True,
                   checkpoint_command=[sys.executable,'-c',f'open({str(checkpoint)!r},"w").write("complete")'],
                   checkpoint_validate_command=[sys.executable,'-c',f'assert open({str(checkpoint)!r}).read()=="complete"'],
                   checkpoint_files=[str(checkpoint)],resume_command=[sys.executable,'-c',f'open({str(output)!r},"w").write("resumed")'])
            sel=dict(profile=s['profiles'][0]);bundle=dict(id='x',spec=s,selection=sel,created=time.time())
            atomic(a/'spec.json',bundle)
            env=dict(os.environ,PYTHONPATH=str(BASE))
            proc=subprocess.Popen([sys.executable,'-m','cdispatch.worker',tmp],env=env)
            try:
                for _ in range(100):
                    if (a/'started.json').exists():break
                    time.sleep(.02)
                os.kill(proc.pid,signal.SIGUSR1);self.assertEqual(proc.wait(timeout=10),75)
                result=read(a/'result.json');self.assertEqual(result['state'],'PAUSED')
                b=a/'second';b.mkdir();bundle['checkpoint']=result['checkpoint'];atomic(b/'spec.json',bundle)
                subprocess.run([sys.executable,'-m','cdispatch.worker',str(b)],env=env,check=True,timeout=10)
                self.assertEqual(read(b/'result.json')['state'],'SUCCEEDED');self.assertTrue(output.exists())
                checkpoint.write_text('corrupted');c=a/'third';c.mkdir();atomic(c/'spec.json',bundle)
                r=subprocess.run([sys.executable,'-m','cdispatch.worker',str(c)],env=env,timeout=10)
                self.assertEqual(r.returncode,1);self.assertEqual(read(c/'result.json')['state'],'FAILED')
            finally:
                if proc.poll() is None:proc.kill();proc.wait()

if __name__=='__main__':unittest.main()

class JointPlanningTests(unittest.TestCase):
    def test_two_64_beat_one_128_same_priority(self):
        from cdispatch.scheduler import queue_plan
        c=config();c['max_pending']=5
        a=spec(workdir='/tmp/a');b=spec(workdir='/tmp/b')
        big=copy.deepcopy(a['profiles'][0]);big.update(name='cpu128',ranks_per_node=32,slowdown=1)
        a['profiles'][0]['slowdown']=1.5;a['max_slowdown']=2;a['profiles'].insert(0,big)
        tasks=[dict(id='a',spec=a,created=1),dict(id='b',spec=b,created=2)]
        plan,decisions=queue_plan(tasks,snapshot(),c)
        self.assertEqual(len(plan),2);self.assertEqual(plan[0][1]['profile']['ranks_per_node'],16)
    def test_high_priority_before_many_lower(self):
        from cdispatch.scheduler import queue_plan
        a=spec(priority=200,workdir='/tmp/a');a['profiles'][0]['ranks_per_node']=32
        tasks=[dict(id='a',spec=a,created=1)]+[dict(id=str(i),spec=spec(priority=100,workdir='/tmp/'+str(i)),created=i) for i in (2,3)]
        plan,_=queue_plan(tasks,snapshot(),config());self.assertEqual([x[0] for x in plan],['a'])
    def test_urgent_tag_outranks_number(self):
        from cdispatch.scheduler import queue_plan
        a=spec(priority=10,tags=['urgent'],workdir='/tmp/a');a['profiles'][0]['ranks_per_node']=32
        b=spec(priority=9000,workdir='/tmp/b')
        plan,_=queue_plan([dict(id='a',spec=a,created=1),dict(id='b',spec=b,created=2)],snapshot(),config())
        self.assertEqual(plan[0][0],'a')
    def test_unplaceable_head_does_not_block_other_work(self):
        from cdispatch.scheduler import queue_plan
        c=config();c['plan_window']=1;a=spec(priority=999,workdir='/tmp/a');a['profiles'][0]['mem_mib']=9999999
        b=spec(workdir='/tmp/b')
        plan,d=queue_plan([dict(id='a',spec=a,created=1),dict(id='b',spec=b,created=2)],snapshot(),c)
        self.assertEqual(plan[0][0],'b');self.assertTrue(d['a']['suggestions'])
    def test_pending_null_allocation_uses_request(self):
        result=usage([dict(JobState='PENDING',AllocTRES='(null)',ReqTRES='cpu=32,mem=256G,node=1-1')])
        self.assertEqual(result['mem_mib'],256*1024);self.assertEqual(result['nodes'],1)

class PreemptionTests(DaemonTests):
    def test_urgent_only_interrupts_checkpointable_jobs(self):
        from cdispatch.scheduler import reserve
        tid,t=self.submit(priority=500)
        t['state']='RUNNING'
        self.d.snapshot=snapshot();reserve(self.d.snapshot,t['selection'],'123',t['job_name'])
        urgent=spec(tags=['urgent'],priority=501,workdir='/tmp/urgent');urgent['profiles'][0]['ranks_per_node']=32
        wanted=dict(id='urgent',spec=urgent)
        self.assertFalse(self.d.try_preempt(wanted))
        t['spec']['preemptible']=True
        self.assertTrue(self.d.try_preempt(wanted));self.assertEqual(t['state'],'PAUSING')
    def test_urgency_does_not_preempt_equal_or_higher_priority(self):
        tid,t=self.submit(priority=500);t['state']='RUNNING';t['spec']['preemptible']=True
        self.d.snapshot=snapshot();reserve(self.d.snapshot,t['selection'],'123',t['job_name'])
        for priority in (1,499,500):
            wanted=dict(id='urgent',spec=spec(urgent=True,priority=priority,workdir='/tmp/urgent',
                profiles=[dict(spec()['profiles'][0],ranks_per_node=32)]))
            self.assertFalse(self.d.try_preempt(wanted));self.assertEqual(t['state'],'RUNNING')
        wanted['spec']['priority']=501
        self.assertTrue(self.d.try_preempt(wanted))
    def test_elastic_requires_explicit_checkpoint_contract(self):
        with self.assertRaises(ValueError):spec(elastic=True)
