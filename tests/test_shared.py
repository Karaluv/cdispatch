import copy
from datetime import datetime,timedelta
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from cdispatch.common import atomic,enqueue,read,request,validate,lock
from cdispatch.daemon import Daemon
from cdispatch.planning import forecast
from cdispatch.scheduler import choose,reserve
from cdispatch.shared import pool_snapshot,step_command
from cdispatch.watchdog import check
from test_dispatcher import Fake,config,node,spec


class SharedTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.cfg=config();self.cfg.update(service_job_id='900',submit_interval=0)
        self.cfg['shared_pool'].update(cpus=4,mem_mib=4096,reserve_cpus=1,reserve_mem_mib=512)
        self.cfg.pop('shared_launcher_ssh',None)
        self.fake=Fake();self.fake.snap['nodes']=[node('n1',CPUAlloc='4',AllocMem='4096')]
        self.fake.snap['jobs']=[dict(JobId='900',JobName='cd-service',JobState='RUNNING',UserId='alice',Partition='cpu',NodeList='n1',AllocTRES='cpu=4,mem=4096M,node=1',EndTime=(datetime.now()+timedelta(hours=2)).isoformat())]
        self.fake.snap['steps']=[]
        self.d=Daemon(self.root,self.cfg,self.fake)
        self.s=spec(kind='shared',profiles=[dict(name='helper',partition='cpu',nodes=1,ranks_per_node=1,threads=1,mem_mib=512,wall_minutes=5)],users=['alice'],workdir=str(self.root))

    def submit(self,key):
        with patch('cdispatch.shared.subprocess.Popen'):
            enqueue(self.root,dict(self.s,resource_key=str(self.root/key)),key);self.d.tick()
        return self.d.state['tasks'][key]

    def test_cpu_handler_on_old_gpu_host_requires_explicit_opt_in(self):
        from cdispatch.shared import selection
        self.fake.snap['nodes'][0]['Gres']='gpu:titanx:8'
        pool_snapshot(self.root,self.cfg,self.fake.snap,{})
        self.assertIsNone(selection(self.s,self.fake.snap,self.cfg)[0])
        self.cfg['service_cpu_on_gpu_partitions']=['cpu']
        self.s['profiles'][0]['cpu_only']=False
        self.s['placement']={'avoid_gpu_nodes':False}
        selected,_=selection(self.s,self.fake.snap,self.cfg)
        self.assertIsNotNone(selected)
        self.assertEqual(selected['profile'].get('gpus_per_node',0),0)
        self.s['profiles'][0]['cpu_only']=True
        self.assertIsNone(selection(self.s,self.fake.snap,self.cfg)[0])

    def test_three_handlers_share_one_allocation_fourth_waits(self):
        tasks=[self.submit(str(i)) for i in range(4)]
        self.assertEqual([t['state'] for t in tasks],['PENDING','PENDING','PENDING','QUEUED'])
        self.assertFalse(self.fake.calls)
        self.assertEqual({t['job_id'] for t in tasks[:3]},{'900'})
        self.assertEqual(len(self.d.snapshot['jobs']),1)
        self.assertEqual(self.d.snapshot['shared_pool']['used_cpus'],4)

    def test_isolated_daemon_reservation_reduces_handler_capacity(self):
        self.cfg['shared_pool'].update(reserve_cpus=2,reserve_mem_mib=1536)
        tasks=[self.submit('isolated-'+str(i)) for i in range(3)]
        self.assertEqual([t['state'] for t in tasks],['PENDING','PENDING','QUEUED'])
        self.assertEqual(self.d.snapshot['shared_pool']['used_cpus'],4)
        self.assertEqual(self.d.snapshot['shared_pool']['used_mem_mib'],2560)

    def test_memory_is_bounded_independently_of_cpu_and_gpu_mpi_rejected(self):
        self.s['profiles'][0]['mem_mib']=3000;self.submit('one');second=self.submit('two')
        self.assertEqual(second['state'],'QUEUED')
        invalid=copy.deepcopy(self.s);invalid['profiles'][0]['ranks_per_node']=2
        with self.assertRaises(ValueError):validate(invalid)

    def test_cancel_exact_step_does_not_cancel_parent_or_sibling(self):
        one=self.submit('one');two=self.submit('two')
        self.fake.snap['steps']=[dict(id='900.1',name=one['step_name']),dict(id='900.2',name=two['step_name'])]
        for task,step in ((one,'1'),(two,'2')):atomic(Path(task['attempt'])/'started.json',dict(job_id='900',step_id=step,time=time.time()))
        self.d.tick();request(self.root,'cancel','one');self.d.tick()
        self.assertEqual(self.fake.calls,[('cancel','900.1')]);self.assertEqual(two['state'],'RUNNING')
        self.assertTrue((Path(one['attempt'])/'cancel.json').exists());self.assertFalse((Path(two['attempt'])/'cancel.json').exists())
        one['step_id']='900'
        from cdispatch.shared import cancel
        with self.assertRaises(ValueError):cancel(self.d,one)

    def test_exit_releases_only_its_reservation_after_step_disappears(self):
        one=self.submit('one');two=self.submit('two');a=Path(one['attempt'])
        atomic(a/'result.json',dict(state='SUCCEEDED',reason='done'));atomic(a/'driver.json',dict(returncode=0,time=time.time()))
        self.fake.snap['steps']=[dict(id='900.1',name=one['step_name'])]
        self.d.tick();self.assertIn(one['state'],('PENDING','RUNNING'))
        self.fake.snap['steps']=[];self.d.tick()
        self.assertEqual(one['state'],'SUCCEEDED');self.assertEqual(two['state'],'PENDING')
        self.assertEqual(self.d.snapshot['shared_pool']['handlers'],1);self.assertFalse(self.fake.calls)

    def test_controller_restart_does_not_relaunch_existing_handler(self):
        one=self.submit('one');self.d.save()
        with patch('cdispatch.shared.subprocess.Popen') as popen:
            restored=Daemon(self.root,self.cfg,self.fake);restored.tick();popen.assert_not_called()
        self.assertEqual(restored.state['tasks']['one']['attempt'],one['attempt'])

    def test_accounting_identifies_step_that_failed_before_start_marker(self):
        from cdispatch.shared import reconcile
        task=self.submit('early-failure')
        record=dict(id='900.7',name=task['step_name'],state='CANCELLED')
        reconcile(self.d,task,{'900':self.fake.snap['jobs'][0]},{'900.7':record})
        self.assertEqual(task['state'],'FAILED')
        self.assertEqual(task['step_id'],'900.7')
        self.assertTrue((Path(task['attempt'])/'cancel.json').exists())
        self.assertFalse(self.fake.calls)

    def test_accounting_does_not_release_unknown_or_foreign_step(self):
        from cdispatch.shared import reconcile
        task=self.submit('unknown')
        records={'901.7':dict(id='901.7',name=task['step_name'],state='CANCELLED'),
                 '900.8':dict(id='900.8',name='other-attempt',state='FAILED')}
        reconcile(self.d,task,{'900':self.fake.snap['jobs'][0]},records)
        self.assertEqual(task['state'],'PENDING')
        self.assertNotIn('step_id',task)

    def test_broker_reply_failure_keeps_reservation_without_local_replay(self):
        from types import SimpleNamespace
        self.cfg['shared_launcher_ssh']=dict(identity='/key',destination='alice@login')
        with patch('cdispatch.shared.subprocess.run',return_value=SimpleNamespace(returncode=255)), \
                patch('cdispatch.shared.subprocess.Popen') as local:
            enqueue(self.root,dict(self.s,resource_key=str(self.root/'relay')),'relay')
            self.d.tick()
        task=self.d.state['tasks']['relay']
        self.assertEqual(task['state'],'PENDING')
        self.assertFalse((Path(task['attempt'])/'driver.json').exists())
        local.assert_not_called()

    def test_cancel_before_attach_is_safe_and_releases_slot(self):
        self.s.update(interactive=True,interactive_command=['/bin/true'])
        task=self.submit('session');request(self.root,'cancel','session');self.d.tick();self.d.tick()
        self.assertEqual(task['state'],'CANCELLED');self.assertFalse(self.fake.calls)

    def test_step_command_has_independent_limits_and_no_overlap_or_parent_cancel(self):
        task=self.submit('one');args=step_command(task['attempt'])
        self.assertIn('--jobid=900',args);self.assertIn('--exact',args);self.assertIn('--mem=512M',args)
        self.assertIn('--oom-kill-step=1',args);self.assertNotIn('--overlap',args);self.assertNotIn('sbatch',args)

    def test_resize_waits_for_active_handlers_and_admits_no_new_ones(self):
        self.submit('one');request(self.root,'pool-size','*',cpus=8,mem_mib=8192);self.d.tick()
        two=self.submit('two');self.assertEqual(two['state'],'QUEUED');self.assertIn('draining',two['reason'])
        with patch('cdispatch.watchdog.Slurm') as backend:
            self.assertIn('active handlers',check(self.root,self.cfg));backend.assert_not_called()

    def test_stale_controller_does_not_kill_shared_allocation(self):
        self.submit('one');data=read(self.root/'status.json');data['heartbeat']=0;atomic(self.root/'status.json',data)
        with patch('cdispatch.watchdog.Slurm',return_value=self.fake):
            self.assertIn('protected',check(self.root,self.cfg));self.assertFalse(self.fake.calls)

    def test_safe_explicit_retry_forgets_old_step_identity(self):
        self.s['restart_from_scratch_safe']=True
        task=self.submit('retryable');task.update(state='FAILED',step_id='900.2',cancel_sent_step='900.2')
        request(self.root,'retry','retryable')
        with patch('cdispatch.shared.subprocess.Popen'):self.d.tick()
        self.assertEqual(task['state'],'PENDING');self.assertNotIn('step_id',task);self.assertNotIn('cancel_sent_step',task)

    def test_forecast_consumes_pool_capacity_without_new_allocations(self):
        self.d.tick();data=read(self.root/'status.json');before=copy.deepcopy(data)
        result=forecast(self.s,data,self.cfg);self.assertTrue(result['selection']['shared']);self.assertEqual(result['selection']['pool_job_id'],'900')
        self.assertEqual(data,before);self.assertFalse(self.fake.calls)


if __name__=='__main__':unittest.main()
