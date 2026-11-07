from contextlib import redirect_stdout
import io
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch,MagicMock

from cdispatch.cli import service_start
from cdispatch.common import atomic,read
from cdispatch.watchdog import check
from test_dispatcher import config,snapshot,node


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.cfg=config();self.cfg.update(service_partitions=['cpu'],service_preferred_nodes=['old'],service_wall_minutes=60)
        # This fixture tests a 4CPU/4GiB service independently of the live pool size.
        self.cfg['shared_pool'].update(cpus=4,mem_mib=4096)
        self.backend=MagicMock();self.snap=snapshot();self.backend.snapshot.return_value=self.snap;self.backend.user_run.return_value='987'

    def test_service_chooses_cpu_only_and_preferred_old_node(self):
        self.snap['nodes']=[node('gpu',Gres='gpu:a100:4'),node('new'),node('old')]
        with patch('cdispatch.cli.Slurm',return_value=self.backend),redirect_stdout(io.StringIO()):service_start(self.root,self.cfg)
        args=self.backend.user_run.call_args.args[1]
        self.assertIn('--nodelist=old',args);self.assertIn('--mem=4096M',args);self.assertIn('--cpus-per-task=4',args)
        self.assertEqual(read(self.root/'bootstrap-intent.json')['job_id'],'987')

    def test_ambiguous_bootstrap_cannot_be_repeated_and_existing_reconciles(self):
        self.backend.user_run.side_effect=TimeoutError('lost acknowledgement')
        with patch('cdispatch.cli.Slurm',return_value=self.backend):
            with self.assertRaises(TimeoutError):service_start(self.root,self.cfg)
            with self.assertRaises(RuntimeError):service_start(self.root,self.cfg)
            self.assertEqual(self.backend.user_run.call_count,1)
            self.snap['jobs']=[dict(JobName='cd-service',JobState='RUNNING',UserId='alice(1)',JobId='987')]
            with redirect_stdout(io.StringIO()):service_start(self.root,self.cfg)
        self.assertEqual(read(self.root/'bootstrap-intent.json')['state'],'SUBMITTED')

    def running_service(self):
        self.snap['jobs']=[dict(JobName='cd-service',JobState='RUNNING',UserId='alice(1)',JobId='123',NodeList='n1',NumCPUs='4',AllocTRES='cpu=4,mem=4G')]
        self.snap['nodes'][0].update(CPUAlloc='4',AllocMem='4096')

    def test_renewal_is_dependency_bound_and_does_not_pause_current(self):
        self.running_service()
        with patch('cdispatch.cli.Slurm',return_value=self.backend),redirect_stdout(io.StringIO()):
            service_start(self.root,self.cfg,'123')
        args=self.backend.user_run.call_args.args[1]
        self.assertIn('--dependency=afterany:123',args)
        self.backend.pause.assert_not_called()
        self.assertEqual(self.snap['nodes'][0]['CPUAlloc'],'4')
        self.assertEqual(read(self.root/'service-renewal-intent.json')['after_current'],'123')

    def test_renewal_plan_creates_no_job_and_duplicate_reconciles(self):
        self.running_service()
        with patch('cdispatch.cli.Slurm',return_value=self.backend),redirect_stdout(io.StringIO()):
            service_start(self.root,self.cfg,'123',True)
            self.backend.user_run.assert_not_called()
            service_start(self.root,self.cfg,'123')
            self.snap['jobs'].append(dict(JobName='cd-service',JobState='PENDING',UserId='alice(1)',JobId='987'))
            service_start(self.root,self.cfg,'123')
        self.assertEqual(self.backend.user_run.call_count,1)

    def test_renewal_rejects_foreign_or_unknown_predecessor(self):
        self.running_service()
        with patch('cdispatch.cli.Slurm',return_value=self.backend):
            with self.assertRaises(RuntimeError):service_start(self.root,self.cfg,'999')
            with self.assertRaises(ValueError):service_start(self.root,self.cfg,'123;bad')
        self.backend.user_run.assert_not_called()

    def test_service_falls_back_to_cpu_partition_without_gpu(self):
        self.cfg['service_partitions']=['cpu','old']
        self.snap['nodes']=[node('busy',CPUAlloc='128'),node('fallback',part='old'),node('gpu',part='old',Gres='gpu:a100:4')]
        with patch('cdispatch.cli.Slurm',return_value=self.backend),redirect_stdout(io.StringIO()):
            service_start(self.root,self.cfg)
        args=self.backend.user_run.call_args.args[1]
        self.assertIn('--partition=old',args)
        self.assertIn('--nodelist=fallback',args)

    def test_unavailable_capacity_does_not_consume_restart_budget(self):
        with patch('cdispatch.watchdog.Slurm',return_value=self.backend),patch('cdispatch.cli.service_start',side_effect=RuntimeError('no fitting nodes')) as bootstrap:
            for _ in range(4):
                with self.assertRaises(RuntimeError):check(self.root,self.cfg)
            self.assertEqual(bootstrap.call_count,4)
        self.assertEqual(read(self.root/'watchdog.json')['starts'],[])

    def test_ambiguous_submission_consumes_restart_budget(self):
        def uncertain(*args,**kwargs):
            atomic(self.root/'bootstrap-intent.json',dict(state='SUBMIT_UNKNOWN',time=time.time()))
            raise TimeoutError('lost acknowledgement')
        with patch('cdispatch.watchdog.Slurm',return_value=self.backend),patch('cdispatch.cli.service_start',side_effect=uncertain):
            with self.assertRaises(TimeoutError):check(self.root,self.cfg)
        self.assertEqual(len(read(self.root/'watchdog.json')['starts']),1)

    def test_healthy_service_renews_before_walltime_without_cancellation(self):
        from datetime import datetime
        self.cfg['service_renew_before_minutes']=360
        atomic(self.root/'service-status.json',dict(job_id='123'))
        atomic(self.root/'status.json',dict(heartbeat=time.time(),snapshot=dict(jobs=[dict(JobId='123',JobState='RUNNING',EndTime=datetime.fromtimestamp(time.time()+3600).isoformat())])))
        with patch('cdispatch.watchdog.Slurm') as backend,patch('cdispatch.cli.service_start') as bootstrap:
            self.assertIn('renewal',check(self.root,self.cfg))
            bootstrap.assert_called_once_with(self.root,self.cfg,after_current='123')
            backend.assert_not_called()

    def test_healthy_watchdog_performs_no_slurm_calls(self):
        atomic(self.root/'status.json',dict(heartbeat=time.time()))
        with patch('cdispatch.watchdog.Slurm') as backend:
            self.assertEqual(check(self.root,self.cfg),'healthy');backend.assert_not_called()

    def test_stale_service_signalled_once_without_duplicate_bootstrap(self):
        self.snap['jobs']=[dict(JobName='cd-service',JobState='RUNNING',UserId='alice(1)',JobId='987')]
        with patch('cdispatch.watchdog.Slurm',return_value=self.backend),patch('cdispatch.cli.service_start') as bootstrap:
            check(self.root,self.cfg);check(self.root,self.cfg)
            self.backend.pause.assert_called_once_with('alice','987');bootstrap.assert_not_called()

    def test_watchdog_recovers_missing_allocation_and_bounds_restart_storms(self):
        with patch('cdispatch.watchdog.Slurm',return_value=self.backend),patch('cdispatch.cli.service_start') as bootstrap:
            for _ in range(3):check(self.root,self.cfg)
            self.assertIn('restart circuit',check(self.root,self.cfg));self.assertEqual(bootstrap.call_count,3)
        (self.root/'watchdog-disabled').touch();self.assertEqual(check(self.root,self.cfg),'disabled')


if __name__=='__main__':unittest.main()
