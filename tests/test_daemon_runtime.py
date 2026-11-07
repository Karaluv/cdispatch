import unittest
from unittest.mock import MagicMock, patch
from cdispatch.daemon_runtime import isolated_command, run_isolated


class DaemonRuntimeTests(unittest.TestCase):
    def config(self):
        return {'daemon_step': {'job_id': '900', 'mem_mib': 1024},
                'shared_pool': {'reserve_cpus': 2, 'reserve_mem_mib': 1536}}

    def test_exact_current_allocation_only_and_no_nested_recursion(self):
        cfg=self.config()
        command=isolated_command('/root','/config',cfg,{'SLURM_JOB_ID':'900'})
        for arg in ('--jobid=900','--cpus-per-task=1','--mem=1024M','--exclusive','--immediate=20'):
            self.assertIn(arg,command)
        self.assertEqual(command[-1],'daemon')
        for env in ({'SLURM_JOB_ID':'901'},{'SLURM_JOB_ID':'900','CDISPATCH_DAEMON_STEP':'1'},{}):
            self.assertIsNone(isolated_command('/root','/config',cfg,env))

    def test_undeclared_cpu_or_memory_reservation_rejected(self):
        for change in ({'reserve_cpus':1},{'reserve_mem_mib':1024}):
            cfg=self.config();cfg['shared_pool'].update(change)
            with self.assertRaises(ValueError):isolated_command('/root','/config',cfg,{'SLURM_JOB_ID':'900'})

    def test_supervisor_signals_stop_the_exact_child_and_restore_handlers(self):
        child=MagicMock();child.poll.return_value=None;child.wait.return_value=0
        with patch('cdispatch.daemon_runtime.subprocess.Popen',return_value=child) as popen,patch('cdispatch.daemon_runtime.signal.signal') as sig:
            def wait():
                sig.call_args_list[0].args[1]()
                return 0
            child.wait.side_effect=wait
            self.assertEqual(run_isolated(['srun']),0)
            child.terminate.assert_called_once()
            self.assertEqual(popen.call_args.kwargs['env']['CDISPATCH_DAEMON_STEP'],'1')
            self.assertEqual(sig.call_count,6)
