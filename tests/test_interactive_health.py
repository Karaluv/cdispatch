"""Interactive clients expose failed admission and cleanly cancel Ctrl+C."""
import argparse
import contextlib
import io
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

from cdispatch.cli import interactive, main
from cdispatch.common import atomic, read
from cdispatch.failure_circuits import workload_scope, failure_scope
from cdispatch.alerts import health
from test_dispatcher import config


class InteractiveHealthTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.root=Path(tmp.name);self.cfg=config()
        self.args=argparse.Namespace(command=['/bin/true'],name='codex-node',priority=1000,mem=1536,minutes=10,shared=True,wait=30)
        self.status=dict(heartbeat=time.time(),snapshot=dict(time=time.time()),tasks={})

    def test_global_circuit_is_reported_before_enqueue(self):
        self.status['circuit']='ambiguous sbatch'
        atomic(self.root/'status.json',self.status)
        with patch('cdispatch.cli.enqueue') as enqueue:
            with self.assertRaisesRegex(RuntimeError,'controller circuit: ambiguous sbatch'):
                interactive(self.root,self.cfg,self.args)
            enqueue.assert_not_called()

    def test_stale_controller_is_reported_before_enqueue(self):
        self.status['heartbeat']=time.time()-120
        atomic(self.root/'status.json',self.status)
        with patch('cdispatch.cli.enqueue') as enqueue:
            with self.assertRaisesRegex(RuntimeError,'heartbeat stale'):
                interactive(self.root,self.cfg,self.args)
            enqueue.assert_not_called()

    def test_circuit_opening_during_wait_cancels_only_this_request(self):
        atomic(self.root/'status.json',self.status)
        def blocked(*_):
            self.status['circuit']='repeated failures within 10 minutes'
            atomic(self.root/'status.json',self.status)
        with patch('cdispatch.cli.time.sleep',side_effect=blocked),contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError,'controller circuit:'):
                interactive(self.root,self.cfg,self.args)
        ops=[read(p)['op'] for p in (self.root/'inbox').glob('*.json')]
        self.assertEqual(sorted(ops),['cancel','submit'])

    def test_ctrl_c_returns_130_without_traceback_and_spools_cancellation(self):
        atomic(self.root/'status.json',self.status)
        cfg=self.root/'config.json';atomic(cfg,self.cfg)
        proc=subprocess.Popen([sys.executable,'-m','cdispatch.cli','--root',str(self.root),'--config',str(cfg),
                               'interactive','--shared','--','/bin/true'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        self.addCleanup(lambda: proc.poll() is None and proc.kill())
        self.assertIn('Interactive ',proc.stderr.readline())
        proc.send_signal(signal.SIGINT)
        out,err=proc.communicate(timeout=10)
        self.assertEqual(proc.returncode,130)
        self.assertNotIn('Traceback',err)
        self.assertIn('cancellation requested',err)
        self.assertIn('cancel',[read(p)['op'] for p in (self.root/'inbox').glob('*.json')])

    def test_legacy_application_circuit_does_not_report_admission_block(self):
        self.status['workload_circuits']={'group:broken':{}}
        result=health(self.status,now=time.time())
        self.assertTrue(result['ok'])
        self.assertFalse(result['warnings'])

    def test_shared_handler_exit_is_scoped_even_without_application_result(self):
        task=dict(spec=dict(group='broken'),attempts=[])
        event=dict(kind='TERMINAL',state='FAILED',task='handler',shared=True,job_id='123')
        self.assertEqual(failure_scope(event,{'handler':task}),'group:broken')
        event['slurm_state']='OUT_OF_MEMORY'
        self.assertEqual(failure_scope(event,{'handler':task}),'global')
