"""Transient stale observations preserve the exact interactive request and lease."""
import contextlib
import io
from pathlib import Path
import time
from unittest.mock import patch
import unittest
import test_interactive_health as health_tests
from cdispatch.cli import interactive
from cdispatch.common import atomic, read

class SnapshotWaitTests(unittest.TestCase):
    setUp = health_tests.InteractiveHealthTests.setUp
    def test_waits_for_refresh_before_enqueue(self):
        self.status['snapshot']['time']=time.time()-90
        atomic(self.root/'status.json',self.status)
        attempt=self.root/'attempt';attempt.mkdir()
        task=dict(state='PENDING',job_id='123',attempt=str(attempt),selection=dict(shared=True,nodes=['node'],profile=dict(wall_minutes=10)))
        def refresh(_):
            enqueue.assert_not_called()
            self.status['snapshot']['time']=time.time();atomic(self.root/'status.json',self.status)
        def submit(*_):
            self.status['tasks']['test']=task;atomic(self.root/'status.json',self.status);return 'test'
        with patch('cdispatch.cli.enqueue',side_effect=submit) as enqueue,patch('cdispatch.cli.time.sleep',side_effect=refresh),patch('cdispatch.shared_launcher.launch',return_value=0) as launch,contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(interactive(self.root,self.cfg,self.args),0)
        enqueue.assert_called_once();launch.assert_called_once_with(attempt,interactive=True)
        self.assertFalse(list((self.root/'inbox').glob('cancel-*')))

    def test_pending_lease_renews_before_refresh_and_launch(self):
        atomic(self.root/'status.json',self.status)
        attempt=self.root/'attempt';attempt.mkdir()
        task=dict(state='PENDING',job_id='123',attempt=str(attempt),selection=dict(shared=True,nodes=['node'],profile=dict(wall_minutes=10)))
        def submit(*_):
            self.status['tasks']['test']=task;self.status['snapshot']['time']=time.time()-90
            atomic(self.root/'status.json',self.status);return 'test'
        def refresh(_):
            self.assertLess(time.time()-read(attempt/'client.json')['time'],2)
            launch.assert_not_called()
            self.status['snapshot']['time']=time.time();atomic(self.root/'status.json',self.status)
        with patch('cdispatch.cli.enqueue',side_effect=submit) as enqueue,patch('cdispatch.cli.time.sleep',side_effect=refresh),patch('cdispatch.shared_launcher.launch',return_value=0) as launch,contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(interactive(self.root,self.cfg,self.args),0)
        enqueue.assert_called_once();launch.assert_called_once()
        self.assertFalse(list((self.root/'inbox').glob('cancel-*')))

    def test_no_submission_when_refresh_deadline_expires(self):
        self.args.wait=0;self.status['snapshot']['time']=time.time()-90
        atomic(self.root/'status.json',self.status)
        with patch('cdispatch.cli.enqueue') as enqueue,patch('cdispatch.shared_launcher.launch') as launch,contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError,'no request submitted'):interactive(self.root,self.cfg,self.args)
        enqueue.assert_not_called();launch.assert_not_called()

    def test_circuit_aborts_pending_request_even_with_stale_snapshot(self):
        atomic(self.root/'status.json',self.status)
        def submit(*_):
            self.status['snapshot']['time']=time.time()-90;self.status['circuit']='ambiguous sbatch'
            atomic(self.root/'status.json',self.status);return 'test'
        with patch('cdispatch.cli.enqueue',side_effect=submit),patch('cdispatch.shared_launcher.launch') as launch,contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError,'controller circuit:'):interactive(self.root,self.cfg,self.args)
        launch.assert_not_called()
        self.assertEqual([read(p)['op'] for p in (self.root/'inbox').glob('*.json')],['cancel'])
