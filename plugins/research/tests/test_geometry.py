import copy
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import threading
import unittest
from unittest.mock import patch

from cdispatch.client import Client, DispatcherUnavailable, RequestRejected
from cdispatch.common import atomic, read, lock
from cdispatch.daemon import Daemon
from cdispatch_research.geometry import GeometryWorkers
from support import BASE, config, spec, Fake


class GeometryTests(unittest.TestCase):
    def test_poll_waits_for_external_journal_lock_without_crashing(self):
        started=threading.Event();done=threading.Event();errors=[]
        def poll():
            started.set()
            try:self.workers.poll()
            except BaseException as exc:errors.append(exc)
            finally:done.set()
        with lock(self.workers.lock_path):
            thread=threading.Thread(target=poll);thread.start()
            self.assertTrue(started.wait(1))
            self.assertFalse(done.wait(.1))
        thread.join(2)
        self.assertTrue(done.is_set());self.assertEqual(errors,[])

    def test_global_lock_still_rejects_second_owner(self):
        path=self.here/'daemon.lock'
        with lock(path):
            with self.assertRaises(BlockingIOError):
                with lock(path):pass

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.here = Path(self.tmp.name)
        self.root = self.here / 'dispatcher'
        self.client = Client(self.root)
        self.fake = Fake()
        self.daemon = Daemon(self.root, config(), self.fake)
        self.fake.snap['jobs'] = [dict(JobId='900', JobName='parent', JobState='RUNNING',
                                      UserId='alice', AllocTRES='cpu=1,mem=512M,node=1')]
        self.daemon.state['tasks']['parent'] = dict(id='parent', spec=spec(), state='RUNNING',
                                                  job_id='900', user='alice', created=time.time(), attempts=[])
        self.daemon.tick()
        self.worker = self.here / 'worker.sh'
        self.worker.write_text("#!/bin/bash\nexec /usr/bin/python3 -c 'import json,sys; "
                               "json.dump({\"ok\":True,\"state\":{\"electronic_gate_pass\":True}}, "
                               "open(sys.argv[1]+\".result.json\",\"w\"))' \"$1\"\n")
        self.request = self.here / 'trial.request.json'
        atomic(self.request, dict(point=str(self.here / 'trial'), role='excited',
                                  profile=dict(ranks=16, threads=4, memory_GB=100,
                                               partition='cpu', node='n1')))
        self.journal = self.here / 'geometry-journal.json'
        self.workers = self.reopen()

    def reopen(self):
        return GeometryWorkers(self.client, self.journal, self.worker, 'geometry-test', 'parent')

    def submit(self):
        tid = self.workers.submit(self.request, context={'radius': 0.005, 'slot': 0})
        self.daemon.tick()
        return tid, self.daemon.state['tasks'][tid]

    def finish(self, task, state='COMPLETED'):
        self.fake.acct = [dict(id=task['job_id'], name=task['job_name'], user='alice',
                               state=state, exit_code='0:0', elapsed='1', cpus='64',
                               cpu_seconds='64', total_cpu='00:00:01', tres='cpu=64',
                               nodes='n1', max_rss='')]
        self.daemon.tick()

    def test_worker_runs_through_application_and_electronic_validation(self):
        tid, task = self.submit()
        subprocess.run([sys.executable, '-m', 'cdispatch.worker', task['attempt']],
                       env=dict(os.environ, PYTHONPATH=os.pathsep.join([str(BASE),str(Path(__import__('cdispatch').__file__).resolve().parent.parent)])), check=True, timeout=10)
        # Even a complete output is not consumed until Slurm terminal accounting.
        self.assertEqual(self.workers.poll(), {})
        self.finish(task)
        outcome = self.workers.poll()[tid]
        self.assertEqual(outcome['state'], 'SUCCEEDED')
        self.assertTrue(outcome['result']['ok'])
        self.assertFalse(outcome['result']['scheduler_interruption'])
        self.assertEqual(outcome['context']['radius'], 0.005)
        self.assertIn(tid, self.reopen().poll())  # Durable at-least-once delivery.
        self.workers.acknowledge(tid)
        self.assertFalse(self.reopen().active())
        self.assertEqual(self.workers.poll(), {})

    def test_crash_before_spool_publication_recovers_one_task(self):
        with patch.object(self.client, 'submit', side_effect=OSError('simulated crash')):
            with self.assertRaises(OSError):
                self.workers.submit(self.request)
        journal = read(self.journal)
        tid = next(iter(journal['workers']))
        self.assertFalse(journal['workers'][tid]['published'])
        self.reopen().recover()
        self.reopen().recover()
        self.daemon.tick()
        self.assertEqual(len(self.fake.calls), 1)
        self.assertIn(tid, self.daemon.state['tasks'])

    def test_crash_after_spool_publication_reuses_id(self):
        original_submit = self.client.submit
        def interrupted_submit(*args, **kwargs):
            original_submit(*args, **kwargs)
            raise OSError('crash after publishing')
        with patch.object(self.client, 'submit', side_effect=interrupted_submit):
            with self.assertRaises(OSError):
                self.workers.submit(self.request)
        self.reopen().recover()
        self.daemon.tick()
        self.reopen().recover()
        self.daemon.tick()
        self.assertEqual(len(self.fake.calls), 1)

    def test_lost_controller_snapshot_retains_workers(self):
        tid, task = self.submit()
        self.fake.error = True
        self.daemon.tick()
        with self.assertRaises(DispatcherUnavailable):
            self.workers.poll()
        self.assertIn(tid, self.reopen().active())
        self.assertEqual(len(self.fake.calls), 1)

    def test_unknown_submission_does_not_retry_or_report_scientific_failure(self):
        self.fake.submit = lambda *args: (_ for _ in ()).throw(TimeoutError('reply lost'))
        tid, task = self.submit()
        self.assertEqual(task['state'], 'SUBMIT_UNKNOWN')
        self.assertEqual(self.workers.poll(), {})
        self.reopen().recover()
        self.daemon.tick()
        self.assertEqual(len(task['attempts']), 1)
        self.assertIn(tid, self.workers.active())

    def test_node_failure_preserves_context_without_replacement_submission(self):
        tid, task = self.submit()
        self.finish(task, 'NODE_FAIL')
        outcome = self.workers.poll()[tid]
        self.assertFalse(outcome['result']['ok'])
        self.assertTrue(outcome['result']['scheduler_interruption'])
        self.assertEqual(outcome['context'], {'radius': 0.005, 'slot': 0})
        self.assertEqual(len(self.fake.calls), 1)

    def test_application_failure_is_distinct_from_infrastructure(self):
        tid, task = self.submit()
        atomic(str(self.request) + '.result.json', dict(ok=False, error='electronic gate'))
        self.finish(task, 'FAILED')
        outcome = self.workers.poll()[tid]['result']
        self.assertEqual(outcome['error'], 'electronic gate')
        self.assertFalse(outcome['scheduler_interruption'])

    def test_changed_request_is_rejected_at_launch_and_recovery(self):
        tid, task = self.submit()
        changed = read(self.request)
        changed['profile']['ranks'] = 32
        atomic(self.request, changed)
        with self.assertRaisesRegex(ValueError, 'changed'):
            self.reopen().recover()
        result = subprocess.run([sys.executable, '-m', 'cdispatch.worker', task['attempt']],
                                env=dict(os.environ, PYTHONPATH=os.pathsep.join([str(BASE),str(Path(__import__('cdispatch').__file__).resolve().parent.parent)])), timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertFalse(Path(str(self.request) + '.result.json').exists())
        self.assertEqual(read(Path(task['attempt']) / 'result.json')['state'], 'FAILED')

    def test_existing_scientific_result_cannot_be_implicitly_restarted(self):
        atomic(str(self.request) + '.result.json', dict(ok=True))
        with self.assertRaisesRegex(ValueError, 'Existing result'):
            self.workers.submit(self.request)

    def test_expired_request_is_not_reported_as_electronic_failure(self):
        data = read(self.request)
        data['deadline_epoch'] = time.time() - 1
        atomic(self.request, data)
        tid, task = self.submit()
        result = subprocess.run([sys.executable, '-m', 'cdispatch.worker', task['attempt']],
                                env=dict(os.environ, PYTHONPATH=os.pathsep.join([str(BASE),str(Path(__import__('cdispatch').__file__).resolve().parent.parent)])), timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertFalse(Path(str(self.request) + '.result.json').exists())
        self.finish(task, 'FAILED')
        self.assertTrue(self.workers.poll()[tid]['result']['scheduler_interruption'])

    def test_cancel_survives_crash_before_original_submission(self):
        with patch.object(self.client, 'submit', side_effect=OSError('crash')):
            with self.assertRaises(OSError):
                self.workers.submit(self.request)
        tid = next(iter(self.workers.active()))
        with patch.object(self.client, 'cancel', side_effect=OSError('crash')):
            with self.assertRaises(OSError):
                self.workers.cancel()
        self.reopen().recover()
        self.daemon.tick()
        self.assertEqual(self.daemon.state['tasks'][tid]['state'], 'CANCELLED')
        self.assertIn(tid, self.workers.poll())
        self.assertFalse(self.fake.calls)

    def test_cancel_retains_ids_until_terminal_confirmation(self):
        tid, task = self.submit()
        self.workers.cancel()
        self.daemon.tick()
        self.assertIn(tid, self.reopen().active())
        self.assertEqual(self.workers.poll(), {})
        self.finish(task, 'CANCELLED by 42')
        self.assertTrue(self.workers.poll()[tid]['result']['scheduler_interruption'])
        self.workers.acknowledge(tid)
        self.assertFalse(self.workers.active())

    def test_missing_parent_receipt_is_exposed(self):
        self.daemon.state['tasks'].pop('parent')
        tid = self.workers.submit(self.request)
        self.daemon.tick()
        with self.assertRaisesRegex(RequestRejected, 'Unknown parent'):
            self.workers.poll()

    def test_cannot_acknowledge_running_worker_or_rebind_journal(self):
        tid, task = self.submit()
        with self.assertRaisesRegex(ValueError, 'unconfirmed'):
            self.workers.acknowledge(tid)
        with self.assertRaisesRegex(ValueError, 'different controller'):
            GeometryWorkers(self.client, self.journal, self.worker, 'geometry-test', 'other-parent')


if __name__ == '__main__':
    unittest.main()
