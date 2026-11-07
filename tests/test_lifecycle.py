import concurrent.futures
import copy
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from cdispatch.client import Client, DispatcherUnavailable, RequestRejected
from cdispatch.common import atomic, enqueue, read, request
from cdispatch.daemon import Daemon
from cdispatch.scheduler import reserve
from test_dispatcher import config, spec, Fake


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.fake = Fake()
        self.config = config()
        self.config['cancel_retry_interval'] = 0
        self.daemon = Daemon(self.root, self.config, self.fake)
        self.client = Client(self.root)

    def submit(self, key='task', **kwargs):
        task_spec = spec(workdir=str(self.root), **kwargs)
        enqueue(self.root, task_spec, key)
        self.daemon.tick()
        return self.daemon.state['tasks'][key]

    def test_concurrent_replay_before_ingest(self):
        task_spec = spec(workdir=str(self.root))
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            ids = list(pool.map(lambda _: enqueue(self.root, task_spec, 'same'), range(20)))
        self.assertEqual(set(ids), {'same'})
        self.assertEqual(len(list((self.root / 'inbox').glob('*.json'))), 1)
        self.daemon.tick()
        self.assertEqual(len(self.fake.calls), 1)

    def test_conflicting_key_before_and_after_consumption(self):
        enqueue(self.root, spec(), 'same')
        for consumed in (False, True):
            if consumed:
                self.daemon.tick()
            with self.assertRaisesRegex(ValueError, 'different spec'):
                enqueue(self.root, spec(priority=101), 'same')

    def test_replay_after_priority_change_preserves_priority(self):
        original = spec(workdir=str(self.root))
        task = self.submit()
        request(self.root, 'priority', 'task', value=500)
        self.daemon.tick()
        enqueue(self.root, original, 'task')
        self.daemon.tick()
        self.assertEqual(task['spec']['priority'], 500)
        self.assertTrue(read(self.root / 'receipts/submit-task.json')['ok'])
        self.assertEqual(len(self.fake.calls), 1)

    def test_invalid_raw_task_id_rejected_before_path_use(self):
        atomic(self.root / 'inbox/raw.json', dict(op='submit', id='../escape', spec=spec()))
        self.daemon.tick()
        self.assertFalse(self.daemon.state['tasks'])
        self.assertFalse(read(self.root / 'receipts/raw.json')['ok'])

    def test_cancel_before_submit_is_not_lost(self):
        token = self.client.cancel('later')[0]
        self.daemon.tick()
        self.assertTrue(self.client.wait_request(token, 0)['ok'])
        task = self.submit('later')
        self.assertEqual(task['state'], 'CANCELLED')
        self.assertFalse(self.fake.calls)

    def test_cancel_survives_unknown_submission_and_recovers_job(self):
        def ambiguous(*args):
            raise TimeoutError('sbatch reply lost')
        self.fake.submit = ambiguous
        task = self.submit()
        self.client.cancel('task')
        self.daemon.tick()
        self.assertEqual(task['state'], 'SUBMIT_UNKNOWN')
        reserve(self.fake.snap, task['selection'], '321', task['job_name'])
        self.daemon.tick()
        self.assertEqual(task['state'], 'CANCELLING')
        self.assertIn(('cancel', '321'), self.fake.calls)
        self.assertEqual(len(task['attempts']), 1)

    def test_failed_cancel_retried_after_controller_restart(self):
        self.submit()
        self.client.cancel('task')
        with patch.object(self.fake, 'cancel', side_effect=TimeoutError('connection lost')):
            self.daemon.tick()
        self.assertTrue(self.daemon.state['tasks']['task']['cancel_requested'])
        self.daemon = Daemon(self.root, self.config, self.fake)
        self.daemon.tick()
        self.assertEqual(self.daemon.state['tasks']['task']['state'], 'CANCELLING')
        self.assertIn(('cancel', '123'), self.fake.calls)
        self.daemon.tick()
        self.assertEqual(self.fake.calls.count(('cancel', '123')), 1)

    def test_cancel_retry_is_rate_limited(self):
        self.submit()
        self.config['cancel_retry_interval'] = 30
        self.client.cancel('task')
        with patch.object(self.fake, 'cancel', side_effect=TimeoutError('connection lost')) as cancel:
            self.daemon.tick()
            self.daemon.tick()
            self.assertEqual(cancel.call_count, 1)

    def test_cancel_works_during_snapshot_failure(self):
        task = self.submit()
        self.fake.error = True
        self.client.cancel('task')
        self.daemon.tick()
        self.assertEqual(task['state'], 'CANCELLING')
        self.assertIn(('cancel', '123'), self.fake.calls)
        self.assertIsNotNone(self.daemon.error)

    def test_parent_cleanup_ignores_admission_gates_and_cascades(self):
        parent = self.submit()
        self.daemon.state['paused'] = True
        self.daemon.state['circuit'] = 'test circuit'
        self.config['submit_interval'] = 1000
        for key, state in [('queued', 'QUEUED'), ('held', 'HELD'), ('paused', 'PAUSED'), ('running', 'RUNNING')]:
            child = copy.deepcopy(parent)
            child.update(id=key, state=state, spec=spec(parent='task', workdir='/tmp/' + key))
            if state == 'RUNNING':
                child['job_id'] = '456'
            self.daemon.state['tasks'][key] = child
        grandchild = copy.deepcopy(self.daemon.state['tasks']['queued'])
        grandchild.update(id='grandchild', spec=spec(parent='running'))
        self.daemon.state['tasks']['grandchild'] = grandchild
        self.client.cancel('task')
        self.daemon.tick()
        for key in ('queued', 'held', 'paused', 'grandchild'):
            self.assertEqual(self.daemon.state['tasks'][key]['state'], 'CANCELLED')
        self.assertIn(('cancel', '456'), self.fake.calls)
        self.assertIn(('cancel', '123'), self.fake.calls)

    def test_group_cancel_uses_daemon_tasks_without_client_cache(self):
        enqueue(self.root, spec(group='campaign'), 'one')
        enqueue(self.root, spec(group='other'), 'two')
        token = self.client.cancel_group('campaign')
        self.daemon.state['paused'] = True
        self.daemon.tick()
        self.assertTrue(self.client.wait_request(token, 0)['ok'])
        self.assertEqual(self.daemon.state['tasks']['one']['state'], 'CANCELLED')
        self.assertEqual(self.daemon.state['tasks']['two']['state'], 'QUEUED')

    def test_group_cancel_covers_late_replay_but_allows_new_work(self):
        enqueue(self.root, spec(group='campaign'), 'old')
        (self.root / 'inbox/submit-old.json').unlink()  # crash after durable claim
        self.client.cancel_group('campaign')
        self.daemon.state['paused'] = True
        self.daemon.tick()
        enqueue(self.root, spec(group='campaign'), 'old')
        enqueue(self.root, spec(group='campaign'), 'new')
        self.daemon.tick()
        self.assertEqual(self.daemon.state['tasks']['old']['state'], 'CANCELLED')
        self.assertEqual(self.daemon.state['tasks']['new']['state'], 'QUEUED')

    def test_stale_and_failed_observations_are_not_empty_success(self):
        self.daemon.tick()
        self.assertEqual(self.client.states(), {})
        for update in ({'heartbeat': 0}, {'error': 'Slurm unavailable'}, {'snapshot': {'time': 0}}):
            data = read(self.root / 'status.json')
            original = copy.deepcopy(data)
            data.update(update)
            atomic(self.root / 'status.json', data)
            with self.assertRaises(DispatcherUnavailable):
                self.client.states()
            atomic(self.root / 'status.json', original)

    def test_submission_and_operation_rejections_are_visible(self):
        enqueue(self.root, spec(depends_on=['missing']), 'bad')
        token = request(self.root, 'pause', 'missing')
        self.daemon.tick()
        with self.assertRaisesRegex(RequestRejected, 'Unknown dependency'):
            self.client.wait('bad', 0)
        with self.assertRaises(RequestRejected):
            self.client.wait_request(token, 0)

    def test_wait_uses_monotonic_timeout_without_cancelling_work(self):
        self.daemon.tick()
        with self.assertRaises(TimeoutError):
            self.client.wait('unknown', 0)
        self.assertFalse(list((self.root / 'inbox').glob('*.json')))

    def test_shared_id_prefix_has_distinct_recovery_names(self):
        task = self.submit('same-prefix-123456-A')
        first_name = task['job_name']
        task['state'] = 'SUCCEEDED'
        second = self.submit('same-prefix-123456-B')
        self.assertNotEqual(first_name, second['job_name'])
        self.assertIn(second['id'], second['job_name'])

    def test_legacy_upgrade_keeps_original_spec_from_receipt(self):
        task = self.submit()
        original = copy.deepcopy(task['spec'])
        task.pop('submitted_spec')
        task['spec']['priority'] = 700
        (self.root / 'submissions/task.json').unlink()
        self.daemon.save()
        self.daemon = Daemon(self.root, self.config, self.fake)
        enqueue(self.root, original, 'task')
        self.daemon.tick()
        self.assertTrue(read(self.root / 'receipts/submit-task.json')['ok'])
        self.assertEqual(self.daemon.state['tasks']['task']['spec']['priority'], 700)

    def test_rejected_submission_can_be_replayed_after_predecessor_arrives(self):
        dependent = spec(depends_on=['predecessor'])
        enqueue(self.root, dependent, 'child')
        self.daemon.tick()
        with self.assertRaises(RequestRejected):
            self.client.status('child')
        self.submit('predecessor')
        enqueue(self.root, dependent, 'child')
        self.assertIsNone(self.client.status('child'))
        self.daemon.tick()
        self.assertEqual(self.client.status('child')['state'], 'QUEUED')


if __name__ == '__main__':
    unittest.main()
