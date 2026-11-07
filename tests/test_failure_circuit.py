"""External Slurm preemption must not stop unrelated queue admissions."""
from pathlib import Path
import tempfile
import unittest

from cdispatch.common import enqueue, atomic, request
from cdispatch.daemon import Daemon
from test_dispatcher import Fake, config, spec


class FailureCircuitTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.fake = Fake()
        self.daemon = Daemon(self.root, config(), self.fake)
        self.next_job = 100
        self.fake.submit = self.submit_job

    def submit_job(self, user, selection, attempt):
        self.next_job += 1
        self.fake.calls.append(selection)
        return str(self.next_job)

    def finish(self, state, application=False, group='default'):
        work = self.root / str(self.next_job)
        work.mkdir()
        tid = enqueue(self.root, spec(workdir=str(work), group=group))
        self.daemon.tick()
        task = self.daemon.state['tasks'][tid]
        self.assertEqual(task['state'], 'PENDING')
        self.fake.acct.append(dict(id=task['job_id'], name=task['job_name'],
                                   user='alice', state=state, exit_code='0:0'))
        if application:
            atomic(Path(task['attempt'])/'result.json', dict(state='FAILED',returncode=1,reason='application/validation'))
        self.daemon.tick()
        self.assertEqual(task['state'], 'FAILED')
        self.assertEqual(len(task['attempts']), 1)
        return tid

    def test_preemption_burst_keeps_queue_open_without_replaying_attempts(self):
        ids = [self.finish(state) for state in ('PREEMPTED', 'PREEMPTED+', 'PREEMPTED')]
        self.assertFalse(self.daemon.state.get('circuit'))
        self.assertEqual(len(self.fake.calls), 3)
        events = [e for e in self.daemon.state['events'] if e['kind'] == 'TERMINAL']
        self.assertEqual([e['slurm_state'] for e in events],
                         ['PREEMPTED', 'PREEMPTED+', 'PREEMPTED'])
        self.finish('FAILED')  # A later independent request is admitted.
        self.assertFalse(self.daemon.state.get('circuit'))
        self.assertTrue(all(self.daemon.state['tasks'][tid]['state'] == 'FAILED' for tid in ids))

    def test_preemption_does_not_count_toward_real_failure_burst(self):
        for state in ('FAILED', 'PREEMPTED', 'OUT_OF_MEMORY'):
            self.finish(state)
        self.assertFalse(self.daemon.state.get('circuit'))
        self.finish('NODE_FAIL')
        self.assertEqual(self.daemon.state.get('circuit'), 'repeated failures within 10 minutes')
        waiting = self.root / 'waiting'
        waiting.mkdir()
        tid = enqueue(self.root, spec(workdir=str(waiting)))
        self.daemon.tick()
        self.assertEqual(self.daemon.state['tasks'][tid]['state'], 'QUEUED')
        self.assertEqual(len(self.fake.calls), 4)

    def queued(self, group='default', name='test'):
        work = Path(tempfile.mkdtemp(prefix='waiting-',dir=self.root))
        return enqueue(self.root, spec(workdir=str(work), group=group, name=name))

    def test_application_burst_keeps_same_group_open_without_retrying_failures(self):
        failed = [self.finish('FAILED', application=True, group='broken') for _ in range(3)]
        self.assertFalse(self.daemon.state.get('circuit'))
        self.assertFalse(self.daemon.state.get('workload_circuits'))
        waiting = self.queued('broken')
        self.daemon.tick()
        self.assertEqual(self.daemon.state['tasks'][waiting]['state'], 'PENDING')
        self.assertEqual(len(self.fake.calls), 4)
        for tid in failed:
            task = self.daemon.state['tasks'][tid]
            self.assertEqual(task['state'], 'FAILED')
            self.assertEqual(len(task['attempts']), 1)

    def test_default_group_application_burst_keeps_same_application_open(self):
        for _ in range(3):self.finish('FAILED', application=True)
        waiting = self.queued()
        self.daemon.tick()
        self.assertEqual(self.daemon.state['tasks'][waiting]['state'], 'PENDING')

    def test_different_application_groups_do_not_form_global_burst(self):
        for group in ('a','b','c'):self.finish('FAILED', application=True, group=group)
        self.assertFalse(self.daemon.state.get('circuit'))
        self.assertFalse(self.daemon.state.get('workload_circuits'))

    def test_oom_stays_global_even_if_worker_has_application_result(self):
        for _ in range(3):self.finish('OUT_OF_MEMORY', application=True)
        self.assertTrue(self.daemon.state.get('circuit'))
        self.assertFalse(self.daemon.state.get('workload_circuits'))

    def test_global_block_explains_queued_task_reason(self):
        for _ in range(3):self.finish('FAILED')
        waiting = self.queued()
        self.daemon.tick()
        self.assertIn('controller circuit:', self.daemon.state['tasks'][waiting]['reason'])

    def test_legacy_circuit_is_retired_once_and_stays_retired_after_reload(self):
        for _ in range(3):self.finish('FAILED', application=True, group='broken')
        legacy = {'group:broken': dict(reason='repeated application failures', tasks=['original'])}
        self.daemon.state['workload_circuits'] = legacy
        waiting = self.queued('broken')
        self.daemon.tick()
        self.assertEqual(self.daemon.state['tasks'][waiting]['state'], 'PENDING')
        reloaded=Daemon(self.root,self.daemon.config,self.fake)
        reloaded.tick()
        self.assertFalse(reloaded.state.get('workload_circuits'))
        self.assertFalse(reloaded.state.get('circuit'))
        retired = reloaded.state['retired_workload_circuits']
        self.assertEqual(len(retired), 1)
        self.assertEqual(retired[0]['circuits'], legacy)
        self.assertEqual(sum(e['kind'] == 'APPLICATION_CIRCUITS_RETIRED'
                             for e in reloaded.state['events']), 1)

    def test_global_clear_also_migrates_legacy_application_flags(self):
        for _ in range(3):self.finish('FAILED', application=True, group='broken')
        self.daemon.state['workload_circuits'] = {'group:broken': {}}
        self.daemon.state['circuit']='repeated failures within 10 minutes'
        request(self.root,'clear-circuit','*')
        self.daemon.tick()
        self.assertFalse(self.daemon.state.get('circuit'))
        self.assertFalse(self.daemon.state.get('workload_circuits'))
        self.assertEqual(len(self.daemon.state['retired_workload_circuits']), 1)

    def test_infrastructure_reset_does_not_reopen_for_the_same_failures(self):
        for _ in range(3):self.finish('FAILED')
        request(self.root,'clear-circuit','*')
        self.daemon.tick();self.daemon.tick()
        self.assertFalse(self.daemon.state.get('circuit'))

    def test_forecast_ignores_legacy_application_circuit(self):
        from cdispatch.planning import forecast
        for _ in range(3):self.finish('FAILED', application=True, group='broken')
        self.daemon.publish()
        from cdispatch.common import read
        data=read(self.root/'status.json')
        data['workload_circuits'] = {'group:broken': {}}
        result=forecast(spec(group='broken'),data,self.daemon.config)
        self.assertTrue(result['would_submit'], result)
