import copy
from pathlib import Path
import tempfile
import time
import unittest

from cdispatch.common import validate
from cdispatch.daemon import Daemon
from cdispatch.planning import checkpoint_barrier, forecast
from cdispatch.scheduler import choose, preemption_plan, reserve
from test_dispatcher import config, snapshot, spec, Fake


class ForcedDisplacementTests(unittest.TestCase):
    def setUp(self):
        self.cfg=config();self.cfg['plugins']=[{'module':'fixture_plugin'}]
        self.snap=snapshot()
        self.snap['nodes'][0]['CPUTot']='64'
        self.snap['nodes'][0]['CPUEfctv']='64'
        self.low=spec(parent='science-controller',priority=100,
                      labels={'project':'demo','stage':'worker'},max_attempts=1)
        sel,_=choose(self.low,self.snap,self.cfg)
        reserve(self.snap,sel,'1','low')
        self.victim=dict(id='low',state='RUNNING',spec=self.low,created=1,
                         selection=sel,job_id='1',user='alice',attempts=[{}])
        self.high=spec(workdir='/tmp/high',urgent=True,priority=700,
                       force_displace_tasks=['low'])
        self.wanted=dict(id='high',state='QUEUED',spec=self.high,created=2,attempts=[])
        self.tasks={'low':self.victim,'high':self.wanted}

    def action(self):
        return preemption_plan(self.wanted,self.tasks,self.snap,self.cfg)

    def test_explicit_lower_partial_work_is_selected_despite_single_attempt(self):
        action=self.action()
        self.assertEqual(action['force_victims'],['low'])
        self.assertEqual(action['selection']['nodes'],['n1'])

    def test_equal_higher_and_urgent_workers_stay_protected(self):
        for priority,urgent in [(700,False),(701,False),(100,True)]:
            with self.subTest(priority=priority,urgent=urgent):
                self.victim['spec']=dict(self.low,priority=priority,urgent=urgent)
                self.assertIsNone(self.action())

    def test_unlisted_shared_controller_and_explicitly_protected_are_excluded(self):
        variants=[dict(self.low,labels={'stage':'geometry-controller'}),
                  dict(self.low,kind='shared'),dict(self.low,protected=True),
                  dict(self.low,parent=None),dict(self.low,adopt_job={'JobId':'1'})]
        for variant in variants:
            self.victim['spec']=variant
            self.assertIsNone(self.action())
        self.victim['spec']=self.low
        self.wanted['spec']=dict(self.high,force_displace_tasks=['another'])
        self.assertIsNone(self.action())

    def test_no_forced_displacement_without_explicit_request(self):
        self.wanted['spec']=dict(self.high)
        self.wanted['spec'].pop('force_displace_tasks')
        self.assertIsNone(self.action())

    def test_contract_rejects_implicit_or_invalid_force_targets(self):
        for change in [dict(urgent=False),dict(force_displace_tasks=[]),
                       dict(force_displace_tasks=['low','low']),
                       dict(force_displace_tasks=[{}]),dict(kind='shared')]:
            with self.subTest(change=change),self.assertRaises(ValueError):
                validate(dict(self.high,**change))

    def test_forecast_reports_cancellation_and_does_not_mutate_victim(self):
        before=copy.deepcopy(self.tasks)
        result=forecast(None,dict(heartbeat=time.time(),snapshot=self.snap,tasks=self.tasks),self.cfg,'high')
        self.assertEqual(result['cancel'],['low'])
        self.assertEqual(result['pause'],[])
        self.assertFalse(result['would_submit'])
        self.assertEqual(before,self.tasks)

    def test_dispatcher_journals_intent_and_waits_for_terminal_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake=Fake();fake.snap=self.snap
            daemon=Daemon(Path(tmp),self.cfg,fake)
            daemon.state['tasks']=self.tasks
            daemon.snapshot=copy.deepcopy(self.snap)
            daemon.schedule()
            self.assertTrue(self.victim['cancel_requested'])
            self.assertEqual(self.victim['displaced_for'],'high')
            self.assertEqual(self.wanted['waiting_checkpoint_victims'],['low'])
            self.assertEqual(checkpoint_barrier(self.tasks),['high'])
            daemon.maintain_lifecycle()
            self.assertEqual(fake.calls,[('cancel','1')])
            daemon.schedule()
            self.assertEqual(fake.calls,[('cancel','1')])
            self.victim['state']='CANCELLED'
            self.assertEqual(checkpoint_barrier(self.tasks),[])


if __name__=='__main__':unittest.main()
