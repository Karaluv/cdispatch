import time
from datetime import datetime
import unittest
from cdispatch.scheduler import choose
from test_dispatcher import config, snapshot, spec


class PlannedBackfillTests(unittest.TestCase):
    def test_explicit_short_window_only_and_never_drained(self):
        snap = snapshot()
        snap['nodes'][0]['State'] = 'MIXED+PLANNED'
        snap['queue_jobs'] = [dict(JobId='higher', JobState='PENDING', SchedNodeList='n1',
                                  StartTime=datetime.fromtimestamp(time.time()+1800).isoformat())]
        task = spec()
        self.assertIsNone(choose(task, snap, config())[0])
        task['placement'] = {'allow_planned_backfill': True}
        self.assertIsNotNone(choose(task, snap, config())[0])
        task['profiles'][0]['wall_minutes'] = 30
        self.assertIsNone(choose(task, snap, config())[0])
        task['profiles'][0]['wall_minutes'] = 10
        snap['nodes'][0]['State'] += '+DRAIN'
        self.assertIsNone(choose(task, snap, config())[0])

    def test_unknown_or_stale_window_stays_blocked(self):
        task = spec(placement={'allow_planned_backfill': True})
        snap = snapshot()
        snap['nodes'][0]['State'] = 'MIXED+PLANNED'
        self.assertIsNone(choose(task, snap, config())[0])
        snap['queue_jobs'] = [dict(JobId='higher', JobState='PENDING', SchedNodeList='n1', StartTime='Unknown')]
        self.assertIsNone(choose(task, snap, config())[0])
        snap['queue_jobs'][0]['StartTime'] = datetime.fromtimestamp(time.time()+1800).isoformat()
        snap['time'] = time.time()-300
        self.assertIsNone(choose(task, snap, config())[0])
