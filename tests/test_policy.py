from datetime import datetime
import copy
import tempfile
from pathlib import Path
import time
import unittest

from cdispatch.common import enqueue, request, fields, read
from cdispatch.daemon import Daemon
from cdispatch.policy import host_matches, pending_advice
from cdispatch.scheduler import choose, queue_plan
from test_dispatcher import config, spec, snapshot, Fake


class PolicyTests(unittest.TestCase):
    def test_colon_slurm_fields_do_not_corrupt_partition(self):
        record = fields('Partition=cpu AllocNode:Sid=calc:123 ReqNodeList=n1')
        self.assertEqual(record['Partition'], 'cpu')
        self.assertEqual(record['AllocNode:Sid'], 'calc:123')

    def test_slurm_future_reservation_allows_only_short_backfill(self):
        snap = snapshot()
        snap['queue_jobs'] = [dict(JobId='other', JobState='PENDING', SchedNodeList='n1',
                                   StartTime=datetime.fromtimestamp(time.time()+1800).isoformat(), Reason='Resources')]
        s = spec()
        s['profiles'][0]['wall_minutes'] = 60
        self.assertIsNone(choose(s, snap, config())[0])
        s['profiles'][0]['wall_minutes'] = 10
        self.assertIsNotNone(choose(s, snap, config())[0])

    def test_predicted_reservation_affects_only_the_named_node(self):
        snap = snapshot()
        second = copy.deepcopy(snap['nodes'][0]);second['NodeName'] = 'n2';snap['nodes'].append(second)
        snap['queue_jobs'] = [dict(JobId='other', JobState='PENDING', SchedNodeList='n1',
                                   StartTime=datetime.fromtimestamp(time.time()).isoformat(), Reason='Priority')]
        self.assertEqual(choose(spec(), snap, config())[0]['nodes'], ['n2'])

    def test_host_ranges(self):
        for name in ('node005-11','node005-16','node005-17','node006-20'):
            self.assertTrue(host_matches('node005-[11,16-17],node006-20', name))
        self.assertFalse(host_matches('node005-[11,16-17],node006-20','node005-12'))

    def test_pending_same_shape_waits_but_smaller_job_can_pass(self):
        snap = snapshot()
        snap['jobs'] = [dict(JobId='12',JobState='PENDING',UserId='alice',Partition='cpu',
                             Reason='Priority',ReqTRES='cpu=64,mem=100000M,node=1')]
        self.assertIn('pending feedback', choose(spec(),snap,config())[1])
        s = spec();s['profiles'][0].update(ranks_per_node=4,mem_mib=20000)
        self.assertIsNotNone(choose(s,snap,config())[0])

    def test_live_reason_change_releases_admission(self):
        snap = snapshot()
        snap['jobs'] = [dict(JobId='12',JobState='PENDING',UserId='alice',Partition='cpu',
                             Reason='QOSGrpCpuLimit',ReqTRES='cpu=1,mem=1M,node=1')]
        self.assertIsNone(choose(spec(),snap,config())[0])
        snap['jobs'][0]['Reason'] = 'Dependency'
        self.assertIsNotNone(choose(spec(),snap,config())[0])

    def test_large_high_priority_waits_while_small_jobs_fill_free_resources(self):
        snap = snapshot();snap['nodes'][0]['AllocMem']='910000'
        large = spec(priority=900,workdir='/tmp/big')
        small = spec(priority=100,workdir='/tmp/small');small['profiles'][0].update(ranks_per_node=4,mem_mib=20000)
        other = copy.deepcopy(small);other['workdir']='/tmp/other'
        plan, decisions = queue_plan([dict(id='large',spec=large,created=1),dict(id='small',spec=small,created=2),
                                       dict(id='other',spec=other,created=3)],snap,config())
        self.assertEqual([tid for tid,s in plan],['small','other'])
        self.assertIn('large',decisions['small']['bypassed_waiting_tasks'])
        self.assertFalse(decisions['large']['admitted'])

    def test_unknown_reason_does_not_authorize_restart(self):
        self.assertEqual(pending_advice('NewSchedulerReason')['action'],'observe')
        self.assertEqual(pending_advice('InvalidAccount')['action'],'operator_review')


class ManualTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.backend=Fake();self.daemon=Daemon(self.root,config(),self.backend)
        self.daemon.state['paused']=True
        enqueue(self.root,spec(workdir=str(self.root)), 'task');self.daemon.tick()

    def test_manual_run_admits_one_task_while_automatic_queue_stays_paused(self):
        enqueue(self.root,spec(workdir='/tmp/other'),'other')
        request(self.root,'run','task');self.daemon.tick()
        self.assertEqual(self.daemon.state['tasks']['task']['state'],'PENDING')
        self.assertEqual(self.daemon.state['tasks']['other']['state'],'QUEUED')
        self.assertTrue(self.daemon.state['paused'])
        self.assertEqual(len(self.backend.calls),1)

    def test_manual_run_keeps_resource_and_health_gates(self):
        self.backend.snap['nodes'][0]['State']='DOWN'
        request(self.root,'run','task');self.daemon.tick()
        self.assertFalse(self.backend.calls)
        self.assertEqual(self.daemon.state['tasks']['task']['state'],'QUEUED')

    def test_pin_cannot_expand_a_qualified_node_allowlist(self):
        task=self.daemon.state['tasks']['task'];task['spec']['profiles'][0]['allowed_nodes']=['n2']
        request(self.root,'pin','task',nodes=['n1']);request(self.root,'run','task');self.daemon.tick()
        self.assertFalse(self.backend.calls)

    def test_manual_hold_and_release_have_receipts(self):
        request(self.root,'hold','task');self.daemon.tick()
        self.assertEqual(self.daemon.state['tasks']['task']['state'],'HELD')
        token=request(self.root,'resume','task');self.daemon.tick()
        self.assertTrue(read(self.root/'receipts'/(token+'.json'))['ok'])
        self.assertEqual(self.daemon.state['tasks']['task']['state'],'QUEUED')


if __name__=='__main__':unittest.main()
