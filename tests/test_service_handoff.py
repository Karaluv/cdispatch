import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from cdispatch.common import atomic,read,enqueue
from cdispatch.daemon import Daemon
from cdispatch.service_handoff import audit
from test_dispatcher import Fake,config,spec

class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.plan=dict(job_id='901',predecessor='900',tasks=['science'],report=str(self.root/'handoff.json'))

    def status(self,**updates):
        s=dict(heartbeat=100,paused=False,circuit=None,error=None,snapshot=dict(shared_pool=dict(job_id='901')),tasks=dict(science=dict(state='RUNNING',job_id='111')))
        s.update(updates);atomic(self.root/'status.json',s)

    def test_audit_waits_for_new_service_and_fresh_heartbeat(self):
        self.status(snapshot=dict(shared_pool=dict(job_id='900')))
        self.assertFalse(audit(self.root,'901',self.plan,100))
        self.assertFalse((self.root/'handoff.json').exists())
        self.status();self.assertFalse(audit(self.root,'901',self.plan,200))
        self.assertTrue(audit(self.root,'901',self.plan,100))
        self.assertEqual(read(self.root/'handoff.json')['tasks']['science']['job_id'],'111')

    def test_audit_records_missing_tasks_and_blocked_admission(self):
        for updates in [dict(tasks={}),dict(paused=True),dict(circuit='blocked'),dict(error='stale')]:
            self.status(**updates);self.assertFalse(audit(self.root,'901',self.plan,100))
            self.assertFalse(read(self.root/'handoff.json')['ok'])

    def test_replacement_keeps_running_batch_and_launches_saved_queue(self):
        cfg=config();backend=Fake()
        running=spec(workdir=str(self.root));(self.root/'queued').mkdir()
        queued=spec(workdir=str(self.root/'queued'))
        # Build a persisted controller/worker identity; service allocation changes independently.
        tasks=dict(science=dict(id='science',spec=running,submitted_spec=copy.deepcopy(running),state='RUNNING',job_id='111',user='alice',attempt=str(self.root/'existing'),attempts=[dict(number=1,path=str(self.root/'existing'),job_id='111')],created=1))
        atomic(self.root/'state.json',dict(tasks=tasks,last_submit=0,events=[],accounting=[],created=1))
        backend.snap['jobs']=[dict(JobId='111',JobName='science',JobState='RUNNING',UserId='alice',AllocTRES='cpu=64,mem=100000M,node=1')]
        backend.snap['nodes'][0]['CPUAlloc']='64'
        enqueue(self.root,queued,'queued-science')
        with patch.dict('os.environ',{'SLURM_JOB_ID':'901'}):
            restored=Daemon(self.root,cfg,backend);restored.tick()
        self.assertEqual(restored.config['service_job_id'],'901')
        self.assertEqual(restored.state['tasks']['science']['job_id'],'111')
        self.assertEqual(restored.state['tasks']['science']['state'],'RUNNING')
        self.assertEqual(restored.state['tasks']['queued-science']['state'],'PENDING')
        self.assertEqual(len(backend.calls),1)
        self.assertFalse(any(isinstance(c,tuple) and c[0]=='cancel' for c in backend.calls))

if __name__=='__main__':unittest.main()
