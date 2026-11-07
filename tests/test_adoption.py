from pathlib import Path
import tempfile
import unittest

from cdispatch.common import enqueue, read, request
from cdispatch.daemon import Daemon
from test_dispatcher import config, spec, Fake


class AdoptionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.fake=Fake();self.daemon=Daemon(self.root,config(),self.fake)
        self.identity=dict(JobId='789',JobName='old-job',UserId='alice(1)',Command='/old/run.sh',SubmitTime='2026-09-29T10:00:00')
        self.fake.snap['jobs']=[dict(self.identity,JobState='RUNNING',Partition='cpu',NodeList='n1',AllocTRES='cpu=64,mem=100000M,node=1')]
        self.output=self.root/'OUTCAR'
        self.task_spec=spec(workdir=str(self.root),adopt_job=self.identity,artifact_contract=[dict(path=str(self.output),contains=['finished','iteration 8'])])

    def finish(self):
        self.fake.snap['jobs']=[]
        self.fake.acct=[dict(id='789',name='old-job',state='COMPLETED',exit_code='0:0',user='alice')]
        self.daemon.tick()

    def test_adopts_running_job_without_submit_or_cancel_and_gates_dependents(self):
        enqueue(self.root,self.task_spec,'adopted');self.daemon.tick()
        task=self.daemon.state['tasks']['adopted']
        self.assertEqual(task['state'],'RUNNING');self.assertTrue(task['adopted']);self.assertFalse(self.fake.calls)
        enqueue(self.root,spec(workdir=str(self.root),depends_on=['adopted']),'child');self.daemon.tick()
        self.assertEqual(self.daemon.state['tasks']['child']['reason'],'dependencies')
        self.output.write_text('finished\niteration 8\n');self.finish()
        self.assertEqual(task['state'],'SUCCEEDED');self.assertEqual(self.daemon.state['tasks']['child']['state'],'PENDING')
        self.assertEqual(len(self.fake.calls),1)

    def test_slurm_zero_without_artifacts_does_not_release_dependency(self):
        enqueue(self.root,self.task_spec,'adopted');self.daemon.tick()
        enqueue(self.root,spec(depends_on=['adopted']),'child');self.daemon.tick();self.finish()
        self.assertEqual(self.daemon.state['tasks']['adopted']['state'],'FAILED')
        self.assertEqual(self.daemon.state['tasks']['child']['state'],'BLOCKED')
        self.assertFalse(self.fake.calls)

    def test_identity_mismatch_cannot_be_resumed_as_new_job(self):
        self.fake.snap['jobs'][0]['Command']='/wrong/job.sh'
        enqueue(self.root,self.task_spec,'adopted');self.daemon.tick()
        self.assertEqual(self.daemon.state['tasks']['adopted']['state'],'HELD')
        token=request(self.root,'resume','adopted');self.daemon.tick()
        self.assertFalse(read(self.root/'receipts'/(token+'.json'))['ok']);self.assertFalse(self.fake.calls)

    def test_other_account_adoption_rejected_without_leaving_task(self):
        self.task_spec['adopt_job']['UserId']='bob(2)'
        enqueue(self.root,self.task_spec,'adopted');self.daemon.tick()
        self.assertNotIn('adopted',self.daemon.state['tasks'])

    def test_cancel_waits_for_identity_verification(self):
        enqueue(self.root,self.task_spec,'adopted');request(self.root,'cancel','adopted')
        self.fake.error=True;self.daemon.tick();self.assertFalse(self.fake.calls)
        self.fake.error=False;self.daemon.tick();self.assertIn(('cancel','789'),self.fake.calls)


if __name__=='__main__':unittest.main()
