import hashlib,json,tempfile,unittest
from pathlib import Path
from cdispatch_research.geometry_outcomes import expected_geometry_timeout,circuit_failure

class BudgetOutcomeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.point=self.root/'point';(self.point/'excited').mkdir(parents=True)
        self.req=self.root/'r.json';self.req.write_text(json.dumps(dict(point=str(self.point),execution_budget_seconds=9000)))
        digest=hashlib.sha256(self.req.read_bytes()).hexdigest()
        self.task=dict(job_id='42',attempts=[dict(selection=dict(profile=dict(command=['python3','-m','cdispatch_research.adapters','run-geometry',str(self.req),'worker.sh',digest])))])
        self.result=Path(str(self.req)+'.result.json');self.result.write_text(json.dumps(dict(ok=False,time_budget_exhausted=True,error='electronic_failure')))
        self.execution=self.point/'excited/execution.json';self.execution.write_text(json.dumps(dict(returncode=124,slurm_job_id='42')))
    def tearDown(self):self.tmp.cleanup()
    def test_declared_actual_timeout_excluded_but_raw_failure_retained(self):
        self.assertTrue(expected_geometry_timeout(self.task,'FAILED'))
        e=dict(kind='TERMINAL',task='t',state='FAILED',slurm_state='FAILED',job_id='42')
        self.assertFalse(circuit_failure(e,{'t':self.task}));self.assertEqual(e['state'],'FAILED')
    def test_missing_or_wrong_attempt_proof_counts(self):
        self.execution.write_text(json.dumps(dict(returncode=124,slurm_job_id='41')))
        self.assertFalse(expected_geometry_timeout(self.task,'FAILED'))
        self.execution.unlink();self.assertFalse(expected_geometry_timeout(self.task,'FAILED'))
    def test_actual_electronic_failure_counts(self):
        self.result.write_text(json.dumps(dict(ok=False,time_budget_exhausted=False)))
        self.assertFalse(expected_geometry_timeout(self.task,'FAILED'))
        e=dict(kind='TERMINAL',task='t',state='FAILED',slurm_state='FAILED',job_id='42')
        self.assertTrue(circuit_failure(e,{'t':self.task}))
    def test_changed_request_cannot_claim_timeout(self):
        self.req.write_text(self.req.read_text()+' ')
        self.assertFalse(expected_geometry_timeout(self.task,'FAILED'))
    def test_oom_node_failure_and_wall_kill_still_count(self):
        for state in ['OUT_OF_MEMORY','NODE_FAIL','BOOT_FAIL','TIMEOUT']:
            self.assertFalse(expected_geometry_timeout(self.task,state))
    def test_other_attempt_event_still_counts(self):
        self.assertFalse(expected_geometry_timeout(self.task,'FAILED',job_id='99'))
    def test_preemption_keeps_existing_exclusion(self):
        self.assertFalse(circuit_failure(dict(kind='TERMINAL',state='FAILED',slurm_state='PREEMPTED'),{}))
    def test_startup_budget_proof(self):
        self.execution.unlink();self.result.write_text(json.dumps(dict(ok=False,time_budget_exhausted=True,error="StopRun('stopped_time_budget')")))
        Path(str(self.req)+'.budget.json').write_text(json.dumps(dict(slurm_job_id='42',execution_seconds=200)))
        self.assertTrue(expected_geometry_timeout(self.task,'FAILED'))
    def test_malformed_artifact_fails_closed(self):
        self.result.write_text('null');self.assertFalse(expected_geometry_timeout(self.task,'FAILED'))
    def test_three_expected_budgets_do_not_form_failure_burst(self):
        ee=[dict(kind='TERMINAL',task='t',state='FAILED',slurm_state='FAILED',job_id='42') for _ in range(3)]
        self.assertFalse(any(circuit_failure(e,{'t':self.task}) for e in ee))

if __name__=='__main__':unittest.main()
