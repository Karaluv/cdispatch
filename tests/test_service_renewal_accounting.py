import copy
import unittest

from cdispatch.scheduler import usage, choose
from cdispatch.service_renewal import deferred_service_successors
from test_dispatcher import config, snapshot, spec


def pair():
    common = dict(JobName='cd-service', UserId='alice(1)', Account='lab',
                  Command='/home/alice/cluster-dispatcher/var/service.sh')
    return [dict(common, JobId='123', JobState='RUNNING', AllocTRES='cpu=4,mem=4G,node=1'),
            dict(common, JobId='456', JobState='PENDING', AllocTRES='(null)',
                 ReqTRES='cpu=4,mem=4G,node=1', Reason='Dependency',
                 Dependency='afterany:123(unfulfilled)')]


class ServiceRenewalAccountingTests(unittest.TestCase):
    def test_renewal_shares_local_slot_and_resources_without_mutating_raw_jobs(self):
        jobs=pair();before=copy.deepcopy(jobs)
        self.assertEqual(usage(jobs), dict(cpus=4,mem_mib=4096,gpus=0,nodes=1,jobs=1,pending=0))
        self.assertEqual(usage(iter(jobs),'alice')['jobs'],1)
        self.assertEqual(usage(jobs,'bob')['jobs'],0)
        self.assertEqual(jobs,before)

    def test_other_dependency_jobs_and_unproven_renewals_still_count(self):
        for patch in [dict(JobName='science'), dict(UserId='bob(2)'), dict(Account='other'),
                      dict(Command='/tmp/service.sh'), dict(Dependency='afterok:123'),
                      dict(Dependency='afterany:123,afterany:789'), dict(Dependency='afterany:999'),
                      dict(Reason='Priority'), dict(JobState='CONFIGURING'),
                      dict(AllocTRES='cpu=4,mem=4G,node=1'), dict(JobId='456_1')]:
            with self.subTest(patch=patch):
                jobs=pair();jobs[1].update(patch)
                self.assertFalse(deferred_service_successors(jobs))
                self.assertEqual(usage(jobs)['jobs'],2)

    def test_parent_must_be_proven_running_and_identity_must_match(self):
        for patch in [dict(JobState='COMPLETING'),dict(JobState='PENDING'),
                      dict(JobName='other'),dict(Command='/tmp/service.sh'),dict(UserId='')]:
            jobs=pair();jobs[0].update(patch)
            self.assertFalse(deferred_service_successors(jobs))
        jobs=pair()
        for job in jobs:job.pop('Account')
        self.assertFalse(deferred_service_successors(jobs))

    def test_duplicate_successors_are_not_exempt(self):
        jobs=pair();jobs.append(dict(jobs[1],JobId='789'))
        self.assertFalse(deferred_service_successors(jobs))
        self.assertEqual(usage(jobs)['jobs'],3)

    def test_admission_uses_same_pool_user_and_pending_accounting(self):
        snap=snapshot();snap['jobs']=pair();cfg=config()
        cfg['pool_limits']['jobs']=2;cfg['users']['alice']['limits']['jobs']=2;cfg['max_pending']=1
        selected,reason=choose(spec(),snap,cfg)
        self.assertIsNotNone(selected,reason)
        # A real queued scientific allocation still fills both slots.
        snap['jobs'].append(dict(JobId='789',JobState='PENDING',UserId='alice(1)',ReqTRES='cpu=1,mem=1G,node=1'))
        self.assertIsNone(choose(spec(),snap,cfg)[0])

    def test_live_slurm_submit_limit_still_counts_the_raw_successor(self):
        snap=snapshot();snap['jobs']=pair();snap['account_jobs']=copy.deepcopy(snap['jobs'])
        snap['limits']=dict(associations=[dict(account='lab',user='alice',max_submit='2')],qos=[],enforcement_enabled=True)
        self.assertIsNone(choose(spec(),snap,config())[0])


if __name__=='__main__':unittest.main()
