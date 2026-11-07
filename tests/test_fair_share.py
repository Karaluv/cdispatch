import copy
import math
import unittest
from test_dispatcher import config,snapshot,spec,node
from cdispatch.fair_share import contract,groups,lottery,report,cpu_goals
from cdispatch.scheduler import queue_plan,shape
from cdispatch.common import validate


def descriptor(group,priority=300):
    return dict(cohort='scan',group=group,priority=priority,tier=300,weight=1.,demand_cpus=320,max_parallel=5)


def task(tid,group,priority=300,state='QUEUED',cpus=32):
    s=spec(workdir='/tmp/'+tid,priority=priority,fair_share=descriptor(group,priority),max_slowdown=4.)
    s['profiles'][0].update(name='p32',ranks_per_node=8,threads=4,mem_mib=65536,slowdown=2.)
    t=dict(id=tid,spec=s,state=state,created=1,attempts=[])
    if state!='QUEUED':
        t.update(job_id=tid,selection=dict(profile=dict(s['profiles'][0],ranks_per_node=cpus//4)))
    return t


class FairShareTests(unittest.TestCase):
    def setUp(self):
        self.c=config();self.c['pool_limits'].update(jobs=100,cpus=10000,nodes=100);self.c['max_pending']=100
        self.c['users']['alice']['limits'].update(jobs=100,cpus=10000,nodes=100)
        self.c['fair_share']=dict(temperature=20.,cpu_quantum=32.)
        self.snap=snapshot();self.snap['nodes']=[node(cpus=1024,mem=10000000)]

    def test_adjacent_priority_weight_is_smooth(self):
        ts=[task('a','a',301),task('b','b',302)]
        e=groups(ts,self.snap,self.c)
        self.assertAlmostEqual(e['scan/b']['weight']/e['scan/a']['weight'],math.exp(1/20))

    def test_lottery_distribution_and_replay(self):
        e=groups([task('a','a',301),task('b','b',302)],self.snap,self.c)
        hits=0
        for i in range(3000):
            snap=dict(self.snap,time=i)
            order=lottery(e,self.c,snap)
            self.assertEqual(order,lottery(e,self.c,snap))
            hits+=order[0]=='scan/b'
        self.assertTrue(.49<hits/3000<.54,hits/3000)

    def test_extra_workers_do_not_multiply_group_lottery(self):
        one=groups([task('a','a'),task('b','b')],self.snap,self.c)
        many=groups([task('a'+str(i),'a') for i in range(8)]+[task('b','b')],self.snap,self.c)
        for i in range(50):self.assertEqual(lottery(one,self.c,dict(time=i)),lottery(many,self.c,dict(time=i)))

    def test_192_vs_32_debt_suppresses_overallocated_direction(self):
        e=groups([task('old-a','a',state='RUNNING',cpus=192),task('old-b','b',state='RUNNING'),task('a','a'),task('b','b')],self.snap,self.c)
        hits=sum(lottery(e,self.c,dict(time=i))[0]=='scan/b' for i in range(1000))
        self.assertGreater(hits,975)

    def test_plan_reserves_by_group_and_honors_parallel_limit(self):
        ts=[task('a'+str(i),'a') for i in range(10)]+[task('b'+str(i),'b') for i in range(10)]
        plan,decisions=queue_plan(ts,self.snap,self.c)
        counts={g:sum(tid.startswith(g) for tid,_ in plan) for g in ('a','b')}
        self.assertEqual(counts,dict(a=5,b=5))
        self.assertTrue(all(d.get('fair_share') for d in decisions.values()))

    def test_actual_shape_not_job_count_accounts_cpu(self):
        ts=[task('a','a',state='RUNNING',cpus=128),task('b','b',state='RUNNING',cpus=32)]
        e=groups(ts,self.snap,self.c)
        self.assertEqual(e['scan/a']['reserved_cpus'],128)
        self.assertEqual(e['scan/b']['reserved_cpus'],32)

    def test_pending_and_uncertain_submit_hold_share(self):
        for state in ['PENDING','SUBMITTING','SUBMIT_UNKNOWN','CANCELLING']:
            e=groups([task('x','a',state=state,cpus=64)],self.snap,self.c)
            self.assertEqual(e['scan/a']['reserved_cpus'],64)

    def test_unrelated_high_priority_stays_first(self):
        ts=[task('a','a'),dict(id='urgent',spec=spec(workdir='/tmp/urgent',priority=500),state='QUEUED',created=2)]
        self.assertEqual(queue_plan(ts,self.snap,self.c)[0][0][0],'urgent')

    def test_small_validated_shape_preserves_peers_share(self):
        ts=[task('a','a'),task('b','b')]
        for t in ts:t['spec']['profiles'].append(dict(t['spec']['profiles'][0],name='p128',ranks_per_node=32,slowdown=1.))
        self.snap['nodes'][0].update(CPUTot='64',CPUEfctv='64')
        plan,_=queue_plan(ts,self.snap,self.c)
        self.assertEqual(len(plan),2)
        self.assertEqual([shape(s['profile'])['cpus'] for _,s in plan],[32,32])

    def test_quota_and_dead_node_are_never_bypassed(self):
        self.c['users']['alice']['limits']['cpus']=16
        self.assertFalse(queue_plan([task('a','a')],self.snap,self.c)[0])
        self.c['users']['alice']['limits']['cpus']=10000;self.snap['nodes'][0]['State']='MIXED+DRAIN'
        self.assertFalse(queue_plan([task('a','a')],self.snap,self.c)[0])

    def test_invalid_conflicting_and_shared_contract_rejected(self):
        for changes in [dict(weight=float('nan')),dict(max_parallel=0),dict(priority=True),dict(group=''),dict(unknown=1)]:
            with self.assertRaises((ValueError,TypeError)):validate(spec(fair_share=dict(descriptor('a'),**changes)))
        with self.assertRaises(ValueError):validate(spec(kind='shared',fair_share=descriptor('a')))
        with self.assertRaisesRegex(ValueError,'Conflicting'):
            groups([task('a','a',301),task('b','a',302)],self.snap,self.c)

    def test_selector_preserves_immutable_spec(self):
        s=spec();before=copy.deepcopy(s)
        self.c['fair_share']['rules']=[dict(labels=dict(project='glass',purpose='test'),contract=descriptor('a'))]
        self.assertEqual(contract(s,self.c),descriptor('a'));self.assertEqual(s,before)
        self.assertIsNone(contract(dict(s,kind='shared'),self.c))

    def test_duplicate_job_and_terminal_state_do_not_overcount(self):
        a=task('a','a',state='RUNNING');b=copy.deepcopy(a);b['id']='alias'
        c=task('c','a',state='SUCCEEDED')
        self.assertEqual(groups([a,b,c],self.snap,self.c)['scan/a']['reserved_cpus'],32)

    def test_pending_reservation_does_not_invent_physical_cpu(self):
        ts=[task('pending','a',state='PENDING',cpus=128),task('b','b')]
        self.snap['nodes'][0].update(CPUTot='64',CPUEfctv='64')
        self.snap['jobs']=[dict(JobId='pending',JobState='PENDING',NumCPUs='128',ReqTRES='cpu=128')]
        e=groups(ts,self.snap,self.c)
        self.assertEqual(e['scan/a']['reserved_cpus'],128)
        self.assertEqual(e['scan/a']['allocated_cpus'],0)
        self.assertAlmostEqual(sum(cpu_goals(e,self.snap,self.c).values()),64)

    def test_physical_capacity_is_deduplicated_and_includes_running_cohort(self):
        self.snap['nodes']=[node(cpus=128,CPUAlloc='64')]*2
        self.snap['jobs']=[dict(JobId='running',JobState='RUNNING',NumCPUs='64',AllocTRES='cpu=64')]
        e=groups([task('running','a',state='RUNNING',cpus=64),task('b','b')],self.snap,self.c)
        self.assertAlmostEqual(sum(cpu_goals(e,self.snap,self.c).values()),128)

    def test_incompatible_peer_does_not_reserve_idle_cpu(self):
        a=task('a','a');a['spec']['profiles'].append(dict(a['spec']['profiles'][0],name='p128',ranks_per_node=32,slowdown=1.))
        b=task('b','b');b['spec']['profiles'][0]['allowed_nodes']=['unavailable']
        self.snap['nodes'][0].update(CPUTot='128',CPUEfctv='128')
        plan,_=queue_plan([a,b],self.snap,self.c)
        self.assertEqual(len(plan),1);self.assertEqual(shape(plan[0][1]['profile'])['cpus'],128)

    def test_conflicting_submission_rejected_without_stopping_other_tasks(self):
        import tempfile
        from pathlib import Path
        from cdispatch.common import enqueue
        from cdispatch.daemon import Daemon
        from test_dispatcher import Fake
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);d=Daemon(root,self.c,Fake())
            a=task('a','a')['spec'];bad=copy.deepcopy(a);bad['workdir']='/tmp/bad';bad['fair_share']['weight']=2.
            enqueue(root,a,'a');enqueue(root,bad,'bad');enqueue(root,task('b','b')['spec'],'b')
            d.ingest()
            self.assertIn('a',d.state['tasks']);self.assertNotIn('bad',d.state['tasks']);self.assertIn('b',d.state['tasks'])
            d.publish();self.assertTrue((root/'status.json').exists())

    def test_lottery_cannot_leapfrog_heavily_underallocated_feasible_peer(self):
        e=groups([task('a','a',state='RUNNING',cpus=192),task('b','b',state='RUNNING',cpus=32)],self.snap,self.c)
        for i in range(50):self.assertEqual(lottery(e,self.c,dict(time=i)),['scan/b'])
