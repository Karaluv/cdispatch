import copy
import unittest

from cdispatch.limits import live_blockers, report
from cdispatch.scheduler import choose, reserve, release, usage
from test_dispatcher import config, snapshot, spec, node


class ScopedLimitsTests(unittest.TestCase):
    def setUp(self):
        self.c = config();self.s = spec();self.snap = snapshot()
        self.snap['limit_jobs'] = []
        self.snap['limits'] = dict(enforcement_enabled=True,
            associations=[dict(account='lab',user='alice',default_qos='normal')],
            qos=[dict(name='normal')], account_parents={'lab':'parent','parent':'root'})

    def blocked(self):
        return live_blockers('alice',self.c['users']['alice'],self.s['profiles'][0],self.snap)

    def job(self, user='alice', account='lab', qos='normal', state='RUNNING', cpu=32, **kw):
        j=dict(JobId=str(len(self.snap['limit_jobs'])+1),UserId=user,Account=account,
               QOS=qos,Partition='cpu',JobState=state,ReqTRES='cpu=%s,mem=100M,node=1'%cpu)
        j.update(kw);self.snap['limit_jobs'].append(j);return j

    def test_running_and_submit_limits_have_different_counters(self):
        q=self.snap['limits']['qos'][0];q.update(user_jobs='1',user_submit='3')
        self.job(state='PENDING');self.job(state='PENDING')
        self.assertFalse(self.blocked())
        self.job(state='PENDING')
        self.assertEqual([b['dimension'] for b in self.blocked()],['user_submit'])
        self.snap['limit_jobs'][0]['JobState']='RUNNING'
        self.assertIn('user_jobs',[b['dimension'] for b in self.blocked()])

    def test_group_limits_include_users_outside_local_pool(self):
        self.snap['limits']['qos'][0]['group_tres']='cpu=80'
        self.job(user='outsider',account='another',cpu=32)
        self.assertTrue(self.blocked())
        self.snap['limits']['qos'][0]['group_tres']='cpu=96'
        self.assertFalse(self.blocked())

    def test_ancestor_group_counts_all_descendant_accounts(self):
        self.snap['limits']['account_parents']['sibling']='parent'
        self.snap['limits']['associations'].append(dict(account='parent',user='',group_tres='cpu=80'))
        self.job(user='outsider',account='sibling',cpu=32)
        self.assertTrue(self.blocked())
        self.snap['limits']['account_parents']['sibling']='elsewhere'
        self.assertFalse(self.blocked())

    def test_intersection_checks_each_group_without_adding_ceilings(self):
        self.snap['limits']['associations'] += [dict(account='lab',user='',group_tres='cpu=128'),
            dict(account='parent',user='',group_tres='cpu=100')]
        self.job(cpu=32);self.job(user='outsider',account='parent',cpu=8)
        self.assertTrue(self.blocked())
        self.snap['limits']['associations'][-1]['group_tres']='cpu=104'
        self.assertFalse(self.blocked())

    def test_qos_user_limit_spans_accounts(self):
        self.snap['limits']['qos'][0]['user_tres']='cpu=80'
        self.job(account='another',cpu=32)
        self.assertTrue(self.blocked())

    def test_partition_qos_precedence_and_overpartqos(self):
        self.snap['partitions']['cpu']['QoS']='partition'
        self.snap['limits']['qos'] += [dict(name='partition',job_tres='cpu=128')]
        self.snap['limits']['qos'][0]['job_tres']='cpu=32'
        self.assertFalse(self.blocked())
        self.snap['limits']['qos'][0]['flags']='OverPartQOS'
        self.assertTrue(self.blocked())

    def test_precedence_is_per_tres_dimension(self):
        self.snap['partitions']['cpu']['QoS']='partition'
        self.snap['limits']['qos'] += [dict(name='partition',job_tres='cpu=128')]
        self.snap['limits']['qos'][0]['job_tres']='cpu=32,mem=50000M'
        self.assertEqual([b['dimension'] for b in self.blocked()],['mem'])

    def test_qos_max_jobs_overrides_association_max_jobs(self):
        self.snap['limits']['qos'][0]['user_jobs']='4'
        self.snap['limits']['associations'][0]['max_jobs']='1'
        self.job()
        self.assertFalse(self.blocked())

    def test_pending_tres_not_counted_as_running_quota(self):
        self.snap['limits']['qos'][0]['user_tres']='cpu=64'
        self.job(state='PENDING',cpu=64)
        self.assertFalse(self.blocked())
        self.snap['limit_jobs'][0]['Virtual']='YES'
        self.assertTrue(self.blocked())

    def test_virtual_reservations_and_release_update_every_scope(self):
        self.snap['limits']['qos'][0]['group_tres']='cpu=64'
        selected,_=choose(self.s,self.snap,self.c)
        reserve(self.snap,selected,'provisional','fixture')
        self.assertTrue(self.blocked())
        self.assertFalse(live_blockers('alice',self.c['users']['alice'],self.s['profiles'][0],
            release(self.snap,dict(selection=selected,job_id='provisional'))))

    def test_per_job_node_wall_and_minimum_limits(self):
        self.snap['limits']['qos'][0].update(node_tres='cpu=32',max_wall='00:05:00',min_job_tres='gres/gpu=1')
        b=self.blocked();self.assertEqual({x['dimension'] for x in b},{'cpu','wall_seconds','gres/gpu'})

    def test_typed_gpu_quota(self):
        self.s['profiles'][0].update(gpu_type='h200nvl',gpus_per_node=3)
        self.snap['limits']['qos'][0]['user_tres']='gres/gpu:h200nvl=2'
        self.assertEqual(self.blocked()[0]['dimension'],'gres/gpu:h200nvl')

    def test_timed_and_relative_quotas_require_usage_integration(self):
        self.snap['limits']['qos'][0].update(group_tres_mins='cpu=100',flags='Relative')
        self.assertEqual(len(self.blocked()),2)
        self.snap['limits']['enforcement_enabled']=False
        self.assertFalse(self.blocked())

    def test_compressed_arrays_count_all_elements_and_requests(self):
        self.job(state='PENDING',ArrayTaskId='[1-7:2,12]%2',cpu=2)
        used=usage(self.snap['limit_jobs'])
        self.assertEqual(used['jobs'],5);self.assertEqual(used['cpus'],10)

    def test_physical_capacity_deduplicates_users_and_partition_aliases(self):
        self.snap['nodes'][0]['Partitions']='cpu,alias'
        self.c['users']['alice']['partitions']=['cpu','alias']
        self.c['users']['bob']=copy.deepcopy(self.c['users']['alice'])
        result=report(self.snap,self.c)
        self.assertEqual(result['physical_pool']['cpus'],128)
        self.assertEqual(result['physical_pool']['node_names'],['n1'])
        self.assertEqual(sum(a['physical_capacity']['cpus'] for a in result['users'].values()),256)

    def test_partition_node_cpu_and_memory_limits(self):
        p=self.snap['partitions']['cpu'];p['MaxCPUsPerNode']='32'
        self.assertIsNone(choose(self.s,self.snap,self.c)[0])
        p['MaxCPUsPerNode']='128';p['MaxMemPerNode']='50000M'
        self.assertIsNone(choose(self.s,self.snap,self.c)[0])

    def test_global_job_count_and_partition_group_access(self):
        self.snap['limits']['scheduler_settings']={'MaxJobCount':'1'};self.job()
        self.assertIsNone(choose(self.s,self.snap,self.c)[0])
        self.snap['limit_jobs']=[];self.snap['partitions']['cpu']['AllowGroups']='study'
        self.assertIsNone(choose(self.s,self.snap,self.c)[0])
        self.c['users']['alice']['unix_groups']=['study']
        self.assertIsNotNone(choose(self.s,self.snap,self.c)[0])

    def test_numeric_hardware_envelope_preserves_live_client_compatibility(self):
        import tempfile
        from cdispatch.daemon import Daemon
        from cdispatch.common import read
        from cdispatch.monitor import mobile
        from test_dispatcher import Fake
        for key in ('cpus','mem_mib','gpus','nodes'):self.c['pool_limits'][key]=None
        with tempfile.TemporaryDirectory() as root:
            daemon=Daemon(root,self.c,Fake());daemon.snapshot=self.snap;daemon.publish()
            data=read(daemon.root/'status.json')
        self.assertEqual(data['config']['pool_limits']['cpus'],128)
        self.assertIsNone(data['config']['local_pool_limits']['cpus'])
        self.assertIsNone(self.c['pool_limits']['cpus'])
        self.assertIn('без локального потолка',mobile(data,'limits'))

    def test_group_feedback_applies_across_users_and_partitions(self):
        self.job(user='outsider',account='another',state='PENDING',Reason='QOSGrpCpuLimit',Partition='elsewhere')
        selected,reason=choose(self.s,self.snap,self.c)
        self.assertIsNone(selected);self.assertIn('pending feedback',reason)
        self.snap['limit_jobs'][0]['QOS']='unrelated'
        self.assertIsNotNone(choose(self.s,self.snap,self.c)[0])

    def test_hidden_jobs_do_not_prove_remaining_group_entitlement(self):
        self.snap['limits']['full_usage_visible']=False
        self.snap['limits']['qos'][0]['group_tres']='cpu=128'
        self.assertIn('full usage audit',self.blocked()[0]['error'])
        self.snap['limits']['qos'][0].pop('group_tres')
        self.snap['limits']['associations'].append(dict(account='lab',user='',group_tres='cpu=128'))
        self.assertIn('full usage audit',self.blocked()[0]['error'])
