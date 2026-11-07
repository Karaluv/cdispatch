import copy
import time
import unittest
from unittest.mock import patch
from cdispatch.common import read
from cdispatch.slurm import Slurm
from cdispatch.scheduler import choose, reserve
from cdispatch.monitor import mobile
from test_dispatcher import config, snapshot, spec, BASE


class LimitTests(unittest.TestCase):
    def test_disabled_database_limits_do_not_override_local_policy(self):
        snap=snapshot();c=config()
        snap['limits']={'enforcement_enabled':False,'associations':[dict(account='lab',user='alice',group_tres='cpu=1')],'qos':[]}
        self.assertIsNotNone(choose(spec(),snap,c)[0])
        c['users']['alice']['limits']['cpus']=32
        self.assertIsNone(choose(spec(),snap,c)[0])

    def test_five_plus_two_plus_two_all_fit_and_tenth_does_not(self):
        c=read(BASE/'tests/fixtures/config.json');snap=snapshot()
        # Fixed historical fixture tests per-user and aggregate guards regardless
        # of the currently deployed production policy.
        c['pool_limits']['jobs']=9
        for user,limit in zip(c['users'],(5,2,2)):c['users'][user]['limits']['jobs']=limit
        for account in c['users'].values():account['partitions']=['cpu']
        s=spec();p=s['profiles'][0];p.update(ranks_per_node=1,threads=1,mem_mib=128)
        counts={u:0 for u in c['users']}
        for i in range(9):
            selected,reason=choose(s,snap,c)
            self.assertIsNotNone(selected,reason)
            counts[selected['user']]+=1
            reserve(snap,selected,str(i),'fixture')
            snap['jobs'][-1]['JobState']='RUNNING'
        self.assertEqual(sorted(counts.values()),[2,2,5])
        self.assertIsNone(choose(s,snap,c)[0])
        self.assertEqual(c['pool_limits']['jobs'],sum(a['limits']['jobs'] for a in c['users'].values()))
        self.assertTrue(all(c['pool_limits'][k] is None for k in ('cpus','mem_mib','gpus','nodes')))

    def test_live_enforcement_and_ancestor_filter(self):
        c=config();rows='root|||normal||||||20||||\nlab||cpu|normal||||||20|root|||\nlab|alice|cpu|normal||||||20||||\nlab|someone_else||normal||||||20||||'
        with patch('cdispatch.slurm.run',side_effect=[rows,'normal|||||||||','AccountingStorageEnforce = none\nJobSubmitPlugins = (null)']) as run:
            slurm=Slurm(c);got=slurm.limits()
            self.assertFalse(got['enforcement_enabled'])
            self.assertEqual({a['user'] for a in got['associations']},{'','alice'})
            self.assertEqual(slurm.limits(),got);self.assertEqual(run.call_count,3)

    def test_enforced_ancestor_group_limit_is_not_guessed_from_own_jobs(self):
        snap=snapshot();snap['limits']={'enforcement_enabled':True,'associations':[
            dict(account='lab',user='',parent='parent'),
            dict(account='parent',user='',group_tres='cpu=100')], 'qos':[]}
        selected,reason=choose(spec(),snap,config())
        self.assertIsNone(selected);self.assertIn('ancestor account group limit',reason)

    def test_mobile_slots_and_database_source_are_separate(self):
        c=read(BASE/'tests/fixtures/config.json');now=time.time()
        c['pool_limits'].update(cpus=320,mem_mib=5242880)
        jobs=[dict(JobId='1',UserId=c['owner'],JobState='RUNNING',AllocTRES='cpu=36,mem=904G,node=2')]
        jobs+=[dict(JobId='2',UserId=c['owner'],JobState='RUNNING',AllocTRES='cpu=0,mem=0M,node=0')]
        d=dict(heartbeat=now,config=c,user_limits={u:a['limits'] for u,a in c['users'].items()},
               snapshot=dict(time=now,jobs=jobs,limits={'accounting_storage_enforce':'none','enforcement_enabled':False}))
        out=mobile(d,'limits')
        self.assertIn(str(c['pool_limits']['jobs']-2)+'</b> локальных слотов',out)
        self.assertIn('36/320',out);self.assertIn('904/5120',out)
        self.assertIn('не включены',out);self.assertLess(len(out),4096)
        d['snapshot']['time']=now-120
        self.assertIn('устарел',mobile(d,'limits'));self.assertNotIn('🟢 Свободно',mobile(d,'limits'))
