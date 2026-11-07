import copy
from pathlib import Path
import tempfile
import unittest
from cdispatch.common import atomic
from cdispatch.profile_library import save,record,build,search,entry_path,check


def entry(name='mpi2',seconds=20,gpu=False):
    obs=dict(outcome='success',application_validated=True,validation_scope='fixed input energy and forces',
             evidence=[dict(path='/evidence/result.json',sha256='a'*64)],elapsed_seconds=seconds,
             timing_scope='application',comparison_key='same-input')
    return dict(schema_version=1,id='solver/energy/cpu/'+name,program='solver',workload='energy',hardware='cpu',
                status='validated',kind='batch',context={'input_class':'fixed-test','software':'solver-v1'},
                profile=dict(name=name,partition='cpu',nodes=1,ranks_per_node=2,threads=1,mem_mib=1000,wall_minutes=10,
                             gpus_per_node=1 if gpu else 0,gpu_type='h200nvl' if gpu else '',command=['/old/live/writer']),
                sources=obs['evidence'],observations=[obs],
                gpu=dict(gres_type='h200nvl',runtime_verified=True) if gpu else {},
                performance=dict(expected_seconds=seconds,basis='measured on frozen input',comparison_key='same-input',timing_scope='application'),
                adaptation=dict(mode='enumerated-only',checkpoint_restart_verified=False))


class LibraryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.base=dict(name='fresh',workdir='/isolated/new/run',command=['/new/validated/writer'])

    def test_matched_pool_preserves_shapes_and_freezes_provenance(self):
        slow=entry();fast=entry('mpi4',10);fast['profile']['ranks_per_node']=4
        save(self.root,slow);save(self.root,fast)
        task=build(self.root,[slow['id'],fast['id']],self.base,slow['context'])
        self.assertEqual([p['slowdown'] for p in task['profiles']],[2,1])
        self.assertEqual([p['ranks_per_node'] for p in task['profiles']],[2,4])
        self.assertEqual(task['profiles'][0]['command'],self.base['command'])
        self.assertTrue(task['profile_library']['entries'][0]['sha256'])

    def test_candidate_only_single_fixed_experiment_and_no_implicit_command(self):
        e=entry();e.update(status='candidate',observations=[]);save(self.root,e)
        ordinary=build(self.root,[e['id']],self.base,e['context'])
        self.assertFalse(ordinary['experimental'])
        self.assertTrue(any('candidate' in x for x in ordinary['profile_library']['warnings']))
        task=build(self.root,[e['id']],self.base,e['context'],experimental=True)
        self.assertEqual(task['max_attempts'],1)
        with self.assertRaises(ValueError):build(self.root,[e['id']],dict(name='x',workdir='/new'),e['context'],experimental=True)

    def test_different_inputs_and_unmatched_speed_not_silently_adapted(self):
        a=entry();b=entry('other');b['performance']['comparison_key']='different-input';save(self.root,a);save(self.root,b)
        changed=build(self.root,[a['id']],self.base,{'input_class':'another'})
        self.assertFalse(changed['profile_library']['timing_comparable'])
        task=build(self.root,[a['id'],b['id']],self.base,a['context'])
        self.assertFalse(task['profile_library']['timing_comparable'])
        self.assertTrue(all('expected_seconds' not in p for p in task['profiles']))

    def test_slurm_success_without_application_gate_cannot_promote(self):
        e=entry();e.update(status='candidate',observations=[]);save(self.root,e)
        obs=dict(outcome='success',application_validated=False,evidence=e['sources'])
        with self.assertRaises(ValueError):record(self.root,e['id'],obs,promote=True)
        self.assertEqual(search(self.root)[0]['status'],'candidate')

    def test_gpu_type_and_runtime_verification_are_required(self):
        e=entry(gpu=True);e['gpu']['runtime_verified']=False
        with self.assertRaises(ValueError):check(e)
        e['gpu']['runtime_verified']=True;e['gpu']['gres_type']='a100'
        with self.assertRaises(ValueError):check(e)

    def test_hardware_risk_is_visible_and_dangerous_layout_cannot_build(self):
        e=entry();e['operational_risk']=dict(label='ok_with_logging',summary='Capture node health',
                                              required_logs=['GPU errors','post-job node state'])
        save(self.root,e)
        task=build(self.root,[e['id']],self.base,e['context'])
        self.assertTrue(any('IMPROVED LOGGING' in w for w in task['profile_library']['warnings']))
        bad=entry('bad');bad['status']='rejected';bad['operational_risk']=dict(
            label='dangerous',summary='Do not repeat',required_logs=['node state'])
        save(self.root,bad)
        with self.assertRaisesRegex(ValueError,'Rejected/retired'):
            build(self.root,[bad['id']],self.base,bad['context'])
        bad['status']='candidate'
        with self.assertRaisesRegex(ValueError,'Dangerous profile'):
            check(bad)

    def test_agent_can_choose_compatible_changed_pinning_without_forced_experiment(self):
        e=entry();e['profile']['pinning']={'mpi':'intel-core','slurm_hint':'nomultithread'};e['pinning_ref']='original'
        pinning=self.root/'pinning';pinning.mkdir()
        atomic(pinning/'original.json',dict(id='original',compatible=dict(programs=['solver'],application_threads=1,gpu_mode='none'),
                                             pinning=e['profile']['pinning']))
        atomic(pinning/'alternative.json',dict(id='alternative',compatible=dict(programs=['solver'],application_threads=1,gpu_mode='none'),
                                                pinning=dict(mpi='intel-omp',slurm_hint='nomultithread',application_threads=1)))
        save(self.root,e)
        changed=build(self.root,[e['id']],self.base,e['context'],pinning_override='alternative')
        self.assertFalse(changed['experimental'])
        self.assertFalse(changed['profile_library']['timing_comparable'])
        self.assertTrue(any('changed pinning' in x for x in changed['profile_library']['warnings']))
        self.assertEqual(changed['profiles'][0]['pinning_profile'],'alternative')

    def test_observations_idempotent_and_profiles_do_not_overwrite(self):
        e=entry();save(self.root,e);record(self.root,e['id'],e['observations'][0]);record(self.root,e['id'],e['observations'][0])
        self.assertEqual(len(search(self.root)[0]['observations']),1)
        e['profile']['mem_mib']=500
        with self.assertRaises(ValueError):save(self.root,e)
        with self.assertRaises(ValueError):entry_path(self.root,'../a/b/c')

    def test_promotion_updates_scoped_timing_only_after_validated_result(self):
        e=entry();e.update(status='candidate',observations=[]);e['performance']={'expected_seconds':None,'basis':'unmeasured','comparison_key':None,'timing_scope':None}
        save(self.root,e)
        obs=dict(outcome='success',application_validated=True,validation_scope='fixed input',context=e['context'],
                 evidence=e['sources'],elapsed_seconds=47,timing_scope='application',comparison_key='same-input',
                 peak_ram_mib=800,peak_ram_scope='job total',note='checked finished outputs')
        promoted=record(self.root,e['id'],obs,promote=True)
        self.assertEqual(promoted['performance']['expected_seconds'],47)
        self.assertEqual(promoted['resources']['peak_ram_mib'],800)
        self.assertIn('47 s',entry_path(self.root,e['id']).with_name('NOTES.md').read_text())
