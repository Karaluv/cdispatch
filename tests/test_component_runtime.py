import unittest
from unittest.mock import MagicMock,patch
from cdispatch.component_runtime import control_shape,isolated_command,run_isolated

class ComponentRuntimeTests(unittest.TestCase):
 def config(self):return {'shared_pool':{'enabled':True,'reserve_cpus':3,'reserve_mem_mib':2048},'component_steps':{'enabled':True,'supervisor_mem_mib':512,'daemon':{'mem_mib':1024},'telegram':{'mem_mib':512}}}
 def test_each_component_has_independent_declared_step(self):
  cfg=self.config();self.assertEqual(control_shape(cfg),(1,512))
  for component,memory in (('daemon',1024),('telegram',512)):
   cmd=isolated_command(component,['python','component'],cfg,{'SLURM_JOB_ID':'900'})
   for token in ('--jobid=900','--exclusive','--mem='+str(memory)+'M','--job-name=cd-'+component):self.assertIn(token,cmd)
   self.assertEqual(cmd[-2:],['python','component'])
   self.assertIsNone(isolated_command(component,[],cfg,{'SLURM_JOB_ID':'900','CDISPATCH_COMPONENT_STEP':component}))
 def test_missing_compute_allocation_or_inconsistent_reservation_rejected(self):
  cfg=self.config()
  with self.assertRaises(ValueError):isolated_command('daemon',[],cfg,{})
  for cpu,mem in ((2,2048),(3,1536)):
   cfg=self.config();cfg['shared_pool'].update(reserve_cpus=cpu,reserve_mem_mib=mem)
   with self.assertRaises(ValueError):control_shape(cfg)
 def test_legacy_service_control_shape_preserved(self):
  self.assertEqual(control_shape({'shared_pool':{'reserve_cpus':2,'reserve_mem_mib':1536}}),(2,1536))
 def test_reload_one_launcher_stops_only_its_child_and_restores_signals(self):
  child=MagicMock();child.poll.return_value=None
  with patch('cdispatch.component_runtime.subprocess.Popen',return_value=child) as spawn,patch('cdispatch.component_runtime.signal.signal') as signals:
   child.wait.side_effect=lambda:(signals.call_args_list[0].args[1](),0)[1]
   self.assertEqual(run_isolated('telegram',['srun']),0)
   child.terminate.assert_called_once();self.assertEqual(signals.call_count,6)
   self.assertEqual(spawn.call_args.kwargs['env']['CDISPATCH_COMPONENT_STEP'],'telegram')
