import tempfile,unittest
from pathlib import Path
from cdispatch.common import atomic
from cdispatch.monitor import mobile
from cdispatch.monitor_cache import monitor_snapshot,read_monitor_snapshot
from test_monitor_cache import fixture

class MonitorContractTests(unittest.TestCase):
 def test_versioned_bot_view_survives_unreadable_scheduler_state(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);data=fixture();view=monitor_snapshot(data);atomic(root/'monitor.json',view);(root/'status.json').write_text('broken scheduler cache')
   read=read_monitor_snapshot(root);self.assertEqual(read,view)
   for command in ('status','queue','nodes','users','limits','decisions','events','report'):
    self.assertEqual(mobile(data,command,''),mobile(read,command,''))
   self.assertEqual(mobile(data,'why','geometry-one'),mobile(read,'why','geometry-one'))
 def test_old_writer_fallback_and_unknown_version_fail_closed(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);data=fixture();atomic(root/'status.json',data)
   self.assertEqual(mobile(read_monitor_snapshot(root),'queue',''),mobile(data,'queue',''))
   view=monitor_snapshot(data);view['monitor_schema_version']=999;atomic(root/'monitor.json',view)
   with self.assertRaisesRegex(ValueError,'version'):read_monitor_snapshot(root)
 def test_monitor_projection_failure_does_not_break_scheduler(self):
  from unittest.mock import patch
  from cdispatch.daemon import Daemon
  from cdispatch.common import read
  from test_dispatcher import Fake,config
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);daemon=Daemon(root,config(),Fake())
   with patch('cdispatch.monitor_cache.monitor_snapshot',side_effect=RuntimeError('bad bot update')):
    daemon.tick()
   self.assertIsNone(read(root/'status.json')['error'])
   self.assertIn('bad bot update',read(root/'monitor-error.json')['error'])
   daemon.tick();self.assertTrue((root/'monitor.json').exists());self.assertFalse((root/'monitor-error.json').exists())
 def test_exact_id_takes_precedence_over_other_matching_prefixes(self):
  from cdispatch.monitor import task_lookup
  data={'tasks':{'job':{'id':'job'},'job-more':{'id':'job-more'}}}
  self.assertEqual(task_lookup(data,'job')['id'],'job')
  with self.assertRaises(ValueError):task_lookup(data,'jo')
 def test_missing_observer_module_cannot_break_core_persistence_or_scheduling(self):
  import sys
  from unittest.mock import patch
  from cdispatch.daemon import Daemon
  from cdispatch.common import read
  from test_dispatcher import Fake,config
  with tempfile.TemporaryDirectory() as directory,patch.dict(sys.modules,{'cdispatch.monitor_cache':None}):
   cfg=config();cfg['task_history']={'enabled':True}
   daemon=Daemon(directory,cfg,Fake());daemon.tick()
   self.assertIsNone(read(Path(directory)/'status.json')['error'])
   self.assertEqual(read(Path(directory)/'state.json')['task_record_schema_version'],2)
   self.assertIn('ModuleNotFoundError',read(Path(directory)/'monitor-error.json')['error'])
