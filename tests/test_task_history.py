import copy,json,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
from cdispatch.common import atomic,read,enqueue,request
from cdispatch.task_history import TaskHistory,TaskIndex
from cdispatch.client import Client
from cdispatch.daemon import Daemon
from test_monitor_cache import fixture
from test_dispatcher import config,Fake,spec

class TaskHistoryTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);self.store=TaskHistory(self.root)
 def task(self):
  task=fixture(20000)['tasks']['geometry-one'];task['spec'].update(group='science',parent='controller',depends_on=['dep'],workdir='/science',restart_from_scratch_safe=True);task['accounting']={'state':'CANCELLED by 1234','id':'123','exit_code':'0:15'};return task
 def test_archive_is_lossless_compact_and_published_before_reference(self):
  original=self.task();compact=self.store.pack(original)
  self.assertEqual(self.store.load(compact),original)
  self.assertLess(len(json.dumps(compact)),len(json.dumps(original))//10)
  self.assertEqual(compact['accounting']['state'],'CANCELLED by 1234')
  self.assertEqual(compact['spec']['parent'],'controller')
  self.assertNotIn('submitted_spec',compact)
  self.assertIs(self.store.pack(compact),compact)
 def test_active_and_ambiguous_tasks_never_archived(self):
  for state in ('RUNNING','SUBMIT_UNKNOWN','PENDING','PAUSED','HELD','QUEUED','CANCELLING'):
   task=self.task();task['state']=state;self.assertIs(self.store.pack(task),task)
 def test_explicit_lookup_loads_one_record_iteration_does_not(self):
  full=self.task();compact=self.store.pack(full);index=TaskIndex({'geometry-one':compact},self.root)
  with patch.object(index.history,'load',wraps=index.history.load) as load:
   self.assertEqual(list(index.values()),[compact]);self.assertEqual(list(index.items()),[('geometry-one',compact)]);load.assert_not_called()
   self.assertEqual(index['geometry-one'],full);self.assertEqual(index.get('geometry-one'),full);self.assertIsNone(index.get('missing'));self.assertEqual(load.call_count,2)
 def test_client_lookup_remains_full_and_snapshot_iteration_is_compact(self):
  full=self.task();atomic(self.root/'status.json',{'tasks':{'geometry-one':self.store.pack(full)}})
  client=Client(self.root);self.assertEqual(client.status('geometry-one'),full)
  self.assertIn('_history',next(iter(client.snapshot(fresh=False)['tasks'].values())))
 def test_corrupt_archive_and_unknown_schema_fail_closed(self):
  compact=self.store.pack(self.task());compact['_history']['version']=2
  with self.assertRaises(ValueError):self.store.load(compact)
  compact=self.store.pack(self.task());path=self.root/'history/tasks/geometry-one'/(compact['_history']['sha256']+'.json');path.write_text('{}')
  with self.assertRaises(ValueError):self.store.load(compact)
 def test_streaming_migration_restart_and_recent_event_protection(self):
  task=self.task();atomic(self.root/'state.json',{'tasks':{'geometry-one':task},'events':[]})
  state=self.store.read_state(self.root/'state.json',{});self.assertEqual(self.store.load(state['tasks']['geometry-one']),task)
  atomic(self.root/'state.json',state);self.assertEqual(self.store.read_state(self.root/'state.json',{}),state)
  state={'tasks':{'geometry-one':task},'events':[{'task':'geometry-one','time':time.time()}]};self.store.compact(state);self.assertIs(state['tasks']['geometry-one'],task)
 def test_archived_retry_restores_execution_and_idempotency_contract(self):
  cfg=config();cfg['task_history']={'enabled':True};fake=Fake();d=Daemon(self.root,cfg,fake)
  original=spec(restart_from_scratch_safe=True,max_attempts=2,workdir=str(self.root))
  from cdispatch.common import validate
  original=validate(original);full={'id':'old','state':'FAILED','spec':copy.deepcopy(original),'submitted_spec':copy.deepcopy(original),'created':1,'attempts':[{'number':1,'selection':{'profile':original['profiles'][0]}}],'reason':'old failure'}
  d.state['tasks']['old']=self.store.pack(full)
  enqueue(self.root,original,'old');d.ingest();self.assertEqual(d.state['tasks']['old']['state'],'FAILED')
  changed=copy.deepcopy(original);changed['priority']+=1
  with self.assertRaises(ValueError):enqueue(self.root,changed,'old')
  request(self.root,'retry','old');d.ingest();restored=d.state['tasks']['old'];self.assertEqual(restored['state'],'QUEUED');self.assertNotIn('_history',restored);self.assertEqual(restored['spec'],original);self.assertEqual(len(restored['attempts']),1)
 def test_failed_archive_write_does_not_publish_compacted_state(self):
  full=self.task()
  with patch('cdispatch.task_history.atomic',side_effect=OSError('disk failure')):
   with self.assertRaises(OSError):self.store.pack(full)
  self.assertNotIn('_history',full)
 def test_rollback_restores_current_full_records_without_old_snapshot(self):
  full=self.task();compact=self.store.pack(full);atomic(self.root/'state.json',{'tasks':{'geometry-one':compact},'events':[],'accounting':[],'created':1,'last_submit':0,'task_record_schema_version':2})
  cfg=config();cfg['task_history']={'enabled':False};daemon=Daemon(self.root,cfg,Fake())
  self.assertEqual(daemon.state['tasks']['geometry-one'],full);self.assertEqual(daemon.state['task_record_schema_version'],1)
 def test_archived_terminal_dependency_retains_admission_gate(self):
  from cdispatch.planning import ready_tasks
  from cdispatch.common import validate
  original=self.task();original['state']='SUCCEEDED';archived=self.store.pack(original)
  pending={'id':'new','state':'QUEUED','created':time.time(),'attempts':[],'spec':validate(spec(depends_on=['geometry-one'],workdir=str(self.root)))}
  ready,blocked=ready_tasks({'geometry-one':archived,'new':pending})
  self.assertEqual([x['id'] for x in ready],['new']);self.assertEqual(blocked,{})
  original['state']='FAILED';archived=self.store.pack(original);ready,blocked=ready_tasks({'geometry-one':archived,'new':pending})
  self.assertFalse(ready);self.assertTrue(blocked['new']['terminal'])
