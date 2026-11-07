import os,socket,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
from cdispatch.common import atomic
from cdispatch.runtime_control import reload_component

class RuntimeControlTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.status={'time':time.time(),'host':socket.gethostname(),'job_id':'900','controller_pid':os.getpid(),'telegram_pid':os.getpid()};atomic(self.root/'service-status.json',self.status)
 def test_only_requested_component_is_signalled_after_identity_check(self):
  original=Path.read_text
  def text(path,*args,**kw):return '0::/slurm/job_900/step_0/user/task_0' if path.name=='cgroup' else original(path,*args,**kw)
  with patch.object(Path,'read_bytes',return_value=b'python\0-m\0cdispatch.telegram\0'),patch.object(Path,'read_text',text),patch('cdispatch.runtime_control.os.kill') as kill:
   reload_component(self.root,'bot');kill.assert_called_once()
 def test_pid_reuse_wrong_command_and_stale_supervisor_do_not_signal(self):
  with patch('cdispatch.runtime_control.os.kill') as kill:
   with self.assertRaises(ValueError):reload_component(self.root,'bot')
   self.status['time']=0;atomic(self.root/'service-status.json',self.status)
   with self.assertRaises(ValueError):reload_component(self.root,'daemon')
   kill.assert_not_called()
 def test_remote_reload_is_one_bounded_step_never_parent_cancel(self):
  self.status['host']='other-compute';atomic(self.root/'service-status.json',self.status)
  with patch('cdispatch.runtime_control.subprocess.run') as run,patch('cdispatch.runtime_control.os.kill') as kill:
   reload_component(self.root,'bot');kill.assert_not_called();cmd=run.call_args.args[0]
   self.assertIn('--jobid=900',cmd);self.assertIn('--time=1',cmd);self.assertIn('--local',cmd);self.assertNotIn('scancel',cmd)
