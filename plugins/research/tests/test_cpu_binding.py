import copy, unittest
from unittest.mock import patch
from support import spec
from cdispatch_research.application_profiles import apply

class CpuBindingTests(unittest.TestCase):
 def request(self):
  return spec()
 def test_binding_preserves_original_request_and_science(self):
  s=self.request();s['labels']={'project':'glass','direction':'test'};before=copy.deepcopy(s);c={'application_profile_rules':[dict(name='ordered',authorization='user explicit',labels={'direction':'test'},profiles=[dict(id='x',sha256='y')],preferred_cpu_counts=[128,64,32])]}
  with patch('cdispatch_research.application_profiles.reviewed_profile',return_value=s['profiles'][0]):out=apply(s,c)
  self.assertEqual(out['placement']['preferred_cpu_counts'],[128,64,32]);self.assertEqual(s,before);self.assertEqual(out['command'],s['command']);self.assertEqual(out['validate_command'],s['validate_command'])
