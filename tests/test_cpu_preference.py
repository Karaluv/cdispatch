import copy,unittest
from unittest.mock import patch
from test_dispatcher import config,spec,node,snapshot
from cdispatch.scheduler import choose,shape,selection_score
from cdispatch.common import validate
class CpuPreferenceTests(unittest.TestCase):
 def request(self,order=True):
  profiles=[dict(name='cpu'+str(n),partition='cpu',nodes=1,ranks_per_node=n//4,threads=4,mem_mib=64000,wall_minutes=10) for n in [32,64,128]]
  return spec(profiles=profiles,placement={'preferred_cpu_counts':[128,64,32]} if order else {})
 def test_highest_available_shape_then_fallback_and_minimum(self):
  for free,expected in [(128,128),(96,64),(64,64),(63,32),(32,32),(31,None)]:
   snap=snapshot();snap['nodes']=[node(cpus=free)];sel,_=choose(self.request(),snap,config());self.assertEqual(shape(sel['profile'])['cpus'] if sel else None,expected)
 def test_larger_shape_precedes_cheap_small_host(self):
  snap=snapshot();snap['nodes']=[node(name='cheap',cpus=32),node(name='expensive',cpus=128)];c=config();c['node_cost']={'cheap':0,'expensive':1000}
  sel,_=choose(self.request(),snap,c);self.assertEqual(shape(sel['profile'])['cpus'],128);self.assertEqual(sel['nodes'],['expensive'])
  legacy,_=choose(self.request(False),snap,c);self.assertEqual(legacy['nodes'],['cheap'])
 def test_preference_does_not_override_bad_node_or_quota(self):
  snap=snapshot();snap['nodes']=[node(name='large',cpus=128,state='IDLE+DRAIN'),node(name='small',cpus=32)];sel,_=choose(self.request(),snap,config());self.assertEqual(shape(sel['profile'])['cpus'],32)
  c=config();c['users']['alice']['limits']['cpus']=16;self.assertIsNone(choose(self.request(),snap,c)[0])
 def test_input_validation(self):
  for value in [[],[128,128],[0,32],[True,64],['128',64],None]:
   s=self.request();s['placement']['preferred_cpu_counts']=value
   with self.assertRaises(ValueError):validate(s)
if __name__=='__main__':unittest.main()
