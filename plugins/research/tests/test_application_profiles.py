import copy,unittest
from unittest.mock import patch
from cdispatch_research.application_profiles import apply
class Bindings(unittest.TestCase):
 def setUp(self):
  self.spec=dict(kind='batch',labels={'project':'glass','direction':'bond_si11'},profiles=[dict(name='old',partition='AMD9554',ranks_per_node=16,threads=4,mem_mib=131072,command=['worker','immutable.json','digest'],env={'SEED':'kept'},allowed_nodes=['old'])],validate_command=['gate','immutable.json'],parent='parent',fair_share={'weight':1})
  self.config={'application_profile_rules':[dict(name='intel32-bonds',authorization='User explicit',labels={'direction':'bond_si11'},profiles=[dict(id='reviewed',sha256='hash')])]}
 def test_unmatched_and_shared_untouched(self):
  for spec in [dict(self.spec,labels={'direction':'torsion_minus'}),dict(self.spec,kind='shared')]:self.assertEqual(apply(spec,self.config),spec)
 def test_shape_changes_without_changing_science(self):
  original=copy.deepcopy(self.spec)
  with patch('cdispatch_research.application_profiles.reviewed_profile',return_value=dict(name='intel32',partition='IXG6154-AI-common',ranks_per_node=16,threads=2,mem_mib=131072,env={'OMP':'2'},command=['unused'])):
   result=apply(self.spec,self.config)
  self.assertEqual(self.spec,original);self.assertEqual(result['profiles'][0]['command'],original['profiles'][0]['command']);self.assertNotIn('allowed_nodes',result['profiles'][0]);self.assertEqual(result['profiles'][0]['threads'],2)
  for key in ['validate_command','parent','fair_share']:self.assertEqual(result[key],original[key])
  self.assertEqual(result['profiles'][0]['env'],{'SEED':'kept','OMP':'2'})
 def test_delegation_only_blocks_old_parent_future_admission(self):
  config=dict(self.config,application_lane_owners=[dict(lane='bond_si11',previous_parent='parent',owner='new-controller')])
  self.assertEqual(apply(self.spec,config)['profiles'],[])
  with patch('cdispatch_research.application_profiles.reviewed_profile',return_value=dict(name='intel32',partition='IXG6154-AI-common')):
   self.assertTrue(apply(dict(self.spec,parent='new-parent'),config)['profiles'])
 def test_reviewed_intel_preference_allows_amd_fallback_without_science_change(self):
  rule=self.config['application_profile_rules'][0]
  rule['profiles'].append(dict(id='amd-reviewed',sha256='amd-hash'))
  rule['preferred_partitions']=['IXG6154-AI-common']
  shapes=[dict(name='intel32',partition='IXG6154-AI-common',ranks_per_node=16,threads=2),
          dict(name='amd32',partition='AMD9554',ranks_per_node=8,threads=4)]
  original=copy.deepcopy(self.spec)
  with patch('cdispatch_research.application_profiles.reviewed_profile',side_effect=shapes):
   result=apply(self.spec,self.config)
  self.assertEqual(result['placement']['preferred_partitions'],['IXG6154-AI-common'])
  self.assertTrue(result['placement']['allow_fallback'])
  self.assertEqual([p['partition'] for p in result['profiles']],['IXG6154-AI-common','AMD9554'])
  self.assertEqual(self.spec,original)
  for p in result['profiles']:self.assertEqual(p['command'],original['profiles'][0]['command'])
  self.assertEqual(result['validate_command'],original['validate_command'])
 def test_empty_and_conflicting_bindings_rejected(self):
  for config in [dict(application_profile_rules=self.config['application_profile_rules']*2),dict(application_profile_rules=[dict(self.config['application_profile_rules'][0],profiles=[])] )]:
   with self.assertRaises(ValueError):apply(self.spec,config)
if __name__=='__main__':unittest.main()
