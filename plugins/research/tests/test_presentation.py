import unittest
from cdispatch_research.presentation import label

class ResearchPresentationTests(unittest.TestCase):
    def test_readable_worker_name(self):
        task={'spec':{'name':'o56-excited','labels':{'direction':'torsion_minus','stage':'excited'},'profiles':[{'command':['/run/point_005_00135_excited.request.json']}]}}
        self.assertEqual(label(task),'O56 · торсия − · точка 5 · excited')
    def test_ground_bends_retain_direction_names(self):
        for direction,title in [('bend_plus','изгиб +'),('bend_minus','изгиб −')]:
            task={'spec':{'name':'o56-ground','labels':{'direction':direction,'stage':'ground'}}}
            self.assertEqual(label(task),'O56 · '+title+' · ground')
