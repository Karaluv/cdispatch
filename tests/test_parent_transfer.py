import copy
import unittest
from cdispatch.parent_transfer import transfer, parent_id
from cdispatch.planning import ready_tasks


class TransferTests(unittest.TestCase):
    def tasks(self):
        return {
            'old':dict(id='old', state='RUNNING',spec=dict(workdir='/source',labels={'stage':'controller'})),
            'new':dict(id='new', state='RUNNING',user='owner',spec=dict(workdir='/new',kind='shared',labels={'stage':'controller'},controller_transfer={'predecessor':'old','source':'/source'})),
            'child':dict(id='child',state='RUNNING',spec=dict(workdir='/child',parent='old')),
            'other':dict(id='other',state='RUNNING',spec=dict(workdir='/other',parent='foreign')),
        }

    def test_transfer_preserves_original_spec_and_foreign_tasks(self):
        tasks=self.tasks();before=copy.deepcopy(tasks)
        self.assertEqual(transfer(tasks,'old','new','owner'),['child'])
        self.assertEqual(parent_id(tasks['child']),'new')
        self.assertEqual(tasks['child']['spec'],before['child']['spec'])
        self.assertEqual(tasks['other'],before['other'])
        self.assertEqual(transfer(tasks,'old','new','owner'),[])

    def test_rejection_is_atomic(self):
        for field,value in [('state','QUEUED'),('user','foreign'),('cancel_requested',True)]:
            tasks=self.tasks();tasks['new'][field]=value;before=copy.deepcopy(tasks)
            with self.assertRaises(ValueError):transfer(tasks,'old','new','owner')
            self.assertEqual(tasks,before)

    def test_declared_source_must_match(self):
        tasks=self.tasks();tasks['new']['spec']['controller_transfer']['source']='/wrong'
        with self.assertRaises(ValueError):transfer(tasks,'old','new','owner')

    def test_retiring_children_block_atomic_transfer(self):
        tasks=self.tasks();tasks['child']['cancel_requested']=True;before=copy.deepcopy(tasks)
        with self.assertRaises(ValueError):transfer(tasks,'old','new','owner')
        self.assertEqual(tasks,before)

    def test_queued_child_survives_old_parent_terminal(self):
        tasks=self.tasks();transfer(tasks,'old','new','owner');tasks['old']['state']='CANCELLED'
        tasks['child'].update(state='QUEUED',created=0)
        tasks['child']['spec'].update(depends_on=[],workdir='/child',profiles=[])
        ready,blocked=ready_tasks(tasks)
        self.assertEqual([t['id'] for t in ready],['child'])
        self.assertNotIn('child',blocked)
