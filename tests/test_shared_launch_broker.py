import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch
from cdispatch.common import atomic
from cdispatch import shared_launch_broker as broker


class LaunchBrokerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.a = self.base/'var/attempts/task/1'
        self.a.mkdir(parents=True)
        self.bundle = dict(id='task', spec=dict(kind='shared'), selection=dict(user='prep'))
        atomic(self.a/'spec.json', self.bundle)
        self.task = dict(state='PENDING', attempt=str(self.a), selection=self.bundle['selection'])
        atomic(self.base/'var/state.json', dict(tasks=dict(task=self.task)))
        self.addCleanup(patch.stopall)
        patch.object(broker, 'state_root', return_value=self.base/'var').start()
        patch.object(broker.getpass, 'getuser', return_value='prep').start()

    def test_only_current_admitted_attempt_launches(self):
        with patch.object(broker.subprocess, 'Popen') as start:
            self.assertIn('started', broker.execute(['shared-launch', str(self.a)]))
            start.assert_called_once()

    def test_no_shell_foreign_path_or_unadmitted_replay(self):
        with patch.object(broker.subprocess, 'Popen') as start:
            for argv in [['sh', '-c', 'anything'], ['shared-launch', '/tmp/unmanaged']]:
                with self.assertRaises(ValueError): broker.execute(argv)
            self.task['state']='FAILED'
            atomic(self.base/'var/state.json', dict(tasks=dict(task=self.task)))
            with self.assertRaises(ValueError): broker.execute(['shared-launch', str(self.a)])
            start.assert_not_called()

    def test_existing_launch_or_cancel_never_replays(self):
        with patch.object(broker.subprocess, 'Popen') as start:
            atomic(self.a/'launch-intent.json', dict(time=1))
            self.assertIn('refusing replay', broker.execute(['shared-launch', str(self.a)]))
            start.assert_not_called()


if __name__ == '__main__': unittest.main()
