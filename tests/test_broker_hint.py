import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch
from cdispatch import broker
from cdispatch.common import atomic


class BrokerHintTests(unittest.TestCase):
    def test_managed_cpu_pinning_reaches_sbatch_but_unknown_hint_does_not(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            attempt = root / 'var/attempts/probe/1'
            attempt.mkdir(parents=True)
            atomic(attempt / 'spec.json', {'selection': {'user': 'student'}})
            argv = ['sbatch', '--parsable', '--hint=nomultithread', str(attempt / 'job.sh')]
            with patch.object(broker, 'state_root', return_value=root/'var'), patch.object(broker.getpass, 'getuser', return_value='student'), patch.object(broker, 'run', return_value='123') as run:
                self.assertEqual(broker.execute(argv), '123')
                run.assert_called_once_with(argv, timeout=40)
                run.reset_mock()
                with self.assertRaisesRegex(ValueError, 'Unsupported Slurm hint'):
                    broker.execute([*argv[:2], '--hint=unmanaged', argv[-1]])
                run.assert_not_called()
