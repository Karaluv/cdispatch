import io
import json
from pathlib import Path
import tempfile
import unittest

from cdispatch.monitor_cache import JSONStream, read_scheduler_state


class SchedulerStateCacheTests(unittest.TestCase):
    def test_full_state_preserved_across_small_chunks(self):
        task = dict(id='worker', state='FAILED', submitted_spec={'command': ['cmd', '$literal']},
                    spec={'parent': 'controller', 'workdir': '/tmp/science',
                          'profiles': [{'env': {'HORIZON': '12345'}, 'command': ['run']}],
                          'artifact_contract': {'extra': ['immutable']}},
                    attempts=[{'number': 1, 'selection': {'profile': {'command': ['old']}}}],
                    slurm={'raw': 'PREEMPTED'}, accounting={'exit_code': '0:15'})
        state = dict(tasks={'worker': task}, paused=False, last_submit=12345.678,
                     events=[{'reason': 'русский текст'}], circuit=None,
                     pending_cancellations={'other': 'explicit'}, custom={'nested': [1, True, None]})
        encoded = json.dumps(state, ensure_ascii=False)
        for chunk_size in (1, 7, 64, 65536):
            with self.subTest(chunk_size=chunk_size):
                stream = JSONStream(io.StringIO(encoded), chunk_size=chunk_size)
                self.assertEqual(stream.root(task_transform=None), state)

    def test_file_default_and_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            default = {'tasks': {}}
            self.assertIs(read_scheduler_state(path, default), default)
            path.write_text('{"tasks": {}} trailing')
            with self.assertRaises(ValueError):
                read_scheduler_state(path, default)

    def test_no_monitor_projection_for_full_state(self):
        state = {'tasks': {'x': {'submitted_spec': {'full': True},
                                'spec': {'command': ['keep'], 'resource_key': 'keep'}}}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            path.write_text(json.dumps(state))
            self.assertEqual(read_scheduler_state(path), state)


if __name__ == '__main__':
    unittest.main()
