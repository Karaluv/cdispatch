import copy
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch
from cdispatch import plugins
from cdispatch.common import atomic, enqueue, read
from cdispatch.daemon import Daemon
from cdispatch.scheduler import choose
from test_dispatcher import config, spec, snapshot, Fake


class PluginTests(unittest.TestCase):
    def setUp(self):
        self.module = types.ModuleType('example_extension')
        self.module.API_VERSION = 1
        self.cfg = config()
        self.cfg['plugins'] = [{'module': self.module.__name__, 'options': {'name': 'custom'}}]
        self.patcher = patch.dict(sys.modules, {self.module.__name__: self.module})
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def test_no_plugins_needs_no_extension_import(self):
        with patch('cdispatch.plugins.importlib.import_module', side_effect=AssertionError('unexpected import')):
            self.assertIsNotNone(choose(spec(), snapshot(), config())[0])

    def test_transformed_shape_is_used_without_mutating_request(self):
        def prepare(task, options):
            task['profiles'][0]['name'] = options['name']
            task['profiles'][0]['ranks_per_node'] = 8
            return task
        self.module.prepare_spec = prepare
        task = spec(); original = copy.deepcopy(task)
        selected, _ = choose(task, snapshot(), self.cfg)
        self.assertEqual(selected['profile']['name'], 'custom')
        self.assertEqual(selected['profile']['ranks_per_node'], 8)
        self.assertEqual(task, original)

    def test_bad_plugin_configuration_fails_before_slurm(self):
        for entries in [[{'module': 'missing_extension'}], [{'module': '../extension'}],
                        [{'module': self.module.__name__}] * 2]:
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as tmp:
                backend = Fake(); self.module.API_VERSION = 99
                with self.assertRaises((ValueError, ImportError)):
                    Daemon(Path(tmp), dict(self.cfg, plugins=entries), backend)
                self.assertEqual(backend.calls, [])

    def test_invalid_or_nonresource_rewrite_is_rejected(self):
        for key, value in [('priority', 999), ('validate_command', ['/bin/true'])]:
            task = spec(validate_command=['/original-validator'])
            def change(result, options):
                result[key] = value
                return result
            self.module.prepare_spec = change
            with self.assertRaisesRegex(ValueError, 'non-resource'):
                plugins.prepare_spec(task, self.cfg)
        self.module.prepare_spec = lambda task, options: dict(task, profiles=[])
        with self.assertRaises(ValueError):
            plugins.prepare_spec(spec(), self.cfg)

    def test_expected_failure_never_suppresses_infrastructure_errors(self):
        self.module.expected_failure = lambda *args: True
        for raw in ['OUT_OF_MEMORY', 'NODE_FAIL', 'TIMEOUT', 'BOOT_FAIL']:
            self.assertFalse(plugins.expected_failure({}, raw, '1', self.cfg))
        self.assertTrue(plugins.expected_failure({}, 'FAILED', '1', self.cfg))

    def test_displacement_requires_explicit_plugin_and_boolean_result(self):
        self.assertFalse(plugins.allow_displacement({}, config()))
        self.module.allow_displacement = lambda *args: 'yes'
        with self.assertRaises(ValueError): plugins.allow_displacement({}, self.cfg)
        self.module.allow_displacement = lambda *args: True
        self.assertTrue(plugins.allow_displacement({}, self.cfg))

    def test_broken_display_hook_preserves_authoritative_status(self):
        def broken(*args): raise RuntimeError('display failure')
        self.module.display_labels = broken
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); daemon = Daemon(root, self.cfg, Fake())
            enqueue(root, spec(workdir=tmp), 'test'); daemon.tick()
            self.assertIsNone(read(root/'status.json')['error'])
            self.assertTrue((root/'monitor-error.json').exists())
            self.assertEqual(daemon.state['tasks']['test']['state'], 'PENDING')

    def test_monitor_labels_do_not_mutate_scheduler_state(self):
        self.module.display_labels = lambda *args: {'display_name': 'Custom'}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); daemon = Daemon(root, self.cfg, Fake())
            enqueue(root, spec(workdir=tmp), 'test'); daemon.tick()
            self.assertEqual(read(root/'monitor.json')['tasks']['test']['spec']['labels']['display_name'], 'Custom')
            self.assertNotIn('display_name', read(root/'state.json')['tasks']['test']['spec']['labels'])
            self.assertNotIn('display_name', daemon.state['tasks']['test']['spec']['labels'])
