import contextlib
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from cdispatch.cli import main
from cdispatch.paths import config_path, state_root, profile_root

class ConfigurationTests(unittest.TestCase):
    def test_init_and_check_are_offline_and_never_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {}, clear=True), contextlib.redirect_stdout(io.StringIO()):
            target = Path(tmp)/'config.json'
            argv = ['--config',str(target),'init','--owner','alice','--account','lab','--partition','cpu']
            with patch('cdispatch.slurm.run', side_effect=AssertionError('Slurm must not run')):
                self.assertEqual(main(argv),0)
                self.assertEqual(main(['--config',str(target),'check-config']),0)
                before=target.read_bytes()
                with self.assertRaises(FileExistsError): main(argv)
                self.assertEqual(target.read_bytes(),before)

    def test_explicit_paths_are_independent_of_package_location(self):
        with patch.dict(os.environ, {'CDISPATCH_CONFIG':'/site/config.json','CDISPATCH_ROOT':'/shared/state','CDISPATCH_PROFILES':'/shared/profiles'}):
            self.assertEqual(config_path(),Path('/site/config.json'))
            self.assertEqual(state_root(),Path('/shared/state'))
            self.assertEqual(profile_root(),Path('/shared/profiles'))

    def test_profile_library_cli_needs_no_site_config(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            self.assertIsNone(main(['--config',str(Path(tmp)/'absent.json'),
                'profiles','--library',tmp,'validate']))
