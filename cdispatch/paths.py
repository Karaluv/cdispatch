"""Writable runtime paths are independent of the installed package."""
import os
from pathlib import Path


def config_path():
    return Path(os.environ.get('CDISPATCH_CONFIG', str(Path(os.environ.get('XDG_CONFIG_HOME', str(Path.home()/'.config')))/'cdispatch/config.json'))).expanduser()


def state_root():
    return Path(os.environ.get('CDISPATCH_ROOT', str(Path(os.environ.get('XDG_STATE_HOME', str(Path.home()/'.local/state')))/'cdispatch'))).expanduser()


def profile_root():
    return Path(os.environ.get('CDISPATCH_PROFILES', str(Path(os.environ.get('XDG_DATA_HOME', str(Path.home()/'.local/share')))/'cdispatch/profiles'))).expanduser()
