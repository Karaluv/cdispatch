"""Restricted login-host transport for already admitted shared steps."""
import getpass
import json
import os
from pathlib import Path
import subprocess
import sys

from .paths import state_root
from .common import read
from .broker import inside

BASE = Path(__file__).resolve().parent.parent


def execute(argv):
    if not isinstance(argv, list) or len(argv) != 2 or argv[0] != 'shared-launch':
        raise ValueError('Only an admitted shared-launch attempt is allowed')
    a = Path(argv[1]).resolve()
    root = state_root()
    if not inside(a, root/'attempts'):
        raise ValueError('Attempt outside dispatcher')
    bundle = read(a/'spec.json')
    task = read(root/'state.json')['tasks'].get(bundle['id'], {})
    if (bundle['spec']['kind'] != 'shared' or bundle['spec'].get('interactive')
            or bundle['selection']['user'] != getpass.getuser()
            or task.get('state') not in ('PENDING', 'RUNNING')
            or task.get('attempt') != str(a)
            or task.get('selection') != bundle['selection']):
        raise ValueError('Not this owner\'s currently admitted shared attempt')
    if any((a/n).exists() for n in ('cancel.json', 'started.json', 'driver.json', 'launch-intent.json')):
        return 'Already launched or stopped; refusing replay'
    with (a/'launcher.log').open('ab') as log:
        subprocess.Popen([sys.executable, '-m', 'cdispatch.shared_launcher', str(a)],
                         cwd=BASE, env=dict(os.environ, PYTHONPATH=str(BASE)),
                         stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                         start_new_session=True)
    return 'Managed shared launcher started'


def main():
    try:
        print(execute(json.loads(os.environ.get('SSH_ORIGINAL_COMMAND', ''))))
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
