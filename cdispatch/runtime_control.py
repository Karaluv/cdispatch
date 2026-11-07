"""Reload one supervised component; never cancel its parent allocation."""
import argparse
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from .common import read


def reload_component(root, component, local_only=False):
    if component not in ('daemon', 'bot'):
        raise ValueError('Reload component must be daemon or bot')
    root = Path(root)
    status = read(root/'service-status.json', {})
    if time.time()-status.get('time', 0) > 90:
        raise ValueError('Service supervisor heartbeat is stale')
    local = socket.gethostname().split('.')[0]
    if local != status['host'].split('.')[0]:
        if local_only:
            raise ValueError('Component reload reached a different service host')
        command = ['srun', '--jobid='+status['job_id'], '--overlap', '--exact',
                   '--nodes=1', '--ntasks=1', '--cpus-per-task=1', '--mem=64M',
                   '--time=1', '--immediate=10', sys.executable, '-m',
                   'cdispatch.runtime_control', '--root', str(root),
                   '--component', component, '--local']
        subprocess.run(command, check=True, timeout=60)
        return
    pid = status.get('controller_pid' if component == 'daemon' else 'telegram_pid')
    if not isinstance(pid, int) or pid <= 1:
        raise ValueError('Component is not running')
    proc = Path('/proc')/str(pid)
    if proc.stat().st_uid != os.getuid():
        raise ValueError('Component belongs to another user')
    command = (proc/'cmdline').read_bytes().replace(b'\0', b' ').decode()
    cgroup = (proc/'cgroup').read_text()
    expected = 'cdispatch.cli' if component == 'daemon' else 'cdispatch.telegram'
    if expected not in command or '/job_'+status['job_id']+'/' not in cgroup:
        raise ValueError('Component process identity does not match the service')
    os.kill(pid, signal.SIGUSR1)
    print('Requested '+component+' reload in service '+status['job_id'])


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--component', choices=('daemon', 'bot'), required=True)
    parser.add_argument('--local', action='store_true')
    args=parser.parse_args()
    reload_component(args.root, args.component, args.local)
