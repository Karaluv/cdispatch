"""Move a daemon out of an undersized existing control step, within its job."""
import os
import re
import signal
import subprocess
import sys


def isolated_command(root, config_path, config, environ):
    settings = config.get('daemon_step', {})
    job = environ.get('SLURM_JOB_ID', '')
    if environ.get('CDISPATCH_DAEMON_STEP') == '1' or settings.get('job_id') != job:
        return None
    if not re.fullmatch(r'\d+', job):
        raise ValueError('Daemon step requires an exact existing allocation')
    memory = int(settings['mem_mib'])
    pool = config['shared_pool']
    if memory < 512 or pool.get('reserve_cpus', 1) < 2 or memory + 512 > pool.get('reserve_mem_mib', 512):
        raise ValueError('Isolated daemon must fit the declared control reservation')
    return ['srun', '--jobid='+job, '--exact', '--exclusive', '--nodes=1',
            '--ntasks=1', '--cpus-per-task=1', '--mem='+str(memory)+'M',
            '--immediate=20', '--cpu-bind=cores', '--oom-kill-step=1',
            '--job-name=cd-daemon', sys.executable, '-m', 'cdispatch.cli',
            '--root', str(root), '--config', str(config_path), 'daemon']


def run_isolated(command):
    env = dict(os.environ, CDISPATCH_DAEMON_STEP='1')
    child = subprocess.Popen(command, env=env)
    # The existing supervisor continues to own this launcher. Reload and stop
    # end its exact srun step; the supervisor then starts one replacement.
    previous = {}
    def stop(*_):
        if child.poll() is None:
            child.terminate()
    try:
        for sig in (signal.SIGUSR1, signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, stop)
        return child.wait()
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
