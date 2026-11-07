"""Compute-only, independently limited service components and reload forwarding."""
import os
import re
import signal
import subprocess


COMPONENTS = ('daemon', 'telegram')


def control_shape(config):
    steps = config.get('component_steps', {})
    pool = config.get('shared_pool', {})
    if not steps.get('enabled'):
        return pool.get('reserve_cpus', 1), pool.get('reserve_mem_mib', 512)
    memory = int(steps.get('supervisor_mem_mib', 512))
    total = memory + sum(int(steps[name]['mem_mib']) for name in COMPONENTS)
    if memory < 128 or any(int(steps[name]['mem_mib']) < 256 for name in COMPONENTS):
        raise ValueError('Service component memory limit is too small')
    if not pool.get('enabled') or pool.get('reserve_cpus', 1) < 3 or pool.get('reserve_mem_mib', 512) < total:
        raise ValueError('Service components exceed the declared infrastructure reservation')
    return 1, memory


def isolated_command(component, command, config, environ):
    if component not in COMPONENTS:
        raise ValueError('Unknown service component')
    steps = config.get('component_steps', {})
    if not steps.get('enabled') or environ.get('CDISPATCH_COMPONENT_STEP') == component:
        return None
    control_shape(config)
    job = environ.get('SLURM_JOB_ID', '')
    if not re.fullmatch(r'\d+', job):
        raise ValueError('Service component requires a compute allocation')
    return ['srun', '--jobid='+job, '--exact', '--exclusive', '--nodes=1',
            '--ntasks=1', '--cpus-per-task=1', '--mem='+str(steps[component]['mem_mib'])+'M',
            '--immediate=20', '--cpu-bind=cores', '--oom-kill-step=1',
            '--job-name=cd-'+component] + command


def run_isolated(component, command):
    child = subprocess.Popen(command, env=dict(os.environ, CDISPATCH_COMPONENT_STEP=component))
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
