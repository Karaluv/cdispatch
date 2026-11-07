"""One cgroup-limited Slurm step, with an independent application result."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from .common import atomic,read


def main(attempt):
    a=Path(attempt);bundle=read(a/'spec.json');spec=bundle['spec'];p=bundle['selection']['profile']
    if str(os.environ.get('SLURM_JOB_ID'))!=bundle['selection']['pool_job_id'] or not os.environ.get('SLURM_STEP_ID','').isdigit():raise RuntimeError('Shared worker requires its own numeric Slurm step')
    if (a/'cancel.json').exists():atomic(a/'result.json',dict(state='CANCELLED',reason='cancelled before application start'));return 143
    if not spec.get('interactive'):
        from .worker import main as worker
        return worker(a)
    env=dict(os.environ,DISPATCH_TASK_ID=bundle['id'],DISPATCH_ATTEMPT=str(a),CDISPATCH_ROOT=bundle['root'],
             OMP_NUM_THREADS=str(p['threads']),DISPATCH_CPUS=str(p['threads']),DISPATCH_MEM_MIB=str(bundle['selection']['mem_mib']),DISPATCH_PROFILE=p['name'])
    env.update(p.get('env',{}))
    atomic(a/'started.json',dict(time=time.time(),job_id=os.environ['SLURM_JOB_ID'],step_id=os.environ['SLURM_STEP_ID'],nodes=os.environ.get('SLURM_JOB_NODELIST')))
    proc=None;stopping=False
    def stop(*_):
        nonlocal stopping
        stopping=True
        if proc and proc.poll() is None:proc.terminate()
    for sig in (signal.SIGTERM,signal.SIGINT,signal.SIGHUP):signal.signal(sig,stop)
    try:
        proc=subprocess.Popen(spec['interactive_command'],cwd=spec['workdir'],env=env)
        while proc.poll() is None:
            lease=read(a/'client.json',{})
            if (a/'cancel.json').exists() or time.time()-lease.get('time',bundle['created'])>180:stop()
            time.sleep(.5)
        state='CANCELLED' if stopping else 'SUCCEEDED' if proc.returncode==0 else 'FAILED'
        atomic(a/'result.json',dict(state=state,returncode=proc.returncode,reason='interactive command exited',finished=time.time()))
        return proc.returncode
    finally:
        if proc and proc.poll() is None:proc.terminate()


if __name__=='__main__':raise SystemExit(main(sys.argv[1]))
