"""Runs inside a Slurm allocation. Never submits another Slurm allocation."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from .common import atomic, read, sha

pause_requested=False
stop_requested=False

def receive(signum,frame):
    global pause_requested,stop_requested
    if signum==signal.SIGUSR1: pause_requested=True
    else: stop_requested=True

def hook(argv,env,cwd,log,timeout=120):
    try:
        return subprocess.run(argv,env=env,cwd=cwd,stdout=log,stderr=log,timeout=timeout).returncode==0
    except (OSError,subprocess.TimeoutExpired) as e:
        log.write(('Hook failed: '+str(e)+'\n').encode());log.flush();return False

def terminate(proc,grace=30):
    if proc.poll() is None:
        os.killpg(proc.pid,signal.SIGTERM)
        try: proc.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid,signal.SIGKILL);proc.wait()

def main(attempt):
    global pause_requested
    a=Path(attempt);bundle=read(a/'spec.json');spec=bundle['spec'];p=bundle['selection']['profile']
    env=os.environ.copy();env.update(p.get('env',{}))
    env.update(DISPATCH_ATTEMPT=str(a),DISPATCH_TASK_ID=bundle['id'],DISPATCH_PROFILE=p['name'],
               DISPATCH_CPUS=str(p['nodes']*p['ranks_per_node']*p['threads']),
               DISPATCH_RANKS=str(p['nodes']*p['ranks_per_node']))
    pin=p.get('pinning',{})
    env['OMP_NUM_THREADS']=str(pin.get('application_threads',p['threads']))
    if pin.get('mpi') in ('intel-core','intel-omp'):
        env.update(I_MPI_PIN='1',I_MPI_PIN_DOMAIN='core' if pin['mpi']=='intel-core' else 'omp',I_MPI_PIN_ORDER='compact')
    elif pin.get('mpi')=='none':env['I_MPI_PIN']='0'
    if pin.get('omp_proc_bind'):env['OMP_PROC_BIND']=pin['omp_proc_bind']
    if pin.get('omp_places'):env['OMP_PLACES']=pin['omp_places']
    if pin.get('mkl_threads'):env['MKL_NUM_THREADS']=str(pin['mkl_threads'])
    if bundle.get('root'):env['CDISPATCH_ROOT']=bundle['root']
    signal.signal(signal.SIGUSR1,receive);signal.signal(signal.SIGTERM,receive);signal.signal(signal.SIGINT,receive)
    atomic(a/'started.json',dict(time=time.time(),job_id=os.environ.get('SLURM_JOB_ID'),step_id=os.environ.get('SLURM_STEP_ID'),nodes=os.environ.get('SLURM_JOB_NODELIST')))
    if spec['kind']=='interactive':
        while not stop_requested:
            heartbeat=read(a/'client.json',{})
            if heartbeat.get('finished'): break
            if time.time()-heartbeat.get('time',bundle['created'])>spec.get('attach_timeout',180):
                atomic(a/'result.json',dict(state='FAILED',reason='interactive client lease expired'));return 1
            time.sleep(1)
        atomic(a/'result.json',dict(state=('SUCCEEDED' if heartbeat.get('exit_code',0)==0 else 'FAILED') if not stop_requested else 'CANCELLED'))
        return 0
    proc=None
    with (a/'application.log').open('ab',buffering=0) as log:
        try:
            argv=p.get('command',spec.get('command'))
            checkpoint=bundle.get('checkpoint')
            if checkpoint:
                # Resume only immutable, verified complete checkpoint files.
                if any(not Path(f).is_file() or sha(f)!=h for f,h in checkpoint['files'].items()):
                    raise RuntimeError('Checkpoint changed or missing')
                if not hook(spec['checkpoint_validate_command'],env,spec['workdir'],log): raise RuntimeError('Restart validation failed')
                env['DISPATCH_RESUME']='1';argv=p.get('resume_command',spec['resume_command'])
            proc=subprocess.Popen(argv,cwd=spec['workdir'],env=env,stdout=log,stderr=log,start_new_session=True)
            env['DISPATCH_CHILD_PID']=str(proc.pid)
            while proc.poll() is None:
                if stop_requested:
                    terminate(proc);atomic(a/'result.json',dict(state='CANCELLED',reason='TERM'));return 143
                if pause_requested:
                    pause_requested=False
                    if spec.get('preemptible'):
                        ok=hook(spec['checkpoint_command'],env,spec['workdir'],log,spec.get('checkpoint_timeout',120))
                        ok=ok and hook(spec['checkpoint_validate_command'],env,spec['workdir'],log)
                        if ok:
                            terminate(proc,spec.get('stop_grace',30))
                            if not hook(spec['checkpoint_validate_command'],env,spec['workdir'],log):
                                raise RuntimeError('Checkpoint invalid after stop')
                            files={f:sha(f) for f in spec['checkpoint_files']}
                            cp=dict(files=files,time=time.time(),attempt=str(a))
                            atomic(a/'checkpoint.json',cp)
                            atomic(a/'result.json',dict(state='PAUSED',checkpoint=cp));return 75
                        atomic(a/'pause-rejected.json',dict(time=time.time(),reason='checkpoint hook/validation failed'))
                time.sleep(0.5)
            code=proc.returncode
            state='FAILED' if code else 'FINISHED_UNVALIDATED'
            if code==0 and spec.get('validate_command'):
                state='SUCCEEDED' if hook(spec['validate_command'],env,spec['workdir'],log) else 'FAILED'
            if code==0 and state!='FAILED' and spec.get('artifact_contract'):
                from .artifacts import check
                atomic(a/'artifact-validation.json',dict(files=check(spec['artifact_contract']),time=time.time()))
                state='SUCCEEDED'
            atomic(a/'result.json',dict(state=state,returncode=code,reason='application/validation',finished=time.time()))
            return 0 if state in ('SUCCEEDED','FINISHED_UNVALIDATED') else 1
        except Exception as e:
            if proc is not None: terminate(proc)
            atomic(a/'result.json',dict(state='FAILED',reason=str(e),finished=time.time()))
            return 1

if __name__=='__main__': sys.exit(main(sys.argv[1]))
