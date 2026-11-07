"""Forced SSH command: only this dispatcher's Slurm allocations, no shell."""
import getpass
import json
import os
from pathlib import Path
import re
import sys
import subprocess
from .paths import state_root
from .common import fields,read,run

BASE=Path(__file__).resolve().parent.parent

def inside(value,root):
    try:Path(value).resolve().relative_to(root.resolve());return True
    except ValueError:return False

def execute(argv):
    if not isinstance(argv,list) or not argv or any(not isinstance(x,str) for x in argv):raise ValueError('Expected argv')
    if argv==['probe']:return getpass.getuser()
    var=state_root();attempts=var/'attempts'
    if argv[0]=='access' and len(argv)==3:
        a=Path(argv[1]);work=Path(argv[2])
        if not inside(a,attempts):raise ValueError('Attempt outside dispatcher')
        if not os.access(a,os.R_OK|os.W_OK|os.X_OK) or not os.access(work,os.R_OK|os.W_OK|os.X_OK):raise ValueError('Missing delegated filesystem access')
        return 'OK'
    if argv[0]=='sbatch':
        if not inside(argv[-1],attempts) or Path(argv[-1]).name!='job.sh':raise ValueError('Unmanaged script')
        if any(not a.startswith('--') for a in argv[1:-1]):raise ValueError('Unexpected sbatch positional argument')
        allowed={'parsable','test-only','hold','job-name','account','partition','nodes','ntasks-per-node','cpus-per-task','mem','time','nodelist','no-requeue','signal','chdir','output','error','qos','gres','hint'}
        for a in argv[1:-1]:
            key=a[2:].split('=',1)[0]
            if key not in allowed:raise ValueError('Unmanaged option '+key)
            if key=='hint' and a.split('=',1)[-1] not in ('nomultithread','multithread','compute_bound','memory_bound'):
                raise ValueError('Unsupported Slurm hint')
            if key in ('chdir','output','error') and not inside(a.split('=',1)[1],attempts):raise ValueError('Unmanaged output path')
        bundle=read(Path(argv[-1]).parent/'spec.json')
        if bundle['selection']['user']!=getpass.getuser():raise ValueError('Wrong dispatch user')
        if '--test-only' in argv:
            result=subprocess.run(argv,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=40)
            return json.dumps(dict(returncode=result.returncode,stdout=result.stdout,stderr=result.stderr))
        return run(argv,timeout=40)
    if argv[0]=='scancel' and 2<=len(argv)<=4:
        job=argv[-1]
        if not re.fullmatch(r'\d+',job):raise ValueError('Invalid job ID')
        if argv[1:-1] not in ([],['--signal=USR1','--batch']):raise ValueError('Unsupported signal')
        info=fields(run(['scontrol','show','job',job,'-o']))
        if info.get('UserId','').split('(')[0]!=getpass.getuser() or not info.get('JobName','').startswith('cd-') or not inside(info.get('Command','/'),attempts):raise ValueError('Unmanaged job')
        return run(argv)
    raise ValueError('Only dispatcher sbatch, scancel, access and probe are allowed')

def main():
    try:print(execute(json.loads(os.environ.get('SSH_ORIGINAL_COMMAND',''))))
    except Exception as e:print(str(e),file=sys.stderr);return 2
    return 0

if __name__=='__main__':sys.exit(main())
