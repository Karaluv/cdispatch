"""Terminal views attached to the service allocation, never a login-node loop."""
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time

from .common import read
from .monitor import render
from .monitor_cache import read_monitor_snapshot

VIEWS=('overview','queue','nodes','users','limits','decisions','events')


def attach(root, config, arguments):
    data=read_monitor_snapshot(root,{})
    if time.time()-data.get('heartbeat',0)>90:raise RuntimeError('Service heartbeat stale; run service-start or wait for watchdog')
    services=[j for j in (data.get('snapshot') or {}).get('jobs',[]) if j.get('JobName')=='cd-service' and j.get('JobState')=='RUNNING' and j.get('UserId','').split('(')[0]==config['owner']]
    if len(services)!=1:raise RuntimeError('No unique running cd-service allocation')
    entry=[sys.executable,'-m','cdispatch.cli']
    cmd=['srun','--jobid='+services[0]['JobId'],'--overlap','--exact','--nodes=1','--ntasks=1','--cpus-per-task=1','--mem=256M']
    if sys.stdin.isatty() and sys.stdout.isatty():cmd.append('--pty')
    return subprocess.call(cmd+entry+['--root',str(root)]+arguments)


def watch(root, view='overview', interval=3, once=False):
    if interval<2:raise ValueError('Watch interval must be at least 2 seconds')
    while True:
        data=read_monitor_snapshot(root,{})
        text=render(data,'status' if view=='overview' else view)
        if view=='overview':text+='\n\n'+render(data,'queue')
        width,height=shutil.get_terminal_size((120,40))
        if once:print(text);return 0
        title=' CDISPATCH | '+view.upper()+' | '+time.strftime('%H:%M:%S')+' | cache only | Ctrl-C: exit '
        print('\033[2J\033[H\033[1;36m'+title[:width]+'\033[0m')
        lines=text.splitlines()
        for line in lines[:max(1,height-4)]:
            color='\033[31m' if any(x in line for x in ('FAILED','DOWN','DRAIN','Ошибка','устарел')) else '\033[32m' if 'RUNNING' in line else ''
            print(color+line[:width]+('\033[0m' if color else ''))
        if len(lines)>height-4:print('… Полные детали: cdispatch '+('show ID' if view in ('overview','queue','decisions') else 'status'))
        sys.stdout.flush();time.sleep(interval)


def panels(root, detach=False):
    tmux=shutil.which('tmux')
    if not tmux:raise RuntimeError('tmux is required on the compute node')
    if not os.environ.get('SLURM_JOB_ID'):raise RuntimeError('Panels must run inside a compute allocation')
    session='cdispatch-'+os.environ['SLURM_JOB_ID']
    base=[sys.executable,'-m','cdispatch.cli']
    def command(view):return shlex.join(base+['--root',str(root),'watch','--view',view])
    existing=subprocess.run([tmux,'has-session','-t',session],capture_output=True).returncode==0
    if not existing:
        size=shutil.get_terminal_size((140,48))
        subprocess.run([tmux,'new-session','-d','-x',str(size.columns),'-y',str(size.lines),'-s',session,'-n','queue',command('overview')],check=True)
        subprocess.run([tmux,'split-window','-v','-t',session+':queue',command('queue')],check=True)
        for window,left,right in [('resources','nodes','users'),('decisions','decisions','events')]:
            subprocess.run([tmux,'new-window','-t',session,'-n',window,command(left)],check=True)
            subprocess.run([tmux,'split-window','-h','-t',session+':'+window,command(right)],check=True)
        subprocess.run([tmux,'select-window','-t',session+':queue'],check=True)
    if subprocess.run([tmux,'has-session','-t',session+':limits'],capture_output=True).returncode:
        subprocess.run([tmux,'new-window','-d','-t',session,'-n','limits',command('limits')],check=True)
    if detach:print(session);return 0
    operation='switch-client' if os.environ.get('TMUX') else 'attach-session'
    return subprocess.call([tmux,operation,'-t',session])
