"""Bounded CPU handlers inside the service allocation; no per-handler sbatch."""
import copy
import json
from datetime import datetime
import math
from pathlib import Path
import re
import subprocess
import sys
import time

from .common import ACTIVE,atomic,read,tres,mib,lock


def envelope(root,config):
    cfg=config.get('shared_pool',{})
    desired=read(Path(root)/'pool-size.json',{})
    cpus=int(desired.get('cpus',cfg.get('cpus',1)));mem=int(desired.get('mem_mib',cfg.get('mem_mib',512)))
    if not cfg.get('enabled'):return 1,512
    if not cfg.get('reserve_cpus',1)<cpus<=cfg.get('max_cpus',8) or not cfg.get('reserve_mem_mib',512)<mem<=cfg.get('max_mem_mib',16384):raise ValueError('Shared pool size outside configured bounds')
    return cpus,mem


def pool_snapshot(root,config,snapshot,tasks):
    cfg=config.get('shared_pool',{})
    if not cfg.get('enabled'):return
    job_id=config.get('service_job_id')
    handoff=config.get('shared_allocation_handoff',{})
    if handoff and job_id==handoff.get('predecessor'):
        target=handoff.get('target','')
        if not re.fullmatch(r'\d+',target):raise ValueError('Invalid shared allocation handoff target')
        job_id=target
    job=next((j for j in snapshot['jobs'] if j.get('JobId')==job_id and j.get('JobState')=='RUNNING'),None)
    if not job:return
    tr=tres(job.get('AllocTRES',''));cpus=int(tr.get('cpu',job.get('NumCPUs',0)));mem=int(mib(tr.get('mem','0')))
    active=[t for t in tasks.values() if t['state'] in ACTIVE and t.get('selection',{}).get('shared') and t.get('job_id')==job['JobId']]
    from .scheduler import shape
    used=[shape(t['selection']['profile']) for t in active]
    target=envelope(root,config)
    try:remaining=max(0,datetime.fromisoformat(job['EndTime']).timestamp()-time.time()-150)
    except (KeyError,ValueError):remaining=0
    snapshot['shared_pool']=dict(job_id=job['JobId'],node=job.get('NodeList'),partition=job.get('Partition'),
        cpus=cpus,mem_mib=mem,used_cpus=cfg.get('reserve_cpus',1)+sum(x['cpus'] for x in used),
        used_mem_mib=cfg.get('reserve_mem_mib',512)+sum(x['mem_mib'] for x in used),handlers=len(active),
        remaining_minutes=int(remaining//60),draining=target!=(cpus,mem),target_cpus=target[0],target_mem_mib=target[1])


def selection(spec,snapshot,config):
    from .scheduler import memory,selection_score
    from .hardware import has_gpu
    pool=snapshot.get('shared_pool');cfg=config.get('shared_pool',{})
    if not pool:return None,'shared pool is not running'
    if pool['draining']:return None,'shared pool draining for resize; active handlers are protected'
    if config['owner'] not in spec.get('users',[config['owner']]):return None,'shared pool runs only under its owner account'
    if pool['handlers']>=cfg.get('max_handlers',16):return None,'shared handler count limit'
    node=next((n for n in snapshot['nodes'] if n['NodeName']==pool['node']),{})
    if not node or any(k in node.get('State','') for k in ('DOWN','DRAIN','FAIL','UNKNOWN','INVALID','NOT_RESPONDING')):return None,'shared pool node is not healthy CPU-only hardware'
    options=[];reasons=set();placement=spec.get('placement',{})
    for original in spec['profiles']:
        p=copy.deepcopy(original)
        if has_gpu(node) and (pool['partition'] not in config.get('service_cpu_on_gpu_partitions',[]) or p.get('cpu_only',True) or placement.get('avoid_gpu_nodes',True)):
            reasons.add('shared CPU use of GPU host requires explicit qualified profile');continue
        if p['slowdown']>spec['max_slowdown']:continue
        if p['partition']!=pool['partition']:reasons.add('shared pool partition mismatch');continue
        if p.get('allowed_nodes') and pool['node'] not in p['allowed_nodes']:continue
        if placement.get('allow_fallback') is False:
            if placement.get('preferred_nodes') and pool['node'] not in placement['preferred_nodes']:continue
            if placement.get('preferred_partitions') and p['partition'] not in placement['preferred_partitions']:continue
        if spec.get('interactive') and not spec.get('experimental') and spec.get('allow_shorter_walltime'):
            p['wall_minutes']=min(p['wall_minutes'],pool['remaining_minutes'])
        budget=spec.get('shared_time_budget')
        if budget:
            p['wall_minutes']=min(p['wall_minutes'],pool['remaining_minutes']-5)
            if p['wall_minutes']<budget['min_wall_minutes']:
                reasons.add('shared handler needs next allocation for minimum safe drain window');continue
            key=budget['env']
            p['env'][key]=str(min(int(p['env'][key]),(p['wall_minutes']-budget['reserve_minutes'])*60))
        if p['wall_minutes']<1 or p['wall_minutes']>pool['remaining_minutes']:reasons.add('shared allocation remaining walltime is insufficient');continue
        if pool['used_cpus']+p['threads']>pool['cpus'] or pool['used_mem_mib']+memory(p)>pool['mem_mib']:
            reasons.add('shared pool CPU/RAM capacity; wait for handlers or safe resize');continue
        selected=dict(shared=True,pool_job_id=pool['job_id'],user=config['owner'],profile=p,nodes=[pool['node']],mem_mib=memory(p))
        options.append(selected)
    if not options:return None,'; '.join(sorted(reasons)) or 'no qualified shared profile'
    return min(options,key=lambda s:selection_score(spec,s,config)),'ready in shared allocation'


def step_command(attempt,pty=False):
    a=Path(attempt);bundle=read(a/'spec.json');sel=bundle['selection'];p=sel['profile']
    job=sel['pool_job_id']
    if not re.fullmatch(r'\d+',job):raise ValueError('Invalid shared allocation ID')
    args=['srun','--jobid='+job,'--exact','--exclusive','--nodes=1','--ntasks=1',
          '--cpus-per-task='+str(p['threads']),'--mem='+str(sel['mem_mib'])+'M',
          '--time='+str(p['wall_minutes']),'--immediate=20','--cpu-bind=cores','--oom-kill-step=1',
          '--job-name='+bundle['step_name']]
    if pty:args+=['--pty']
    return args+[sys.executable,'-m','cdispatch.shared_worker',str(a)]


def start(daemon,task,selection):
    if not Path(task['spec']['workdir']).is_dir():
        task.update(state='HELD',reason='workdir does not exist');return
    number=len(task['attempts'])+1;a=daemon.root/'attempts'/task['id']/str(number);a.mkdir(parents=True,exist_ok=True)
    step_name='cds-'+task['id']+'-'+str(number)
    bundle=dict(id=task['id'],spec=task['spec'],selection=selection,created=time.time(),root=str(daemon.root.resolve()),step_name=step_name)
    atomic(a/'spec.json',bundle)
    for key in ('step_id','cancel_sent_step','accounting'):task.pop(key,None)
    task.update(state='PENDING',attempt=str(a),selection=selection,user=daemon.config['owner'],job_id=selection['pool_job_id'],step_name=step_name,reason='shared resources reserved; waiting for step')
    task.pop('manual_dispatch',None)
    task['attempts'].append(dict(number=number,path=str(a),selection=selection,job_id=task['job_id'],shared=True))
    daemon.state['last_submit']=time.time();daemon.event(task['id'],'SHARED_RESERVED',selection=selection);daemon.save()
    if not task['spec'].get('interactive'):
        try:
            with (a/'launcher.log').open('ab') as log:
                relay=daemon.config.get('shared_launcher_ssh')
                argv=([sys.executable,'-m','cdispatch.shared_launcher',str(a)] if not relay else
                      ['ssh','-F','/dev/null','-T','-i',relay['identity'],'-o','IdentitiesOnly=yes',
                       '-o','BatchMode=yes','-o','StrictHostKeyChecking=yes',
                       '-o','HostKeyAlias='+relay.get('host_key_alias','localhost'),
                       '-o','ConnectTimeout=10',relay['destination'],
                       json.dumps(['shared-launch',str(a)])])
                if relay:
                    result=subprocess.run(argv,stdin=subprocess.DEVNULL,stdout=log,stderr=log,timeout=20)
                    if result.returncode:
                        # A broken SSH reply may follow successful acceptance.
                        # Never release an uncertain launch reservation.
                        daemon.event(task['id'],'SHARED_LAUNCH_UNKNOWN',returncode=result.returncode)
                else:
                    subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
        except OSError as error:atomic(a/'driver.json',dict(returncode=127,error=str(error),time=time.time()))
        except subprocess.TimeoutExpired as error:
            # The broker may already have accepted the unique attempt. Keep its
            # reservation; accounting/start markers resolve that uncertainty.
            daemon.event(task['id'],'SHARED_LAUNCH_UNKNOWN',error=str(error))
    from .scheduler import reserve
    reserve(daemon.snapshot,selection,task['job_id'],step_name)


def cancel(daemon,task):
    a=Path(task['attempt']);atomic(a/'cancel.json',dict(time=time.time(),reason=task.get('cancel_reason','cancel')))
    step=task.get('step_id')
    if step:
        if not re.fullmatch(re.escape(task['job_id'])+r'\.\d+',step):raise ValueError('Refusing allocation-wide shared cancellation')
    if step and not task.get('cancel_sent_step'):
        daemon.slurm.cancel(daemon.config['owner'],step)
        task['cancel_sent_step']=step;daemon.event(task['id'],'SHARED_CANCEL_SENT',step_id=step)
    if not step:
        try:
            with lock(a/'launch.lock'):
                if not (a/'launch-intent.json').exists():atomic(a/'driver.json',dict(returncode=143,time=time.time(),cancelled_before_launch=True))
        except BlockingIOError:pass
    task['state']='CANCELLING'


def reconcile(daemon,task,jobs,accounting):
    a=Path(task['attempt']);started=read(a/'started.json',{});driver=read(a/'driver.json');result=read(a/'result.json')
    steps=daemon.snapshot.get('steps',[])
    step=next((s for s in steps if s['name']==task['step_name']),None)
    if step:task['step_id']=step['id']
    elif started.get('step_id'):task['step_id']=str(started['job_id'])+'.'+str(started['step_id'])
    elif not task.get('step_id'):
        # A step can fail before Python writes started.json. Its unique name
        # still identifies this attempt in accounting, including during handoff.
        matches=[r for r in accounting.values() if r.get('name')==task['step_name']
                 and re.fullmatch(re.escape(task['job_id'])+r'\.\d+',r.get('id',''))]
        if len(matches)==1:task['step_id']=matches[0]['id']
    if step:
        if task['state']!='CANCELLING':task['state']='RUNNING' if started else 'PENDING'
        task['reason']='shared step '+task['step_id'];return
    parent=jobs.get(task['job_id']);parent_record=accounting.get(task['job_id'])
    parent_finished=parent_record and parent_record['state'].split()[0].rstrip('+') not in ('RUNNING','PENDING','COMPLETING','CONFIGURING')
    lease=read(a/'client.json',{})
    if not started and not driver and task['spec'].get('interactive') and time.time()-lease.get('time',task['attempts'][-1].get('reserved_at',read(a/'spec.json')['created']))>180:
        daemon.cancel_intent(task,'shared interactive client did not attach')
        # Cancellation marker is checked under the launch lock before starting srun.
        atomic(a/'cancel.json',dict(time=time.time(),reason='client lease expired'))
    record=accounting.get(task.get('step_id'))
    ended=record and record['state'].split()[0].rstrip('+') not in ('RUNNING','PENDING','COMPLETING','CONFIGURING')
    if not (driver or ended or (not parent and parent_finished)):
        task['reason']='shared step starting or awaiting confirmed completion';return
    if ended and not driver:
        # Stop an orphaned launcher before releasing the reservation. Never
        # replay this attempt or infer success from Slurm without a validator.
        atomic(a/'cancel.json',dict(time=time.time(),reason='confirmed terminal shared step'))
    # A vanished step alone never releases a reservation: need launcher exit or accounting.
    final='CANCELLED' if task.get('cancel_requested') else (result or {}).get('state','FAILED')
    if not parent and parent_finished and not result:final='FAILED' if not task.get('cancel_requested') else 'CANCELLED'
    task.update(state=final,reason=(result or {}).get('reason','shared launcher/step ended'))
    task['attempts'][-1].update(state=final,result=result,driver=driver,step_id=task.get('step_id'))
    terminal_record=record or (parent_record if parent_finished else {}) or {}
    daemon.event(task['id'],'TERMINAL',state=final,reason=task['reason'],job_id=task['job_id'],step_id=task.get('step_id'),shared=True,
                 slurm_state=terminal_record.get('state',''),exit_code=terminal_record.get('exit_code'))


def maybe_grow(daemon,ready):
    cfg=daemon.config.get('shared_pool',{});pool=daemon.snapshot.get('shared_pool')
    if not cfg.get('auto_grow') or not pool or pool['draining']:return
    from .scheduler import memory
    for task in ready:
        if task['spec']['kind']!='shared' or time.time()-task['created']<cfg.get('grow_after_seconds',60):continue
        _,reason=selection(task['spec'],daemon.snapshot,daemon.config)
        if not reason.startswith('shared pool CPU/RAM capacity'):continue
        for p in task['spec']['profiles']:
            if p['partition']!=pool['partition'] or p['slowdown']>task['spec']['max_slowdown']:continue
            cpus=max(pool['cpus'],pool['used_cpus']+p['threads']);mem=max(pool['mem_mib'],pool['used_mem_mib']+memory(p))
            cpus=min(cfg['max_cpus'],2**math.ceil(math.log2(cpus)));mem=int(math.ceil(mem/1024)*1024)
            if cpus<pool['used_cpus']+p['threads'] or mem>cfg['max_mem_mib']:continue
            atomic(daemon.root/'pool-size.json',dict(cpus=cpus,mem_mib=mem,reason='queued handler '+task['id'],time=time.time()))
            pool['draining']=True;daemon.event(task['id'],'SHARED_RESIZE_REQUESTED',cpus=cpus,mem_mib=mem);return
