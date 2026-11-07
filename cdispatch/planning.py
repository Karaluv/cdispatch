"""Shared admission preparation and side-effect-free allocation forecasts."""
import copy
import os
import time

from .common import ACTIVE, TERMINAL, validate
from .scheduler import queue_plan, preemption_plan, shape, urgent, placement_policy
from .parent_transfer import parent_id


def checkpoint_barrier(tasks):
    return [task['id'] for task in tasks.values() if task['state']=='QUEUED' and
            any(tasks.get(tid,{}).get('state') in ('PAUSING','COMPLETING','CANCELLING')
                or (tasks.get(tid,{}).get('state') in ACTIVE and tasks.get(tid,{}).get('cancel_requested'))
                for tid in task.get('waiting_checkpoint_victims',[]))]


def ready_tasks(tasks, paused=False, now=None):
    now=time.time() if now is None else now
    ready=[];blocked={}
    for task in tasks.values():
        if task['state']!='QUEUED':continue
        s=task['spec'];reason=None;terminal=False
        parent=tasks.get(parent_id(task))
        deps=[tasks.get(d) for d in s['depends_on']]
        if paused and not task.get('manual_dispatch'):reason='automatic submissions paused; use run ID for one guarded admission'
        elif task.get('not_before',0)>now:reason='retry delay not elapsed'
        elif parent_id(task) and not parent:reason='unknown parent'
        elif parent and parent['state'] in TERMINAL:reason='parent ended';terminal=True
        elif any(d is None for d in deps):reason='unknown dependency'
        elif any(d['state'] in TERMINAL and d['state']!='SUCCEEDED' for d in deps):reason='dependency did not pass application validation';terminal=True
        elif any(d['state']!='SUCCEEDED' for d in deps):reason='dependencies'
        elif any(x['id']!=task['id'] and x['state'] in ACTIVE and os.path.realpath(x['spec'].get('resource_key',x['spec']['workdir']))==os.path.realpath(s.get('resource_key',s['workdir'])) for x in tasks.values()):reason='workdir held by active task'
        if reason:
            blocked[task['id']]=dict(reason=reason,terminal=terminal);continue
        planning=copy.deepcopy(task);override=task.get('placement_override',{})
        if override.get('user'):planning['spec']['users']=[override['user']]
        profiles=[]
        for profile in planning['spec']['profiles']:
            if override.get('profile') and profile['name']!=override['profile']:continue
            if override.get('nodes'):
                if profile.get('allowed_nodes') and not set(override['nodes']).issubset(profile['allowed_nodes']):continue
                profile['allowed_nodes']=override['nodes']
            profiles.append(profile)
        planning['spec']['profiles']=profiles
        if task.get('checkpoint'):
            allowed=s.get('resume_compatible_profiles',[task['selection']['profile']['name']])
            if task.get('migration_target'):allowed=[task['migration_target']]
            planning['spec']['profiles']=[p for p in profiles if p['name'] in allowed]
        ready.append(planning)
    return ready,blocked


def queue_actions(ready,tasks,snapshot,config):
    plan,decisions=queue_plan(ready,snapshot,config,all_tasks=tasks)
    selections=dict(plan)
    ordered=sorted(ready,key=lambda t:(not urgent(t['spec']),-t['spec']['priority'],t['created']))
    # Match the daemon: the first feasible urgent checkpoint action precedes sbatch.
    for task in ordered:
        action=preemption_plan(task,tasks,snapshot,config,selections.get(task['id']))
        if action:return plan,decisions,action
    return plan,decisions,None


def forecast(spec,data,config,task_id=None):
    now=time.time();snapshot=data.get('snapshot') or {};tasks=copy.deepcopy(data.get('tasks',{}))
    if task_id:
        if task_id not in tasks:raise ValueError('Unknown task ID')
        task=tasks[task_id]
    else:
        spec=validate(spec);task_id='forecast-request'
        while task_id in tasks:task_id+='-new'
        task=dict(id=task_id,spec=spec,state='QUEUED',created=now,attempts=[],reason='forecast')
        tasks[task_id]=task
    spec=task['spec']
    result=dict(task=task_id,name=spec['name'],experimental=spec.get('experimental',False),policy=placement_policy(spec),
                snapshot_age_seconds=round(now-snapshot.get('time',0),1),selection=None,pause=[],would_submit=False,
                forecast_only=True,estimated_start=None,
                limits='Forecast uses the cached Slurm snapshot and qualified profiles. Other users, scheduler priority, checkpoints and access checks can change admission; start time is not guaranteed.')
    if task['state']!='QUEUED':result.update(reason='task is '+task['state']);return result
    if not snapshot or now-data.get('heartbeat',0)>max(30,config.get('poll_interval',10)*3) or result['snapshot_age_seconds']>max(30,config.get('poll_interval',10)*3) or data.get('error'):
        result.update(reason='stale or failed scheduler snapshot');return result
    from .failure_circuits import blocked_reason, filter_ready
    circuit_reason=blocked_reason(data,task)
    if circuit_reason:result.update(reason=circuit_reason);return result
    waiting=checkpoint_barrier(tasks)
    if waiting:result.update(reason='waiting for checkpoint completion for '+', '.join(waiting));return result
    ready,blocked=ready_tasks(tasks,data.get('paused',False),now)
    ready,circuit_blocked=filter_ready(ready,data)
    blocked.update(circuit_blocked)
    if task_id in blocked:result.update(reason=blocked[task_id]['reason']);return result
    plan,decisions,action=queue_actions(ready,tasks,snapshot,config)
    result['decision']=decisions.get(task_id,{})
    result['reason']=result['decision'].get('reason','not admitted')
    result['dispatch_order']=[tid for tid,_ in plan]
    result['next_controller_action']=action or ({'submit':plan[0][0]} if plan else {'wait':True})
    delay=max(0,config.get('submit_interval',10)-(now-data.get('last_submit',0)))
    result['admission_delay_seconds']=round(delay,1)
    if action:
        result['cancel']=action.get('force_victims',[])
        result['pause']=[tid for tid in action['victims'] if tid not in result['cancel']]
        result['pause_for_task']=action['task']
        if action['task']==task_id:
            result.update(selection=action['selection'],reason='after explicitly authorized cancellation and confirmed resource release' if result['cancel'] else 'after successful checkpoints and confirmed resource release')
    if result['selection'] is None:result['selection']=dict(plan).get(task_id)
    if result['selection']:
        selected=result['selection'];p=selected['profile']
        result['resources']=shape(p)
        result['new_allocations']=0 if selected.get('shared') else 1
        result['layout']=dict(nodes=p['nodes'],ranks_per_node=p['ranks_per_node'],threads_per_rank=p['threads'],mem_mib_per_node=selected['mem_mib'],gpus_per_node=p.get('gpus_per_node',0))
        result['qualified_fallback']=p['slowdown']>min(x['slowdown'] for x in spec['profiles'])
        result['would_submit']=action is None and bool(plan) and plan[0][0]==task_id
        result['estimated_start']='eligible after checkpoint' if action and action['task']==task_id else 'eligible in local plan; Slurm decides start'
    blocked_urgent=any(urgent(t['spec']) and not decisions[t['id']]['admitted'] for t in ready)
    checkpoint_wait=blocked_urgent and any(t['state']=='PAUSING' and t.get('auto_resume') for t in tasks.values())
    if checkpoint_wait or delay:
        result['would_submit']=False
        result['next_controller_action']={'wait':'checkpoint confirmation' if checkpoint_wait else 'submission rate limit'}
        if action:result['pause']=[];result['cancel']=[]  # Daemon checks the submission interval before displacement.
    return result


def forecast_text(result):
    rows=['Прогноз: '+result['name'], 'Политика: '+result['policy']+(' · 🧪 фиксированный опыт' if result['experimental'] else ''),
          'Возраст снимка: %s с'%result['snapshot_age_seconds'],'Решение: '+result['reason']]
    selected=result.get('selection')
    if selected:
        p=selected['profile'];r=result['resources']
        rows+=['Профиль: '+p['name']+' · аккаунт '+selected['user'],'Ноды: '+', '.join(selected['nodes']),
               'Всего: %d CPU · %.1f GiB · %d GPU'%(r['cpus'],r['mem_mib']/1024,r['gpus']),
               'На ноду: %d ranks × %d threads · %.1f GiB'%(p['ranks_per_node'],p['threads'],selected['mem_mib']/1024),
               'Выбран допустимый запасной профиль; замедление %g×'%p['slowdown'] if result.get('qualified_fallback') else 'Выбрана доступная допустимая конфигурация.']
        if selected.get('shared'):rows.append('Внутри cd-service '+selected['pool_job_id']+'; новых allocations: 0')
    rows+=['Паузы: '+(', '.join(result['pause']) if result['pause'] else 'нет'),
           'Явное вытеснение: '+(', '.join(result.get('cancel',[])) or 'нет'),
           'Порядок допуска: '+', '.join(result.get('dispatch_order',[])),
           'Это прогноз по кэшу; заявка не отправлена. Точное время старта Slurm не гарантируется.']
    return '\n'.join(rows)
