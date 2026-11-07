"""Compact cache-only bot views; allocations are counted once per Slurm job."""
import html
import re
import json
from pathlib import Path
import time
from .common import tres, mib, TERMINAL

ALLOCATED = {'RUNNING', 'COMPLETING', 'CONFIGURING', 'SUSPENDED', 'RESIZING', 'STAGE_OUT'}
BAD = ('DOWN', 'DRAIN', 'FAIL', 'UNKNOWN', 'INVALID', 'NOT_RESPONDING', 'PLANNED', 'RESERVED', 'MAINT', 'POWER')

def resources(job):
    allocation=job.get('AllocTRES')
    tr=tres(allocation if allocation not in (None,'','(null)','N/A') else job.get('ReqTRES') or '')
    if not tr: return (int(job.get('NumCPUs',0)),0,0)
    gpu=int(tr['gres/gpu']) if 'gres/gpu' in tr else sum(int(v) for k,v in tr.items() if k.startswith('gres/gpu:'))
    return int(tr.get('cpu',0)),mib(tr.get('mem','0')),gpu

def fmt(value):
    return '%s CPU · %.1f GiB · %s GPU'%(value[0],value[1]/1024,value[2])

def allocated_jobs(data):
    return {str(j['JobId']):j for j in (data.get('snapshot') or {}).get('jobs',[]) if j.get('JobState') in ALLOCATED}

def total(data):
    values=[resources(j) for j in allocated_jobs(data).values()]
    return tuple(sum(v[i] for v in values) for i in range(3))

def group(task):
    spec = task.get('spec', {})
    return spec.get('labels', {}).get('display_group', spec.get('name', 'Task'))

def label(task):
    spec = task.get('spec', {})
    return spec.get('labels', {}).get('display_name', group(task))

def reason(value):
    value=str(value or '')
    if value in ('','None','none'):return ''
    if 'dependencies' in value:return 'Ждёт предыдущий этап'
    if 'DOWN' in value or 'DRAINED' in value:return 'Ждёт доступные ресурсы или окончание резервирования'
    if any(x in value.lower() for x in ('resources','fitting nodes','reserved time','partition not allowed')):return 'Ждёт подходящие ресурсы и окно запуска'
    if any(x in value.lower() for x in ('quota','qos','limit')):return 'Ждёт освобождения квоты'
    if 'Priority' in value:return 'Ждёт своей очереди в Slurm'
    return value.replace('_',' ')[:150]

def free(data):
    nodes={n['NodeName']:n for n in (data.get('snapshot') or {}).get('nodes',[]) if not any(s in n.get('State','') for s in BAD)}
    physical=data.get('physical_capacity')
    if physical is not None:nodes={k:n for k,n in nodes.items() if k in physical.get('node_names',[])}
    vals=[]
    for n in nodes.values():
        cfg=tres(n.get('CfgTRES',''));used=tres(n.get('AllocTRES',''))
        def gpu(tr):return int(tr['gres/gpu']) if 'gres/gpu' in tr else sum(int(v) for k,v in tr.items() if k.startswith('gres/gpu:'))
        vals.append((max(0,int(n.get('CPUEfctv',n.get('CPUTot',0)))-int(n.get('CPUAlloc',0))),max(0,int(n.get('RealMemory',0))-int(n.get('AllocMem',0))),max(0,gpu(cfg)-gpu(used))))
    return tuple(sum(v[i] for v in vals) for i in range(3))

def status(data):
    esc=lambda s:html.escape(str(s))
    jobs=allocated_jobs(data);by_job={}
    for t in data.get('tasks',{}).values():
        if t.get('state') not in TERMINAL and not t.get('selection',{}).get('shared'):by_job[str(t.get('job_id'))]=t
    rows=['<b>📊 Ресурсы</b>', 'Выделено моему пулу: <b>'+fmt(total(data))+'</b>', 'Свободно на доступных нодах: <b>'+fmt(free(data))+'</b>', 'Свободный запас общий; старт зависит от профиля, очереди и квот.', '', '<b>На что выделено</b>']
    from .scheduler import usage
    occupied=usage((data.get('snapshot') or {}).get('jobs',[]))['jobs']
    from .service_renewal import deferred_service_successors
    renewals=deferred_service_successors((data.get('snapshot') or {}).get('jobs',[]))
    if renewals:rows.append('Сервис-преемник '+', '.join(sorted(renewals))+': без дополнительного местного слота.')
    cap=data.get('config',{}).get('local_pool_limits',data.get('config',{}).get('pool_limits',{})).get('jobs')
    if cap is not None:
        rows.insert(3,'Слоты заданий: <b>'+str(occupied)+'/'+str(cap)+'</b>'+(' · новые jobs ждут освобождения слота' if occupied>=cap else ' · свободно '+str(cap-occupied)))

    groups={}
    for jid,j in jobs.items():
        name='Служба' if j.get('JobName')=='cd-service' else group(by_job[jid]) if jid in by_job else j.get('JobName','Внешнее задание')
        value=resources(j);entry=groups.setdefault(name,[0,0,0,0])
        for i in range(3):entry[i]+=value[i]
        entry[3]+=1
    for name,v in groups.items():rows.append(esc(name)+': <b>'+fmt(v)+'</b>'+(' · '+str(v[3])+' jobs' if v[3]>1 else ''))
    if not groups:rows.append('Выделений нет.')
    waiting=[t for t in data.get('tasks',{}).values() if t.get('state') in ('QUEUED','PENDING','HELD','PAUSED')]
    rows+=['','Ожидают: '+str(len(waiting))+' · ресурсы не включены в расход.', 'Автозапуск: '+('⏸ пауза' if data.get('paused') else '🟢 включён'), 'Снимок '+time.strftime('%H:%M:%S',time.localtime((data.get('snapshot') or {}).get('time',0)))]
    if data.get('circuit'):rows.append('🔴 Запуски заблокированы: '+esc(data['circuit']))
    return '\n'.join(rows)

def event_line(event,data):
    task=data.get('tasks',{}).get(event.get('task'),{})
    name=label(task) if task else event.get('name') or 'Задание'
    raw=str(event.get('slurm_state') or event.get('reason') or event.get('error') or event.get('state') or '')
    if 'PREEMPTED' in raw:level=1;detail='Slurm прервал попытку (PREEMPTED); это не падение ноды'
    elif 'OUT_OF_MEMORY' in raw or 'OOM' in raw:level=2;detail='Не хватило памяти (OOM); нужна проверка профиля'
    elif 'NODE_FAIL' in raw or 'BOOT_FAIL' in raw:
        level=2;detail='Сбой ноды; попытка прервана'
        nodes=(task.get('selection') or {}).get('nodes',[])
        if nodes:detail+=' · нода '+', '.join(nodes)
    elif 'TIMEOUT' in raw:level=1;detail='Истёк лимит времени; попытка прервана'
    elif 'CANCELLED' in raw:level=1;detail='Задание отменено'
    else:level=2;detail='Ошибка расчёта или заявки; требуется проверка'
    if raw in ('FAILED','COMPLETED') and event.get('expected_time_budget'):
        level=1;detail='Application time budget exhausted; attempt not accepted'
    return level,html.escape(str(name))+'\n'+html.escape(detail)+((' · Slurm <code>'+html.escape(str(event['job_id']))+'</code>') if event.get('job_id') else '')
