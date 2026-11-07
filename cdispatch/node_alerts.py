"""Fresh-cache node transitions; unrelated pending reasons are never evidence."""
import html
import time


def condition(node):
    state=str(node.get('State',''))
    if any(x in state for x in ('DOWN','FAIL','NOT_RESPONDING','INVALID')):return 'unavailable'
    if 'DRAIN' in state:return 'drain'
    return 'healthy'


def transitions(data, previous, now):
    snapshot=data.get('snapshot') or {}
    if now-snapshot.get('time',0)>90 or not snapshot.get('nodes'):
        return [],previous,[]
    current={n['NodeName']:dict(condition=condition(n),state=n.get('State',''),reason=str(n.get('Reason') or 'Причина не указана')[:240]) for n in snapshot['nodes']}
    if previous is None:return [],current,[]  # Existing bad nodes are baseline, not new deaths.
    notices=[];recovered=[]
    for name,node in current.items():
        old=previous.get(name)
        if old is None:continue
        if node['condition']!='healthy' and node['condition']!=old['condition']:
            level=2 if node['condition']=='unavailable' else 1
            detail='Нода недоступна' if level==2 else 'Нода переведена в DRAIN; новые jobs не принимаются'
            jobs=[j for j in snapshot.get('jobs',[]) if name in str(j.get('NodeList','')).split(',') and j.get('JobState') in ('RUNNING','COMPLETING','SUSPENDED')]
            tasks=data.get('tasks',{})
            from .presentation import label
            affected=[]
            for job in jobs:
                task=next((t for t in tasks.values() if str(t.get('job_id'))==str(job.get('JobId'))),None)
                affected.append((label(task) if task else job.get('JobName','Задание'))+' · Slurm '+str(job.get('JobId')))
            message='<b>'+html.escape(name)+'</b> · '+detail+'\n'+html.escape(node['state'])+' · '+time.strftime('%H:%M:%S',time.localtime(snapshot['time']))+'\nПричина: '+html.escape(node['reason'])
            if affected:message+='\nНаши jobs: '+html.escape('; '.join(affected)[:400])
            notices.append((level,message))
        elif node['condition']=='healthy' and old['condition']!='healthy':recovered.append(name)
    # Missing nodes are not evidence of recovery. Retain their last observation.
    return notices,{**previous,**current},recovered
