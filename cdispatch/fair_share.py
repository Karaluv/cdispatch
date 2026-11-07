"""Opt-in group CPU fairness. Pure snapshot policy, never preempts a worker."""
import hashlib
import math
import random
import re
from .common import ACTIVE, tres


def validate_contract(value):
    if not isinstance(value, dict):
        raise ValueError('fair_share must be an object')
    allowed={'cohort','group','weight','priority','tier','demand_cpus','max_parallel'}
    if set(value)-allowed:raise ValueError('Unknown fair_share keys')
    for key in ('cohort','group'):
        if not isinstance(value.get(key),str) or not re.fullmatch(r'[\w.-]{1,100}',value[key]):
            raise ValueError('fair_share.'+key+' must be an explicit stable name')
    for key in ('priority','tier'):
        if type(value.get(key)) is not int:raise ValueError('fair_share.'+key+' must be integer')
    for key in ('demand_cpus','max_parallel'):
        if type(value.get(key)) is not int or value[key]<1:raise ValueError('fair_share.'+key+' must be positive integer')
    weight=value.get('weight',1.)
    if type(weight) not in (int,float) or not math.isfinite(weight) or not .01<=weight<=100:
        raise ValueError('fair_share.weight must be finite and within .01..100')


def contract(spec,config):
    if spec.get('kind')=='shared':return None
    value=spec.get('fair_share')
    if value is None:
        # Explicit administrator migration selectors; submitted requests stay immutable.
        labels=spec.get('labels',{})
        for rule in config.get('fair_share',{}).get('rules',[]):
            if all(labels.get(k)==v for k,v in rule['labels'].items()):
                value=rule['contract'];break
    if value is None:return None
    validate_contract(value)
    return value


def identity(value):return value['cohort']+'/'+value['group']


def groups(tasks,snapshot,config):
    result={}; seen=set()
    jobs={str(j['JobId']):j for j in snapshot.get('jobs',[])}
    for task in tasks.values() if isinstance(tasks,dict) else tasks:
        if task.get('state') not in ACTIVE|{'QUEUED'}:continue
        value=contract(task['spec'],config)
        if not value:continue
        key=identity(value)
        entry=result.setdefault(key,dict(contract=dict(value),reserved_cpus=0,allocated_cpus=0,active_jobs=0,queued_jobs=0,parents=[]))
        # A group contract is stable across roles, attempts, accounts and renewal.
        if entry['contract']!=value:raise ValueError('Conflicting fair_share contract for '+key)
        parent=task['spec'].get('parent')
        if parent and parent not in entry['parents']:entry['parents'].append(parent)
        if task.get('state')=='QUEUED':entry['queued_jobs']+=1;continue
        jid=str(task.get('job_id') or task['id'])
        if jid in seen:continue
        seen.add(jid)
        profile=(task.get('selection') or {}).get('profile',{})
        selected=profile.get('nodes',1)*profile.get('ranks_per_node',0)*profile.get('threads',1)
        job=jobs.get(jid,{})
        observed=int(tres(job.get('AllocTRES') or job.get('ReqTRES') or '').get('cpu',job.get('NumCPUs',0)))
        entry['reserved_cpus']+=max(selected,observed)
        if job.get('JobState') in ('RUNNING','CONFIGURING','COMPLETING','SUSPENDED','RESIZING','SIGNALING','STAGE_OUT'):
            entry['allocated_cpus']+=observed
        entry['active_jobs']+=1
    for key,e in result.items():
        c=e['contract'];temperature=config.get('fair_share',{}).get('temperature',20.)
        if not math.isfinite(temperature) or temperature<=0:raise ValueError('fair_share.temperature must be positive')
        # Cohort baseline prevents overflow; clamping gives a finite policy envelope.
        e['weight']=c.get('weight',1.)*math.exp(max(-20,min(20,(c['priority']-c['tier'])/temperature)))
    return result


def lottery(entries,config,snapshot,step=0):
    """One ticket per GROUP; extra queued workers never multiply probability.

    Restrict to feasible groups within one normalized CPU quantum of the least
    allocated peer. Break ties with Boltzmann priority weight and a reserved-CPU
    penalty. Equal-weight CPU192 cannot leapfrog CPU32 at q32.
    Snapshot-seeded exponential races make CLI and daemon forecasts replayable.
    """
    quantum=config.get('fair_share',{}).get('cpu_quantum',32.)
    if not math.isfinite(quantum) or quantum<=0:raise ValueError('fair_share.cpu_quantum must be positive')
    loads={k:e['reserved_cpus']/e['weight'] for k,e in entries.items()}
    floor=min(loads.values())
    # Lottery breaks degeneracy among comparable shares, never lets a heavily
    # overallocated peer leapfrog feasible underallocated work.
    keys=sorted(k for k in entries if loads[k]<=floor+quantum+1e-9)
    seed=str(snapshot.get('time',0))+'|'+str(step)+'|'+'|'.join(keys)
    rng=random.Random(int(hashlib.sha256(seed.encode()).hexdigest(),16))
    logs={k:math.log(entries[k]['weight'])-loads[k]/quantum for k in keys}
    baseline=max(logs.values())
    rates={k:math.exp(max(-700,v-baseline)) for k,v in logs.items()}
    return sorted(keys,key=lambda k:-math.log(max(rng.random(),1e-300))/rates[k])


def cpu_goals(entries,snapshot,config):
    """Weighted water filling of actual free healthy CPU plus cohort reservations."""
    from .scheduler import BAD
    nodes={n['NodeName']:n for n in snapshot.get('nodes',[])}
    free=sum(max(0,int(n.get('CPUEfctv',n.get('CPUTot',0)))-int(n.get('CPUAlloc',0)))
             for n in nodes.values() if not any(s in n.get('State','') for s in BAD))
    total=sum(e['allocated_cpus'] for e in entries.values())
    cap=config.get('pool_limits',{}).get('cpus')
    if cap is not None:
        from .scheduler import usage
        free=min(free,max(0,cap-usage(snapshot.get('jobs',[]))['cpus']))
    remaining=free+total; pending=set(entries); goals={}
    while pending:
        weights=sum(entries[k]['weight'] for k in pending)
        capped=[k for k in pending if entries[k]['contract']['demand_cpus']<=remaining*entries[k]['weight']/weights]
        if not capped:
            goals.update({k:remaining*entries[k]['weight']/weights for k in pending});break
        for k in capped:
            goals[k]=entries[k]['contract']['demand_cpus'];remaining-=goals[k];pending.remove(k)
    return goals


def report(tasks,snapshot,config):
    entries=groups(tasks,snapshot,config)
    # Cohorts have separate importance tiers, but still share physical limits.
    for cohort in {e['contract']['cohort'] for e in entries.values()}:
        subset={k:e for k,e in entries.items() if e['contract']['cohort']==cohort}
        goals=cpu_goals(subset,snapshot,config)
        for k,e in subset.items():e['cpu_goal']=round(goals[k],1)
    return dict(temperature=config.get('fair_share',{}).get('temperature',20.),
                cpu_quantum=config.get('fair_share',{}).get('cpu_quantum',32.),groups=entries,
                policy='group Boltzmann lottery with reserved CPU penalty; no fairness preemption')


def validate_membership(spec,tasks,config):
    """Reject a conflicting client declaration before it can poison the queue."""
    value=contract(spec,config)
    if not value:return
    for task in tasks.values():
        if task.get('state') not in ACTIVE|{'QUEUED'}:continue
        peer=contract(task['spec'],config)
        if not peer or peer['cohort']!=value['cohort']:continue
        if peer['tier']!=value['tier']:raise ValueError('fair_share cohort has conflicting strict tier')
        if peer['group']==value['group'] and peer!=value:
            raise ValueError('fair_share group conflicts with its live contract')
