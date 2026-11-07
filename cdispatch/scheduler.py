import copy
import math
import re
import time
import itertools
from .common import ACTIVE, tres, mib, seconds
from .policy import reservation_blockers, submission_blockers, planned_backfill_fits
from .hardware import has_gpu
from .limits import live_blockers, job_count

BAD = ('DOWN','DRAIN','FAIL','UNKNOWN','INVALID','NOT_RESPONDING','PLANNED','RESERVED','MAINT','POWER')
SLURM_ACTIVE = {'PENDING','RUNNING','CONFIGURING','COMPLETING','SUSPENDED','RESIZING','REQUEUED','SIGNALING','STAGE_OUT'}

def memory(p):
    m=p.get('memory_model',{})
    estimate=(m.get('base_mib',0)+m.get('per_rank_mib',0)*p['ranks_per_node']+
              m.get('per_thread_mib',0)*p['ranks_per_node']*p['threads'])*m.get('safety_factor',1)
    return max(p['mem_mib'],math.ceil(estimate))

def gpu_free(node, kind):
    total=sum(int(n) for k,n in re.findall(r'gpu:([^:,()]+):(\d+)',node.get('Gres','')) if k==kind)
    allocated=tres(node.get('AllocTRES',''))
    # Typed allocations may be absent in scontrol; subtract aggregate conservatively.
    used=int(allocated.get('gres/gpu',0))
    return max(0,total-used)

def shape(p):
    return dict(cpus=p['nodes']*p['ranks_per_node']*p['threads'],
                mem_mib=memory(p)*p['nodes'],gpus=p.get('gpus_per_node',0)*p['nodes'],nodes=p['nodes'],jobs=1)

def usage(jobs, user=None):
    from .service_renewal import deferred_service_successors
    jobs=list(jobs)
    deferred=deferred_service_successors(jobs)
    total=dict(cpus=0,mem_mib=0,gpus=0,nodes=0,jobs=0,pending=0)
    for j in jobs:
        if j.get('JobId') in deferred:continue
        if j.get('JobState') not in SLURM_ACTIVE: continue
        if user and j.get('UserId','').split('(')[0]!=user: continue
        allocated=j.get('AllocTRES','')
        t=tres(allocated if allocated not in ('','(null)','N/A') else j.get('ReqTRES',''))
        total['cpus']+=int(t.get('cpu',j.get('NumCPUs',0)))
        total['mem_mib']+=mib(t.get('mem','0'))
        total['gpus']+=int(t.get('gres/gpu','0'))
        total['nodes']+=int(str(t.get('node',j.get('NumNodes',0))).split('-')[-1])
        mult=job_count(j)
        if mult>1:
            total['cpus']+=int(t.get('cpu',j.get('NumCPUs',0)))*(mult-1)
            total['mem_mib']+=mib(t.get('mem','0'))*(mult-1)
            total['gpus']+=int(t.get('gres/gpu','0'))*(mult-1)
            total['nodes']+=int(str(t.get('node',j.get('NumNodes',0))).split('-')[-1])*(mult-1)
        total['jobs']+=mult
        total['pending']+=mult if j['JobState']=='PENDING' else 0
    return total

def fits_limits(used, requested, limits):
    return all(used.get(k,0)+requested.get(k,0)<=v for k,v in limits.items() if k in used and v is not None)

def eligible_nodes(profile, snapshot, config, placement=None):
    placement=placement or {}
    part=snapshot['partitions'].get(profile['partition'])
    if not part or part.get('State')!='UP': return []
    if seconds(part.get('MaxTime','0')) < profile['wall_minutes']*60: return []
    if part.get('ReqResv')=='YES' or part.get('RootOnly')=='YES': return []
    cpus=profile['ranks_per_node']*profile['threads'];mem=memory(profile)
    from .limits import count
    max_nodes=count(part.get('MaxNodes'));min_nodes=count(part.get('MinNodes'))
    if max_nodes is not None and profile['nodes']>max_nodes:return []
    if min_nodes is not None and profile['nodes']<min_nodes:return []
    max_cpu=count(part.get('MaxCPUsPerNode'))
    if max_cpu is not None and cpus>max_cpu:return []
    max_mem=part.get('MaxMemPerNode')
    if max_mem not in (None,'','UNLIMITED','N/A') and mem>mib(max_mem):return []
    max_mem_cpu=part.get('MaxMemPerCPU')
    if max_mem_cpu not in (None,'','UNLIMITED','N/A') and mem>mib(max_mem_cpu)*cpus:return []
    out=[]
    for n in snapshot['nodes']:
        name=n['NodeName']
        if profile.get('cpu_only') and has_gpu(n):continue
        if not profile.get('gpus_per_node') and placement.get('avoid_gpu_nodes',True) and has_gpu(n):continue
        if placement.get('allow_fallback') is False and placement.get('preferred_nodes') and name not in placement['preferred_nodes']:continue
        state=n.get('State','')
        if any(x in state for x in BAD if x!='PLANNED'): continue
        if 'PLANNED' in state and not (placement.get('allow_planned_backfill',False) and planned_backfill_fits(name,profile,snapshot,config)): continue
        if profile['partition'] not in n.get('Partitions','').split(','): continue
        if profile.get('allowed_nodes') and name not in profile['allowed_nodes']: continue
        if name in config.get('excluded_nodes',[]): continue
        if reservation_blockers(name,profile,snapshot,config):continue
        if int(n.get('CPUEfctv',n.get('CPUTot',0)))-int(n.get('CPUAlloc',0))<cpus: continue
        if int(n.get('RealMemory',0))-int(n.get('AllocMem',0))<mem: continue
        if profile.get('gpus_per_node',0)>gpu_free(n,profile.get('gpu_type','')): continue
        # Small jobs prefer old, small machines; reserve scarce GPU nodes for GPU work.
        cost=config.get('node_cost',{}).get(name,50)
        if not profile.get('gpus_per_node') and has_gpu(n):cost+=100
        preferred=placement.get('preferred_nodes',[])
        if preferred:cost+=10000*(preferred.index(name) if name in preferred else len(preferred))
        if cpus<=4 and mem<=16384: cost+=int(n.get('CPUTot',0))
        remaining=int(n.get('CPUEfctv',n.get('CPUTot',0)))-int(n.get('CPUAlloc',0))-cpus
        out.append((cost,remaining,name))
    return sorted(out)


def placement_policy(spec):
    policy=spec.get('placement',{}).get('policy','auto')
    if policy!='auto':return policy
    small=all(shape(p)['cpus']<=4 and shape(p)['mem_mib']<=16384 and not shape(p)['gpus'] for p in spec['profiles'])
    return 'economy' if small or spec['kind']=='interactive' else 'fast'


def selection_score(spec,selection,config):
    p=selection['profile'];placement=spec.get('placement',{});preferred=placement.get('preferred_partitions',[])
    partition_rank=(preferred.index(p['partition']) if p['partition'] in preferred else len(preferred)) if preferred else 0
    cpu_order=placement.get('preferred_cpu_counts',[])
    if cpu_order:
        cpus=shape(p)['cpus']
        cpu_rank=cpu_order.index(cpus) if cpus in cpu_order else len(cpu_order)
        # CPU-count policy is independent of measured speed and dominates host
        # cost/partition preferences, only among already feasible allocations.
        partition_rank=cpu_rank*(len(preferred)+1)+partition_rank
    preferred_nodes=placement.get('preferred_nodes',[])
    node_rank=sum(preferred_nodes.index(n) if n in preferred_nodes else len(preferred_nodes) for n in selection['nodes']) if preferred_nodes else 0
    cost=selection.get('placement_cost',sum(config.get('node_cost',{}).get(n,50) for n in selection['nodes']))
    # Preferences choose among qualified feasible shapes; they never expand allowlists.
    return (partition_rank,node_rank)+((cost,p['slowdown']) if placement_policy(spec)=='economy' else (p['slowdown'],cost))

def choose(spec,snapshot,config):
    from .plugins import prepare_spec as apply
    spec=apply(spec,config)
    if spec.get('_application_lane_delegated'):return None,'lane delegated to '+spec['_application_lane_delegated']
    if spec['kind']=='shared':
        from .shared import selection
        return selection(spec,snapshot,config)
    jobs=[j for j in snapshot['jobs'] if config.get('shared_pool',{}).get('enabled') or j.get('JobId')!=config.get('service_job_id')]
    used=usage(jobs)
    limits=dict(config['pool_limits'])
    if spec['kind']!='interactive': limits['jobs']-=config.get('interactive_reserve_jobs',1)
    if used['pending']>=config.get('max_pending',2): return None,'global pending limit'
    global_cap=snapshot.get('limits',{}).get('scheduler_settings',{}).get('MaxJobCount')
    if global_cap not in (None,'','unknown','UNLIMITED') and 'limit_jobs' in snapshot:
        if sum(job_count(j) for j in snapshot['limit_jobs'])+1>int(global_cap):return None,'Slurm MaxJobCount limit'
    options=[];reasons=set()
    for user in spec.get('users',[u for u,a in config['users'].items() if a.get('enabled') and a.get('authorized')]):
        account=config['users'].get(user,{})
        if not account.get('enabled'): reasons.add('user disabled');continue
        if user!=config['owner'] and not account.get('authorized'): reasons.add('user not authorized');continue
        u=usage(jobs,user)
        for p in spec['profiles']:
            placement=spec.get('placement',{})
            if placement.get('allow_fallback') is False and placement.get('preferred_partitions') and p['partition'] not in placement['preferred_partitions']:continue
            if p['slowdown']>spec['max_slowdown']: continue
            if p['partition'] not in account.get('partitions',[]): reasons.add('partition not allowed');continue
            part=snapshot['partitions'].get(p['partition'],{})
            allowed=part.get('AllowAccounts','ALL').split(',');denied=part.get('DenyAccounts','').split(',')
            if ('ALL' not in allowed and account['account'] not in allowed) or account['account'] in denied:
                reasons.add('Slurm partition account access');continue
            groups=part.get('AllowGroups','ALL').split(',')
            if 'ALL' not in groups and not set(account.get('unix_groups',[])) & set(groups):
                reasons.add('Slurm partition group access');continue
            blocked_by=submission_blockers(user,p,snapshot,config)
            if blocked_by:
                reasons.add('Slurm pending feedback: '+', '.join(x['job_id']+' '+str(x['reason']) for x in blocked_by));continue
            requested=shape(p)
            blockers=live_blockers(user,account,p,snapshot)
            if blockers:
                reasons.add('live association/QOS limit: '+ '; '.join(b.get('error') or b['source']+' '+b['dimension'] for b in blockers));continue
            if not fits_limits(used,requested,limits): reasons.add('pool resource/slot limit');continue
            if not fits_limits(u,requested,account['limits']): reasons.add('user quota');continue
            nodes=eligible_nodes(p,snapshot,config,placement)
            if len(nodes)<p['nodes']: reasons.add('no healthy fitting nodes or reserved time window');continue
            picked=nodes[:p['nodes']]
            # Prefer faster validated shape. Node quality is primary for interactive.
            selection=dict(user=user,account=account['account'],qos=account.get('qos') or next((a.get('default_qos') or a.get('qos','').split(',')[0] for a in snapshot.get('limits',{}).get('associations',[]) if a.get('user')==user and a.get('account')==account['account']),'normal'),profile=copy.deepcopy(p),nodes=[n[2] for n in picked],mem_mib=memory(p),placement_cost=sum(n[0] for n in picked))
            if spec.get('_application_profile_rule'):selection['application_profile_rule']=spec['_application_profile_rule']
            options.append((selection_score(spec,selection,config),selection))
    if not options: return None, '; '.join(sorted(reasons)) or 'no allowed profile'
    return min(options,key=lambda x:x[0])[1], 'ready'

def reserve(snapshot, selection, job_id, name):
    p=selection['profile'];r=shape(p)
    if selection.get('shared'):
        pool=snapshot['shared_pool'];pool['used_cpus']+=r['cpus'];pool['used_mem_mib']+=r['mem_mib'];pool['handlers']+=1
        return
    planned=dict(JobId=job_id,JobName=name,JobState='PENDING',Virtual='YES',UserId=selection['user'],Account=selection.get('account',''),QOS=selection.get('qos',''),Partition=p['partition'],
        ReqTRES=f"cpu={r['cpus']},mem={r['mem_mib']}M,gres/gpu={r['gpus']},node={r['nodes']}")
    snapshot['jobs'].append(planned)
    if 'account_jobs' in snapshot:snapshot['account_jobs'].append(planned)
    if 'limit_jobs' in snapshot:snapshot['limit_jobs'].append(planned)
    for n in snapshot['nodes']:
        if n['NodeName'] in selection['nodes']:
            n['CPUAlloc']=str(int(n.get('CPUAlloc',0))+p['ranks_per_node']*p['threads'])
            n['AllocMem']=str(int(n.get('AllocMem',0))+selection['mem_mib'])
            t=tres(n.get('AllocTRES',''));t['gres/gpu']=str(int(t.get('gres/gpu',0))+p.get('gpus_per_node',0))
            n['AllocTRES']=','.join(k+'='+v for k,v in t.items())

def urgent(spec):
    return bool(spec.get('urgent') or 'urgent' in spec.get('tags',[]) or spec.get('kind')=='interactive' or spec.get('interactive'))

def choices(spec,snapshot,config,limit=8):
    from .plugins import prepare_spec as apply
    spec=apply(spec,config)
    """Alternative validated shapes/users, including alternate first-fit node sets."""
    options=[];seen=set()
    for p in spec['profiles']:
        for user in spec.get('users',[u for u,a in config['users'].items() if a.get('enabled') and a.get('authorized')]):
            narrow=copy.deepcopy(spec);narrow['profiles']=[p];narrow['users']=[user]
            narrow.setdefault('placement',{})['policy']=placement_policy(spec)
            selection,reason=choose(narrow,snapshot,config)
            if selection:
                key=(user,p['name'],tuple(selection['nodes']))
                if key not in seen:options.append(selection);seen.add(key)
    return sorted(options,key=lambda x:selection_score(spec,x,config))[:limit]

def release(snapshot,t):
    snap=copy.deepcopy(snapshot);sel=t['selection'];p=sel['profile']
    snap['jobs']=[j for j in snap['jobs'] if j['JobId']!=t.get('job_id')]
    snap['account_jobs']=[j for j in snap.get('account_jobs',[]) if j['JobId']!=t.get('job_id')]
    if 'limit_jobs' in snap:snap['limit_jobs']=[j for j in snap['limit_jobs'] if j['JobId']!=t.get('job_id')]
    for n in snap['nodes']:
        if n['NodeName'] in sel['nodes']:
            n['CPUAlloc']=str(max(0,int(n['CPUAlloc'])-p['ranks_per_node']*p['threads']))
            n['AllocMem']=str(max(0,int(n['AllocMem'])-sel['mem_mib']))
            tr=tres(n.get('AllocTRES',''));tr['gres/gpu']=str(max(0,int(tr.get('gres/gpu',0))-p.get('gpus_per_node',0)))
            n['AllocTRES']=','.join(k+'='+v for k,v in tr.items())
    return snap

def queue_plan(tasks,snapshot,config,all_tasks=None):
    """Bounded beam search across several jobs and validated shapes; no Slurm RPC.

    Lexicographic priority tiers; within tiers maximize admitted count, then
    predicted completion rate, resource use, and preference for cheaper nodes.
    """
    from .fair_share import contract
    if any(contract(t['spec'],config) for t in tasks):
        return fair_queue_plan(tasks,snapshot,config,all_tasks or {t['id']:t for t in tasks})
    ordered=sorted(tasks,key=lambda t:(not urgent(t['spec']),-t['spec']['priority'],t['created']))
    considered=[]
    for t in ordered:
        if choices(t['spec'],snapshot,config):considered.append(t)
        if len(considered)>=config.get('plan_window',12):break
    tiers=sorted({(int(urgent(t['spec'])),t['spec']['priority']) for t in considered},reverse=True)
    def score(plan):
        counts=[sum((int(urgent(t['spec'])),t['spec']['priority'])==tier for t,s in plan) for tier in tiers]
        preference=tuple(-sum(selection_score(t['spec'],s,config)[i] for t,s in plan) for i in range(4))
        util=sum(shape(s['profile'])['cpus']+32*shape(s['profile'])['gpus'] for t,s in plan)
        cost=sum(sum(config.get('node_cost',{}).get(n,50) for n in s['nodes']) for t,s in plan)
        # For interactive jobs allocating expensive nodes never improves utility.
        return tuple(counts)+(len(plan),)+preference+(util,-cost)
    beam=[([],copy.deepcopy(snapshot))]
    for t in considered:
        expanded=[]
        for plan,snap in beam:
            expanded.append((plan,snap))
            if any(x['spec'].get('resource_key',x['spec']['workdir'])==t['spec'].get('resource_key',t['spec']['workdir']) for x,_ in plan):continue
            for sel in choices(t['spec'],snap,config):
                nxt=copy.deepcopy(snap);reserve(nxt,sel,'plan-'+t['id'],t['spec']['name'])
                expanded.append((plan+[(t,sel)],nxt))
        expanded.sort(key=lambda entry:score(entry[0]),reverse=True)
        beam=expanded[:config.get('plan_beam',32)]
    best=beam[0][0] if beam else []
    selected={t['id']:s for t,s in best}
    decisions={}
    for t in ordered:
        tid=t['id'];s=t['spec']
        if tid in selected:
            sel=selected[tid]
            skipped=[x['id'] for x in ordered[:ordered.index(t)] if x['id'] not in selected]
            decisions[tid]=dict(admitted=True,selection=sel,reason='joint priority and resource packing plan',bypassed_waiting_tasks=skipped,
                rationale=f"priority={s['priority']}, urgent={urgent(s)}, {len(best)} jobs fit jointly; validated slowdown={sel['profile']['slowdown']} <= {s['max_slowdown']}")
        else:
            alone,reason=choose(s,snapshot,config)
            decisions[tid]=dict(admitted=False,reason='selected competing tasks by priority/packing' if alone else reason,
                suggestions=explain_constraints(s,snapshot,config),planning_window=config.get('plan_window',12))
    return [(t['id'],s) for t,s in best],decisions


def preemption_plan(wanted,tasks,snapshot,config,current_selection=None):
    """Pure forecast used by the daemon and CLI; pausing itself requires a checkpoint."""
    if wanted['spec']['kind']=='shared' or not config.get('auto_preempt') or not urgent(wanted['spec']):return None
    speed_request=wanted['spec'].get('placement',{}).get('preempt_for_speed',False)
    if current_selection and not speed_request:return None
    if current_selection is None and choose(wanted['spec'],snapshot,config)[0]:return None
    force_ids=set(wanted['spec'].get('force_displace_tasks',[]))
    from .plugins import allow_displacement
    def force_allowed(t):
        s=t['spec']
        return (t['id'] in force_ids and s.get('kind','batch')=='batch'
                and s.get('parent') and allow_displacement(t,config)
                and not s.get('protected') and not s.get('adopt_job')
                and not t.get('selection',{}).get('shared'))
    victims=[t for t in tasks.values() if t['state']=='RUNNING' and (t['spec'].get('preemptible') or force_allowed(t))
             and t['spec']['priority']<wanted['spec']['priority']
             and not t['spec'].get('experimental') and not urgent(t['spec']) and not t.get('cancel_requested')
             and (force_allowed(t) or len(t['attempts'])<t['spec']['max_attempts']) and time.time()-t.get('pause_requested',0)>600]
    victims.sort(key=lambda t:(t['spec']['priority'],t['created']))
    for count in range(1,min(3,len(victims))+1):
        for subset in itertools.combinations(victims[:6],count):
            snap=snapshot
            for task in subset:snap=release(snap,task)
            selection,_=choose(wanted['spec'],snap,config)
            if not selection:continue
            overhead=sum(t['spec'].get('migration_overhead_seconds',300) for t in subset)
            if current_selection:
                before=current_selection['profile'];after=selection['profile']
                if after['slowdown']>=before['slowdown']:continue
                if not before.get('expected_seconds') or not after.get('expected_seconds'):continue
                if before['expected_seconds']-after['expected_seconds']<=max(120,2*overhead):continue
            forced=[t['id'] for t in subset if not t['spec'].get('preemptible') and force_allowed(t)]
            return dict(task=wanted['id'],victims=[t['id'] for t in subset],force_victims=forced,selection=selection,
                        priority=wanted['spec']['priority'],victim_priorities={t['id']:t['spec']['priority'] for t in subset},
                        estimated_overhead_seconds=overhead,reason='explicit urgent displacement discards listed partial attempts' if forced else ('faster measured profile after checkpoint' if current_selection else 'strictly higher priority urgent task cannot fit without checkpoint'))
    return None

def explain_constraints(spec,snapshot,config):
    diagnostics=[]
    for p in spec['profiles']:
        nodes=[n for n in snapshot['nodes'] if p['partition'] in n.get('Partitions','').split(',')]
        healthy=[n for n in nodes if not any(x in n.get('State','') for x in BAD)]
        diagnostics.append(dict(profile=p['name'],partition=p['partition'],requested=shape(p),slowdown=p['slowdown'],
            allowed_slowdown=spec['max_slowdown'],healthy_nodes=len(healthy),
            largest_free_cpu=max([int(n.get('CPUEfctv',n.get('CPUTot',0)))-int(n.get('CPUAlloc',0)) for n in healthy] or [0]),
            largest_free_mem_mib=max([int(n.get('RealMemory',0))-int(n.get('AllocMem',0)) for n in healthy] or [0]),
            nodes=[dict(node=n['NodeName'],state=n.get('State'),free_cpu=int(n.get('CPUEfctv',n.get('CPUTot',0)))-int(n.get('CPUAlloc',0)),
                        free_mem_mib=int(n.get('RealMemory',0))-int(n.get('AllocMem',0)),
                        allowed=not p.get('allowed_nodes') or n['NodeName'] in p['allowed_nodes'],
                        reservations=reservation_blockers(n['NodeName'],p,snapshot,config)) for n in nodes],
            pending_feedback={u:submission_blockers(u,p,snapshot,config) for u in spec.get('users',config['users'])},
            note='Add a validated smaller/alternative profile, raise max_slowdown within validated profiles, or wait. Memory minima and scientific inputs are never reduced automatically.'))
    return diagnostics


def fair_queue_plan(tasks,snapshot,config,all_tasks):
    """Guarded greedy group lottery; exact priority retained outside opt-in cohorts.

    Every iteration reserves the selected shape in the SAME virtual snapshot.
    Group accounting includes in-flight submissions and pending allocations.
    """
    from .fair_share import contract,identity,groups,lottery,cpu_goals
    entries=groups(all_tasks,snapshot,config)
    virtual=copy.deepcopy(snapshot);remaining={t['id']:t for t in tasks}
    plan=[];decisions={};held=set();step=0
    limit=config.get('plan_window',12)
    while remaining and len(plan)<limit:
        candidates={}; descriptors={}
        for tid,t in remaining.items():
            s=t['spec'];c=contract(s,config);descriptors[tid]=c
            if s.get('resource_key',s['workdir']) in held:continue
            if c:
                e=entries[identity(c)]
                if e['active_jobs']>=c['max_parallel'] or e['reserved_cpus']>=c['demand_cpus']:continue
            opts=choices(s,virtual,config,limit=max(8,len(s['profiles'])*len(config['users'])))
            if opts:candidates[tid]=opts
        if not candidates:break
        tier=max((int(urgent(remaining[tid]['spec'])),descriptors[tid]['tier'] if descriptors[tid] else remaining[tid]['spec']['priority']) for tid in candidates)
        eligible=[tid for tid in candidates if (int(urgent(remaining[tid]['spec'])),descriptors[tid]['tier'] if descriptors[tid] else remaining[tid]['spec']['priority'])==tier]
        ordinary=[tid for tid in eligible if not descriptors[tid]]
        if ordinary:
            tid=min(ordinary,key=lambda i:(-remaining[i]['spec']['priority'],remaining[i]['created'],i))
            sel=candidates[tid][0]
        else:
            bygroup={}
            for tid in eligible:bygroup.setdefault(identity(descriptors[tid]),[]).append(tid)
            key=lottery({k:entries[k] for k in bygroup},config,snapshot,step)[0]
            tid=min(bygroup[key],key=lambda i:(-remaining[i]['spec']['priority'],remaining[i]['created'],i))
            c=descriptors[tid];e=entries[key]
            cohort={k:copy.deepcopy(v) for k,v in entries.items() if v['contract']['cohort']==c['cohort']}
            for peer,entry in cohort.items():
                if peer not in bygroup:
                    # Borrow capacity peers cannot currently use (dependency,
                    # profile, quota, phase or max-parallel block). No fake hold.
                    entry['contract']['demand_cpus']=entry['reserved_cpus']
            goal=cpu_goals(cohort,virtual,config)[key]
            opts=candidates[tid];minimum=min(shape(o['profile'])['cpus'] for o in opts)
            # Admit at least one validated quantum; allow indivisible shape rounding.
            allowance=max(minimum,goal-e['reserved_cpus'])
            allowed=[o for o in opts if shape(o['profile'])['cpus']<=allowance+1e-9 and
                     e['reserved_cpus']+shape(o['profile'])['cpus']<=c['demand_cpus']]
            if not allowed:
                allowed=[o for o in opts if e['reserved_cpus']+shape(o['profile'])['cpus']<=c['demand_cpus']]
                if not allowed:remaining.pop(tid);continue
                smallest=min(shape(o['profile'])['cpus'] for o in allowed)
                allowed=[o for o in allowed if shape(o['profile'])['cpus']==smallest]
            sel=min(allowed,key=lambda o:selection_score(remaining[tid]['spec'],o,config))
            decisions[tid]=dict(fair_share=dict(group=key,weight=e['weight'],reserved_before=e['reserved_cpus'],cpu_goal=round(goal,1),
                                temperature=config.get('fair_share',{}).get('temperature',20.),cpu_quantum=config.get('fair_share',{}).get('cpu_quantum',32.)))
            e['reserved_cpus']+=shape(sel['profile'])['cpus'];e['allocated_cpus']+=shape(sel['profile'])['cpus'];e['active_jobs']+=1;e['queued_jobs']=max(0,e['queued_jobs']-1)
        task=remaining.pop(tid);plan.append((tid,sel));step+=1
        held.add(task['spec'].get('resource_key',task['spec']['workdir']))
        reserve(virtual,sel,'plan-'+tid,task['spec']['name'])
        decisions.setdefault(tid,{}).update(admitted=True,selection=sel,
            reason='group Boltzmann CPU fair-share admission' if descriptors[tid] else 'strict priority outside fair-share cohort')
    for t in tasks:
        if t['id'] in decisions:continue
        c=contract(t['spec'],config);alone,reason=choose(t['spec'],snapshot,config)
        decisions[t['id']]=dict(admitted=False,reason='waiting for fair-share turn or group demand/parallel limit' if alone and c else reason,
                               planning_window=limit)
        if c:decisions[t['id']]['fair_share']=dict(group=identity(c),reserved_cpus=entries[identity(c)]['reserved_cpus'])
    return plan,decisions
