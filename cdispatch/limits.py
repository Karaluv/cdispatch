"""Scoped Slurm admission checks and deduplicated physical capacity reports."""
from .common import tres, mib, seconds

EMPTY = (None, '', 'N/A', 'NONE', 'UNLIMITED', 'INFINITE', '(null)', '-1')


def count(value):
    return None if value in EMPTY else int(value)


def ancestry(account, parents):
    result = [];seen = set()
    while account and account not in seen:
        result.append(account);seen.add(account);account = parents.get(account, '')
    return result


def job_count(job):
    """A compressed array record represents every listed element, including strides."""
    import re
    expression = job.get('ArrayTaskId', '').split('%')[0].strip('[]')
    if not expression or expression in ('N/A', '4294967294'):
        return 1
    total = 0
    for item in expression.split(','):
        match = re.fullmatch(r'(\d+)(?:-(\d+)(?::(\d+))?)?', item)
        if not match:
            return 1
        first, last, step = match.groups()
        total += (int(last or first) - int(first)) // int(step or 1) + 1
    return total


def live_blockers(user, account, profile, snapshot):
    """Max precedence follows Slurm; all Grp limits intersect their own usage scopes.

    Running limits use running allocations and provisional planner reservations;
    submit limits include held/pending jobs. Never infer global QoS/ancestor usage
    from the dispatcher's three users. Timed/relative entitlements need a richer
    accounting source and explicitly block admission while enforcement is on.
    """
    from .scheduler import shape, SLURM_ACTIVE, memory
    audit = snapshot.get('limits', {})
    if audit.get('enforcement_enabled') is False:
        return []
    associations = audit.get('associations', [])
    parents = audit.get('account_parents', {}) or {
        a['account']:a.get('parent', '') for a in associations if not a.get('user')}
    chain = ancestry(account['account'], parents)
    matching = [a for a in associations if a.get('account') == account['account']
                and a.get('user') == user and a.get('partition', '') in ('', profile['partition'])]
    matching.sort(key=lambda a:bool(a.get('partition')), reverse=True)
    inherited = [a for name in chain for a in associations
                 if a.get('account') == name and not a.get('user')
                 and a.get('partition', '') in ('', profile['partition'])]
    qos_name = account.get('qos') or next((a.get('default_qos') or
                 a.get('qos', '').split(',')[0] for a in matching if a.get('default_qos') or a.get('qos')), '')
    job_qos = next((q for q in audit.get('qos', []) if q['name'] == qos_name), {})
    part_qos_name = snapshot['partitions'].get(profile['partition'], {}).get('QoS', '')
    part_qos = next((q for q in audit.get('qos', []) if q['name'] == part_qos_name), {})
    qos_order = ([job_qos, part_qos] if 'OverPartQOS' in job_qos.get('flags', '').split(',')
                 else [part_qos, job_qos])
    qos_order = [q for q in qos_order if q]
    rows = [(q, 'QoS '+q['name']) for q in qos_order]
    rows += [(a, 'association '+a['account']+('/'+user if a.get('user') else '')) for a in matching+inherited]
    complete = 'limit_jobs' in snapshot and audit.get('full_usage_visible') is not False
    all_jobs = snapshot.get('limit_jobs', snapshot.get('account_jobs', snapshot.get('jobs', [])))
    # One record per allocation; steps and repeated scope memberships never add copies.
    jobs = {j['JobId']:j for j in all_jobs if j.get('JobState') in SLURM_ACTIVE}
    wanted = shape(profile)
    request = dict(cpu=wanted['cpus'], mem=wanted['mem_mib'], node=wanted['nodes'])
    request['gres/gpu'] = wanted['gpus']
    if profile.get('gpu_type'):
        request['gres/gpu:'+profile['gpu_type']] = wanted['gpus']
    per_node = {k:v/profile['nodes'] for k,v in request.items()}
    per_node['node'] = 1
    blocked = []

    def add(source, dimension, used, requested, cap, scope):
        if cap is not None and used+requested > cap:
            blocked.append(dict(source=source, dimension=dimension, used=used,
                                requested=requested, limit=cap, scope=scope))

    def running(items):
        return [j for j in items if j.get('JobState') != 'PENDING' or j.get('Virtual') == 'YES']

    def totals(items, key):
        total = 0
        for j in items:
            allocated = j.get('AllocTRES', '')
            values = tres(allocated if allocated not in EMPTY else j.get('ReqTRES', ''))
            value = values.get(key, '0')
            total += (mib(value) if key == 'mem' else int(str(value).split('-')[-1])) * job_count(j)
        return total

    def check_tres(value, items, source, scope, standalone=None, minimum=False):
        for key, raw in tres(value or '').items():
            cap = mib(raw) if key == 'mem' else count(raw)
            if cap is None:
                continue
            if key not in request:
                blocked.append(dict(source=source, dimension=key, scope=scope,
                                    error='TRES entitlement needs explicit request/usage integration'))
                continue
            req = (standalone or request)[key]
            if minimum:
                if req < cap:
                    blocked.append(dict(source=source, dimension=key, requested=req, minimum=cap, scope=scope))
            else:
                add(source, key, totals(running(items), key) if standalone is None else 0, req, cap, scope)

    def scoped(row, source, items, scope, prefix):
        if any(row.get(k) not in EMPTY for k in ('group_tres_mins','group_tres_run_mins','group_wall')):
            blocked.append(dict(source=source, scope=scope, error='timed group limit needs accounting usage audit'))
        add(source, prefix+'jobs', sum(job_count(j) for j in running(items)), 1,
            count(row.get(prefix+'jobs')), scope)
        add(source, prefix+'submit', sum(job_count(j) for j in items), 1,
            count(row.get(prefix+'submit')), scope)
        check_tres(row.get(prefix+'tres'), items, source, scope)

    seen_max = set()
    def first_tres(row, key, source, scope, items=None, standalone=None, minimum=False):
        selected = {}
        for dimension, value in tres(row.get(key, '') or '').items():
            marker = key+':'+dimension
            if marker not in seen_max and value not in EMPTY:
                seen_max.add(marker);selected[dimension] = value
        check_tres(','.join(k+'='+v for k,v in selected.items()), items or [], source,
                   scope, standalone=standalone, minimum=minimum)

    for row, source in rows:
        is_qos = 'name' in row
        if is_qos:
            if 'Relative' in row.get('flags', '').split(',') or row.get('limit_factor') not in EMPTY+("1", "1.0", "1.000000"):
                blocked.append(dict(source=source, error='relative QoS/LimitFactor needs explicit entitlement integration'))
            names = {p['PartitionName'] for p in snapshot['partitions'].values()
                     if p.get('QoS') == row['name'] and p.get('PartitionName')}
            qjobs = [j for j in jobs.values() if j.get('QOS') == row['name'] or
                     (row is part_qos and (j.get('Partition') in names or
                      snapshot['partitions'].get(j.get('Partition'), {}).get('QoS') == row['name']))]
            uitems = [j for j in qjobs if j.get('UserId', '').split('(')[0] == user]
            aitems = [j for j in qjobs if account['account'] in ancestry(j.get('Account', ''), parents)]
            if not complete and any(row.get(k) not in EMPTY for k in ('group_jobs','group_submit','group_tres','account_jobs','account_submit','account_tres')):
                blocked.append(dict(source=source, error='group QOS requires full usage audit'))
                continue
            scoped(row, source, qjobs, 'qos', 'group_')
            for prefix, items, scope in [('user_',uitems,'qos/user'), ('account_',aitems,'qos/account')]:
                for suffix in ('jobs','submit','tres'):
                    key = prefix+suffix
                    # Partition QoS overrides job QoS for the same Max field.
                    if key in seen_max or row.get(key) in EMPTY:
                        continue
                    if suffix == 'tres':
                        first_tres(row,key,source,scope,items=items)
                        continue
                    seen_max.add(key)
                    one = {key:row[key]};scoped(one, source, items, scope, prefix)
        else:
            if row.get('user'):
                items = [j for j in jobs.values() if j.get('Account',account['account']) == account['account']
                         and j.get('UserId','').split('(')[0] == user
                         and (not row.get('partition') or j.get('Partition') == row['partition'])]
            else:
                items = [j for j in jobs.values() if row['account'] in ancestry(j.get('Account',account['account']), parents)]
                if not complete and (row['account'] != account['account'] or audit.get('full_usage_visible') is False) and any(row.get(k) not in EMPTY for k in ('group_jobs','group_submit','group_tres')):
                    blocked.append(dict(source=source, error=('ancestor account group limit' if row['account'] != account['account'] else 'account group limit')+' needs full usage audit'))
                    continue
            scoped(row, source, items, 'association group', 'group_')
            user_items = [j for j in jobs.values() if j.get('UserId','').split('(')[0] == user
                          and j.get('Account',account['account']) == account['account']
                          and (not row.get('partition') or j.get('Partition') == row['partition'])]
            for key, preceding in [('max_jobs','user_jobs'), ('max_submit','user_submit')]:
                if preceding in seen_max or row.get(key) in EMPTY:
                    continue
                seen_max.add(preceding)
                add(source,key,sum(job_count(j) for j in (running(user_items) if key=='max_jobs' else user_items)),1,count(row[key]),'user association')
        for key in ('job_tres','node_tres','min_job_tres','max_wall','job_tres_mins'):
            if key in seen_max or row.get(key) in EMPTY:
                continue
            if key == 'max_wall':
                seen_max.add(key)
                add(source,'wall_seconds',0,profile['wall_minutes']*60,seconds(row[key]),'job')
            elif key == 'job_tres_mins':
                first_tres(row,key,source,'job TRES minutes',standalone={k:v*profile['wall_minutes'] for k,v in request.items()})
            else:
                first_tres(row,key,source,'node' if key=='node_tres' else 'job',
                           standalone=per_node if key=='node_tres' else request,minimum=key=='min_job_tres')
    return blocked


def report(snapshot, config):
    from .scheduler import BAD, usage
    nodes = {n['NodeName']:n for n in snapshot.get('nodes', [])
             if not any(b in n.get('State','') for b in BAD)
             and n['NodeName'] not in config.get('excluded_nodes', [])}
    def capacity(partitions):
        selected = [n for n in nodes.values() if set(n.get('Partitions','').split(',')) & set(partitions)]
        return dict(node_names=sorted(n['NodeName'] for n in selected),
                    cpus=sum(int(n.get('CPUEfctv',n.get('CPUTot',0))) for n in selected),
                    gpus=sum(int(tres(n.get('CfgTRES','')).get('gres/gpu',0)) for n in selected),
                    mem_mib=sum(int(n.get('RealMemory',0)) for n in selected),
                    free_cpus=sum(max(0,int(n.get('CPUEfctv',n.get('CPUTot',0)))-int(n.get('CPUAlloc',0))) for n in selected),
                    free_mem_mib=sum(max(0,int(n.get('RealMemory',0))-int(n.get('AllocMem',0))) for n in selected))
    parts = {p for a in config['users'].values() if a.get('enabled') for p in a.get('partitions', [])}
    audit=snapshot.get('limits',{})
    associations=audit.get('associations',[])
    parents=audit.get('account_parents',{})
    def entitlement(user,account):
        own=[a for a in associations if a.get('account')==account['account'] and a.get('user')==user]
        chosen=account.get('qos') or next((a.get('default_qos') or a.get('qos','').split(',')[0]
                                        for a in own if a.get('default_qos') or a.get('qos')),'')
        return dict(account=account['account'],ancestor_chain=ancestry(account['account'],parents),
                    selected_qos=chosen,associations=own,
                    qos=next((q for q in audit.get('qos',[]) if q['name']==chosen),{}),
                    database_limits_enforced=audit.get('enforcement_enabled'))
    return dict(time=snapshot.get('time'), limits=snapshot.get('limits',{}),
                local_policy=config['pool_limits'], pool_usage=usage(snapshot.get('jobs',[])),
                physical_pool=capacity(parts), users={u:dict(local_policy=a['limits'],
                    entitlement=entitlement(u,a),
                    usage=usage(snapshot.get('jobs',[]),u), physical_capacity=capacity(a.get('partitions',[])))
                    for u,a in config['users'].items() if a.get('enabled')},
                cross_user_associations=[a for a in associations if not a.get('user')],
                cross_user_qos=[q for q in audit.get('qos',[]) if q['name'] in
                    {entitlement(u,a)['selected_qos'] for u,a in config['users'].items() if a.get('enabled')} or
                    q['name'] in {p.get('QoS') for p in snapshot.get('partitions',{}).values()}],
                partitions=snapshot.get('partitions',{}),
                note='Physical nodes are deduplicated across users/partitions; user capacities overlap and must not be summed. Null local ceilings leave hardware and enforced Slurm limits in charge.')
