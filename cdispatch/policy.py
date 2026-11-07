"""Explain Slurm feedback and avoid repeating known blocked submissions."""
from datetime import datetime
import re
import time

from .common import mib, tres


def pending_advice(reason):
    reason = (reason or 'None').strip('()')
    if reason in ('Dependency', 'BeginTime', 'JobArrayTaskLimit'):
        return dict(category='dependency', action='wait', scope='none',
                    explanation='Wait for dependency, eligibility time or existing array throttle.')
    if reason in ('DependencyNeverSatisfied', 'InvalidAccount', 'InvalidQOS', 'AccountNotAllowed',
                  'QOSNotAllowed', 'BadConstraints', 'PartitionConfig', 'PartitionTimeLimit',
                  'JobHeldAdmin', 'JobHeldUser') or reason.startswith(('QOSMax', 'AssocMax')):
        return dict(category='configuration', action='operator_review', scope='user_partition',
                    explanation='Inspect account/profile/limits or held job; do not repeat this blocked request.')
    if reason.startswith(('QOSGrp', 'AssocGrp')):
        return dict(category='quota', action='wait_for_shared_quota',
                    scope='qos_group' if reason.startswith('QOSGrp') else 'association_group',
                    explanation='Shared quota blocks every user in its scope; account switching does not create extra entitlement.')
    if reason in ('AssociationJobLimit', 'QOSJobLimit', 'AccountingPolicy'):
        return dict(category='quota', action='wait_or_other_account', scope='user_partition',
                    explanation='Inspect the quota scope before selecting another authorized account/profile.')
    if reason in ('Priority', 'Resources'):
        return dict(category='priority' if reason == 'Priority' else 'resources',
                    action='wait_or_smaller_profile', scope='shape',
                    explanation='Keep this Slurm queue position; consider a strictly smaller qualified shape or another partition.')
    if reason.startswith(('ReqNodeNotAvail', 'Nodes required')) or reason in ('PartitionDown', 'PartitionInactive'):
        return dict(category='nodes', action='wait_or_alternative_profile', scope='shape',
                    explanation='Check node/partition health and reservations; use an available qualified alternative.')
    return dict(category='scheduler', action='observe', scope='none',
                explanation='Observe the next cached Slurm snapshot; an unfamiliar reason never triggers automatic restart.')


def feedback(snapshot):
    result = []
    for job in snapshot.get('jobs', []):
        if job.get('JobState') != 'PENDING':
            continue
        result.append(dict(job_id=job['JobId'], user=job.get('UserId', '').split('(')[0],
                           partitions=job.get('Partition', '').split(','),
                           reason=job.get('Reason', 'None'), **pending_advice(job.get('Reason'))))
    return result


def host_matches(expression, name):
    """Match Slurm's ordinary comma-separated host/range syntax without RPC."""
    for item in re.findall(r'[^,\[\]]+(?:\[[^\]]*\][^,]*)?', expression or ''):
        if '[' not in item:
            if item == name:
                return True
            continue
        match = re.fullmatch(r'([^\[]*)\[([^\]]+)\](.*)', item)
        if not match:
            continue
        prefix, ranges, suffix = match.groups()
        number = re.fullmatch(re.escape(prefix) + r'(\d+)' + re.escape(suffix), name)
        if not number:
            continue
        for span in ranges.split(','):
            parts = span.split('-')
            if all(part.isdigit() for part in parts):
                lo, hi = int(parts[0]), int(parts[-1])
                if lo <= int(number[1]) <= hi:
                    return True
    return False


def planned_backfill_fits(node, profile, snapshot, config):
    """An opt-in PLANNED node needs a known, fresh, bounded backfill window."""
    now = time.time()
    stamp = snapshot.get('time')
    if stamp is None or now - stamp > max(30, 2 * config.get('poll_interval', 10)):
        return False
    if not config.get('respect_slurm_reservations', True):
        return False
    finish = now + profile['wall_minutes'] * 60 + config.get('backfill_margin_seconds', 120)
    starts = []
    for job in snapshot.get('queue_jobs', []):
        if job.get('JobState') != 'PENDING' or not host_matches(job.get('SchedNodeList'), node):
            continue
        try:
            start = datetime.fromisoformat(job.get('StartTime', '')).timestamp()
        except (TypeError, ValueError):
            return False
        starts.append(start)
    return bool(starts) and min(starts) > finish


def reservation_blockers(node, profile, snapshot, config):
    if not config.get('respect_slurm_reservations', True):
        return []
    now = snapshot.get('time', time.time())
    finish = now + profile['wall_minutes'] * 60 + config.get('backfill_margin_seconds', 120)
    result = []
    for job in snapshot.get('queue_jobs', []):
        if job.get('JobState') != 'PENDING':
            continue
        if not host_matches(job.get('SchedNodeList'), node):
            continue
        try:
            start = datetime.fromisoformat(job.get('StartTime', '')).timestamp()
        except (TypeError, ValueError):
            continue
        if start <= finish:
            result.append(dict(job_id=job['JobId'], start=job['StartTime'],
                               reason=job.get('Reason'), node=node))
    return result


def submission_blockers(user, profile, snapshot, config):
    if not config.get('respect_pending_feedback', True):
        return []
    result = []
    cpus = profile['nodes'] * profile['ranks_per_node'] * profile['threads']
    from .scheduler import memory
    mem = memory(profile) * profile['nodes']
    gpus = profile.get('gpus_per_node', 0) * profile['nodes']
    from .limits import ancestry
    audit=snapshot.get('limits',{})
    associations=audit.get('associations',[])
    account=config['users'][user]
    qos=account.get('qos') or next((a.get('default_qos') or a.get('qos','').split(',')[0]
        for a in associations if a.get('user')==user and a.get('account')==account['account']),'')
    partition_qos=snapshot['partitions'].get(profile['partition'],{}).get('QoS','')
    candidates={j['JobId']:j for j in snapshot.get('limit_jobs',snapshot.get('jobs',[]))}
    candidates.update({j['JobId']:j for j in snapshot.get('jobs',[])})
    for job in candidates.values():
        if job.get('JobState') != 'PENDING':
            continue
        advice = pending_advice(job.get('Reason'))
        if advice['scope'] == 'none':
            continue
        if advice['scope']=='qos_group':
            job_qos=job.get('QOS')
            part_qos=snapshot['partitions'].get(job.get('Partition',''),{}).get('QoS','')
            if job_qos or part_qos:
                if not ({qos,partition_qos}-{'' ,'N/A'}) & ({job_qos,part_qos}-{None,'' ,'N/A'}):continue
            elif job.get('UserId','').split('(')[0]!=user:continue
        elif advice['scope']=='association_group':
            job_account=job.get('Account',account['account'])
            parents=audit.get('account_parents',{})
            common=set(ancestry(job_account,parents)) & set(ancestry(account['account'],parents))
            shared={a['account'] for a in associations if not a.get('user') and any(a.get(k) for k in ('group_jobs','group_submit','group_tres'))}
            if job_account!=account['account'] and not common & shared:continue
        else:
            if job.get('UserId','').split('(')[0]!=user:continue
            if profile['partition'] not in job.get('Partition','').split(','):continue
        if advice['scope'] == 'shape':
            # Smaller jobs may backfill. CPU/GPU shapes are not interchangeable.
            requested = tres(job.get('ReqTRES', ''))
            previous = (int(requested.get('cpu', 0)), mib(requested.get('mem', '0')),
                        int(requested.get('gres/gpu', 0)), int(str(requested.get('node', '1')).split('-')[-1]))
            wanted = (cpus, mem, gpus, profile['nodes'])
            if all(a <= b for a, b in zip(wanted, previous)) and any(a < b for a, b in zip(wanted, previous)):
                continue
        result.append(dict(job_id=job['JobId'], reason=job.get('Reason'), **advice))
    return result
