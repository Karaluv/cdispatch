"""Pure resource accounting aggregation shared by CLI and monitor contracts."""
from .common import tres, mib, seconds


def report(d):
    if "resource_totals" in d:return d["resource_totals"]
    rows=[r for r in d.get('accounting',[]) if '.' not in r['id']]
    task_by_job={a.get('job_id'):t for t in d.get('tasks',{}).values() for a in t['attempts'] if not a.get('shared')}
    groups={}
    for r in rows:
        t=task_by_job.get(r['id']);label=t['spec'].get('labels',{}).get('project','unlabelled') if t else 'shared-pool' if r.get('name')=='cd-service' else 'external'
        key=r['user']+' / '+label;g=groups.setdefault(key,dict(allocated_cpu_hours=0,actual_cpu_hours=0,gpu_hours=0,allocated_gib_hours=0,jobs=0,failed=0,cancelled=0))
        tr=tres(r['tres']);elapsed=float(r['elapsed'] or 0)
        g['allocated_cpu_hours']+=float(r['cpu_seconds'] or 0)/3600
        g['actual_cpu_hours']+=seconds(r['total_cpu'] or '00:00:00')/3600
        g['gpu_hours']+=int(tr.get('gres/gpu',0))*elapsed/3600
        g['allocated_gib_hours']+=mib(tr.get('mem','0'))/1024*elapsed/3600
        g['jobs']+=1;g['failed']+=r['state'].split()[0] in ('FAILED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','BOOT_FAIL')
        g['cancelled']+=r['state'].startswith('CANCELLED')
    return groups
