"""Optional persistent evidence that a replacement service loaded its queue."""
import time
from pathlib import Path
from .common import atomic, read


def audit(root, job_id, plan, now=None):
    now = time.time() if now is None else now
    if str(job_id) != str(plan['job_id']):
        return False
    status = read(Path(root) / 'status.json', {})
    pool = (status.get('snapshot') or {}).get('shared_pool') or {}
    if pool.get('job_id') != str(job_id) or now - status.get('heartbeat', 0) > 60:
        return False
    tasks = status.get('tasks', {})
    expected = plan.get('tasks', [])
    missing = [tid for tid in expected if tid not in tasks]
    report = dict(time=now, job_id=str(job_id), predecessor=plan.get('predecessor'),
                  ok=not missing and not status.get('paused') and not status.get('circuit') and not status.get('error'),
                  missing=missing, paused=status.get('paused'), circuit=status.get('circuit'), error=status.get('error'),
                  tasks={tid:dict(state=tasks[tid]['state'],job_id=tasks[tid].get('job_id'),reason=tasks[tid].get('reason')) for tid in expected if tid in tasks},
                  note='Durable tasks loaded; existing batch jobs reconciled. Queued tasks remain subject to normal admission. Codex shared session is not migrated.')
    atomic(Path(plan['report']), report)
    return report['ok']
