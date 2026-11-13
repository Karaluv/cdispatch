"""Evidence-based expected geometry budget exhaustion, preserving raw failure."""
import hashlib,json
from pathlib import Path

def expected_geometry_timeout(task, slurm_state, job_id=None):
    # OOM/node failures and unexplained Slurm wall kills remain circuit failures.
    if not isinstance(slurm_state,str) or slurm_state.split(' ',1)[0].rstrip('+') not in ('FAILED','COMPLETED'):
        return False
    try:
        actual_job=str(task['job_id'])
        if not actual_job or (job_id is not None and str(job_id)!=actual_job):return False
        command=task['attempts'][-1]['selection']['profile']['command']
        if not {'cdispatch.adapters','cdispatch_research.adapters'}.intersection(command):return False
        i=command.index('run-geometry');request=Path(command[i+1])
        if hashlib.sha256(request.read_bytes()).hexdigest()!=command[-1]:return False
        req=json.loads(request.read_text());point=Path(req['point'])
        if float(req['execution_budget_seconds'])<=0:return False
        result=json.loads(Path(str(request)+'.result.json').read_text())
        if result.get('ok') or not result.get('time_budget_exhausted'):return False
        for path in point.glob('*/execution.json'):
            ev=json.loads(path.read_text())
            if ev.get('returncode')==124 and str(ev.get('slurm_job_id'))==actual_job:
                return True
        # Startup can legitimately have <10 min left in a declared allocation.
        budget=json.loads(Path(str(request)+'.budget.json').read_text())
        return (str(budget.get('slurm_job_id'))==actual_job and
                0<=float(budget['execution_seconds'])<600 and
                'stopped_time_budget' in result.get('error',''))
    except (OSError,ValueError,KeyError,IndexError,TypeError,AttributeError):
        return False

def circuit_failure(event,tasks):
    if event.get('kind')!='TERMINAL' or event.get('state')!='FAILED':return False
    if event.get('slurm_state','').split(' ',1)[0].rstrip('+')=='PREEMPTED':return False
    if event.get('expected_time_budget'):return False
    return not expected_geometry_timeout(tasks.get(event.get('task'),{}),event.get('slurm_state',''),event.get('job_id'))
