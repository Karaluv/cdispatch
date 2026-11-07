"""Keep application failures local to attempts; retain infrastructure circuits."""
def circuit_failure(event, tasks):
    if event.get('kind') != 'TERMINAL' or event.get('state') != 'FAILED':
        return False
    raw = event.get('slurm_state', '').split(' ', 1)[0].rstrip('+')
    if raw == 'PREEMPTED':
        return False
    return not (raw in ('FAILED', 'COMPLETED') and event.get('expected_time_budget'))



def workload_scope(task):
    spec = task.get('spec', {})
    group = spec.get('group')
    if group and group != 'default':
        return 'group:' + group
    labels = spec.get('labels', {})
    return 'application:' + str((labels.get('project', ''), spec.get('name', '')))


def failure_scope(event, tasks):
    if not circuit_failure(event, tasks):
        return None
    task = tasks.get(event.get('task'), {})
    # Only a completed wrapper's explicit application result is evidence for
    # narrowing the circuit. OOM/node/wall failures and missing results stay global.
    attempts = task.get('attempts', [])
    attempt = next((a for a in reversed(attempts)
                    if str(a.get('job_id')) == str(event.get('job_id'))), {})
    result = attempt.get('result') or {}
    raw = event.get('slurm_state', '').split(' ', 1)[0].rstrip('+')
    # A shared handler's untyped exit is local to that handler, not evidence
    # that several independent allocations failed across the whole pool.
    if event.get('shared') and raw not in ('OUT_OF_MEMORY', 'NODE_FAIL', 'BOOT_FAIL'):
        return workload_scope(task)
    if (result.get('state') == 'FAILED' and result.get('reason') == 'application/validation'
            and raw in ('FAILED', 'COMPLETED')):
        return workload_scope(task)
    return 'global'


def blocked_reason(data, task):
    if data.get('circuit'):
        return 'controller circuit: ' + data['circuit'] + '; inspect events, then clear-circuit'
    return None


def filter_ready(ready, data):
    allowed, blocked = [], {}
    for task in ready:
        reason = blocked_reason(data, task)
        if reason:
            blocked[task['id']] = dict(reason=reason, terminal=False)
        else:
            allowed.append(task)
    return allowed, blocked


def update_circuits(daemon):
    from .task_history import TaskIndex
    state = daemon.state
    tasks = TaskIndex(state['tasks'], daemon.root) if daemon.history else state['tasks']
    import time
    now = time.time()
    # Application failures must never stop independent work. Retire flags
    # written by older versions through ordinary durable state persistence.
    legacy = state.pop('workload_circuits', {})
    if legacy:
        state.setdefault('retired_workload_circuits', []).append(dict(
            time=now, reason='application failure bursts no longer block admission',
            circuits=legacy))
        daemon.event(None, 'APPLICATION_CIRCUITS_RETIRED', scopes=list(legacy))
    reset = state.get('circuit_reset_at', {})
    bursts = {}
    for event in state['events']:
        if event['time'] <= now - 600 or event.get('kind') != 'TERMINAL' or event.get('state') != 'FAILED':
            continue
        scope = failure_scope(event, tasks)
        if scope != 'global' or event['time'] <= reset.get(scope, 0):
            continue
        bursts.setdefault(scope, []).append(event)
    for scope, events in bursts.items():
        events = list({e.get('job_id') or e.get('task'): e for e in events}.values())
        if len(events) < daemon.config.get('failure_burst_limit', 3):
            continue
        if state.get('circuit'):
            continue
        state['circuit'] = 'repeated failures within 10 minutes'
        daemon.event(None, 'CIRCUIT_OPEN', scope=scope,
                     failures=[{k: e.get(k) for k in ('task', 'job_id', 'time', 'reason')} for e in events])
