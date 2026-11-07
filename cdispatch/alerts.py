"""Cache-only operational warnings shared by terminal and private bot."""
import hashlib
import json
import time


def warnings(data, now=None, pending_seconds=600, pending_since=None):
    now = time.time() if now is None else now
    snapshot = data.get('snapshot') or {}
    found = {}
    if not data or now - data.get('heartbeat', 0) > 60:
        found['heartbeat'] = 'Нет свежего heartbeat диспетчера более 60 с; показаны последние известные данные.'
    if data and now - snapshot.get('time', 0) > 90:
        found['slurm-cache'] = 'Снимок Slurm старше 90 с; текущие выделения требуют проверки.'
    if data.get('error'):
        found['controller-error'] = 'Ошибка диспетчера: ' + str(data['error'])[:500]
    if data.get('circuit'):
        found['circuit'] = 'Новые запуски заблокированы: ' + str(data['circuit'])[:500]
    jobs = {j['JobId']: j for j in snapshot.get('jobs', [])}
    for tid, task in data.get('tasks', {}).items():
        state = task['state']
        label = task['spec']['name'] + ' · ' + tid
        reason = str(task.get('reason') or '')
        if state in ('SUBMIT_UNKNOWN', 'BLOCKED') or (state == 'HELD' and not task.get('manual_hold')):
            found['task:' + tid + ':' + state] = label + '\n' + state + ': ' + reason[:400]
        if state == 'PENDING':
            submitted = [e['time'] for e in data.get('events', [])
                         if e.get('task') == tid and e['kind'] in ('SUBMITTED', 'SUBMISSION_RECOVERED')
                         and e.get('job_id') == task.get('job_id')]
            # Use the attempt's creation, never the task's time in the local queue.
            attempt_times = [a['time'] for a in task.get('attempts', [])[-1:] if a.get('time')]
            job = jobs.get(task.get('job_id'), {})
            job_times = []
            if job.get('JobState') == 'PENDING':
                try: job_times = [time.mktime(time.strptime(job['SubmitTime'], '%Y-%m-%dT%H:%M:%S'))]
                except (KeyError, ValueError): pass
            observed = (pending_since or {}).get(tid + ':' + str(task.get('job_id')), now)
            since = max(submitted or attempt_times or job_times or [observed])
            if now - since >= pending_seconds:
                job = jobs.get(task.get('job_id'), {})
                found['pending:' + tid + ':' + str(task.get('job_id'))] = (
                    label + '\nSlurm ' + str(task.get('job_id')) + ' ждёт RUNNING более '
                    + str(pending_seconds // 60) + ' мин: ' + str(job.get('Reason') or reason))
    return found


def health(data, bot=None, service=None, now=None):
    now = time.time() if now is None else now
    found = warnings(data, now)
    if service is not None and now - service.get('time', 0) > 30:
        found['supervisor'] = 'Supervisor службы не обновлялся более 30 с.'
    if bot is not None:
        if bot.get('error'): found['telegram-error'] = 'Ошибка Telegram: ' + str(bot['error'])
        if now - bot.get('time', 0) > 90: found['telegram-stale'] = 'Бот не обновлял состояние более 90 с.'
    return dict(ok=not found, time=now, warnings=found,
                heartbeat_age=now-data.get('heartbeat', 0),
                slurm_age=now-(data.get('snapshot') or {}).get('time', 0),
                telegram_notifications=(bot or {}).get('notifications'))


def event_warning(event):
    kind = event['kind']
    if kind == 'TERMINAL':
        if event.get('state') != 'FAILED' and not event.get('external'): return None
    elif kind == 'EXTERNAL_JOB_TERMINAL':
        # Allocation probes are explicitly stopped by their recorded-ID cleanup.
        if str(event.get('name', '')).startswith(('cd-cpu-quota-', 'cd-running-quota-', 'cd-quota-')): return None
        if str(event.get('slurm_state', '')).split()[0] not in ('FAILED', 'TIMEOUT', 'OUT_OF_MEMORY', 'NODE_FAIL', 'BOOT_FAIL', 'CANCELLED'): return None
    elif kind not in ('REQUEST_REJECTED', 'CHECKPOINT_REJECTED', 'ADOPTION_REJECTED'):
        return None
    identity = event.get('task') or event.get('name') or event.get('path') or 'Диспетчер'
    detail = event.get('error') or event.get('reason') or event.get('detail') or event.get('slurm_state') or event.get('state') or kind
    return str(identity) + '\n' + kind + ': ' + str(detail)[:500] + (
        '\nSlurm ' + str(event['job_id']) if event.get('job_id') else '')


def event_key(event):
    return hashlib.sha256(json.dumps(event, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
