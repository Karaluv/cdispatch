"""Durable full task records and compact scheduler dependency/status records.

Archives are immutable and content addressed. They are published before the
state file references them. Active and ambiguous tasks always remain complete.
"""
import hashlib
import json
from pathlib import Path
import re
import time

from .common import TERMINAL, atomic, read, task_identifier
from .state_reader import JSONStream
from .record_views import task_view


def digest(task):
    return hashlib.sha256(json.dumps(task, ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':'), allow_nan=False).encode()).hexdigest()


class TaskHistory:
    def __init__(self, root):
        self.root = Path(root)

    def load(self, task):
        ref = task.get('_history')
        if not ref:
            return task
        tid = task_identifier(task['id'])
        if ref.get('version') != 1 or not re.fullmatch(r'[0-9a-f]{64}', ref.get('sha256', '')):
            raise ValueError('Unsupported or invalid task history reference')
        path = self.root/'history'/'tasks'/tid/(ref['sha256']+'.json')
        record = read(path)
        if record is None or record.get('id') != tid or digest(record) != ref['sha256']:
            raise ValueError('Missing or corrupt task history: '+tid)
        return record

    def pack(self, task):
        if task.get('_history'):
            self.load(task)  # Validate persisted references without retaining full history.
            return task
        if task['state'] not in TERMINAL:
            return task
        tid = task_identifier(task['id'])
        checksum = digest(task)
        path = self.root/'history'/'tasks'/tid/(checksum+'.json')
        if not path.exists():
            atomic(path, task)
        summary = task_view(task)
        summary['spec'].update({k: v for k, v in task['spec'].items() if k in (
            'group', 'parent', 'depends_on', 'workdir', 'users', 'fair_share',
            'restart_from_scratch_safe', 'preemptible')})
        summary['attempts'] = [{k: v for k, v in attempt.items() if k in (
            'number', 'time', 'path', 'job_id', 'shared', 'state', 'step_id')}
            for attempt in task.get('attempts', [])]
        for compact, attempt in zip(summary['attempts'], task.get('attempts', [])):
            if attempt.get('result'):
                compact['result'] = {k: v for k, v in attempt['result'].items()
                                     if k in ('state', 'returncode', 'reason', 'finished')}
            if attempt.get('driver'):
                compact['driver'] = {k: v for k, v in attempt['driver'].items()
                                     if k in ('returncode', 'time', 'error')}
        # Geometry lifecycle and cancellation attribution need raw terminal state.
        if task.get('accounting'):
            summary['accounting'] = {k: v for k, v in task['accounting'].items()
                                     if k in ('id', 'state', 'exit_code', 'elapsed')}
        summary['_history'] = dict(version=1, sha256=checksum)
        return summary

    def compact(self, state):
        # Keep recent full outcomes available for authenticated circuit decisions.
        protected = {e.get('task') for e in state.get('events', [])
                     if e.get('time', 0) >= time.time()-600}
        for tid, task in state['tasks'].items():
            if tid not in protected and not task.get('_history'):
                state['tasks'][tid] = self.pack(task)

    def read_state(self, path, default):
        try:
            with Path(path).open(encoding='utf-8') as stream:
                state=JSONStream(stream, pool_keys=True).root(task_transform=self.pack)
                if state.get('task_record_schema_version',1) not in (1,2):
                    raise ValueError('Unsupported scheduler task record version')
                return state
        except FileNotFoundError:
            return default

    def expand(self, state):
        """Reversible migration: reconstruct current full records for rollback."""
        result=dict(state)
        result['tasks']={tid:self.load(task) for tid,task in state['tasks'].items()}
        result['task_record_schema_version']=1
        return result


class TaskIndex(dict):
    """Explicit lookups load one full record; iteration stays compact and bounded."""
    def __init__(self, tasks, root):
        super().__init__(tasks)
        self.history = TaskHistory(root)

    def __getitem__(self, key):
        return self.history.load(super().__getitem__(key))

    def get(self, key, default=None):
        value = super().get(key)
        return self.history.load(value) if value is not None else default
