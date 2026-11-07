"""Local spool API for controllers; reads cached status, emits no Slurm RPC."""
from pathlib import Path
import time

from .common import enqueue, request, read, task_identifier, TERMINAL


class DispatcherUnavailable(RuntimeError):
    """Keep existing worker IDs and wait for a fresh controller snapshot."""


class RequestRejected(RuntimeError):
    """The controller rejected a spool request; no successful action is implied."""


class Client:
    def __init__(self, root, max_age=60):
        self.root = Path(root)
        self.max_age = max_age

    def submit(self, spec, key=None):
        return enqueue(self.root, spec, key)

    def cancel(self, ids):
        if isinstance(ids, str):
            ids = [ids]
        ids = [task_identifier(tid) for tid in ids]
        return [request(self.root, 'cancel', tid) for tid in ids]

    def cancel_group(self, group):
        if not isinstance(group, str) or not group:
            raise ValueError('Expected group name')
        return request(self.root, 'cancel-group', group)

    def snapshot(self, fresh=True):
        data = read(self.root / 'status.json', {})
        if fresh:
            now = time.time()
            if now - data.get('heartbeat', 0) > self.max_age:
                raise DispatcherUnavailable('Dispatcher heartbeat stale; preserve existing workers and wait')
            if data.get('error'):
                raise DispatcherUnavailable('Dispatcher observation failed: ' + data['error'])
            if now - (data.get('snapshot') or {}).get('time', 0) > self.max_age:
                raise DispatcherUnavailable('Slurm snapshot stale; preserve existing workers and wait')
        from .task_history import TaskIndex
        data['tasks'] = TaskIndex(data.get('tasks', {}), self.root)
        return data

    def receipt(self, token):
        task_identifier(token)
        receipt = read(self.root / 'receipts' / (token + '.json'))
        if receipt and not receipt.get('ok'):
            raise RequestRejected(receipt.get('error', 'Request rejected'))
        return receipt

    def status(self, tid, fresh=False):
        task_identifier(tid)
        data = self.snapshot(fresh=fresh)
        task = data.get('tasks', {}).get(tid)
        if task is None and not (self.root / 'inbox' / ('submit-' + tid + '.json')).exists():
            receipt = read(self.root / 'receipts' / ('submit-' + tid + '.json'))
            if receipt and not receipt.get('ok'):
                raise RequestRejected(receipt.get('error', 'Submission rejected'))
        return task

    def states(self):
        return {tid: task['state'] for tid, task in self.snapshot()['tasks'].items()}

    def wait_request(self, token, timeout, poll_interval=0.5):
        until = time.monotonic() + timeout
        while True:
            receipt = self.receipt(token)
            if receipt is not None:
                return receipt
            remaining = until - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(token)
            time.sleep(min(poll_interval, remaining))

    def wait(self, tid, timeout, poll_interval=2):
        until = time.monotonic() + timeout
        while True:
            task = self.status(tid, fresh=True)
            if task and (task['state'] in TERMINAL or task['state'] == 'PAUSED'):
                return task
            remaining = until - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(tid)
            time.sleep(min(poll_interval, remaining))
