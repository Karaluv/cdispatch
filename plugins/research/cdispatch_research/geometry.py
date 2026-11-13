"""Durable geometry-worker lifecycle, independent of the optimizer numerics."""
import copy
import hashlib
from pathlib import Path
import time

from .adapters import geometry_spec
from cdispatch.client import RequestRejected
from cdispatch.common import atomic, lock, read, sha, task_identifier, TERMINAL


class GeometryWorkers:
    """Journal before submission, retain unknown workers, acknowledge after save.

    A controller should persist its optimizer state and consumed task IDs before
    calling acknowledge(). poll() deliberately redelivers unacknowledged results.
    """

    def __init__(self, client, journal, worker_script, group, parent):
        self.client = client
        self.journal = Path(journal).resolve()
        self.lock_path = self.journal.with_suffix(self.journal.suffix + '.lock')
        self.worker_script = str(Path(worker_script).resolve())
        self.identity = dict(version=1, root=str(client.root.resolve()),
                             worker=self.worker_script, group=group,
                             parent=task_identifier(parent))
        with lock(self.lock_path, blocking=True):
            data = self._read()
            atomic(self.journal, data)

    def _read(self):
        data = read(self.journal)
        if data is None:
            return dict(self.identity, workers={})
        if any(data.get(key) != value for key, value in self.identity.items()):
            raise ValueError('Geometry journal belongs to a different controller or configuration')
        return data

    @staticmethod
    def _check_input(entry):
        if sha(entry['request']) != entry['request_sha256']:
            raise ValueError('Geometry request changed after journaling: ' + entry['request'])

    def submit(self, request_file, context=None):
        """Return a logical ID; replay of the same request never creates a trial."""
        request_file = Path(request_file).resolve()
        digest = sha(request_file)
        key_source = self.identity['parent'] + '\n' + str(request_file)
        tid = 'geometry-' + hashlib.sha256(key_source.encode()).hexdigest()[:40]
        with lock(self.lock_path, blocking=True):
            data = self._read()
            entry = data['workers'].get(tid)
            if entry is None:
                if Path(str(request_file) + '.result.json').exists():
                    raise ValueError('Existing result requires explicit migration, not a new submission')
                task_spec = geometry_spec(request_file, self.worker_script,
                                          self.identity['group'], self.identity['parent'])
                entry = dict(request=str(request_file), request_sha256=digest,
                             spec=task_spec, context=copy.deepcopy(context), created=time.time(),
                             published=False, acknowledged=False, cancel_requested=False)
                data['workers'][tid] = entry
                atomic(self.journal, data)
            elif entry['request_sha256'] != digest or entry['context'] != context:
                raise ValueError('Geometry request or optimizer context changed for an existing task')
            if entry['cancel_requested']:
                self.client.cancel(tid)
                if not entry['published']:
                    self.client.submit(entry['spec'], key=tid)
                    entry['published'] = True
                    atomic(self.journal, data)
            elif not entry['acknowledged']:
                self._check_input(entry)
                self.client.submit(entry['spec'], key=tid)
                entry['published'] = True
                atomic(self.journal, data)
        return tid

    def recover(self):
        """Replay durable intents after a crash, using their original task IDs."""
        with lock(self.lock_path, blocking=True):
            data = self._read()
            for tid, entry in data['workers'].items():
                if entry['acknowledged']:
                    continue
                if entry['cancel_requested']:
                    self.client.cancel(tid)
                    if not entry['published']:
                        self.client.submit(entry['spec'], key=tid)
                        entry['published'] = True
                else:
                    self._check_input(entry)
                    self.client.submit(entry['spec'], key=tid)
                    entry['published'] = True
            atomic(self.journal, data)
        return self.active()

    def active(self):
        return {tid: copy.deepcopy(entry) for tid, entry in self._read()['workers'].items()
                if not entry['acknowledged']}

    def poll(self):
        """Return confirmed terminal outcomes; pending/unknown work is retained."""
        snapshot = self.client.snapshot()
        ready = {}
        with lock(self.lock_path, blocking=True):
            data = self._read()
            for tid, entry in data['workers'].items():
                if entry['acknowledged']:
                    continue
                task = snapshot.get('tasks', {}).get(tid)
                if task is None:
                    receipt = read(self.client.root / 'receipts' / ('submit-' + tid + '.json'))
                    pending = (self.client.root / 'inbox' / ('submit-' + tid + '.json')).exists()
                    if receipt and not receipt.get('ok') and not pending:
                        raise RequestRejected(receipt.get('error', 'Geometry submission rejected'))
                    continue
                if task['state'] not in TERMINAL:
                    continue
                self._check_input(entry)
                raw = task.get('accounting', {}).get('state', '')
                slurm_state = raw.split()[0].rstrip('+') if raw else ''
                interrupted = task['state'] in ('CANCELLED', 'BLOCKED') or slurm_state in (
                    'CANCELLED', 'NODE_FAIL', 'BOOT_FAIL', 'TIMEOUT', 'PREEMPTED', 'OUT_OF_MEMORY')
                result = read(entry['request'] + '.result.json', {})
                # An absent application result cannot establish an electronic
                # failure (e.g. expired request, failed executable or node loss).
                interrupted = interrupted or not result or task['state'] == 'FINISHED_UNVALIDATED'
                if task['state'] == 'SUCCEEDED':
                    if not result.get('ok') or not result.get('state', {}).get('electronic_gate_pass'):
                        raise ValueError('Successful geometry task lacks its validated electronic result: ' + tid)
                else:
                    result = dict(result, ok=False,
                                  error=result.get('error') or task.get('reason') or task['state'])
                result['scheduler_interruption'] = interrupted
                result['scheduler_state'] = raw
                result['scheduler_reason'] = task.get('cancel_reason') or task.get('reason') or ''
                ready[tid] = dict(task_id=tid, job_id=task.get('job_id'),
                                  state=task['state'], slurm_state=raw,
                                  context=copy.deepcopy(entry['context']), result=result)
                entry['terminal'] = task['state']
            if ready:
                atomic(self.journal, data)
        return ready

    def acknowledge(self, tid):
        """Call only after the consumer has durably recorded this task's outcome."""
        with lock(self.lock_path, blocking=True):
            data = self._read()
            entry = data['workers'][tid]
            if 'terminal' not in entry:
                raise ValueError('Cannot acknowledge an unconfirmed worker')
            entry['acknowledged'] = True
            atomic(self.journal, data)

    def cancel(self, ids=None):
        """Persist cancellation before publishing it; retain IDs until confirmed."""
        with lock(self.lock_path, blocking=True):
            data = self._read()
            if ids is None:
                ids = [tid for tid, entry in data['workers'].items() if not entry['acknowledged']]
            elif isinstance(ids, str):
                ids = [ids]
            else:
                ids = list(ids)
            for tid in ids:
                data['workers'][tid]['cancel_requested'] = True
            atomic(self.journal, data)
            tokens = self.client.cancel(ids)
            for tid in ids:
                entry = data['workers'][tid]
                if not entry['published']:
                    self.client.submit(entry['spec'], key=tid)
                    entry['published'] = True
            atomic(self.journal, data)
            return tokens
