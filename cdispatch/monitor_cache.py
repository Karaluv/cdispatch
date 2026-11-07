"""Versioned read-only observer view, independent of scheduler persistence."""
from pathlib import Path
from .state_reader import JSONStream as StateJSONStream, read_scheduler_state
from .record_views import profile_view, selection_view, task_view


class JSONStream(StateJSONStream):
    def root(self, task_transform=task_view):
        return super().root(task_transform)


def read_monitor_cache(path, default=None):
    try:
        with Path(path).open(encoding='utf-8') as stream:return JSONStream(stream,pool_keys=True).root()
    except FileNotFoundError:return default



MONITOR_SCHEMA_VERSION = 1


def monitor_snapshot(status):
    """The read-only monitoring contract has no scheduler execution payloads."""
    result = dict(status)
    result['monitor_schema_version'] = MONITOR_SCHEMA_VERSION
    result['tasks'] = {tid: task_view(task) for tid, task in status.get('tasks', {}).items()}
    from .resource_report import report
    result['resource_totals'] = report(status)
    result.pop('accounting', None)
    return result


def read_monitor_snapshot(root, default=None):
    """Prefer the independent versioned view; legacy fallback supports rollout."""
    root = Path(root)
    if not (root/'monitor.json').exists():
        return read_monitor_cache(root/'status.json', default)
    with (root/'monitor.json').open(encoding='utf-8') as stream:
        result = JSONStream(stream,pool_keys=True).root(task_transform=None)
    if result.get('monitor_schema_version') != MONITOR_SCHEMA_VERSION:
        raise ValueError('Unsupported monitor snapshot version')
    return result
