"""Explicit, trusted extension hooks. No automatic discovery or global registry.

Plugins are ordinary installed Python modules selected by administrator config.
They execute with controller privileges; this is an API boundary, not a sandbox.
"""
import copy
import importlib
import re

API_VERSION = 1
HOOKS = ('prepare_spec', 'expected_failure', 'allow_displacement', 'display_labels')


def configured(config):
    entries = config.get('plugins', [])
    if not isinstance(entries, list):
        raise ValueError('plugins must be a list')
    seen = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) - {'module', 'options'}:
            raise ValueError('Plugin requires module and optional options')
        name = entry.get('module', '')
        if not isinstance(name, str) or not re.fullmatch(r'[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*', name):
            raise ValueError('Plugin module must be an importable dotted name')
        if name in seen:
            raise ValueError('Duplicate plugin: ' + name)
        seen.add(name)
        options = entry.get('options', {})
        if not isinstance(options, dict):
            raise ValueError('Plugin options must be an object')
        module = importlib.import_module(name)
        if getattr(module, 'API_VERSION', None) != API_VERSION:
            raise ValueError('Unsupported plugin API: ' + name)
        for hook in HOOKS:
            if hasattr(module, hook) and not callable(getattr(module, hook)):
                raise ValueError('Plugin hook is not callable: ' + name + '.' + hook)
        yield module, copy.deepcopy(options)


def check(config):
    if any(config.get(key) for key in ('application_profile_rules', 'application_lane_owners')):
        raise ValueError('Move legacy application rules into explicitly configured plugin options')
    return [module.__name__ for module, _ in configured(config)]


def prepare_spec(spec, config):
    """Transform a private planning copy; the submitted request stays immutable."""
    from .common import validate
    result = copy.deepcopy(spec)
    for module, options in configured(config):
        hook = getattr(module, 'prepare_spec', None)
        if hook:
            result = hook(copy.deepcopy(result), options)
            if not isinstance(result, dict):
                raise ValueError('prepare_spec must return a task object')
            # Only resource shapes/preferences and explicit admission metadata
            # may change. Commands/validation/lifecycle remain application-owned.
            allowed = {'profiles', 'placement', '_application_profile_rule', '_application_lane_delegated'}
            if {k:v for k,v in result.items() if k not in allowed} != {k:v for k,v in spec.items() if k not in allowed}:
                raise ValueError('Plugin changed non-resource task fields')
            if result.get('_application_lane_delegated'):
                if result.get('profiles') != []:
                    raise ValueError('Delegated task must have no admissible profiles')
                return result
            result = validate(result)
    return result


def expected_failure(task, slurm_state, job_id, config):
    """Classify verified application budget exhaustion, never OOM/node failure."""
    raw = str(slurm_state).split(' ', 1)[0].rstrip('+')
    if raw not in ('FAILED', 'COMPLETED'):
        return False
    for module, options in configured(config):
        hook = getattr(module, 'expected_failure', None)
        if hook:
            value = hook(copy.deepcopy(task), slurm_state, job_id, options)
            if type(value) is not bool:
                raise ValueError('expected_failure must return bool')
            if value:
                return True
    return False


def allow_displacement(task, config):
    for module, options in configured(config):
        hook = getattr(module, 'allow_displacement', None)
        if hook:
            value = hook(copy.deepcopy(task), options)
            if type(value) is not bool:
                raise ValueError('allow_displacement must return bool')
            if value:
                return True
    return False


def display_labels(task, config):
    result = {}
    for module, options in configured(config):
        hook = getattr(module, 'display_labels', None)
        if hook:
            labels = hook(copy.deepcopy(task), options)
            if not isinstance(labels, dict) or set(labels) - {'display_name', 'display_group'} or any(not isinstance(v, str) for v in labels.values()):
                raise ValueError('display_labels must return display_name/display_group strings')
            result.update(labels)
    return result
