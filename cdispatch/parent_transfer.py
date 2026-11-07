"""Explicit owner-supervisor handoff; original scientific specifications stay immutable."""
from .common import TERMINAL


def parent_id(task):
    return task.get('lifecycle_parent', task['spec'].get('parent'))


def transfer(tasks, old_id, new_id, owner, stages=('controller',)):
    old, new = tasks[old_id], tasks[new_id]
    if old_id == new_id or old['state'] != 'RUNNING' or new['state'] != 'RUNNING':
        raise ValueError('Both distinct supervisors must be RUNNING')
    if old.get('cancel_requested') or new.get('cancel_requested'):
        raise ValueError('Supervisor cancellation already requested')
    if new['spec'].get('kind') != 'shared' or new.get('user') != owner:
        raise ValueError('Target must be an owner shared step')
    proof = new['spec'].get('controller_transfer', {})
    if proof.get('predecessor') != old_id or proof.get('source') != old['spec']['workdir']:
        raise ValueError('Target does not declare this exact predecessor checkpoint')
    if old['spec'].get('labels', {}).get('stage') not in stages:
        raise ValueError('Only an explicit eligible supervisor can transfer children')
    if new['spec'].get('labels', {}).get('stage') not in stages:
        raise ValueError('Target is not an eligible supervisor')
    children = [t for t in tasks.values() if parent_id(t) == old_id and t['state'] not in TERMINAL]
    if any(t.get('cancel_requested') for t in children):
        raise ValueError('Wait for already retiring children before transfer')
    # Validate the whole operation before updating any child. The daemon saves
    # this transaction before lifecycle cleanup or any cancellation RPC.
    for child in children:
        child['lifecycle_parent'] = new_id
    return [t['id'] for t in children]
