"""A minimal opt-in cdispatch plugin; no scientific dependencies."""
API_VERSION = 1

def allow_displacement(task, options):
    return task.get('spec', {}).get('labels', {}).get('stage') in options.get('displacement_stages', [])

def display_labels(task, options):
    spec = task.get('spec', {})
    name = spec.get('name', 'Task')
    project = spec.get('labels', {}).get('project')
    return {'display_name': (project + ' / ' if project else '') + name}
