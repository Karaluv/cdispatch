API_VERSION = 1

def allow_displacement(task, options):
    return task['spec'].get('labels', {}).get('stage') == 'worker'
