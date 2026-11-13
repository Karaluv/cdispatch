"""Research extension implementing cdispatch plugin API v1."""
from .application_profiles import apply
from .geometry_outcomes import expected_geometry_timeout
from .presentation import group, label

API_VERSION = 1

def prepare_spec(spec, options):
    return apply(spec, options)

def expected_failure(task, slurm_state, job_id, options):
    return expected_geometry_timeout(task, slurm_state, job_id)

def allow_displacement(task, options):
    return task['spec'].get('labels', {}).get('stage') in options.get('displacement_stages', [])

def display_labels(task, options):
    return dict(display_name=label(task), display_group=group(task))
