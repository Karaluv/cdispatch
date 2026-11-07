"""Recognize a serial service renewal for local admission accounting only."""
import re


def deferred_service_successors(jobs):
    """A single unallocated afterany successor shares its running service's slot.

    Raw jobs remain available to Slurm association/QoS/MaxSubmitJobs checks.
    Missing identity, ambiguous renewals and every other pending job count normally.
    """
    jobs = list(jobs)
    result = set()
    for current in jobs:
        jid = current.get('JobId', '')
        command = current.get('Command', '')
        user = current.get('UserId', '').split('(')[0]
        if (current.get('JobName') != 'cd-service' or current.get('JobState') != 'RUNNING'
                or not re.fullmatch(r'\d+', jid) or not user or not current.get('Account')
                or not command.startswith('/') or not command.endswith('/cluster-dispatcher/var/service.sh')):
            continue
        successors = [job for job in jobs if
                      job.get('JobName') == 'cd-service'
                      and job.get('JobState') == 'PENDING'
                      and job.get('UserId', '').split('(')[0] == user
                      and job.get('Account') == current.get('Account')
                      and job.get('Command') == command
                      and job.get('Reason') == 'Dependency'
                      and job.get('Dependency') in ('afterany:' + jid, 'afterany:' + jid + '(unfulfilled)')
                      and job.get('AllocTRES', '') in ('', '(null)', 'N/A')
                      and re.fullmatch(r'\d+', job.get('JobId', ''))]
        if len(successors) == 1:
            result.add(successors[0]['JobId'])
    return result
