"""Stable, execution-free task/resource record projections."""

def profile_view(profile):
    result={k:v for k,v in profile.items() if k in (
        'name','partition','nodes','ranks_per_node','threads','mem_mib',
        'memory_model','wall_minutes','gpus_per_node')}
    return result


def selection_view(selection):
    result=dict(selection)
    if result.get('profile'):result['profile']=profile_view(result['profile'])
    return result


def task_view(task):
    result={k:v for k,v in task.items() if k not in ('submitted_spec','slurm','accounting')}
    spec={k:v for k,v in task.get('spec',{}).items() if k in (
        'name','kind','priority','urgent','tags','labels','profiles','max_attempts','experimental')}
    if 'profiles' in spec:spec['profiles']=[profile_view(p) for p in spec['profiles']]
    result['spec']=spec
    if result.get('selection'):result['selection']=selection_view(result['selection'])
    if result.get('decision'):
        decision=dict(result['decision'])
        decision.pop('selection',None)
        result['decision']=decision
    if 'attempts' in result:
        attempts=[]
        for attempt in result['attempts']:
            item={k:v for k,v in attempt.items() if k in ('number','time','job_id','shared')}
            attempts.append(item)
        result['attempts']=attempts
    return result
