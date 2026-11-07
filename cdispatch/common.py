import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

ACTIVE = {'ADOPTING', 'SUBMITTING', 'SUBMIT_UNKNOWN', 'PENDING', 'CONFIGURING', 'RUNNING', 'COMPLETING', 'PAUSING', 'CANCELLING'}
TERMINAL = {'SUCCEEDED', 'FINISHED_UNVALIDATED', 'FAILED', 'CANCELLED', 'BLOCKED'}

def validate_cpu_preference(value):
    if (not isinstance(value,list) or not value or
            any(type(x) is not int or x<1 for x in value) or len(set(value))!=len(value)):
        raise ValueError('preferred_cpu_counts must be a nonempty list of unique positive integers')
    return list(value)

def read(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default

def atomic(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name('.' + path.name + '.' + uuid.uuid4().hex)
    with tmp.open('x') as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False))
        f.write('\n'); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)
    fd = os.open(str(path.parent), os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)

@contextlib.contextmanager
def lock(path, blocking=False):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'a') as f:
        fcntl.flock(f, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        yield

def run(argv, timeout=30, **kwargs):
    return subprocess.run(argv, check=True, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout, **kwargs).stdout.strip()

def fields(line):
    matches = list(re.finditer(r'(?:^|\s)([A-Za-z][A-Za-z0-9_/:]*)=', line))
    return {m.group(1): line[m.end():matches[i+1].start() if i+1 < len(matches) else len(line)].strip()
            for i, m in enumerate(matches)}

def tres(value):
    return dict(x.split('=', 1) for x in value.split(',') if '=' in x)

def mib(value):
    m = re.fullmatch(r'(\d+(?:\.\d+)?)([KMGTPE]?)(?:[cn])?', str(value))
    if not m: raise ValueError('Unknown memory unit: ' + str(value))
    return float(m[1]) * {'':1,'K':1/1024,'M':1,'G':1024,'T':1024**2,'P':1024**3,'E':1024**4}[m[2]]

def seconds(value):
    if value in ('UNLIMITED', 'INFINITE'): return float('inf')
    days, sep, rest = str(value).partition('-')
    if not sep: rest, days = days, '0'
    parts = [float(x) for x in rest.split(':')]
    if len(parts) == 1: return int(days)*86400 + parts[0]*60
    parts = [0]*(3-len(parts))+parts
    return int(days)*86400 + parts[0]*3600+parts[1]*60+parts[2]

def sha(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for block in iter(lambda:f.read(1024*1024), b''): h.update(block)
    return h.hexdigest()

def command(value, name):
    if not isinstance(value, list) or not value or any(not isinstance(x,str) or '\x00' in x for x in value):
        raise ValueError(name+' must be a nonempty argv list')

def validate(spec):
    if not isinstance(spec,dict): raise ValueError('Expected task object')
    s=json.loads(json.dumps(spec,allow_nan=False))
    for key in ('name','workdir','profiles'):
        if not s.get(key): raise ValueError('Missing '+key)
    if not Path(s['workdir']).is_absolute(): raise ValueError('workdir must be absolute')
    if not isinstance(s['profiles'],list): raise ValueError('profiles must be a list')
    if 'fair_share' in s:
        from .fair_share import validate_contract
        validate_contract(s['fair_share'])
        if s.get('kind') in ('shared','interactive'):raise ValueError('fair_share is for batch workers')
    if 'artifact_contract' in s:
        from .artifacts import validate_contract
        validate_contract(s['artifact_contract'])
    if s.get('adopt_job'):
        a=s['adopt_job']
        if not isinstance(a,dict) or not re.fullmatch(r'\d+',str(a.get('JobId',''))):raise ValueError('Invalid adopted job ID')
        if any(not isinstance(a.get(k),str) or not a[k] for k in ('JobName','UserId','Command','SubmitTime')):raise ValueError('Adoption requires exact Slurm identity')
        if not s.get('artifact_contract'):raise ValueError('Adoption requires an artifact validation contract')
        if s.get('preemptible') or s.get('restart_from_scratch_safe'):raise ValueError('Adopted jobs cannot be automatically restarted or checkpointed')
    s.setdefault('priority',100); s.setdefault('max_slowdown',1.0)
    if type(s.get('experimental',False)) is not bool:raise ValueError('experimental must be boolean')
    if s.get('experimental'):
        if len(s['profiles'])!=1:raise ValueError('Experimental tasks require exactly one fixed profile; submit separate tasks for each configuration')
        if s['max_slowdown']!=1 or s['profiles'][0].get('slowdown',1)!=1:raise ValueError('Experimental tasks cannot relax the fixed profile')
        if any(s.get(k) for k in ('preemptible','elastic','restart_from_scratch_safe')):raise ValueError('Experimental tasks cannot be automatically paused, migrated or retried')
        if s.get('max_attempts',1)!=1:raise ValueError('Experimental tasks require max_attempts=1')
        s['max_attempts']=1
    placement=s.get('placement',{})
    if not isinstance(placement,dict):raise ValueError('placement must be an object')
    unknown=set(placement)-{'policy','preferred_nodes','preferred_partitions','allow_fallback','avoid_gpu_nodes','preempt_for_speed','allow_planned_backfill','preferred_cpu_counts'}
    if unknown:raise ValueError('Unknown placement keys: '+', '.join(sorted(unknown)))
    if 'preferred_cpu_counts' in placement:validate_cpu_preference(placement['preferred_cpu_counts'])
    if type(placement.get('allow_planned_backfill',False)) is not bool:raise ValueError('placement.allow_planned_backfill must be boolean')
    if placement.get('policy','auto') not in ('auto','economy','fast'):raise ValueError('placement.policy must be auto, economy or fast')
    for key in ('preferred_nodes','preferred_partitions'):
        if key in placement and (not isinstance(placement[key],list) or any(not isinstance(x,str) or not re.fullmatch(r'[\w.-]+',x) for x in placement[key])):raise ValueError('placement.'+key+' must contain explicit names')
    for key in ('allow_fallback','avoid_gpu_nodes','preempt_for_speed'):
        if key in placement and type(placement[key]) is not bool:raise ValueError('placement.'+key+' must be boolean')
    if placement.get('allow_fallback') is False and not (placement.get('preferred_nodes') or placement.get('preferred_partitions')):raise ValueError('No placement preferences to restrict')
    if placement.get('preempt_for_speed') and (placement.get('policy')!='fast' or not (s.get('urgent') or 'urgent' in s.get('tags',[]) or s.get('kind')=='interactive')):raise ValueError('preempt_for_speed requires urgent and placement.policy=fast')
    s.setdefault('tags',[]);
    if not isinstance(s['tags'],list) or any(not isinstance(x,str) for x in s['tags']):raise ValueError('tags must be strings')
    if 'force_displace_tasks' in s:
        ids=s['force_displace_tasks']
        if (not isinstance(ids,list) or not ids
                or any(not isinstance(x,str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}',x) for x in ids)
                or len(set(ids))!=len(ids)):
            raise ValueError('force_displace_tasks requires distinct explicit task IDs')
        if not s.get('urgent') or s.get('kind','batch')!='batch' or s.get('experimental'):
            raise ValueError('Forced displacement requires an urgent nonexperimental batch task')
    if s.get('elastic') and (not s.get('preemptible') or not s.get('resume_compatible_profiles')):raise ValueError('elastic requires checkpoint contract and resume_compatible_profiles')
    s.setdefault('kind','batch'); s.setdefault('depends_on',[])
    s.setdefault('labels',{});
    if not isinstance(s['labels'],dict) or any(not isinstance(k,str) or not isinstance(v,str) for k,v in s['labels'].items()): raise ValueError('labels must be string key/value pairs')
    s.setdefault('group','default'); s.setdefault('max_attempts',3)
    if s['kind'] not in ('batch','interactive','shared'): raise ValueError('Unknown kind')
    if s['kind']=='shared':
        if any(s.get(k) for k in ('preemptible','elastic','adopt_job')):raise ValueError('Shared handlers cannot migrate, checkpoint-preempt or adopt allocations')
        if type(s.get('interactive',False)) is not bool:raise ValueError('interactive must be boolean')
        if s.get('interactive'):command(s.get('interactive_command'),'interactive_command')
    if type(s['priority']) is not int: raise ValueError('priority must be integer')
    if not isinstance(s['max_slowdown'],(int,float)) or s['max_slowdown'] < 1: raise ValueError('max_slowdown >= 1')
    if not isinstance(s['depends_on'],list) or any(not isinstance(x,str) for x in s['depends_on']): raise ValueError('depends_on must be task IDs')
    if not isinstance(s['group'],str): raise ValueError('group must be string')
    if type(s['max_attempts']) is not int or not 1 <= s['max_attempts'] <= 20: raise ValueError('max_attempts must be 1..20')
    for key in ('validate_command','checkpoint_command','checkpoint_validate_command','resume_command'):
        if key in s: command(s[key],key)
    if s.get('preemptible'):
        if s['kind'] != 'batch': raise ValueError('interactive is not preemptible')
        for key in ('checkpoint_command','checkpoint_validate_command','resume_command','checkpoint_files'):
            if not s.get(key): raise ValueError('preemptible requires '+key)
    if 'checkpoint_files' in s:
        if not isinstance(s['checkpoint_files'],list) or not s['checkpoint_files'] or any(not isinstance(x,str) or not Path(x).is_absolute() for x in s['checkpoint_files']): raise ValueError('checkpoint_files must be absolute paths')
    if 'shared_time_budget' in s:
        b=s['shared_time_budget']
        if s['kind']!='shared' or s.get('interactive') or s.get('experimental'):
            raise ValueError('shared_time_budget requires a noninteractive nonexperimental shared handler')
        if not isinstance(b,dict) or set(b)!={'min_wall_minutes','reserve_minutes','env'}:
            raise ValueError('shared_time_budget requires min_wall_minutes, reserve_minutes and env')
        if any(type(b[k]) is not int for k in ('min_wall_minutes','reserve_minutes')) or not 0<=b['reserve_minutes']<b['min_wall_minutes']:
            raise ValueError('Invalid shared_time_budget minutes')
        if not isinstance(b['env'],str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',b['env']):
            raise ValueError('Invalid shared_time_budget env name')
        for p in s['profiles']:
            value=p.get('env',{}).get(b['env'])
            if not isinstance(value,str) or not value.isdecimal() or int(value)<1:
                raise ValueError('shared_time_budget env must contain positive budget seconds')
    names=set()
    for p in s['profiles']:
        for key in ('name','partition','ranks_per_node','threads','mem_mib','wall_minutes'):
            if key not in p: raise ValueError('profile missing '+key)
        if p['name'] in names: raise ValueError('Duplicate profile name')
        names.add(p['name'])
        if not re.fullmatch(r'[\w.-]+',p['partition']): raise ValueError('One partition per profile')
        p.setdefault('nodes',1);p.setdefault('slowdown',1.0);p.setdefault('gpus_per_node',0)
        for key in ('nodes','ranks_per_node','threads','mem_mib','wall_minutes'):
            if type(p[key]) is not int or p[key]<1: raise ValueError(key+' must be a positive integer')
        if type(p['gpus_per_node']) is not int or p['gpus_per_node']<0: raise ValueError('Invalid GPU count')
        if not isinstance(p['slowdown'],(int,float)) or p['slowdown']<1: raise ValueError('Invalid slowdown')
        if p['gpus_per_node'] and not re.fullmatch(r'[\w.-]+',p.get('gpu_type','')): raise ValueError('GPU type required')
        if s['kind']=='batch' or (s['kind']=='shared' and not s.get('interactive')):command(p.get('command',s.get('command')),'command')
        if s['kind']=='shared' and (p['nodes']!=1 or p['ranks_per_node']!=1 or p.get('gpus_per_node')):raise ValueError('Shared handlers require one node, one process and no GPUs; use threads for local CPU parallelism')
        if 'expected_seconds' in p and (not isinstance(p['expected_seconds'],(int,float)) or p['expected_seconds']<=0):raise ValueError('expected_seconds must be positive')
        if 'resume_command' in p:command(p['resume_command'],'profile resume_command')
        if 'memory_model' in p:
            m=p['memory_model']
            if any(not isinstance(v,(int,float)) or v<0 for v in m.values()): raise ValueError('Invalid memory model')
            if m.get('safety_factor',1)<1: raise ValueError('Memory safety_factor >= 1')
        pin=p.get('pinning',{})
        if not isinstance(pin,dict) or set(pin)-{'slurm_hint','mpi','omp_proc_bind','omp_places','mkl_threads','application_threads'}:
            raise ValueError('Unsupported pinning fields')
        if pin.get('slurm_hint') not in (None,'nomultithread','multithread'):raise ValueError('Invalid Slurm hint')
        if pin.get('mpi') not in (None,'intel-core','intel-omp','none'):raise ValueError('Invalid MPI pinning')
        if pin.get('omp_proc_bind') not in (None,'close','spread','false'):raise ValueError('Invalid OMP_PROC_BIND')
        if pin.get('omp_places') not in (None,'cores','threads'):raise ValueError('Invalid OMP_PLACES')
        if 'mkl_threads' in pin and (type(pin['mkl_threads']) is not int or pin['mkl_threads']<1):raise ValueError('Invalid MKL threads')
        if 'application_threads' in pin and (type(pin['application_threads']) is not int or not 1<=pin['application_threads']<=p['threads']):raise ValueError('Invalid application threads')
        for k,v in p.get('env',{}).items():
            if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',k) or not isinstance(v,str): raise ValueError('Invalid env')
        if s['kind']=='interactive' and (p['nodes']!=1 or p['ranks_per_node']!=1): raise ValueError('interactive requires one node and one rank')
    return s

def enqueue(root, spec, task_id=None):
    spec=validate(spec);task_id=task_id or uuid.uuid4().hex
    task_identifier(task_id)
    root=Path(root)
    # Keep the original request independently of mutable priority and task state.
    # The exclusive link also arbitrates concurrent publishers of the same key.
    claim=root/'submissions'/(task_id+'.json')
    created=time.time()
    if not claim.exists():
        previous=read(root/'receipts'/('submit-'+task_id+'.json'),{}).get('request',{})
        if not previous:
            previous=read(root/'inbox'/('submit-'+task_id+'.json'),{})
        if previous.get('spec') is not None and previous['spec']!=spec:
            raise ValueError('idempotency key has a different spec')
        created=previous.get('created',created)
    publish_once(claim,dict(spec=spec,created=created),compare=lambda old,new:old['spec']==new['spec'])
    path=Path(root)/'inbox'/('submit-'+task_id+'.json')
    data={'op':'submit','id':task_id,'spec':spec,'created':read(claim)['created']}
    publish_once(path,data,compare=lambda old,new: old.get('spec')==new['spec'])
    return task_id

def task_identifier(value):
    if not isinstance(value,str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}',value):
        raise ValueError('Invalid task ID')
    return value

def publish_once(path,data,compare=lambda old,new: old==new):
    path=Path(path)
    tmp=path.with_name('.'+uuid.uuid4().hex)
    atomic(tmp,data)
    try:
        while True:
            try:
                os.link(tmp,path)
                fd=os.open(str(path.parent),os.O_RDONLY)
                try:os.fsync(fd)
                finally:os.close(fd)
                return
            except FileExistsError:
                old=read(path)
                if old is None:continue  # The consumer removed it in the meantime.
                if not compare(old,data):raise ValueError('idempotency key has a different spec')
                return
    finally: tmp.unlink()

def request(root, op, task_id, **extra):
    token=uuid.uuid4().hex
    atomic(Path(root)/'inbox'/(token+'.json'),dict(op=op,id=task_id,created=time.time(),**extra))
    return token
