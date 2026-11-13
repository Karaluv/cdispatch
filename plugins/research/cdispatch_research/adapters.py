"""Application adapters preserve application-specific acceptance gates."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import subprocess
import time
from cdispatch.common import read,validate,atomic,sha

import getpass

def geometry_spec(request_file,worker_script,group='o56-geometry',parent=None):
    request_file=Path(request_file).resolve();worker_script=Path(worker_script).resolve()
    req=read(request_file);p=req['profile'];point=Path(req['point'])
    digest=sha(request_file)
    profile=dict(name='qualified-'+str(p['ranks'])+'x'+str(p['threads']),partition=p['partition'],
                 ranks_per_node=p['ranks'],threads=p['threads'],mem_mib=p['memory_GB']*1024,wall_minutes=160,
                 command=[sys.executable,'-m','cdispatch_research.adapters','run-geometry',str(request_file),str(worker_script),digest])
    # Preserve validated node/ISA constraints. A busy or drained node waits locally.
    if p.get('node'):profile['allowed_nodes']=[p['node']]
    spec=dict(name='o56-'+req['role'],workdir=str(point.parent),group=group,priority=300 if req['role']=='ground' else 200,
              resource_key=str(point),labels=dict(project='glass',purpose='O56 fixed-angle geometry',stage=req['role'],case=point.name,owner=getpass.getuser()),
              profiles=[profile],validate_command=[sys.executable,'-m','cdispatch_research.adapters','validate-geometry',str(request_file),'--sha256',digest])
    # Worker writes a unique point under shared controller root; declared resource key isolates writers.
    if parent:spec['parent']=parent
    return validate(spec)

def mass_specs(project_root,manifest,indices,partitions=None):
    partitions=partitions or os.environ.get('CDISPATCH_MASS_PARTITIONS','').split(',')
    if not all(partitions):raise ValueError('Set CDISPATCH_MASS_PARTITIONS or pass partitions explicitly')
    root=Path(project_root);manifest=Path(manifest).resolve();tasks=read(manifest)
    result=[]
    for index in indices:
        task=tasks[index];work=Path(task['work_dir'])
        state=read(work/'task_state.json',{})
        if state.get('status') in ('complete','complete_from_existing_outputs'):continue
        profiles=[]
        for partition in partitions:
            profiles.append(dict(name='fhi16-'+partition,partition=partition,ranks_per_node=16,threads=1,
                                 mem_mib=96*1024,wall_minutes=720,
                                 command=['bash',str(Path(__file__).with_name('mass-worker.sh')),str(root),str(manifest),str(index)]))
        spec=dict(name=task['task_id'],group='emass-recovery',workdir=str(work),priority=100,
                  labels=dict(project='effective-mass',purpose='mass refinement',stage=task['functional'],case=task['material'],source_array=manifest.name),
                  profiles=profiles,validate_command=[sys.executable,'-m','cdispatch_research.adapters','validate-mass',str(work)],
                  restart_from_scratch_safe=False,max_attempts=1)
        result.append(dict(key='mass-'+hashlib.sha256((str(manifest)+':'+str(index)).encode()).hexdigest()[:24],index=index,spec=validate(spec)))
    return result

def main():
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='op',required=True)
    g=sub.add_parser('validate-geometry');g.add_argument('request');g.add_argument('--sha256')
    g=sub.add_parser('run-geometry');g.add_argument('request');g.add_argument('worker');g.add_argument('sha256')
    g=sub.add_parser('validate-mass');g.add_argument('workdir')
    g=sub.add_parser('geometry-spec');g.add_argument('request');g.add_argument('worker')
    g=sub.add_parser('mass-specs');g.add_argument('root');g.add_argument('manifest');g.add_argument('indices',help='comma-separated indices or ranges, e.g. 121-439');g.add_argument('output')
    a=p.parse_args()
    if a.op=='run-geometry':
        if sha(a.request)!=a.sha256:raise RuntimeError('Geometry request changed after submission')
        if Path(a.request+'.result.json').exists():raise RuntimeError('Geometry result already exists; refusing implicit restart')
        if read(a.request).get('deadline_epoch',float('inf'))<=time.time():raise RuntimeError('Geometry request deadline expired')
        sys.exit(subprocess.call(['bash',a.worker,a.request]))
    elif a.op=='validate-geometry':
        if a.sha256 and sha(a.request)!=a.sha256:raise RuntimeError('Geometry request changed after submission')
        r=read(a.request+'.result.json',{})
        if not r.get('ok') or not r.get('state',{}).get('electronic_gate_pass'):raise RuntimeError('Geometry electronic gate failed')
    elif a.op=='validate-mass':
        r=read(Path(a.workdir)/'task_state.json',{})
        if r.get('status') not in ('complete','complete_from_existing_outputs'):raise RuntimeError('Mass acceptance incomplete')
        if not r.get('results'):raise RuntimeError('No accepted mass artifacts')
        for path in r['results'].values():
            data=read(path,{})
            if not data.get('edge_directional_accepted') or data.get('locator_boundary_changed'):raise RuntimeError('Mass artifact gate failed')
    elif a.op=='geometry-spec':print(json.dumps(geometry_spec(a.request,a.worker),indent=2))
    else:
        indices=[]
        for part in a.indices.split(','):
            if '-' in part:
                lo,hi=map(int,part.split('-'));indices.extend(range(lo,hi+1))
            else:indices.append(int(part))
        atomic(a.output,mass_specs(a.root,a.manifest,indices))

if __name__=='__main__':main()
