"""Evidence-backed resource profiles. This module never submits or executes work."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
import time

from .common import atomic, lock, read, sha, validate

from .paths import profile_root

DEFAULT = profile_root()
STATES = ('candidate', 'validated', 'rejected', 'retired')


def slug(value):
    value=re.sub(r'[^a-z0-9._-]+','-',str(value).lower()).strip('-.')
    if not value or value in ('.','..'):raise ValueError('Empty profile path component')
    return value


def entry_path(root, identifier):
    parts=identifier.split('/')
    if len(parts)!=4 or any(slug(p)!=p for p in parts):raise ValueError('Profile ID must be program/workload/hardware/name')
    path=Path(root).joinpath(*parts,'profile.json')
    if not path.resolve().is_relative_to(Path(root).resolve()):raise ValueError('Profile path escapes library')
    return path


def evidence(items):
    if not isinstance(items,list) or not items:raise ValueError('Evidence paths and SHA256 are required')
    for item in items:
        if not isinstance(item,dict) or not item.get('path') or not re.fullmatch(r'[a-f0-9]{64}',item.get('sha256','')):
            raise ValueError('Each evidence item needs path and SHA256')


def observation(obs):
    if obs.get('outcome') not in ('success','failure','cancelled','running'):raise ValueError('Unknown observation outcome')
    if type(obs.get('application_validated')) is not bool:raise ValueError('application_validated must be explicit')
    evidence(obs.get('evidence'))
    if obs['application_validated'] and (obs['outcome']!='success' or not obs.get('validation_scope')):
        raise ValueError('Validated observation requires success and validation_scope')
    for key in ('elapsed_seconds','peak_ram_mib','peak_vram_mib_per_gpu'):
        v=obs.get(key)
        if v is not None and (type(v) not in (int,float) or v<=0):raise ValueError('Invalid '+key)
    if obs.get('elapsed_seconds') and not (obs.get('timing_scope') and obs.get('comparison_key')):
        raise ValueError('Timing requires timing_scope and comparison_key')


def check(entry):
    if entry.get('schema_version')!=1:raise ValueError('Unsupported profile schema')
    entry_path(DEFAULT,entry['id'])
    if entry['id'].split('/')[:3]!=[entry.get(k) for k in ('program','workload','hardware')]:raise ValueError('ID and categories differ')
    if entry.get('status') not in STATES:raise ValueError('Unknown qualification status')
    if not isinstance(entry.get('context'),dict) or not entry['context']:raise ValueError('Applicability context required')
    profile=copy.deepcopy(entry['profile'])
    probe=dict(name='library validation',workdir='/tmp',kind=entry['kind'],command=['/bin/true'],profiles=[profile])
    validate(probe)
    evidence(entry.get('sources'))
    for obs in entry.get('observations',[]):observation(obs)
    if entry['status']=='validated' and not any(o['application_validated'] for o in entry.get('observations',[])):
        raise ValueError('Slurm completion alone cannot qualify a profile')
    risk=entry.get('operational_risk')
    if risk is not None:
        if not isinstance(risk,dict) or risk.get('label') not in ('dangerous','ok_with_logging'):
            raise ValueError('Invalid operational risk label')
        if not isinstance(risk.get('summary'),str) or not risk['summary'].strip():
            raise ValueError('Operational risk summary required')
        logs=risk.get('required_logs')
        if not isinstance(logs,list) or not logs or any(not isinstance(x,str) or not x.strip() for x in logs):
            raise ValueError('Operational risk needs concrete logging requirements')
        if risk['label']=='dangerous' and entry['status'] not in ('rejected','retired'):
            raise ValueError('Dangerous profile must be rejected or retired')
    adaptation=entry.get('adaptation',{})
    if adaptation.get('mode')!='enumerated-only':raise ValueError('Only explicitly enumerated resource shapes may be selected')
    if profile.get('gpus_per_node'):
        gpu=entry.get('gpu',{})
        if gpu.get('gres_type')!=profile.get('gpu_type'):raise ValueError('GPU metadata does not match typed GRES')
        if entry['status']=='validated' and not gpu.get('runtime_verified'):raise ValueError('GPU runtime must be verified for qualification')
    perf=entry.get('performance',{})
    estimate=perf.get('expected_seconds')
    if estimate is not None and (type(estimate) not in (int,float) or estimate<=0):raise ValueError('Invalid expected runtime')
    if estimate is not None and not all(perf.get(k) for k in ('basis','comparison_key','timing_scope')):
        raise ValueError('Expected speed needs an explicit basis, comparison key and timing scope')
    return entry


def save(root,entry):
    check(entry);path=entry_path(root,entry['id'])
    with lock(Path(root)/'.library.lock'):
        if path.exists():
            if read(path)!=entry:raise ValueError('Profile exists; record an observation or create a new revision ID')
        else:atomic(path,entry)
    return entry['id']


def get(root,identifier):
    item=read(entry_path(root,identifier))
    if item is None:raise ValueError('Profile not found: '+identifier)
    return check(item)


def pinning_preset(root,name):
    if slug(name)!=name:raise ValueError('Invalid pinning preset ID')
    path=Path(root)/'pinning'/(name+'.json')
    preset=read(path)
    if not preset or preset.get('id')!=name or not isinstance(preset.get('pinning'),dict):
        raise ValueError('Pinning preset missing or invalid: '+name)
    return preset


def pinning_check(root,entry,preset_name):
    preset=pinning_preset(root,preset_name);p=entry['profile'];c=preset.get('compatible',{})
    reasons=[]
    if entry['program'] not in c.get('programs',[]):reasons.append('program not listed')
    if c.get('application_threads',1)>p['threads']:reasons.append('application threads exceed allocated CPUs per rank')
    if c.get('ranks_per_node') and p['ranks_per_node']!=c['ranks_per_node']:reasons.append('rank count differs')
    mode=c.get('gpu_mode')
    if mode=='none' and p.get('gpus_per_node'):reasons.append('GPU profile needs another pinning preset')
    if mode=='one-rank-per-gpu' and (not p.get('gpus_per_node') or p['ranks_per_node']!=p['gpus_per_node']):
        reasons.append('one rank per GPU required')
    return dict(compatible=not reasons,reasons=reasons,qualified=(entry.get('pinning_ref')==preset_name and p.get('pinning')==preset['pinning']),
                preset=preset_name,pinning=preset['pinning'])


def search(root,program=None,workload=None,hardware=None,status=None,gpu_type=None):
    result=[]
    for path in sorted(Path(root).glob('*/*/*/*/profile.json')):
        item=check(read(path))
        if any(value and item.get(key)!=value for key,value in [('program',program),('workload',workload),('hardware',hardware),('status',status)]):continue
        if gpu_type and item.get('gpu',{}).get('gres_type')!=gpu_type:continue
        result.append(item)
    return sorted(result,key=lambda e:(e['status']!='validated',e['id']))


def import_task(root,path,program,workload,context):
    task=validate(read(path));digest=sha(path);result=[]
    for original in task['profiles']:
        profile=copy.deepcopy(original)
        if 'command' not in profile and task.get('command'):profile['command']=task['command']
        hardware=slug(profile['partition']);name=slug(profile['name'])+'-'+digest[:8]
        identifier='/'.join([slug(program),slug(workload),hardware,name])
        item=dict(schema_version=1,id=identifier,program=slug(program),workload=slug(workload),hardware=hardware,
                  status='candidate',kind=task['kind'],context=context,profile=profile,
                  software={'binary':None,'version':None,'sha256':None,'environment':None},
                  gpu={'gres_type':profile.get('gpu_type'),'model':None,'vram_mib_per_gpu':None,'runtime_verified':False,'cuda':None},
                  resources={'requested_ram_mib_per_node':profile['mem_mib'],'peak_ram_mib':None,'peak_ram_scope':'unknown','scratch_mib':None},
                  performance={'expected_seconds':None,'basis':'unmeasured','comparison_key':None,'timing_scope':None,'samples':0},
                  adaptation={'mode':'enumerated-only','compatible_profile_ids':[],'checkpoint_restart_verified':False,
                              'notes':'No inferred CPU/GPU substitution, rank changes or memory reduction.'},
                  sources=[{'path':str(Path(path).resolve()),'sha256':digest}],observations=[],
                  notes=['Imported configuration, not proof of a successful application run. Commands and paths are provenance; bind a new isolated run.'])
        result.append(save(root,item))
        notes=entry_path(root,identifier).with_name('NOTES.md')
        if not notes.exists():
            notes.write_text('# '+identifier+'\n\nImported from '+str(Path(path).resolve())+
                             '\n\nConfiguration only. No application run or performance validation has been recorded.\n')
    return result


def record(root,identifier,obs,promote=False):
    observation(obs)
    with lock(Path(root)/'.library.lock'):
        item=get(root,identifier)
        if item['status'] in ('rejected','retired'):raise ValueError('Create a reviewed revision of a rejected/retired profile')
        fresh=obs not in item['observations']
        if fresh:item['observations'].append(obs)
        if promote:
            if not obs['application_validated']:raise ValueError('Promotion requires application validation')
            if obs.get('context')!=item['context']:raise ValueError('Promotion requires exact applicability context')
            if item['profile'].get('gpus_per_node'):
                if obs.get('gpu_runtime_verified') is not True:raise ValueError('GPU execution proof required')
                item['gpu']['runtime_verified']=True
            item['status']='validated'
            if obs.get('elapsed_seconds'):
                comparable=sorted(o['elapsed_seconds'] for o in item['observations'] if o.get('application_validated')
                                  and o.get('context')==item['context'] and o.get('comparison_key')==obs.get('comparison_key')
                                  and o.get('timing_scope')==obs.get('timing_scope') and o.get('elapsed_seconds'))
                item['performance'].update(expected_seconds=comparable[len(comparable)//2],basis='median of validated observations',
                                           comparison_key=obs['comparison_key'],timing_scope=obs['timing_scope'],samples=len(comparable))
            if obs.get('peak_vram_mib_per_gpu'):item.setdefault('gpu',{})['vram_mib_per_gpu']=obs['peak_vram_mib_per_gpu']
            if obs.get('peak_ram_mib'):
                if not obs.get('peak_ram_scope'):raise ValueError('Measured RAM needs a scope')
                item.setdefault('resources',{}).update(peak_ram_mib=obs['peak_ram_mib'],peak_ram_scope=obs['peak_ram_scope'])
        check(item);atomic(entry_path(root,identifier),item)
        if fresh:
            notes=entry_path(root,identifier).with_name('NOTES.md')
            before=notes.read_text() if notes.exists() else '# '+identifier+'\n'
            lines=['','## Observation '+str(obs.get('job_id') or obs.get('time') or len(item['observations'])),
                   'Outcome: '+obs['outcome']+'; application validated: '+str(obs['application_validated'])+'.']
            if obs.get('elapsed_seconds'):lines+=['Elapsed: '+str(obs['elapsed_seconds'])+' s; scope: '+obs['timing_scope']+'.']
            if obs.get('note'):lines+=[str(obs['note'])]
            lines+=['Sources:']+[f"- {e['path']} ({e['sha256']})" for e in obs['evidence']]
            tmp=notes.with_name('.'+notes.name+'.tmp')
            tmp.write_text(before.rstrip()+'\n'+'\n'.join(lines)+'\n');tmp.replace(notes)
    return item


def build(root,identifiers,base,context,experimental=False,allow_unmeasured=False,pinning_override=None):
    if not identifiers or len(set(identifiers))!=len(identifiers):raise ValueError('Choose distinct profiles')
    entries=[get(root,i) for i in identifiers]
    if len({e['program'] for e in entries})!=1 or len({e['workload'] for e in entries})!=1:raise ValueError('Different programs/workloads cannot form one pool')
    if any(e['status'] in ('rejected','retired') for e in entries):raise ValueError('Rejected/retired profile cannot run')
    if experimental and len(entries)!=1:raise ValueError('An experiment has one exact profile')
    warnings=[]
    for entry in entries:
        if entry['status']!='validated':warnings.append(entry['id']+': candidate; review its notes and application gates')
        if entry['context']!=context:warnings.append(entry['id']+': input/context differs; historical speed estimate discarded')
        if entry.get('operational_risk',{}).get('label')=='ok_with_logging':
            warnings.append(entry['id']+': OK WITH IMPROVED LOGGING; '+entry['operational_risk']['summary'])
    task=copy.deepcopy(base);task.pop('profiles',None)
    kinds={e['kind'] for e in entries}
    if len(kinds)!=1 or task.get('kind',next(iter(kinds))) not in kinds:raise ValueError('Allocation kinds cannot be mixed')
    task['kind']=next(iter(kinds));task['experimental']=experimental
    if any(not e['profile'].get('gpus_per_node') and any(x in e['profile']['partition'].lower() for x in ('h200','a100','v100','titan','rtx')) for e in entries):
        task.setdefault('placement',{}).setdefault('avoid_gpu_nodes',False)
    commands=task.pop('profile_commands',{});envs=task.pop('profile_env',{})
    performances=[e.get('performance',{}) for e in entries]
    pinning_changed=any(pinning_override and not pinning_check(root,e,pinning_override)['qualified'] for e in entries)
    comparable=(not pinning_changed and all(e['status']=='validated' and e['context']==context for e in entries)
                and all(p.get('expected_seconds') for p in performances)
                and len({(p.get('comparison_key'),p.get('timing_scope')) for p in performances})==1)
    if not comparable:warnings.append('No matched timing for this input; slowdown=1 is an unranked tie')
    if not comparable and (task.get('elastic') or task.get('placement',{}).get('preempt_for_speed')):raise ValueError('Speed-based migration requires comparable timing')
    fastest=min(p['expected_seconds'] for p in performances) if comparable else None
    profiles=[]
    for entry in entries:
        p=copy.deepcopy(entry['profile'])
        chosen_pin=pinning_override or entry.get('pinning_ref')
        if chosen_pin:
            verdict=pinning_check(root,entry,chosen_pin)
            if not verdict['compatible']:raise ValueError('Incompatible pinning for '+entry['id']+': '+', '.join(verdict['reasons']))
            if not verdict['qualified']:warnings.append(entry['id']+': changed pinning; inspect actual affinity')
            p['pinning']=verdict['pinning'];p['pinning_profile']=chosen_pin
        # Historical commands often point at live scientific workdirs. Bind explicitly.
        p.pop('command',None);p.pop('resume_command',None)
        p.pop('expected_seconds',None)
        command=commands.get(entry['id'],task.get('command'))
        if not command:raise ValueError('Provide command or profile_commands for the new run; archived commands are not reused')
        p['command']=command
        p['env']=dict(p.get('env',{}),**envs.get(entry['id'],{}))
        p['name']=entry['id'].split('/')[-1]
        p['slowdown']=1 if experimental or not comparable else entry['performance']['expected_seconds']/fastest
        if comparable and not experimental:p['expected_seconds']=entry['performance']['expected_seconds']
        profiles.append(p)
    if any(task.get(k) for k in ('elastic','preemptible','resume_compatible_profiles')):
        raise ValueError('Library builds are fresh starts; attach a separately reviewed checkpoint contract afterward')
    task['profiles']=profiles
    task.setdefault('max_slowdown',max(p['slowdown'] for p in profiles))
    task['profile_library']={'entries':[{'id':e['id'],'sha256':sha(entry_path(root,e['id']))} for e in entries],
                             'context':context,'timing_comparable':comparable,'created':time.time(),
                             'warnings':warnings,
                             'note':'Expected timings are scoped estimates; unmeasured slowdown=1 is an unranked tie, not measured equality.'}
    return validate(task)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library',type=Path,default=DEFAULT)
    sub=parser.add_subparsers(dest='action',required=True)
    p=sub.add_parser('search')
    for key in ('program','workload','hardware','status','gpu-type'):p.add_argument('--'+key)
    p.add_argument('--json',action='store_true')
    p=sub.add_parser('show');p.add_argument('id')
    p=sub.add_parser('import-task');p.add_argument('task',type=Path);p.add_argument('--program',required=True);p.add_argument('--workload',required=True);p.add_argument('--context',type=Path,required=True)
    p=sub.add_parser('add');p.add_argument('entry',type=Path)
    p=sub.add_parser('record');p.add_argument('id');p.add_argument('observation',type=Path);p.add_argument('--promote',action='store_true')
    p=sub.add_parser('build');p.add_argument('--select',action='append',required=True);p.add_argument('--base',type=Path,required=True);p.add_argument('--context',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--experimental',action='store_true');p.add_argument('--allow-unmeasured',action='store_true');p.add_argument('--pinning')
    p=sub.add_parser('pinning-check');p.add_argument('id');p.add_argument('preset')
    sub.add_parser('pinning-list')
    sub.add_parser('validate')
    args=parser.parse_args(argv);root=args.library
    if args.action=='search':
        items=search(root,args.program,args.workload,args.hardware,args.status,args.gpu_type)
        if args.json:print(json.dumps(items,ensure_ascii=False,indent=2))
        else:
            for e in items:
                p=e['profile'];seconds=e.get('performance',{}).get('expected_seconds')
                label=e.get('operational_risk',{}).get('label')
                risk_tag={'dangerous':' [DANGEROUS]','ok_with_logging':' [OK+LOG]'}.get(label,'')
                print(f"{e['status']:9} {e['id']}{risk_tag} | {p.get('nodes',1)*p['ranks_per_node']*p['threads']} CPU, {p['mem_mib']/1024:g} GiB/node, {p.get('gpus_per_node',0)} {p.get('gpu_type','GPU')}/node | expected {seconds if seconds is not None else '?'} s")
    elif args.action=='show':print(json.dumps(get(root,args.id),ensure_ascii=False,indent=2))
    elif args.action=='import-task':print('\n'.join(import_task(root,args.task,args.program,args.workload,read(args.context))))
    elif args.action=='add':print(save(root,read(args.entry)))
    elif args.action=='record':print(record(root,args.id,read(args.observation),args.promote)['status'])
    elif args.action=='build':
        task=build(root,args.select,read(args.base),read(args.context),args.experimental,args.allow_unmeasured,args.pinning)
        if args.output.exists():raise ValueError('Output exists; use a new filename')
        atomic(args.output,task);print(args.output)
        for warning in task['profile_library']['warnings']:print('Review: '+warning)
    elif args.action=='pinning-check':print(json.dumps(pinning_check(root,get(root,args.id),args.preset),ensure_ascii=False,indent=2))
    elif args.action=='pinning-list':
        for path in sorted((root/'pinning').glob('*.json')):
            p=read(path)
            if p:print(p['id']+': '+p.get('description',''))
    elif args.action=='validate':print(str(len(search(root)))+' profiles valid')


if __name__=='__main__':main()
