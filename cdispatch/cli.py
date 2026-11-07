import argparse
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import time
import uuid
from .common import read,atomic,enqueue,request,validate,lock,tres,mib,seconds
from .resource_report import report
from .slurm import Slurm
from .paths import config_path, state_root
from .scheduler import choose,usage

BASE=Path(__file__).resolve().parent.parent

def status_text(d):
    if not d: return 'No controller snapshot. Run service-start or tick.'
    age=time.time()-d['heartbeat'];snap=d.get('snapshot') or {}
    snapshot_age=f"{time.time()-snap['time']:.0f}s" if snap.get('time') else 'unknown'
    out=[f"Controller: heartbeat {age:.0f}s old | Slurm snapshot {snapshot_age} old | paused={d.get('paused')} | error={d.get('error') or '-'} | circuit={d.get('circuit') or '-'}"]
    if age>max(30,d.get('config',{}).get('poll_interval',10)*3):out.append('STALE: controller may be stopped; running allocations continue.')
    out.append('ID        STATE                 PRIO  JOB      PROJECT / PURPOSE / STAGE             PROFILE / NODES / REASON')
    for t in sorted(d.get('tasks',{}).values(),key=lambda t:(-t['spec']['priority'],t['created'])):
        s=t['spec'];labels=s.get('labels',{});sel=t.get('selection',{})
        label=' / '.join(str(labels.get(k,'-')) for k in ('project','purpose','stage'))
        out.append(f"{t['id'][:8]:8}  {t['state']:21} {s['priority']:5}  {t.get('job_id','-'):8} {label[:60]:60} {sel.get('profile',{}).get('name','-')} {','.join(sel.get('nodes',[]))} {t.get('reason','')}")
    managed={t.get('job_id') for t in d.get('tasks',{}).values()}
    external=[j for j in snap.get('jobs',[]) if j['JobId'] not in managed]
    if external:
        from .service_renewal import deferred_service_successors
        deferred=deferred_service_successors(snap.get('jobs',[]))
        out.append('\nExisting external jobs:')
        for j in external:
            suffix=' | serial service renewal; no additional local slot' if j['JobId'] in deferred else ' | counts toward admission'
            out.append(' | '.join(j.get(k,'') for k in ('JobId','JobName','UserId','JobState','NodeList','Reason'))+suffix)
    out.append('\nNODE           STATE                 CPU alloc/total    MEMORY GiB alloc/total    GPU allocated/total')
    for n in snap.get('nodes',[]):
        tr=tres(n.get('AllocTRES',''));cfg=tres(n.get('CfgTRES',''))
        out.append(f"{n['NodeName']:14} {n.get('State',''):22} {n.get('CPUAlloc','?'):>4}/{n.get('CPUTot','?'):<4}          {int(n.get('AllocMem',0))/1024:8.1f}/{int(n.get('RealMemory',0))/1024:8.1f}         {tr.get('gres/gpu','0')}/{cfg.get('gres/gpu','0')}")
    pool=usage(snap.get('jobs',[]));out.append('\nObserved pool: '+json.dumps(pool))
    crashes=sum(e.get('kind')=='TERMINAL' and e.get('state')=='FAILED' for e in d.get('events',[]))
    out.append(f'Failures in recent event window: {crashes}; full history: events.jsonl')
    return '\n'.join(out)


def service_start(root,config,after_current=None,plan_only=False):
    slurm=Slurm(config);snap=slurm.snapshot()
    existing=[j for j in snap['jobs'] if j.get('JobName')=='cd-service' and j.get('UserId','').split('(')[0]==config['owner'] and j.get('JobState') in ('PENDING','RUNNING','CONFIGURING','COMPLETING')]
    if after_current:
        if not re.fullmatch(r'\d+',after_current):raise ValueError('Invalid predecessor job ID')
        renewal=read(root/'service-renewal-intent.json',{})
        successor=next((j for j in existing if j['JobId']==renewal.get('job_id') and renewal.get('after_current')==after_current),None)
        if successor:
            print('Existing service successor: '+successor['JobId']);return
        current=next((j for j in existing if j['JobId']==after_current and j['JobState']=='RUNNING'),None)
        if current is None or len(existing)!=1:raise RuntimeError('Renewal requires exactly one matching running service')
        import copy
        snap=copy.deepcopy(snap)
        for key in ('jobs','account_jobs','limit_jobs','queue_jobs'):
            snap[key]=[j for j in snap.get(key,[]) if j.get('JobId')!=after_current]
        current_node=next(n for n in snap['nodes'] if n['NodeName']==current['NodeList'])
        current_node['CPUAlloc']=str(max(0,int(current_node['CPUAlloc'])-int(current['NumCPUs'])))
        released_mem=mib(tres(current.get('AllocTRES','')).get('mem','0'))
        current_node['AllocMem']=str(max(0,int(current_node['AllocMem'])-int(released_mem)))
        # This predicts resources after the predecessor ends; Slurm enforces dependency and placement.
    elif existing:
        if len(existing)!=1:raise RuntimeError('Multiple service allocations; inspect before continuing')
        atomic(root/'bootstrap-intent.json',dict(state='SUBMITTED',time=time.time(),job_id=existing[0]['JobId'],reconciled=True))
        print('Existing service: '+existing[0]['JobId']);return
    from .shared import envelope
    service_cpus,service_mem=envelope(root,config)
    spec=validate(dict(name='dispatcher service',workdir=str(root),kind='interactive',priority=10000,users=[config['owner']],
        placement=dict(preferred_partitions=config.get('service_partitions',config['users'][config['owner']]['partitions']),allow_fallback=True,avoid_gpu_nodes=not bool(config.get('service_cpu_on_gpu_partitions'))),
        profiles=[dict(name='cpu-service-'+part,partition=part,cpu_only=part not in config.get('service_cpu_on_gpu_partitions',[]),ranks_per_node=1,threads=service_cpus,mem_mib=service_mem,wall_minutes=config.get('service_wall_minutes',1440))
                  for part in config.get('service_partitions',config['users'][config['owner']]['partitions'])]))
    import copy
    bootstrap=copy.deepcopy(config)
    if not config.get('shared_pool',{}).get('enabled'):bootstrap['pool_limits']['jobs']+=1
    for rank,node in enumerate(config.get('service_preferred_nodes',[])):
        bootstrap['node_cost'][node]=-100+rank
    if not config.get('shared_pool',{}).get('enabled'):bootstrap['users'][config['owner']]['limits']['jobs']+=1
    sel,reason=choose(spec,snap,bootstrap)
    if not sel: raise RuntimeError('Cannot start controller: '+reason)
    if plan_only:
        print(json.dumps(dict(after_current=after_current,selection=sel,checkpoint_victims=[],note='Forecast after predecessor release; Slurm final admission'),indent=2));return
    root.mkdir(parents=True,exist_ok=True)
    configpath=root/'config.json';atomic(configpath,config)
    import shlex
    script=root/'service.sh'
    supervisor=[config.get('python',sys.executable),'-m','cdispatch.service','--root',str(root),'--config',str(configpath)]
    if config.get('shared_pool',{}).get('enabled'):
        from .component_runtime import control_shape
        control_cpus,control_mem=control_shape(config)
        supervisor=['srun','--exact','--exclusive','--nodes=1','--ntasks=1','--cpus-per-task='+str(control_cpus),
                    '--mem='+str(control_mem)+'M','--cpu-bind=cores','--job-name=cd-control']+supervisor
    script.write_text('#!/bin/bash\nset -eu\nexport PYTHONPATH='+shlex.quote(str(BASE))+'\nexec '+shlex.join(supervisor)+'\n')
    args=['sbatch','--parsable','--job-name=cd-service','--partition='+sel['profile']['partition'],'--nodelist='+sel['nodes'][0],
        '--nodes=1','--ntasks=1','--cpus-per-task='+str(service_cpus),'--mem='+str(service_mem)+'M','--time='+str(sel['profile']['wall_minutes']),'--no-requeue','--signal=B:USR1@120',
        '--output='+str(root/'service-%j.out'),'--error='+str(root/'service-%j.err'),str(script)]
    if after_current:args.insert(2,'--dependency=afterany:'+after_current)
    intent=root/('service-renewal-intent.json' if after_current else 'bootstrap-intent.json');previous=read(intent,{})
    if previous.get('state') in ('SUBMITTING','SUBMIT_UNKNOWN'):raise RuntimeError('Ambiguous service submission: inspect cd-service in Slurm before clearing bootstrap intent')
    atomic(intent,dict(state='SUBMITTING',time=time.time(),selection=sel,after_current=after_current))
    try:
        job=slurm.user_run(config['owner'],args).strip()
        if not re.fullmatch(r'\d+(;[\w.-]+)?',job):raise RuntimeError('Invalid service sbatch acknowledgement')
        job=job.split(';')[0]
    except Exception:
        atomic(intent,dict(state='SUBMIT_UNKNOWN',time=time.time(),selection=sel,after_current=after_current));raise
    atomic(intent,dict(state='SUBMITTED',time=time.time(),job_id=job,selection=sel,after_current=after_current));print(job)

def interactive(root,config,args):
    if not args.command: raise ValueError('interactive needs a command after --')
    command=args.command[1:] if args.command[0]=='--' else args.command
    spec=dict(name=args.name,kind='interactive',users=[config['owner']],priority=args.priority,workdir=os.getcwd(),
              resource_key=os.path.join(os.getcwd(),'.interactive-'+uuid.uuid4().hex),
              labels=dict(project='interactive',purpose=args.name,stage='session',owner=config['owner']),
              placement=dict(avoid_gpu_nodes=not bool(args.shared and config.get('service_cpu_on_gpu_partitions'))),
              profiles=[dict(name='interactive-'+part,partition=part,cpu_only=not (args.shared and part in config.get('service_cpu_on_gpu_partitions',[])),ranks_per_node=1,threads=1,
                             mem_mib=args.mem,wall_minutes=args.minutes)
                        for part in config.get('interactive_partitions',config['users'][config['owner']]['partitions'])])
    if args.shared:spec.update(kind='shared',interactive=True,interactive_command=command,allow_shorter_walltime=True)
    from .failure_circuits import blocked_reason
    def check_controller(data):
        age=time.time()-data.get('heartbeat',0)
        if age>max(60,config.get('poll_interval',10)*3):
            raise RuntimeError('Dispatcher heartbeat stale; inspect cdispatch health and watchdog')
        if data.get('error'):raise RuntimeError('Dispatcher observation failed: '+str(data['error']))
        reason=blocked_reason(data,dict(spec=spec))
        if reason:raise RuntimeError(reason)
        return time.time()-(data.get('snapshot') or {}).get('time',0)<=max(60,config.get('poll_interval',10)*3)
    # A live controller can briefly publish a snapshot older than the threshold
    # while serializing a large queue. Wait within the original client deadline;
    # never launch against stale Slurm data or enqueue duplicate requests.
    start=time.monotonic();waiting_for_snapshot=False
    while not check_controller(read(root/'status.json',{})):
        if not waiting_for_snapshot:
            print('Waiting for a fresh Slurm snapshot; controller is alive.',file=sys.stderr,flush=True)
            waiting_for_snapshot=True
        if time.monotonic()-start>=args.wait:
            raise RuntimeError('Slurm snapshot stale; interactive wait deadline exceeded (no request submitted)')
        time.sleep(1)
    tid=enqueue(root,spec);a=None;proc=None;completed=False;last_reason=None
    try:
        print('Interactive '+tid+' · urgent · priority '+str(args.priority)+' · 1 CPU / '+str(args.mem)+' MiB',file=sys.stderr,flush=True)
        while time.monotonic()-start<args.wait:
            # Release the previous whole-queue snapshot before decoding another.
            d=None
            d=read(root/'status.json',{});t=d.get('tasks',{}).get(tid)
            # Renew this exact attempt's client lease even while observations
            # refresh, so a healthy slow poll cannot look like a vanished client.
            if t and t.get('attempt'):
                a=Path(t['attempt']);atomic(a/'client.json',dict(time=time.time()))
            if not check_controller(d):
                if last_reason!='snapshot stale':
                    print('Waiting for a fresh Slurm snapshot; keeping this request.',file=sys.stderr,flush=True)
                    last_reason='snapshot stale'
                time.sleep(1);continue
            receipt=read(root/'receipts'/('submit-'+tid+'.json'),{})
            if receipt and not receipt.get('ok'):raise RuntimeError(receipt.get('error','Interactive submission rejected'))
            if t:
                reason=t['state']+': '+str(t.get('reason') or '')
                if reason!=last_reason:print(reason,file=sys.stderr,flush=True);last_reason=reason
                if t.get('attempt'):
                    a=Path(t['attempt']);atomic(a/'client.json',dict(time=time.time()))
                if t['state']=='RUNNING': break
                if args.shared and t['state']=='PENDING' and t.get('selection',{}).get('shared'):break
                if t['state'] in ('FAILED','CANCELLED','BLOCKED','SUBMIT_UNKNOWN','HELD'): raise RuntimeError(t.get('reason'))
            time.sleep(1)
        else: raise RuntimeError('Interactive wait deadline exceeded')
        d=None  # Do not retain the historical queue for the whole terminal session.
        if args.shared:
            from .shared_launcher import launch
            print('Shared cd-service '+t['job_id']+' · '+','.join(t['selection']['nodes'])+' · remaining session limit '+str(t['selection']['profile']['wall_minutes'])+' min',file=sys.stderr)
            code=launch(a,interactive=True);completed=True;return code
        argv=['srun','--jobid='+t['job_id'],'--overlap','--nodes=1','--ntasks=1','--cpus-per-task=1','--cpu-bind=cores']
        if sys.stdin.isatty() and sys.stdout.isatty(): argv+=['--pty']
        # Client environment is used here, retaining selected CODEX_HOME/proxy values.
        proc=subprocess.Popen(argv+command,env=dict(os.environ,DISPATCH_TASK_ID=tid,CDISPATCH_ROOT=str(root)))
        while proc.poll() is None:
            atomic(a/'client.json',dict(time=time.time()));time.sleep(1)
        completed=True
        atomic(a/'client.json',dict(time=time.time(),finished=True,exit_code=proc.returncode))
        return proc.returncode
    finally:
        if proc and proc.poll() is None: proc.terminate()
        if not completed:
            if a:atomic(a/'client.json',dict(time=time.time(),finished=True,exit_code=143))
            request(root,'cancel',tid)

def main(argv=None):
    parser=argparse.ArgumentParser(description='Personal bounded Slurm dispatcher')
    parser.add_argument('--root',type=Path,default=state_root())
    parser.add_argument('--config',type=Path,default=config_path())
    sub=parser.add_subparsers(dest='command_name',required=True)
    p=sub.add_parser('init',help='Write a site configuration; never submits jobs')
    p.add_argument('--owner',required=True);p.add_argument('--account',required=True)
    p.add_argument('--partition',action='append',required=True)
    sub.add_parser('check-config',help='Validate local configuration and plugin imports without Slurm')
    p=sub.add_parser('submit');p.add_argument('spec',type=Path);p.add_argument('--id');p.add_argument('--wait-decision',type=float,default=15)
    p=sub.add_parser('limits');p.add_argument('--json',action='store_true');p.add_argument('--refresh',action='store_true')
    p=sub.add_parser('plan');p.add_argument('spec',type=Path);p.add_argument('--json',action='store_true')
    p=sub.add_parser('forecast');source=p.add_mutually_exclusive_group(required=True);source.add_argument('--spec',type=Path);source.add_argument('--task');p.add_argument('--json',action='store_true')
    for name in ('status','report','events'):
        p=sub.add_parser(name);p.add_argument('--json',action='store_true')
    p=sub.add_parser('health');p.add_argument('--json',action='store_true')
    for name in ('cancel','pause','resume','retry','show','logs','hold','run','unpin','explain'):
        p=sub.add_parser(name);p.add_argument('id')
    p=sub.add_parser('urgent');p.add_argument('id');p.add_argument('value',choices=['on','off'])
    p=sub.add_parser('pin');p.add_argument('id');p.add_argument('--profile');p.add_argument('--user');p.add_argument('--nodes',help='comma-separated explicit nodes')
    p=sub.add_parser('priority');p.add_argument('id');p.add_argument('value',type=int)
    p=sub.add_parser('resolve-absent');p.add_argument('id');p.add_argument('--evidence',required=True)
    p=sub.add_parser('cancel-group');p.add_argument('group')
    p=sub.add_parser('dashboard');p.add_argument('--port',type=int,default=8787)
    for name in ('daemon','tick','pause-all','unpause-all','doctor','watchdog'):sub.add_parser(name)
    p=sub.add_parser('clear-circuit');p.add_argument('--scope',help='Workload scope from status/health; default clears the global circuit only')
    p=sub.add_parser('reload');p.add_argument('component',choices=('daemon','bot'))
    p=sub.add_parser('service-start');p.add_argument('--after-current');p.add_argument('--plan',action='store_true')
    p=sub.add_parser('watch');p.add_argument('--view',choices=['overview','queue','nodes','users','limits','decisions','events'],default='overview');p.add_argument('--interval',type=float,default=3);p.add_argument('--once',action='store_true')
    p=sub.add_parser('panels');p.add_argument('--detach',action='store_true')
    p=sub.add_parser('profiles',help='Search, qualify and assemble reusable resource profiles');p.add_argument('--library',type=Path);p.add_argument('profile_args',nargs=argparse.REMAINDER)
    p=sub.add_parser('pool-size');p.add_argument('--cpus',type=int,required=True);p.add_argument('--mem',type=lambda x:int(mib(x)),required=True)
    p=sub.add_parser('interactive');p.add_argument('--name',default='interactive');p.add_argument('--priority',type=int,default=1000)
    p.add_argument('--mem',type=lambda x:int(mib(x)),default=512);p.add_argument('--minutes',type=lambda x:int(seconds(x)/60),default=1440);p.add_argument('--wait',type=int,default=120);p.add_argument('--shared',action='store_true')
    p.add_argument('command',nargs=argparse.REMAINDER)
    args=parser.parse_args(argv);root=args.root.resolve();cmd=args.command_name
    os.environ["CDISPATCH_CONFIG"] = str(args.config.resolve())
    os.environ["CDISPATCH_ROOT"] = str(root)
    if cmd=='init':
        from .configuration import initial
        cfg=initial(args.owner,args.account,args.partition)
        args.config.parent.mkdir(parents=True,exist_ok=True)
        with args.config.open('x') as stream:
            json.dump(cfg,stream,indent=2);stream.write('\n')
        print(args.config)
        return 0
    if cmd=='profiles':
        from .profile_library import main as profile_main
        return profile_main((['--library',str(args.library)] if args.library else [])+args.profile_args)
    config=read(args.config)
    if config is None:
        parser.error("Configuration not found: %s; set --config or CDISPATCH_CONFIG" % args.config)
    if cmd=='check-config':
        from .plugins import check
        if config.get('owner') not in config.get('users',{}):
            raise ValueError('Owner must be present in users')
        if not isinstance(config.get('pool_limits',{}).get('jobs'),int) or config['pool_limits']['jobs']<1:
            raise ValueError('pool_limits.jobs must be a positive integer')
        for user,account in config['users'].items():
            if not account.get('account') or not account.get('partitions') or 'jobs' not in account.get('limits',{}):
                raise ValueError('Incomplete account configuration: '+user)
        print(json.dumps(dict(ok=True,plugins=check(config),root=str(root),config=str(args.config))))
        return 0
    if cmd=='reload':
        from .runtime_control import reload_component
        return reload_component(root,args.component)
    if cmd=='health':
        from .alerts import health
        result=health(read(root/'status.json',{}),
                      read(root/'telegram-status.json',{}) if config.get('telegram_settings') else None,
                      read(root/'service-status.json',{}))
        if args.json:print(json.dumps(result,ensure_ascii=False,indent=2))
        else:
            print(('OK' if result['ok'] else 'WARNING')+' · heartbeat %.0f с · Slurm %.0f с'%(result['heartbeat_age'],result['slurm_age']))
            for warning in result['warnings'].values():print('⚠ '+warning)
            if config.get('telegram_settings'):print('Уведомления Telegram: '+('включены' if result['telegram_notifications'] else 'выключены/нет свежего подтверждения'))
        return 0 if result['ok'] else 1
    if cmd=='submit':
        tid=enqueue(root,read(args.spec),args.id);print(tid,flush=True)
        until=time.monotonic()+args.wait_decision
        while time.monotonic()<until:
            task=read(root/'status.json',{}).get('tasks',{}).get(tid)
            if task and (task.get('decision') or task.get('reason')!='new'):
                print(json.dumps(dict(id=tid,state=task['state'],reason=task.get('reason'),decision=task.get('decision')),ensure_ascii=False,indent=2));break
            receipt=read(root/'receipts'/('submit-'+tid+'.json'))
            if receipt and not receipt.get('ok') and not (root/'inbox'/('submit-'+tid+'.json')).exists():raise ValueError(receipt.get('error'))
            time.sleep(.5)
        else:print('Queued locally; decision pending. Use show '+tid)
    elif cmd in ('plan','forecast'):
        from .planning import forecast,forecast_text
        d=read(root/'status.json',{});tid=getattr(args,'task',None)
        if tid:
            matching=[key for key in d.get('tasks',{}) if key.startswith(tid)]
            if len(matching)!=1:raise ValueError('Use an unambiguous existing task ID')
            tid=matching[0]
        result=forecast(read(args.spec) if args.spec else None,d,config,tid)
        print(json.dumps(result,ensure_ascii=False,indent=2) if args.json else forecast_text(result))
    elif cmd in ('daemon','tick'):
        from .daemon import Daemon
        if cmd=='daemon' and not os.environ.get('SLURM_JOB_ID'):raise RuntimeError('Use service-start; persistent controller requires a compute allocation')
        if cmd=='daemon':
            from .component_runtime import isolated_command as component_command,run_isolated as run_component
            command=component_command('daemon',[sys.executable,'-m','cdispatch.cli','--root',str(root),'--config',str(args.config),'daemon'],config,os.environ)
            if command is not None:return run_component('daemon',command)
            if not config.get('component_steps',{}).get('enabled'):
                from .daemon_runtime import isolated_command, run_isolated
                command=isolated_command(root,args.config,config,os.environ)
                if command is not None:return run_isolated(command)
        Daemon(root,config).run(once=cmd=='tick')
    elif cmd=='limits':
        from .limits import report as limit_report
        snapshot=Slurm(config).snapshot() if args.refresh else read(root/'status.json',{}).get('snapshot',{})
        if not snapshot:raise ValueError('No cached snapshot; use limits --refresh')
        if args.refresh:snapshot['time']=time.time()
        result=limit_report(snapshot,config)
        if args.json:print(json.dumps(result,ensure_ascii=False,indent=2))
        else:
            print('Local jobs:',config['pool_limits']['jobs'],'; resource ceilings:',config['pool_limits'])
            print('Slurm enforcement:',result['limits'].get('accounting_storage_enforce','unknown'))
            pool=result['physical_pool'];print('Unique healthy nodes:',len(pool['node_names']),'; CPU:',pool['cpus'],'; RAM GiB:',round(pool['mem_mib']/1024,1))
            for u,a in result['users'].items():print(u, 'usage:',a['usage'],'; local policy:',a['local_policy'])
            print(result['note'])
    elif cmd=='doctor':
        s=Slurm(config);snap=s.snapshot();snap['time']=time.time();atomic(root/'doctor.json',snap)
        from .common import run
        details={}
        for name,argv in [('associations',['sacctmgr','-nP','show','assoc','format=Account,User,Partition,QOS,DefaultQOS,GrpTRES,GrpJobs,GrpSubmitJobs,MaxJobs,MaxSubmitJobs']),
                          ('qos',['sacctmgr','-nP','show','qos','format=Name,Flags,GrpTRES,MaxTRESPU,MaxJobsPU,MaxSubmitPU']),
                          ('config',['scontrol','show','config'])]:
            try: details[name]=run(argv)
            except Exception as e:details[name]=str(e)
        atomic(root/'limits-audit.json',details)
        print(json.dumps(dict(nodes=len(snap['nodes']),jobs=len(snap['jobs']),audit=str(root/'limits-audit.json')),indent=2))
    elif cmd=='service-start':
        with lock(root/'bootstrap.lock'): service_start(root,config,args.after_current,args.plan)
    elif cmd=='watchdog':
        from .watchdog import check
        print(time.strftime('%Y-%m-%d %H:%M:%S'),check(root,config))
    elif cmd=='interactive': return interactive(root,config,args)
    elif cmd=='pool-size':
        token=request(root,'pool-size','*',cpus=args.cpus,mem_mib=args.mem)
        print('Requested shared pool resize after handlers finish; receipt '+token)
    elif cmd=='dashboard':
        if not os.environ.get('SLURM_JOB_ID'):raise RuntimeError('Dashboard server requires a compute allocation; use cdispatch panels for automatic attachment')
        import http.server,functools,shutil
        root.mkdir(parents=True,exist_ok=True);shutil.copyfile(Path(__file__).with_name('dashboard.html'),root/'index.html')
        server=http.server.ThreadingHTTPServer(('127.0.0.1',args.port),functools.partial(http.server.SimpleHTTPRequestHandler,directory=str(root)))
        print('http://127.0.0.1:'+str(args.port),flush=True);server.serve_forever()
    elif cmd in ('watch','panels'):
        from .panels import attach,watch,panels
        if not os.environ.get('SLURM_JOB_ID') and not getattr(args,'once',False):
            options=['watch','--view',args.view,'--interval',str(args.interval)] if cmd=='watch' else ['panels']+(['--detach'] if args.detach else [])
            return attach(root,config,options)
        return watch(root,args.view,args.interval,args.once) if cmd=='watch' else panels(root,args.detach)
    elif cmd in ('status','report','events','show','logs','cancel-group','explain'):
        d=read(root/'status.json',{})
        if cmd=='status': print(json.dumps(d,indent=2) if args.json else status_text(d))
        elif cmd=='report':print(json.dumps(report(d),indent=2))
        elif cmd=='events':
            print((root/'events.jsonl').read_text() if (root/'events.jsonl').exists() else '')
        elif cmd=='cancel-group':
            token=request(root,'cancel-group',args.group)
            print('Requested group cancellation '+args.group+'; receipt '+token)
        else:
            ids=[tid for tid in d.get('tasks',{}) if tid.startswith(args.id)]
            if args.id in d.get('tasks',{}):ids=[args.id]
            if len(ids)!=1: raise ValueError('Use unambiguous task ID')
            task=d['tasks'][ids[0]]
            if cmd=='show':
                from .task_history import TaskHistory
                task=TaskHistory(root).load(task)
            if cmd=='logs':
                log=Path(task['attempt'])/'application.log'
                with log.open('rb') as f:
                    f.seek(max(0,log.stat().st_size-65536));print(f.read().decode(errors='replace'))
            elif cmd=='explain':
                print(json.dumps({k:task.get(k) for k in ('id','state','reason','decision','pending_advice','placement_override','cancel_error')},ensure_ascii=False,indent=2))
            else:print(json.dumps(task,indent=2))
    else:
        tid=getattr(args,'id','*');d=read(root/'status.json',{})
        if tid!='*':
            ids=[x for x in d.get('tasks',{}) if x.startswith(tid)]
            if tid in d.get('tasks',{}):ids=[tid]
            if len(ids)==1:tid=ids[0]
        extra={k:getattr(args,k) for k in ('value','evidence','scope') if hasattr(args,k)}
        if cmd=='urgent':extra['value']=args.value=='on'
        if cmd=='pin':extra={k:getattr(args,k) for k in ('profile','user') if getattr(args,k)};extra['nodes']=args.nodes.split(',') if args.nodes else None
        request(root,cmd,tid,**extra);print('Requested '+cmd+' '+tid)
    return 0

if __name__=='__main__':
    try:sys.exit(main())
    except KeyboardInterrupt:print('Interrupted; interactive cancellation requested.',file=sys.stderr);sys.exit(130)
    except (ValueError,RuntimeError,KeyError,OSError) as e:print('Error: '+str(e),file=sys.stderr);sys.exit(2)
