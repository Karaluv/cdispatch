import copy
import json
import os
from pathlib import Path
import shlex
import re
import signal
import sys
import time
import traceback
from .common import ACTIVE,TERMINAL,atomic,read,lock,validate,tres,mib,task_identifier
from .scheduler import choose, reserve, usage, SLURM_ACTIVE, queue_plan, urgent, release, choices
from .slurm import Slurm
from .policy import pending_advice, feedback, host_matches
from .plugins import expected_failure, check as check_plugins
from .failure_circuits import update_circuits, filter_ready, blocked_reason
from .state_reader import read_scheduler_state

class Daemon:
    def __init__(self,root,config,backend=None):
        self.root=Path(root);self.config=config;check_plugins(config);self.slurm=backend or Slurm(config)
        from .task_history import TaskHistory
        self.history=TaskHistory(self.root) if config.get('task_history',{}).get('enabled') else None
        loader=self.history.read_state if self.history else read_scheduler_state
        self.state=loader(self.root/'state.json',dict(tasks={},last_submit=0,events=[],accounting=[],created=time.time()))
        if not self.history and any(t.get('_history') for t in self.state['tasks'].values()):
            self.state=TaskHistory(self.root).expand(self.state)
        self.state['task_record_schema_version']=2 if self.history else 1
        for tid,t in self.state['tasks'].items():
            if 'submitted_spec' not in t and not t.get('_history'):
                original=read(self.root/'submissions'/(tid+'.json'))
                if original is None:original=read(self.root/'receipts'/('submit-'+tid+'.json'),{}).get('request',{})
                t['submitted_spec']=copy.deepcopy(original.get('spec',t['spec']))
        self.snapshot=None;self.error=None;self.stop=False;self.last_account=0
        self.root.mkdir(parents=True,exist_ok=True)
        (self.root/'inbox').mkdir(exist_ok=True)
        self.config['service_job_id']=os.environ.get('SLURM_JOB_ID',self.config.get('service_job_id',''))

    def event(self,task,kind,**data):
        e=dict(time=time.time(),task=task,kind=kind,**data)
        self.state['events'].append(e)
        counters=self.state.setdefault('counters',{})
        if kind=='TERMINAL':
            key=data.get('state','unknown');counters[key]=counters.get(key,0)+1
            if data.get('external'):counters['external_cancellations']=counters.get('external_cancellations',0)+1
        self.state['events']=self.state['events'][-500:]
        with (self.root/'events.jsonl').open('a') as f:
            f.write(json.dumps(e,ensure_ascii=False)+'\n');f.flush();os.fsync(f.fileno())

    def save(self):
        if self.history:self.history.compact(self.state)
        atomic(self.root/'state.json',self.state)

    def ingest(self):
        # Bound inbox work so durable saves cannot starve snapshots.
        def inbox_order(path):
            try:
                r=read(path,{})
                category=(0 if r.get('op')!='submit' else
                          1 if r.get('spec',{}).get('labels',{}).get('project')=='interactive' else 2)
                return category,path.stat().st_mtime_ns
            except (OSError,ValueError):return 0,0
        pending=sorted((self.root/'inbox').glob('*.json'),key=inbox_order)
        deadline=time.monotonic()+5
        for index,path in enumerate(pending):
            if index and time.monotonic()>=deadline:break
            try:
                r=read(path);op=r['op'];tid=r['id'];tasks=self.state['tasks']
                if op=='submit':
                    task_identifier(tid)
                    spec=validate(r['spec'])
                    from .fair_share import validate_membership
                    validate_membership(spec,tasks,self.config)
                    claim=read(self.root/'submissions'/(tid+'.json'))
                    if claim and claim['spec']!=spec:raise ValueError('idempotency key has a different spec')
                    if tid in tasks:
                        existing=self.history.load(tasks[tid]) if self.history else tasks[tid]
                        original=existing.get('submitted_spec',existing['spec'])
                        if original!=spec: raise ValueError('idempotency key has a different spec')
                    else:
                        # All predecessors must be known. This also makes cycles impossible.
                        if any(d not in tasks for d in spec['depends_on']): raise ValueError('Unknown dependency; submit predecessors first')
                        if spec.get('parent') and spec['parent'] not in tasks: raise ValueError('Unknown parent')
                        if spec.get('adopt_job'):
                            identity=spec['adopt_job']
                            if identity['UserId'].split('(')[0]!=self.config['owner']:raise ValueError('Only owner allocations support managed adoption')
                            if any(x.get('job_id')==str(identity['JobId']) for x in tasks.values()):raise ValueError('Job already managed by another task')
                        tasks[tid]=dict(id=tid,spec=spec,submitted_spec=copy.deepcopy(spec),created=(claim or r).get('created',time.time()),state='QUEUED',attempts=[],reason='new')
                        if spec.get('adopt_job'):
                            identity=spec['adopt_job']
                            tasks[tid].update(state='ADOPTING',job_id=str(identity['JobId']),job_name=identity['JobName'])
                        self.event(tid,'ENQUEUED',labels=spec.get('labels',{}))
                        if tid in self.state.get('pending_cancellations',{}):
                            self.cancel_intent(tasks[tid],self.state['pending_cancellations'].pop(tid))
                        cutoff=self.state.get('cancelled_groups',{}).get(spec['group'])
                        if cutoff is not None and tasks[tid]['created']<=cutoff:
                            self.cancel_intent(tasks[tid],'group cancellation')
                elif op=='pause-all': self.state['paused']=True;self.event(None,'SUBMISSIONS_PAUSED')
                elif op=='unpause-all': self.state['paused']=False;self.event(None,'SUBMISSIONS_ENABLED')
                elif op=='clear-circuit':
                    if any(t['state'] in ('SUBMITTING','SUBMIT_UNKNOWN') for t in tasks.values()): raise ValueError('Resolve ambiguous submissions first')
                    scope=r.get('scope') or 'global'
                    if scope=='global':self.state.pop('circuit',None)
                    else:
                        if scope not in self.state.get('workload_circuits',{}):raise ValueError('Unknown workload circuit scope')
                        self.state['workload_circuits'].pop(scope)
                    self.state.setdefault('circuit_reset_at',{})[scope]=time.time()
                    self.event(None,'CIRCUIT_CLEARED',scope=scope)
                elif op=='cancel-group':
                    if not isinstance(tid,str) or not tid:raise ValueError('Expected group name')
                    cutoff=r.get('created',time.time())
                    groups=self.state.setdefault('cancelled_groups',{})
                    groups[tid]=max(cutoff,groups.get(tid,0))
                    for t in tasks.values():
                        if t['spec']['group']==tid and t['created']<=groups[tid]:
                            self.cancel_intent(t,'group cancellation')
                    self.event(None,'GROUP_CANCEL_REQUESTED',group=tid)
                elif op=='transfer-parent':
                    from .parent_transfer import transfer
                    children=transfer(tasks,tid,r['new_parent'],self.config['owner'],self.config.get('controller_transfer_stages',['controller']))
                    self.event(tid,'CHILDREN_TRANSFERRED',new_parent=r['new_parent'],children=children)
                    self.save()
                elif op=='cancel':
                    task_identifier(tid)
                    if tid in tasks:self.cancel_intent(tasks[tid],'dispatcher cancellation')
                    else:
                        self.state.setdefault('pending_cancellations',{})[tid]='dispatcher cancellation'
                        self.event(tid,'CANCEL_BEFORE_SUBMIT')
                elif op=='pool-size':
                    from .shared import envelope
                    cpus=r['cpus'];mem=r['mem_mib'];cfg=self.config.get('shared_pool',{})
                    if type(cpus) is not int or type(mem) is not int or not cfg.get('enabled'):raise ValueError('Shared pool disabled or invalid size')
                    if not cfg.get('reserve_cpus',1)<cpus<=cfg.get('max_cpus',8) or not cfg.get('reserve_mem_mib',512)<mem<=cfg.get('max_mem_mib',16384):raise ValueError('Shared pool size outside configured bounds')
                    atomic(self.root/'pool-size.json',dict(cpus=cpus,mem_mib=mem,time=time.time(),reason='operator request'))
                    self.event(None,'SHARED_RESIZE_REQUESTED',cpus=cpus,mem_mib=mem)
                else:
                    if self.history:tasks[tid]=self.history.load(tasks[tid])
                    t=tasks[tid]
                    if op=='pause': self.pause(t,False)
                    elif op=='hold':
                        if t['state'] not in ('QUEUED','HELD'):raise ValueError('hold only applies to local queued tasks; use checkpoint pause for running tasks')
                        t['state']='HELD';t['reason']='manual hold';t['manual_hold']=True;self.event(tid,'HELD_BY_OPERATOR')
                    elif op=='run':
                        if t['state']!='QUEUED':raise ValueError('run requires QUEUED; release hold first')
                        t['manual_dispatch']=True;self.event(tid,'MANUAL_DISPATCH_REQUESTED')
                    elif op=='urgent':
                        if type(r.get('value')) is not bool:raise ValueError('urgent requires boolean')
                        t['spec']['urgent']=r['value']
                        if not r['value']:t['spec']['tags']=[x for x in t['spec']['tags'] if x!='urgent']
                        self.event(tid,'URGENCY',value=r['value'])
                    elif op in ('pin','unpin'):
                        if t['state'] not in ('QUEUED','HELD','PAUSED'):raise ValueError('Placement can change only before an allocation')
                        override={k:r[k] for k in ('profile','user','nodes') if r.get(k)} if op=='pin' else {}
                        if override.get('profile') not in (None,*[p['name'] for p in t['spec']['profiles']]):raise ValueError('Unknown qualified profile')
                        if override.get('user') and override['user'] not in t['spec'].get('users',self.config['users']):raise ValueError('User outside task allowlist')
                        if override.get('nodes') and (not isinstance(override['nodes'],list) or any(not isinstance(n,str) or not re.fullmatch(r'[\w.-]+',n) for n in override['nodes'])):raise ValueError('Expected explicit node names')
                        t['placement_override']=override;self.event(tid,'PLACEMENT_OVERRIDE',selection=override)
                    elif op=='resume':
                        if t['spec'].get('adopt_job') and not t.get('adopted'):raise ValueError('Unverified adoption must not become a fresh submission')
                        if t['state']=='HELD' and Path(t['spec']['workdir']).is_dir():
                            t['state']='QUEUED';t.pop('manual_hold',None);t['reason']='hold released';self.event(tid,'RELEASED_HOLD')
                        else:
                            if t['state']!='PAUSED' or not t.get('checkpoint'): raise ValueError('No validated paused checkpoint')
                            if len(t['attempts'])>=t['spec']['max_attempts']: raise ValueError('Attempt budget exhausted')
                            t['state']='QUEUED';t['reason']='resume requested';self.event(tid,'RESUME_REQUESTED')
                    elif op=='retry':
                        if t['state'] not in ('FAILED','CANCELLED','BLOCKED'): raise ValueError('Not terminal')
                        if not t['spec'].get('restart_from_scratch_safe'): raise ValueError('Explicit restart_from_scratch_safe contract required')
                        if len(t['attempts'])>=t['spec']['max_attempts']: raise ValueError('Attempt budget exhausted')
                        t.pop('checkpoint',None)
                        for key in ('cancel_requested','cancel_reason','cancel_sent_job_id','cancel_last_attempt','cancel_error'):t.pop(key,None)
                        t['state']='QUEUED';self.event(tid,'RETRY_REQUESTED')
                    elif op=='priority': t['spec']['priority']=int(r['value']);self.event(tid,'PRIORITY',value=r['value'])
                    elif op=='resolve-absent':
                        if t['state'] not in ('SUBMITTING','SUBMIT_UNKNOWN'): raise ValueError('Not ambiguous')
                        if not r.get('evidence'): raise ValueError('Document external evidence that sbatch did not accept job')
                        t['state']='FAILED';t['reason']='operator confirmed no submitted job';self.event(tid,'RESOLVED_ABSENT',evidence=r['evidence'])
                    else: raise ValueError('Unknown operation '+op)
                # Persist before removing inbox request. Submit is naturally idempotent.
                self.save()
                atomic(self.root/'receipts'/path.name,dict(ok=True,time=time.time(),request=r))
            except Exception as e:
                self.event(None,'REQUEST_REJECTED',path=path.name,error=str(e))
                self.save();atomic(self.root/'receipts'/path.name,dict(ok=False,error=str(e)))
            path.unlink()

    def cancel_intent(self,t,reason):
        if t['state'] in TERMINAL or t.get('cancel_requested'):return
        t['cancel_requested']=True;t['cancel_reason']=reason
        if t['state'] in ('QUEUED','HELD','PAUSED'):
            t['state']='CANCELLED';t['reason']=reason
        self.event(t['id'],'CANCEL_REQUESTED',job_id=t.get('job_id'),reason=reason)

    def maintain_lifecycle(self):
        # Cancellation and orphan cleanup run even while admissions are paused,
        # rate limited, or blocked by an ambiguous submission/snapshot failure.
        tasks=self.state['tasks']
        changed=True
        while changed:
            changed=False
            for child in tasks.values():
                from .parent_transfer import parent_id
                parent=tasks.get(parent_id(child))
                if (parent and (parent['state'] in TERMINAL or parent.get('cancel_requested'))
                        and child['state'] not in TERMINAL and not child.get('cancel_requested')):
                    self.cancel_intent(child,'parent ended: '+parent['id']);changed=True
                    self.event(child['id'],'PARENT_ENDED',parent=parent['id'])
        self.save()  # Intent must survive a crash or timeout during scancel.
        now=time.time()
        for t in tasks.values():
            if not t.get('cancel_requested') or t['state'] not in ACTIVE or not t.get('job_id'):continue
            if t.get('selection',{}).get('shared'):
                from .shared import cancel
                try:cancel(self,t)
                except Exception as e:t['cancel_error']=str(e)
                continue
            if t['spec'].get('adopt_job') and not t.get('adopted'):continue
            if t.get('cancel_sent_job_id')==t['job_id']:continue
            if now-t.get('cancel_last_attempt',0)<self.config.get('cancel_retry_interval',30):continue
            t['cancel_last_attempt']=now;self.save()
            try:self.slurm.cancel(t['user'],t['job_id'])
            except Exception as e:
                t['cancel_error']=str(e);t['reason']='cancellation pending: '+str(e)
                self.event(t['id'],'CANCEL_RETRY',job_id=t['job_id'],error=str(e))
            else:
                t['cancel_sent_job_id']=t['job_id'];t.pop('cancel_error',None);t['state']='CANCELLING'
                self.event(t['id'],'CANCEL_SENT',job_id=t['job_id'])
            self.save()

    def pause(self,t,automatic):
        if t['state']!='RUNNING' or not t['spec'].get('preemptible'): raise ValueError('Task is not a running checkpointable task')
        t['pause_requested']=time.time();t['auto_resume']=automatic;self.save()
        self.slurm.pause(t['user'],t['job_id']);t['state']='PAUSING'
        self.event(t['id'],'CHECKPOINT_REQUESTED',automatic=automatic)

    def reconcile(self):
        jobs={j['JobId']:j for j in self.snapshot['jobs']}
        by_name={j.get('JobName'):j for j in self.snapshot['jobs']}
        # Completed accounting is queried in one batch, never once per job.
        missing=[t for t in self.state['tasks'].values() if t['state'] in ACTIVE and
                 (not t.get('job_id') or jobs.get(t['job_id'],{}).get('JobState') not in SLURM_ACTIVE)]
        external_missing=[jid for jid in self.state.get('observed_external',{}) if jobs.get(jid,{}).get('JobState') not in SLURM_ACTIVE]
        if missing or external_missing or time.time()-self.last_account>=self.config.get('account_interval',300):
            since=time.strftime('%Y-%m-%d',time.localtime(self.state['created']-86400))
            full=time.time()-self.last_account>=self.config.get('account_interval',300) or any(not t.get('job_id') for t in missing)
            ids=None if full else list({t['job_id'] for t in missing}|set(external_missing))
            records=self.slurm.accounting(since,ids=ids)
            if full:self.state['accounting']=records;self.last_account=time.time()
            else:
                merged={r['id']:r for r in self.state['accounting']};merged.update({r['id']:r for r in records})
                self.state['accounting']=list(merged.values())
        all_acct={r['id']:r for r in self.state['accounting']}
        acct={key:r for key,r in all_acct.items() if '.' not in key}
        anames={r['name']:r for r in acct.values()}
        observed=self.state.setdefault('observed_external',{})
        owned={t.get('job_id') for t in self.state['tasks'].values()}
        for jid,j in jobs.items():
            if jid not in owned and j.get('JobState') in SLURM_ACTIVE:observed[jid]=j
        for jid,j in list(observed.items()):
            r=acct.get(jid)
            if r and r['state'].split()[0].rstrip('+') not in SLURM_ACTIVE:
                self.event(None,'EXTERNAL_JOB_TERMINAL',job_id=jid,name=j.get('JobName'),user=j.get('UserId'),slurm_state=r['state'],exit_code=r['exit_code'],cancellation_actor=r['state'].partition(' by ')[2] or None)
                del observed[jid]
        for t in self.state['tasks'].values():
            if t['state'] not in ACTIVE: continue
            if t.get('selection',{}).get('shared'):
                from .shared import reconcile
                reconcile(self,t,jobs,all_acct);continue
            if t['state']=='ADOPTING':
                identity=t['spec']['adopt_job'];job=jobs.get(t['job_id'])
                if not job:
                    t['reason']='awaiting live identity check for adopted allocation';continue
                if any(job.get(k)!=identity[k] for k in ('JobName','UserId','Command','SubmitTime')):
                    t['state']='HELD';t['reason']='adoption identity mismatch';self.event(t['id'],'ADOPTION_REJECTED');continue
                user=job['UserId'].split('(')[0];profile=copy.deepcopy(t['spec']['profiles'][0])
                nodes=[n['NodeName'] for n in self.snapshot['nodes'] if host_matches(job.get('NodeList',''),n['NodeName'])]
                selection=dict(user=user,profile=profile,nodes=nodes,mem_mib=profile['mem_mib'])
                attempt=self.root/'attempts'/t['id']/'adopted';attempt.mkdir(parents=True,exist_ok=True)
                atomic(attempt/'adoption.json',dict(identity=identity,observed=job,time=time.time()))
                t.update(adopted=True,state=job['JobState'],user=user,selection=selection,attempt=str(attempt),
                         attempts=[dict(number=1,path=str(attempt),job_id=t['job_id'],selection=selection,adopted=True)])
                self.event(t['id'],'ADOPTED',job_id=t['job_id']);self.save()
            if t['state'] in ('SUBMITTING','SUBMIT_UNKNOWN'):
                j=by_name.get(t['job_name']);r=anames.get(t['job_name'])
                if j or r:
                    t['job_id']=j['JobId'] if j else r['id'];t['state']='PENDING'
                    t['attempts'][-1]['job_id']=t['job_id']
                    self.event(t['id'],'SUBMISSION_RECOVERED',job_id=t['job_id'])
                else:
                    t['state']='SUBMIT_UNKNOWN';self.state['circuit']='ambiguous sbatch; reconcile or resolve-absent';continue
            j=jobs.get(t.get('job_id'))
            if j and j.get('JobState') in SLURM_ACTIVE:
                new=j['JobState'];t['slurm']=j
                if t['state'] not in ('PAUSING','CANCELLING'):
                    if t['state']!=new: self.event(t['id'],'SLURM_STATE',state=new,job_id=t['job_id'])
                    t['state']=new if new in ACTIVE else 'RUNNING'
                t['reason']=j.get('Reason','')
                if j['JobState']=='PENDING':
                    advice=dict(reason=j.get('Reason',''),**pending_advice(j.get('Reason')))
                    if t.get('pending_advice')!=advice:
                        t['pending_advice']=advice;self.event(t['id'],'PENDING_POLICY',**advice)
                else:t.pop('pending_advice',None)
                if t['state']=='PAUSING':
                    rejected=read(Path(t['attempt'])/'pause-rejected.json')
                    if rejected and rejected['time']>=t['pause_requested']:
                        self.event(t['id'],'CHECKPOINT_REJECTED',detail=rejected);t['state']='RUNNING'
                continue
            r=acct.get(t.get('job_id'))
            if not r or r['state'].split()[0] in SLURM_ACTIVE:
                t['reason']='awaiting terminal accounting; no resubmission';continue
            raw=r['state'];base=raw.split()[0].rstrip('+');t['accounting']=r
            result=read(Path(t['attempt'])/'result.json')
            if t.get('adopted') and base=='COMPLETED' and r.get('exit_code')=='0:0' and not t.get('cancel_requested'):
                from .artifacts import check
                try:result=dict(state='SUCCEEDED',reason='adopted allocation artifacts validated',files=check(t['spec']['artifact_contract']))
                except (ValueError,OSError) as e:result=dict(state='FAILED',reason=str(e))
                atomic(Path(t['attempt'])/'result.json',result)
            if t.get('cancel_requested'):
                final='CANCELLED';reason=t.get('cancel_reason','dispatcher cancellation')
            elif result and result.get('state')=='PAUSED' and read(Path(t['attempt'])/'checkpoint.json'):
                final='PAUSED';t['checkpoint']=result['checkpoint'];reason='checkpoint confirmed'
            elif base=='COMPLETED' and result:
                final=result['state'];reason=result.get('reason','worker finished')
            else:
                final='FAILED';reason='external cancellation' if base=='CANCELLED' else 'Slurm '+raw
            t['state']=final;t['reason']=reason
            t['attempts'][-1].update(state=final,accounting=r,result=result)
            expected_budget=final=='FAILED' and expected_failure(t,raw,t.get("job_id"),self.config)
            self.event(t['id'],'TERMINAL',state=final,reason=reason,slurm_state=raw,exit_code=r['exit_code'],
                       job_id=t['job_id'],external=base=='CANCELLED' and not t.get('cancel_requested'),
                       expected_time_budget=expected_budget,
                       cancellation_actor=raw.partition(' by ')[2] or None)
            if final=='PAUSED' and t.pop('auto_resume',False) and len(t['attempts'])<t['spec']['max_attempts']:
                t['state']='QUEUED';t['not_before']=time.time()+self.config['poll_interval']
        if self.state.get('circuit','').startswith('ambiguous') and not any(t['state'] in ('SUBMITTING','SUBMIT_UNKNOWN') for t in self.state['tasks'].values()):
            self.state.pop('circuit',None)
        update_circuits(self)

    def submit(self,t,selection):
        if selection.get('shared'):
            from .shared import start
            return start(self,t,selection)
        if not Path(t['spec']['workdir']).is_dir():
            t['state']='HELD';t['reason']='workdir does not exist';self.event(t['id'],'WORKDIR_MISSING');return
        number=len(t['attempts'])+1
        a=self.root/'attempts'/t['id']/str(number);a.mkdir(parents=True,exist_ok=True)
        label=t['spec'].get('labels',{});prefix=re.sub(r'[^a-zA-Z0-9_-]','-',label.get('project','')+'-'+label.get('stage',t['spec']['name']))[:24]
        name='cd-'+prefix+'-'+t['id']+'-'+str(number);selection['job_name']=name
        bundle=dict(id=t['id'],spec=t['spec'],selection=selection,created=time.time(),checkpoint=t.get('checkpoint'),root=str(self.root.resolve()))
        atomic(a/'spec.json',bundle)
        install=Path(__file__).resolve().parent.parent
        script='#!/bin/bash\nset -eu\nexport PYTHONPATH='+shlex.quote(str(install))+'\nexec '+shlex.quote(self.config.get('python',sys.executable))+' -m cdispatch.worker '+shlex.quote(str(a))+'\n'
        (a/'job.sh').write_text(script)
        if hasattr(self.slurm,'prepare'):
            try:self.slurm.prepare(selection['user'],a,t['spec']['workdir'])
            except Exception as e:
                t['state']='HELD';t['reason']='delegated access preflight: '+str(e);self.event(t['id'],'DELEGATED_ACCESS_BLOCKED',error=str(e));return
        t.update(state='SUBMITTING',attempt=str(a),job_name=name,user=selection['user'],selection=selection)
        t.pop('manual_dispatch',None)
        t.pop('job_id',None);t.pop('slurm',None)
        t['attempts'].append(dict(number=number,path=str(a),name=name,selection=selection))
        self.state['last_submit']=time.time();self.event(t['id'],'SUBMIT_INTENT',selection=selection);self.save()
        try:
            job=self.slurm.submit(selection['user'],selection,a)
        except Exception as e:
            t['state']='SUBMIT_UNKNOWN';t['reason']=str(e);self.state['circuit']='ambiguous sbatch; reconcile or resolve-absent'
            self.event(t['id'],'SUBMIT_UNKNOWN',error=str(e));self.save();return
        t['job_id']=job;t['state']='PENDING';t['attempts'][-1]['job_id']=job
        self.event(t['id'],'SUBMITTED',job_id=job);self.save();reserve(self.snapshot,selection,job,name)

    def schedule(self):
        if self.state.get('circuit'):
            for task in self.state['tasks'].values():
                if task['state']=='QUEUED':task['reason']=blocked_reason(self.state,task)
            self.state['plan']=dict(time=time.time(),assignments=[],decisions={})
            return
        if time.time()-self.state['last_submit']<self.config['submit_interval']: return
        tasks=self.state['tasks']
        from .planning import ready_tasks, queue_actions, checkpoint_barrier
        waiting=checkpoint_barrier(tasks)
        if waiting:
            for tid in waiting:tasks[tid]['reason']='waiting for displacement/checkpoint confirmation before preferred placement'
            return
        ready,blocked=ready_tasks(tasks,self.state.get('paused',False))
        ready,circuit_blocked=filter_ready(ready,self.state)
        blocked.update(circuit_blocked)
        for tid,decision in blocked.items():
            tasks[tid]['reason']=decision['reason']
            if decision['terminal']:tasks[tid]['state']='BLOCKED'
        plan,decisions,preemption=queue_actions(ready,tasks,self.snapshot,self.config)
        from .shared import maybe_grow
        maybe_grow(self,ready)
        if (self.snapshot.get('shared_pool') or {}).get('draining'):
            plan=[(tid,sel) for tid,sel in plan if not sel.get('shared')]
        for tid,decision in decisions.items():
            task=tasks[tid];task['reason']=decision['reason']
            # Log only changing decisions, avoiding identical records every poll.
            if task.get('decision')!=decision:
                task['decision']=decision;self.event(tid,'DECISION',decision=decision)
        self.state['plan']=dict(time=time.time(),assignments=plan,decisions=decisions)
        blocked_urgent=[t for t in ready if urgent(t['spec']) and not decisions[t['id']]['admitted']]
        if preemption:
            tasks[preemption['task']]['waiting_checkpoint_victims']=preemption['victims'];self.save()
            forced=set(preemption.get('force_victims',[]))
            for tid in preemption['victims']:
                if tid in forced:
                    self.cancel_intent(tasks[tid],'priority displacement for '+preemption['task']+'; partial progress discarded')
                    tasks[tid]['displaced_for']=preemption['task']
                else:self.pause(tasks[tid],True)
            self.event(preemption['task'],'WAIT_DISPLACEMENT' if forced else 'WAIT_CHECKPOINT',victims=preemption['victims'],reason=preemption['reason'])
            self.save();return
        if blocked_urgent and any(t['state']=='PAUSING' and t.get('auto_resume') for t in tasks.values()):return
        if plan:
            tid,selection=plan[0];self.submit(tasks[tid],selection)
        elif ready and self.config.get('auto_rebalance'):
            self.try_accelerate()

    def try_preempt(self,wanted):
        from .scheduler import preemption_plan
        action=preemption_plan(wanted,self.state['tasks'],self.snapshot,self.config)
        if not action:return False
        for tid in action['victims']:self.pause(self.state['tasks'][tid],True)
        self.event(wanted['id'],'WAIT_CHECKPOINT',victims=action['victims']);return True

    def try_accelerate(self):
        # MPI resizing needs application restart. Only explicitly portable, validated
        # checkpoints with measured duration estimates qualify for automatic migration.
        for t in self.state['tasks'].values():
            if t['state']!='RUNNING' or not t['spec'].get('elastic') or not t['spec'].get('preemptible'):continue
            if time.time()-t.get('pause_requested',0)<600:continue
            current=t['selection']['profile'];expected=current.get('expected_seconds')
            if not expected:continue
            from .common import seconds
            elapsed=seconds(t.get('slurm',{}).get('RunTime','00:00:00'))
            fraction=max(0,1-elapsed/expected)
            narrow=copy.deepcopy(t['spec']);allowed=narrow.get('resume_compatible_profiles',[])
            narrow['profiles']=[p for p in narrow['profiles'] if p['name'] in allowed and p['slowdown']<current['slowdown'] and p.get('expected_seconds')]
            snap=release(self.snapshot,t)
            for sel in choices(narrow,snap,self.config):
                p=sel['profile'];saving=(expected-p['expected_seconds'])*fraction
                overhead=t['spec'].get('migration_overhead_seconds',300)
                if saving>max(120,overhead*2):
                    t['migration_target']=p['name'];self.pause(t,True)
                    self.event(t['id'],'ACCELERATE_BY_RESTART',old=current['name'],target=p['name'],estimated_seconds_saved=saving,overhead_seconds=overhead)
                    return True
        return False

    def publish(self):
        from .limits import report as limit_report
        physical=limit_report(self.snapshot or {},self.config)['physical_pool']
        # Existing live controllers expect numeric pool bounds. Publish the
        # deduplicated hardware envelope for unset local ceilings, while keeping
        # the actual local policy separately. Admission still uses self.config.
        effective=dict(self.config['pool_limits'])
        for key in ('cpus','mem_mib','gpus','nodes'):
            if effective.get(key) is None:
                effective[key]=len(physical['node_names']) if key=='nodes' else physical[key]
        summary=dict(heartbeat=time.time(),error=self.error,paused=self.state.get('paused',False),circuit=self.state.get('circuit'),
                     workload_circuits=self.state.get('workload_circuits',{}),
                     task_record_schema_version=self.state['task_record_schema_version'],
                     physical_capacity=physical,
                     tasks=self.state['tasks'],events=self.state['events'][-100:],snapshot=self.snapshot,
                     plan=self.state.get('plan'),pending_feedback=feedback(self.snapshot or {}),accounting=self.state['accounting'],counters=self.state.get('counters',{}),
                     user_limits={u:a['limits'] for u,a in self.config['users'].items() if a.get('enabled')},
                     user_accounts={u:{k:a[k] for k in ('account','qos','authorized') if k in a} for u,a in self.config['users'].items() if a.get('enabled')},
                     accounting_since=time.strftime('%Y-%m-%d',time.localtime(self.state['created']-86400)),
                     last_submit=self.state.get('last_submit',0),
                     config={k:self.config[k] for k in ('account_labels','pool_limits','limit_policy','poll_interval','submit_interval','max_pending','interactive_reserve_jobs','auto_preempt','auto_rebalance','plan_window','plan_beam','respect_slurm_reservations','respect_pending_feedback','backfill_margin_seconds','cancel_retry_interval') if k in self.config})
        summary['config']['local_pool_limits']=dict(self.config['pool_limits'])
        summary['config']['pool_limits']=effective
        from .fair_share import report as fair_report
        summary['fair_share']=fair_report(self.state['tasks'],self.snapshot or {},self.config)
        summary['config']['fair_share']=self.config.get('fair_share',{})
        atomic(self.root/'status.json',summary)
        # An observer/projection update must not stop scheduling or overwrite
        # authoritative scheduler health. Its own failure is reported separately.
        try:
            from .monitor_cache import monitor_snapshot
            view = monitor_snapshot(summary)
            from .plugins import display_labels
            for tid, task in view.get('tasks', {}).items():
                task.setdefault('spec', {})['labels'] = dict(task.get('spec', {}).get('labels', {}), **display_labels(summary['tasks'][tid],self.config))
            atomic(self.root/'monitor.json',view)
            (self.root/'monitor-error.json').unlink(missing_ok=True)
        except Exception as error:
            try:atomic(self.root/'monitor-error.json',dict(time=time.time(),error=type(error).__name__+': '+str(error)))
            except OSError:pass

    def tick(self):
        self.ingest()
        try:
            fresh=self.slurm.snapshot();fresh['time']=time.time();self.snapshot=fresh
            self.reconcile()
            from .shared import pool_snapshot
            pool_snapshot(self.root,self.config,self.snapshot,self.state['tasks'])
            self.error=None
        except Exception as e:
            self.error=str(e);self.event(None,'CONTROL_ERROR',error=str(e))
        try:
            self.maintain_lifecycle()
            if self.error is None:self.schedule()
        except Exception as e:
            self.error=str(e);self.event(None,'CONTROL_ERROR',error=str(e))
        self.save();self.publish()

    def run(self,once=False):
        with lock(self.root/'controller.lock'):
            signal.signal(signal.SIGTERM,lambda *_:setattr(self,'stop',True))
            signal.signal(signal.SIGUSR1,lambda *_:setattr(self,'stop',True))
            signal.signal(signal.SIGINT,lambda *_:setattr(self,'stop',True))
            self.event(None,'CONTROLLER_START',job_id=self.config.get('service_job_id'))
            while not self.stop:
                start=time.monotonic();self.tick()
                if once: break
                delay=self.config['poll_interval']*(min(6,1+self.state.get('error_count',0)) if self.error else 1)
                self.state['error_count']=self.state.get('error_count',0)+1 if self.error else 0
                while not self.stop and time.monotonic()-start<delay: time.sleep(.2)
            self.event(None,'CONTROLLER_STOP');self.save();self.publish()
