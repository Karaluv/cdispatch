import os
import re
import shlex
import time
import json
from pathlib import Path
from .common import run, fields, tres, mib, seconds

class Slurm:
    def __init__(self, config):
        self.config=config; self.limits_time=0; self.limits_cache={}

    def user_run(self, user, argv, timeout=30):
        if user == self.config['owner']: return run(argv,timeout=timeout)
        account=self.config['users'][user]
        if not account.get('enabled') or not account.get('authorized'): raise ValueError('Delegation disabled')
        if account.get('ssh_identity'):
            return run(['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=10',
                        '-o','ClearAllForwardings=yes','-o','HostKeyAlias='+account.get('ssh_host_key_alias',account['ssh_host']),'-i',account['ssh_identity'],user+'@'+account['ssh_host'],json.dumps(argv)],timeout=timeout)
        socket=account.get('ssh_socket','')
        if not Path(socket).is_socket(): raise ValueError('Missing authorized SSH control socket: '+socket)
        return run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ClearAllForwardings=yes',
                    '-S',socket,user+'@localhost',shlex.join(argv)],timeout=timeout)

    def snapshot(self):
        nodes=[fields(x) for x in run(['scontrol','show','nodes','-o']).splitlines() if x.startswith('NodeName=')]
        partitions=[fields(x) for x in run(['scontrol','show','partition','-o']).splitlines() if x.startswith('PartitionName=')]
        users=','.join(self.config['users'])
        jobs=[fields(x) for x in run(['scontrol','show','jobs','-o']).splitlines() if x.startswith('JobId=')]
        account_jobs=[j for j in jobs if j.get('Account') in {u['account'] for u in self.config['users'].values()}]
        queue_keys=('JobId','JobState','Reason','Partition','Priority','StartTime','SchedNodeList','ReqNodeList','ReqTRES','NumNodes','UserId','Account','QOS')
        queue_jobs=[{k:j.get(k,'') for k in queue_keys} for j in jobs if j.get('JobState')=='PENDING']
        limit_keys=queue_keys+('AllocTRES','NumCPUs','ArrayTaskId','ArrayJobId','AssociationId','NodeList')
        limit_jobs=[{k:j.get(k,'') for k in limit_keys} for j in jobs]
        jobs=[j for j in jobs if j.get('UserId','').split('(')[0] in self.config['users']]
        steps=[];service=self.config.get('service_job_id')
        if service and self.config.get('shared_pool',{}).get('enabled'):
            services=[service]
            handoff=self.config.get('shared_allocation_handoff',{})
            if service==handoff.get('predecessor'):
                target=handoff.get('target','')
                if not re.fullmatch(r'\d+',target):raise ValueError('Invalid service handoff target')
                services.append(target)
            for line in run(['squeue','--steps','--jobs='+','.join(services),'--noheader','--format=%i|%100j']).splitlines():
                parts=line.strip().split('|',1)
                if len(parts)==2 and re.fullmatch(r'\d+\.\d+',parts[0]) and parts[0].split('.')[0] in services:steps.append(dict(id=parts[0],name=parts[1]))
        return {'nodes':nodes,'partitions':{p['PartitionName']:p for p in partitions},'jobs':jobs,
                'account_jobs':account_jobs,'queue_jobs':queue_jobs,'limit_jobs':limit_jobs,'steps':steps,'limits':self.limits()}

    def accounting(self, since, ids=None):
        args=['sacct','-nP','-S',since,'-u',','.join(self.config['users']),
              '--format=JobIDRaw,JobName%100,User,State%80,ElapsedRaw,AllocCPUS,CPUTimeRAW,TotalCPU,AllocTRES%300,MaxRSS,ExitCode,NodeList%200']
        if ids: args += ['-j',','.join(ids)]
        rows=[]
        for line in run(args,timeout=45).splitlines():
            parts=line.split('|')
            if len(parts)<12: continue
            rows.append(dict(zip(['id','name','user','state','elapsed','cpus','cpu_seconds','total_cpu','tres','max_rss','exit_code','nodes'],parts)))
        return rows

    def submit(self, user, selection, attempt):
        p=selection['profile'];a=Path(attempt)
        argv=['sbatch','--parsable','--job-name='+selection['job_name'],
              '--account='+self.config['users'][user]['account'],'--partition='+p['partition'],
              '--nodes='+str(p['nodes']),'--ntasks-per-node='+str(p['ranks_per_node']),
              '--cpus-per-task='+str(p['threads']),'--mem='+str(selection['mem_mib'])+'M',
              '--time='+str(p['wall_minutes']),'--nodelist='+','.join(selection['nodes']),
              '--no-requeue','--signal=B:USR1@120','--chdir='+str(a),
              '--output='+str(a/'slurm-%j.out'),'--error='+str(a/'slurm-%j.err')]
        hint=p.get('pinning',{}).get('slurm_hint')
        if hint:argv+=['--hint='+hint]
        qos=self.config['users'][user].get('qos')
        if qos: argv+=['--qos='+qos]
        if p.get('gpus_per_node'): argv+=['--gres=gpu:'+p['gpu_type']+':'+str(p['gpus_per_node'])]
        argv += [str(a/'job.sh')]
        result=self.user_run(user,argv,timeout=45)
        if not re.fullmatch(r'\d+(?:;[\w.-]+)?',result): raise RuntimeError('Ambiguous sbatch response: '+result)
        return result.split(';')[0]

    def cancel(self,user,job_id):
        return self.user_run(user,['scancel',str(job_id)])

    def pause(self,user,job_id):
        return self.user_run(user,['scancel','--signal=USR1','--batch',str(job_id)])

    def limits(self):
        if time.time()-self.limits_time < 300: return self.limits_cache
        assoc=run(['sacctmgr','-nP','show','assoc','format=Account,User,Partition,QOS,DefaultQOS,GrpTRES,GrpJobs,GrpSubmitJobs,MaxJobs,MaxSubmitJobs,ParentName,MaxTRES,MaxTRESPN,MaxWall,ID,ParentID,GrpTRESMins,GrpTRESRunMins,GrpWall,MaxTRESMins'])
        qos=run(['sacctmgr','-nP','show','qos','format=Name,Flags,GrpTRES,MaxTRESPU,MaxJobsPU,MaxSubmitPU,MaxTRESPJ,MaxTRESPN,MaxWall,GrpJobs,GrpSubmitJobs,MaxTRESPA,MaxJobsPA,MaxSubmitPA,MinTRES,GrpTRESMins,GrpTRESRunMins,GrpWall,MaxTRESMins,LimitFactor'])
        settings={k.strip():v.strip() for line in run(['scontrol','show','config']).splitlines() if '=' in line for k,v in [line.split('=',1)]}
        enforce=settings.get('AccountingStorageEnforce','unknown').lower()
        modes=set(enforce.split(','))
        enabled=bool(modes & {'limits','safe','all'}) if enforce!='unknown' else None
        associations=[dict(zip(['account','user','partition','qos','default_qos','group_tres','group_jobs','group_submit','max_jobs','max_submit','parent','job_tres','node_tres','max_wall','id','parent_id','group_tres_mins','group_tres_run_mins','group_wall','job_tres_mins'],x.split('|'))) for x in assoc.splitlines()]
        account_parents={a['account']:a.get('parent','') for a in associations if not a['user']}
        # Retain our users plus their account ancestors, without publishing other users.
        accounts={a['account'] for a in self.config['users'].values()}
        while True:
            parents={a.get('parent','') for a in associations if not a['user'] and a['account'] in accounts}-{''}
            if parents<=accounts:break
            accounts.update(parents)
        associations=[a for a in associations if a['account'] in accounts and (not a['user'] or a['user'] in self.config['users'])]
        self.limits_cache=dict(associations=associations,
                              qos=[dict(zip(['name','flags','group_tres','user_tres','user_jobs','user_submit','job_tres','node_tres','max_wall','group_jobs','group_submit','account_tres','account_jobs','account_submit','min_job_tres','group_tres_mins','group_tres_run_mins','group_wall','job_tres_mins','limit_factor'],x.split('|'))) for x in qos.splitlines()],
                              account_parents=account_parents,
                              full_usage_visible=settings.get('PrivateData','none').lower() in ('none','(null)',''),
                              scheduler_settings={k:settings.get(k,'unknown') for k in ('MaxJobCount','MaxArraySize','MaxStepCount','EnforcePartLimits','MaxMemPerNode','DefMemPerCPU','SelectTypeParameters','TaskPlugin')},
                              accounting_storage_enforce=enforce,enforcement_enabled=enabled,
                              job_submit_plugins=settings.get('JobSubmitPlugins','unknown'),time=time.time())
        self.limits_time=time.time();return self.limits_cache

    def prepare(self,user,attempt,workdir):
        if user==self.config['owner']:return
        run(['setfacl','-m','u:'+user+':rwx,d:u:'+user+':rwx',str(attempt)])
        # Existing work directories are never recursively chmod'ed.
        self.user_run(user,['access',str(attempt),workdir])
