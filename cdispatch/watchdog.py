"""Short periodic login-node check; all persistent processes live in Slurm."""
from pathlib import Path
import time
from datetime import datetime

from .common import atomic, read, lock
from .slurm import Slurm


def notify(root, config, status, now=None):
    """One short login-side check also reaches the phone if cd-service ends."""
    now=time.time() if now is None else now
    settings_path=config.get('telegram_settings')
    if not settings_path:return
    settings=read(settings_path,{})
    state=read(settings.get('state_file',''),{}) if settings.get('state_file') else {}
    chat=settings.get('chat_id')
    if not settings.get('notifications') or not state.get('alerts',True) or chat not in settings.get('allowed_user_ids',[]):return
    path=Path(root)/'watchdog-alerts.json';record=read(path,{})
    issue=None
    if now-status.get('heartbeat',0)>=120:
        issue='Нет heartbeat диспетчера более 2 мин. Watchdog проверяет службу; расчёты требуют свежей проверки.'
    bot=read(Path(root)/'telegram-status.json',{})
    if bot.get('error') or now-bot.get('time',0)>120:
        issue=(issue+'\n' if issue else '')+'Монитор Telegram недоступен; проверьте службу и прокси.'
    if bool(issue)==bool(record.get('issue')):return
    try:
        from .telegram import TelegramAPI
        token_path=Path(settings['token_file'])
        if Path(settings_path).stat().st_mode&0o077 or token_path.stat().st_mode&0o077:
            raise ValueError('Telegram credentials must have mode 0600')
        proxy='socks5h://127.0.0.1:'+str(settings['proxy'].get('port',10808)) if settings.get('proxy') else None
        api=TelegramAPI(token_path.read_text().strip(),proxy)
        api.call('sendMessage',chat_id=chat,text=('⚠️ Watchdog кластера\n'+issue if issue else '✅ Watchdog: heartbeat и монитор кластера восстановлены.'))
        atomic(path,dict(time=now,issue=issue,error=None))
    except Exception as error:
        # Delivery failure never prevents recovery; retry at the next bounded check.
        from .telegram import TelegramError
        record.update(time=now,error=str(error) if isinstance(error,(TelegramError,ValueError)) else type(error).__name__)
        atomic(path,record)


def check(root, config):
    root=Path(root)
    if (root/'watchdog-disabled').exists():return 'disabled'
    with lock(root/'watchdog.lock'):
        status=read(root/'status.json',{})
        notify(root,config,status)
        resizing=(status.get('snapshot') or {}).get('shared_pool',{}).get('draining',False)
        active=[t for t in status.get('tasks',{}).values() if t.get('selection',{}).get('shared') and t['state'] in ('SUBMITTING','SUBMIT_UNKNOWN','PENDING','RUNNING','CANCELLING','CONFIGURING','COMPLETING')]
        if time.time()-status.get('heartbeat',0)<120 and not (resizing and not active):
            # Forecast a dependent replacement before the service's walltime.
            # Ordinary healthy checks still avoid Slurm RPC entirely.
            service=read(root/'service-status.json',{})
            job=next((j for j in status.get('snapshot',{}).get('jobs',[])
                      if j.get('JobId')==service.get('job_id') and j.get('JobState')=='RUNNING'),{})
            try:remaining=datetime.fromisoformat(job['EndTime']).timestamp()-time.time()
            except (KeyError,ValueError):remaining=None
            lead=config.get('service_renew_before_minutes',0)*60
            if not resizing and lead and remaining is not None and 0<remaining<=lead:
                from .cli import service_start
                with lock(root/'bootstrap.lock'):
                    service_start(root,config,after_current=job['JobId'])
                return 'healthy; dependent service renewal reconciled'
            return 'healthy; resize waits for active handlers' if resizing else 'healthy'
        backend=Slurm(config);snapshot=backend.snapshot()
        services=[j for j in snapshot['jobs'] if j.get('JobName')=='cd-service' and j.get('UserId','').split('(')[0]==config['owner'] and j.get('JobState') in ('PENDING','RUNNING','CONFIGURING','COMPLETING')]
        record=read(root/'watchdog.json',dict(starts=[]))
        if services:
            running=[j for j in services if j['JobState']=='RUNNING']
            if running and active:return 'stale controller; active shared handlers protected; supervisor retries controller only'
            if running and record.get('signalled_job')!=running[0]['JobId']:
                backend.pause(config['owner'],running[0]['JobId'])
                record.update(signalled_job=running[0]['JobId'],time=time.time(),action='resize empty shared pool' if resizing else 'stop stale service')
                atomic(root/'watchdog.json',record)
            return 'existing service; no duplicate submission'
        starts=[stamp for stamp in record.get('starts',[]) if time.time()-stamp<3600]
        if len(starts)>=3:return 'restart circuit: three starts within an hour; inspect watchdog.json'
        # One bootstrap only; service_start persists an ambiguous submission intent.
        from .cli import service_start
        with lock(root/'bootstrap.lock'):
            before=read(root/'bootstrap-intent.json',{})
            try:
                service_start(root,config)
            except Exception as error:
                after=read(root/'bootstrap-intent.json',{})
                # Capacity/forecast errors did not call sbatch. Ambiguous RPCs
                # did, and must still consume the restart budget.
                if after!=before and after.get('state') in ('SUBMITTING','SUBMIT_UNKNOWN','SUBMITTED'):
                    starts.append(time.time())
                record.update(starts=starts,time=time.time(),action='bootstrap failed',error=str(error))
                atomic(root/'watchdog.json',record)
                raise
            starts.append(time.time());record.update(starts=starts,time=time.time(),action='bootstrap',error=None)
            atomic(root/'watchdog.json',record)
        return 'service bootstrap requested'
