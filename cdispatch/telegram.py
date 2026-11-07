"""Private Telegram monitor; cache only, no scheduler RPC and no job mutations."""
import argparse
import html
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

from .common import atomic, lock, read, TERMINAL
from .monitor import render, mobile, ordered, short_id
from .monitor_cache import read_monitor_snapshot
from .alerts import warnings, event_warning, event_key
from .presentation import label, reason, event_line
from .node_alerts import transitions as node_transitions


class TelegramError(RuntimeError):
    def __init__(self, message, retry_after=10, status=None):
        super().__init__(message)
        self.retry_after=retry_after;self.status=status


class TelegramAPI:
    def __init__(self, token, proxy=None):
        self.token=token;self.proxy=proxy
        # Cluster shells may carry a laptop SOCKS proxy. The monitor uses the
        # explicitly configured transport and never relies on that environment.
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def call(self, method, **payload):
        if self.proxy:
            # URL/token and request body go through stdin, never process argv.
            body=json.dumps(payload,ensure_ascii=False)
            config='url = '+json.dumps('https://api.telegram.org/bot'+self.token+'/'+method)+'\n'
            config+='data-binary = '+json.dumps(body,ensure_ascii=False)+'\n'
            try:
                result=subprocess.run(['curl','--silent','--show-error','--max-time','35','--proxy',self.proxy,
                                       '--header','Content-Type: application/json','--write-out','\n%{http_code}',
                                       '--config','-'],input=config,text=True,capture_output=True,timeout=40)
                if result.returncode:raise TelegramError('Telegram proxy connection failed')
                raw,code=result.stdout.rsplit('\n',1);data=json.loads(raw)
                if code!='200' or not data.get('ok'):
                    if code=='400' and 'message is not modified' in data.get('description','').lower():return {'unchanged':True}
                    detail=data.get('description','').replace(self.token,'[redacted]')[:200]
                    raise TelegramError('Telegram HTTP '+code+': '+detail,data.get('parameters',{}).get('retry_after',10),int(code))
                return data.get('result')
            except (OSError,ValueError,subprocess.TimeoutExpired):
                raise TelegramError('Telegram proxy transport failed') from None
        req=urllib.request.Request('https://api.telegram.org/bot'+self.token+'/'+method,
                                   data=json.dumps(payload).encode(),headers={'Content-Type':'application/json'})
        try:
            with self.opener.open(req,timeout=35) as response:result=json.load(response)
        except urllib.error.HTTPError as error:
            try:detail=json.loads(error.read())
            except (ValueError,OSError):detail={}
            if error.code==400 and 'message is not modified' in detail.get('description','').lower():return {'unchanged':True}
            message=detail.get('description','').replace(self.token,'[redacted]')[:200]
            raise TelegramError('Telegram HTTP '+str(error.code)+': '+message,detail.get('parameters',{}).get('retry_after',10),error.code) from None
        except (OSError,ValueError):
            # Never expose a request URL (which contains the bot token).
            raise TelegramError('Telegram network/response error') from None
        if not result.get('ok'):
            raise TelegramError('Telegram API rejected request',result.get('parameters',{}).get('retry_after',10))
        return result.get('result')


MENU={'inline_keyboard':[
    [{'text':'Ресурсы','callback_data':'status'},{'text':'Задания','callback_data':'queue'}],
    [{'text':'Ноды','callback_data':'nodes'},{'text':'Решения','callback_data':'decisions'}],
    [{'text':'Аккаунты','callback_data':'users'},{'text':'Лимиты','callback_data':'limits'},{'text':'Ошибки','callback_data':'incidents'}]]}


def chunks(text, limit=3500):
    while text:
        end=min(len(text),limit)
        if end<len(text):
            newline=text.rfind('\n',0,end)
            if newline>0:end=newline+1
        yield text[:end]
        text=text[end:]


class Monitor:
    def __init__(self,root,settings,api):
        self.root=Path(root);self.settings=settings;self.api=api
        self.state_path=Path(settings['state_file'])
        self.state=read(self.state_path,dict(offset=0,alerts=settings.get('notifications',False)))
        self.allowed=set(settings.get('allowed_user_ids',[]))
        self.stop=False

    def notifications_enabled(self):
        return bool(self.settings.get('notifications',False) and self.state.get('alerts',True))

    def notify(self, data=None, now=None):
        if not self.notifications_enabled(): return
        chat=self.settings.get('chat_id')
        if chat not in self.allowed: return  # Only the explicitly paired private recipient.
        now=time.time() if now is None else now
        data=read_monitor_snapshot(self.root,{}) if data is None else data
        # Persist an activation boundary: old failed jobs are not replayed.
        if 'alerts_since' not in self.state:
            self.state['alerts_since']=now;self.save()
        active=self.state.setdefault('active_warnings',{})
        pending=self.state.setdefault('pending_since',{})
        live={tid+':'+str(t.get('job_id')) for tid,t in data.get('tasks',{}).items() if t['state']=='PENDING'}
        for key in list(pending):
            if key not in live:del pending[key]
        for key in live:pending.setdefault(key,now)
        # Ordinary resource waits are informational, with a longer default threshold.
        current=warnings(data,now,self.settings.get('resource_wait_notice_seconds',1800),pending)
        node_fresh,node_state,node_recovered=node_transitions(data,self.state.get('node_conditions'),now)
        fresh=list(node_fresh);recovered=[]
        for key,message in current.items():
            if key in active:continue
            if key.startswith('pending:'):
                tid=key[len('pending:'):].rsplit(':',1)[0]
                task=data.get('tasks',{}).get(tid,{})
                job=next((j for j in (data.get('snapshot') or {}).get('jobs',[]) if str(j.get('JobId'))==str(task.get('job_id'))),{})
                fresh.append((0,html.escape(label(task))+' · Slurm '+html.escape(str(task.get('job_id')))+'\n'+html.escape(reason(job.get('Reason') or task.get('reason')))+'; ожидание, расчёт ещё не запущен'))
            elif key.startswith('task:'):
                task=data.get('tasks',{}).get(key[len('task:'):].rsplit(':',1)[0],{})
                fresh.append((2,html.escape(label(task))+'\nНужна проверка: '+html.escape(reason(task.get('reason')))))
            else:fresh.append((2,html.escape(message)))
        for key in list(active):
            if key not in current:
                # A pending warning disappearing does not prove scientific recovery.
                if not key.startswith(('pending:','task:')):recovered.append(key)
        seen=self.state.setdefault('alert_events_seen',[]);new_keys=[]
        for event in data.get('events',[]):
            if event['time'] < self.state['alerts_since']:continue
            key=event_key(event)
            if key in seen or not event_warning(event):continue
            fresh.append(event_line(event,data));new_keys.append(key)
        if fresh:
            # One digest per poll. Repeated interruptions update the same digest
            # for 30 min; critical events always generate a visible new notice.
            level=max(x[0] for x in fresh)
            headings={0:'🟡 Ожидание ресурсов · расчёты в очереди',1:'🟠 Попытки прерваны · проверьте продолжение',2:'🔴 Требуется внимание'}
            previous=self.state.get('incident_digest',{})
            reuse=level<2 and previous.get('level')==level and now-previous.get('time',0)<1800 and previous.get('message_id')
            entries=(previous.get('entries',[]) if reuse else [])+[{0:'🟡 ',1:'🟠 ',2:'🔴 '}[x[0]]+x[1] for x in fresh]
            entries=entries[-12:]
            text='<b>'+headings[level]+'</b> · новых событий: '+str(len(fresh))+'\n\n'+'\n\n'.join(entries)
            if len(fresh)>12:text+='\n\nПоказаны последние 12; остальные доступны в /events.'
            result=self.send(chat,text,menu=False,message_id=previous['message_id'] if reuse else None,data=data,task_ids=[e.get('task') for e in data.get('events',[]) if event_key(e) in new_keys])
            self.state['incident_digest']=dict(level=level,time=now,entries=entries,message_id=(result or {}).get('message_id') or (previous.get('message_id') if reuse else None))
            if not node_fresh and not new_keys and any(k not in active and not k.startswith(('pending:','task:')) for k in current) and all(not k.startswith(('pending:','task:')) for k in current if k not in active):
                self.state['health_notice_id']=(result or {}).get('message_id')
            seen.extend(new_keys);self.state['alert_events_seen']=seen[-2000:]
        # Update a known health alert instead of pushing another notification.
        if recovered and not fresh and self.state.get('health_notice_id') and not any(k for k in current if not k.startswith(('pending:','task:'))):
            self.send(chat,'✅ <b>Связь с диспетчером восстановлена</b>\nСвежие данные снова доступны. Это не проверка состояния электронного расчёта.',menu=False,message_id=self.state['health_notice_id'])
            self.state.pop('health_notice_id',None)
        if node_state is not None:self.state['node_conditions']=node_state
        self.state['active_warnings']=current
        self.save()

    def save(self):atomic(self.state_path,self.state)

    def send(self,chat,text,menu=True,message_id=None,command='status',argument='',data=None,task_ids=()):
        keyboard=list(MENU['inline_keyboard']) if menu else []
        if command in ('queue','decisions','nodes') and data:
            try:page=max(1,int(argument or 1))
            except ValueError:page=1
            items=(data.get('snapshot') or {}).get('nodes',[]) if command=='nodes' else [t for t in ordered(data) if t['state'] not in TERMINAL]
            pages=max(1,(len(items)+5)//6);navigation=[]
            if page>1:navigation.append(dict(text='← Назад',callback_data=command+' '+str(page-1)))
            if page<pages:navigation.append(dict(text='Далее →',callback_data=command+' '+str(page+1)))
            if navigation:keyboard.insert(0,navigation)
            for task in ([] if command=='nodes' else items[(page-1)*6:page*6]):
                tid=short_id(task['id'],data['tasks'])
                if len(tid)<=55:keyboard.append([{'text':'ℹ '+label(task)[:55],'callback_data':'why '+tid}])
        for tid in list(dict.fromkeys(task_ids))[-4:]:
            task=(data or {}).get('tasks',{}).get(tid)
            if not task:continue
            prefix=short_id(tid,data['tasks'])
            if len(('why '+prefix).encode())<=64:keyboard.append([dict(text='ℹ '+label(task)[:55],callback_data='why '+prefix)])
        refresh=command+(' '+argument if argument else '')
        if menu and len(refresh.encode())<=64:keyboard.insert(0,[dict(text='↻ Обновить',callback_data=refresh)])
        payload=dict(chat_id=chat,text=text,parse_mode='HTML',reply_markup={'inline_keyboard':keyboard})
        if len(text)>4000:
            plain=html.unescape(re.sub(r'<[^>]*>','',text))
            payload['text']=plain[:3900]+('\n… Полный вид: cdispatch watch --once' if len(plain)>3900 else '')
            payload.pop('parse_mode')
        if message_id is not None:payload['message_id']=message_id
        try:return self.api.call('editMessageText' if message_id is not None else 'sendMessage',**payload)
        except TelegramError as error:
            if message_id is None or error.status!=400:raise
            payload.pop('message_id',None)
            return self.api.call('sendMessage',**payload)

    def handle(self,update):
        callback=update.get('callback_query')
        message=callback.get('message',{}) if callback else update.get('message',{})
        sender=callback.get('from',{}) if callback else message.get('from',{})
        chat=message.get('chat',{})
        if sender.get('id') not in self.allowed or chat.get('type')!='private' or chat.get('id')!=sender.get('id'):
            return  # Do not reveal even cluster status to an unpaired account.
        text=callback.get('data','') if callback else message.get('text','')
        pieces=text.strip().split(maxsplit=1)
        if not pieces:return
        command=pieces[0].lstrip('/').split('@')[0];argument=pieces[1] if len(pieces)>1 else ''
        if callback:
            try:self.api.call('answerCallbackQuery',callback_query_id=callback['id'])
            except TelegramError as error:
                if error.status!=400:raise
        if command=='alerts':
            if argument in ('on','off'):
                self.state['alerts']=argument=='on'
                if argument=='on':self.state['alerts_since']=time.time();self.state['active_warnings']={}
                self.save()
            reply=('Уведомления о сбоях включены.' if self.notifications_enabled() else 'Уведомления о сбоях выключены.')
            reply+='\n<code>/alerts on</code> · <code>/alerts off</code>\nОжидание зависимостей — штатное; Slurm PENDING предупреждает после '+str(self.settings.get('resource_wait_notice_seconds',1800)//60)+' мин.'
            data={}
        elif command=='incidents':
            digest=self.state.get('incident_digest',{})
            reply='<b>Последние предупреждения и ошибки</b>\n\n'+('\n\n'.join(digest.get('entries',[])) or 'Новых уведомлений после включения нет.')
            reply+='\n\nЭто история уведомлений; актуальные ресурсы — /status.'
            data={}
        else:
            data=read_monitor_snapshot(self.root,{})
            try:reply=mobile(data,command,argument)
            except (ValueError,KeyError,TypeError):reply='Не удалось разобрать команду или ID. Используйте /help.'
        self.send(chat['id'],reply,message_id=message.get('message_id') if callback else None,command=command,argument=argument,data=data)

    def run(self):
        for sig in (signal.SIGTERM,signal.SIGINT,signal.SIGUSR1):signal.signal(sig,lambda *_:setattr(self,'stop',True))
        username=self.api.call('getMe')['username']
        if self.api.call('getWebhookInfo').get('url'):
            raise TelegramError('Webhook already configured; refusing to replace another bot consumer')
        self.api.call('setMyCommands',commands=[dict(command=c,description=d) for c,d in (
            ('status','Мои ресурсы и свободный запас'),('queue','Задания и выделенные ресурсы'),('nodes','Ресурсы нод'),
            ('why','Причины и ограничения: /why ID'),('decisions','Решения планировщика'),('limits','Лимиты и политика'),
            ('users','Расход и лимиты аккаунтов'),('events','Последние события'),('report','Учёт ресурсов'),
            ('incidents','Последние предупреждения и ошибки'),('alerts','Уведомления: /alerts on|off'),('help','Справка'))])
        while not self.stop:
            try:
                self.notify()
                for update in self.api.call('getUpdates',offset=self.state.get('offset',0),timeout=20,allowed_updates=['message','callback_query']):
                    try:self.handle(update)
                    except TelegramError as error:
                        if error.status not in (400,403):raise
                        print(str(error),flush=True)  # Bad/expired request must not poison subsequent updates.
                    self.state['offset']=update['update_id']+1;self.save()
                atomic(self.root/'telegram-status.json',dict(time=time.time(),username=username,error=None,host=os.uname().nodename,pid=os.getpid(),
                       notifications=self.notifications_enabled(),active_warnings=len(self.state.get('active_warnings',{}))))
            except (TelegramError,OSError,ValueError) as error:
                message=str(error) if isinstance(error,TelegramError) else type(error).__name__
                atomic(self.root/'telegram-status.json',dict(time=time.time(),username=username,error=message,host=os.uname().nodename,pid=os.getpid()))
                until=time.monotonic()+min(300,max(1,getattr(error,'retry_after',10)))
                while not self.stop and time.monotonic()<until:time.sleep(.5)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,required=True);parser.add_argument('--settings',type=Path,required=True)
    args=parser.parse_args();os.umask(0o077)
    from .component_runtime import isolated_command,run_isolated
    config=read(args.root/'config.json',{})
    command=isolated_command('telegram',[sys.executable,'-m','cdispatch.telegram','--root',str(args.root),'--settings',str(args.settings)],config,os.environ)
    if command is not None:return run_isolated('telegram',command)
    settings=read(args.settings);token_path=Path(settings['token_file'])
    for path in (args.settings,token_path):
        if path.stat().st_mode&0o077:raise ValueError('Telegram credentials must have mode 0600')
    if not os.environ.get('SLURM_JOB_ID'):raise RuntimeError('Telegram monitor must run in a compute allocation')
    token=token_path.read_text().strip()
    with lock(Path(settings['state_file']).with_suffix('.lock')):
        Monitor(args.root,settings,TelegramAPI(token,os.environ.get('CDISPATCH_TELEGRAM_PROXY'))).run()


if __name__=='__main__':
    try:raise SystemExit(main() or 0)
    except (TelegramError,OSError,ValueError,RuntimeError) as error:
        # No traceback may include request objects or secrets.
        print(str(error) if isinstance(error,(TelegramError,ValueError,RuntimeError)) else type(error).__name__,flush=True)
        raise SystemExit(1)
