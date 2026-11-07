"""Read-only Russian views of the shared dispatcher cache."""
from collections import Counter
import json
import html
import time

from .common import TERMINAL, tres, mib
from .hardware import description

STATE_RU={'RUNNING':'▶️ Выполняется','PENDING':'⏳ Ожидает Slurm','QUEUED':'🕒 Внутренняя очередь',
          'SUCCEEDED':'✅ Готово','FAILED':'❌ Ошибка','CANCELLED':'⏹ Отменено','HELD':'⏸ Удерживается',
          'BLOCKED':'⛔ Зависимость не прошла проверку','SUBMIT_UNKNOWN':'❔ Уточняется отправка','ADOPTING':'🔎 Приём под контроль'}


def node_usage(node):
    used=tres(node.get('AllocTRES',''));total=tres(node.get('CfgTRES',''))
    return 'CPU %s/%s · RAM %g/%gG · GPU %s/%s' % (
        node.get('CPUAlloc','?'),node.get('CPUTot','?'),int(node.get('AllocMem',0))/1024,
        int(node.get('RealMemory',0))/1024,used.get('gres/gpu','0'),total.get('gres/gpu','0'))


def compact_nodes(snapshot):
    rows=[]
    for node in snapshot.get('nodes',[]):
        hw=description(node);model=hw['cpu'].replace('Intel Xeon Gold ','Xeon ').replace('Intel Xeon ','Xeon ').replace('AMD EPYC ','EPYC ')
        bad=any(word in node.get('State','') for word in ('DOWN','DRAIN','FAIL','INVALID','UNKNOWN','NOT_RESPONDING'))
        rows.append(('🔴 ' if bad else '')+node['NodeName']+' · '+model+'\n'+node_usage(node))
    return rows


def task_resources(task,snapshot):
    from .scheduler import shape
    if task.get('selection',{}).get('shared'):
        selection=task['selection'];r=shape(selection['profile'])
        return ['Общий cd-service · step '+str(task.get('step_id','ожидает')),
                '%s CPU · %.1f GiB · 0 GPU'%(r['cpus'],r['mem_mib']/1024),
                'Нода: '+','.join(selection['nodes'])]
    job=next((j for j in snapshot.get('jobs',[]) if j.get('JobId')==task.get('job_id')),None)
    if job:
        allocated=job.get('AllocTRES','')
        tr=tres(allocated if allocated not in ('','(null)','N/A') else job.get('ReqTRES',''))
        return ['Выделено' if job.get('JobState')=='RUNNING' else 'Запрошено',
                '%s CPU · %.1f GiB · %s GPU'%(tr.get('cpu',job.get('NumCPUs','?')),mib(tr.get('mem','0'))/1024,tr.get('gres/gpu','0')),
                'Ноды: '+job.get('NodeList','—')]
    profiles=[task['selection']['profile']] if task.get('selection') else task['spec']['profiles']
    shapes=[shape(p) for p in profiles]
    def span(key,scale=1):
        values=[s[key]/scale for s in shapes];lo,hi=min(values),max(values)
        return ('%g'%lo) if lo==hi else '%g–%g'%(lo,hi)
    return ['Запрос: '+span('cpus')+' CPU · '+span('mem_mib',1024)+' GiB · '+span('gpus')+' GPU',
            ('🧪 Фиксированный опыт' if task['spec'].get('experimental') else str(len(profiles))+' допуст. профиль(я)')]


def limits_mobile(data):
    from .scheduler import usage
    snapshot=data.get('snapshot') or {};config=data.get('config',{});jobs=snapshot.get('jobs',[])
    total=usage(jobs);limits=config.get('local_pool_limits',config.get('pool_limits',{}));pool=snapshot.get('shared_pool')
    def esc(value):return html.escape(str(value))
    def number(value):return ('%.1f'%value).rstrip('0').rstrip('.')
    def meter(label,used,limit,scale=1,unit=''):
        used=used/scale;suffix=(' '+unit) if unit else ''
        if limit is None:return label+': <b>'+number(used)+suffix+'</b> · без локального потолка'
        limit=limit/scale;remaining=max(0,limit-used)
        percent=number(used/limit*100)+'%' if limit>0 else '—'
        end='сверх лимита '+number(used-limit) if used>limit else 'осталось '+number(remaining)
        return ('🔴 ' if used>limit or (limit>0 and used>=limit) else '')+label+': <b>'+number(used)+'/'+number(limit)+suffix+'</b> · '+percent+' · '+end+suffix
    def resources(used,caps):
        return [meter(label,used[key],caps.get(key),scale,unit) for key,label,scale,unit in (
            ('jobs','Jobs',1,''),('cpus','CPU',1,''),('mem_mib','RAM',1024,'GiB'),('gpus','GPU',1,''),('nodes','Ноды¹',1,'')) if key in caps]
    rows=['<b>🎛 Лимиты и свободный запас</b>']
    from .service_renewal import deferred_service_successors
    renewals=deferred_service_successors(jobs)
    if renewals:rows.append('Сервис-преемник '+', '.join(sorted(renewals))+': общий местный слот с действующим сервисом; Slurm-квоты учитываются полностью.')
    stale=time.time()-data.get('heartbeat',0)>60 or time.time()-snapshot.get('time',0)>60 or data.get('error')
    if stale:rows+=['⚠️ Снимок устарел или Slurm недоступен; текущий допуск неизвестен.']
    elif data.get('circuit'):rows+=['⛔ Новые запуски заблокированы: '+esc(data['circuit'])]
    elif data.get('paused'):rows+=['⏸ Автоматический запуск на паузе.']
    else:
        reserve=config.get('interactive_reserve_jobs',0)
        free_slots=max(0,limits.get('jobs',0)-reserve-total['jobs'])
        if data.get('user_limits'):
            free_slots=min(free_slots,sum(max(0,caps.get('jobs',0)-usage(jobs,u)['jobs']) for u,caps in data['user_limits'].items()
                                         if data.get('user_accounts',{}).get(u,{}).get('authorized',True)))
        pending_free=max(0,config.get('max_pending',0)-total['pending'])
        blocks=[]
        if not free_slots:blocks.append('слоты обычных jobs')
        if not pending_free:blocks.append('лимит PENDING')
        for key,label in (('cpus','CPU'),('mem_mib','RAM'),('nodes','ноды')):
            if key in limits and limits[key] is not None and total[key]>=limits[key]:blocks.append(label)
        if not limits:rows+=['⚠️ Локальные лимиты ещё не получены.']
        elif blocks:rows+=['⛔ Отдельные CPU-задачи: лимит исчерпан — '+', '.join(blocks)+'.']
        else:rows+=['🟢 Свободно <b>'+str(free_slots)+'</b> локальных слотов; запас PENDING: <b>'+str(pending_free)+'</b>.']
    if pool:
        cpu=max(0,pool['cpus']-pool['used_cpus']);mem=max(0,pool['mem_mib']-pool['used_mem_mib'])/1024
        rows+=['Внутри cd-service свободно: <b>%s CPU · %s GiB</b>.'%(number(cpu),number(mem))]
        if pool.get('draining'):rows+=['⏳ Новые обработчики ждут смены размера пула.']
        elif pool.get('remaining_minutes',0)<1:rows+=['⏳ Время общего allocation заканчивается.']
        elif cpu<=0 or mem<=0:rows+=['⏳ Для новых shared-обработчиков сейчас нет CPU/RAM.']
        else:rows+=['Shared-обработчик должен помещаться в этот запас и оставшееся время.']
    rows+=['','<b>Общий пул · локальные ограничения</b>','Jobs: сумма квот аккаунтов; cd-service занимает один слот владельца.']+resources(total,limits)
    rows.append(meter('PENDING',total['pending'],config.get('max_pending')))
    if config.get('interactive_reserve_jobs'):rows.append('Резерв для отдельного interactive: '+str(config['interactive_reserve_jobs'])+' слот(а).')
    for user,caps in data.get('user_limits',{}).items():
        title=data.get('config',{}).get('account_labels',{}).get(user,user)
        rows+=['','<b>'+esc(title)+'</b> · <code>'+esc(user)+'</code>']+resources(usage(jobs,user),caps)
    if pool:
        rows+=['','<b>Внутри cd-service</b>',meter('CPU',pool['used_cpus'],pool['cpus']),meter('RAM',pool['used_mem_mib'],pool['mem_mib'],1024,'GiB'),
               'Включая резерв службы. Уже учтено в общем пуле.']
    audit=snapshot.get('limits',{});enforce=audit.get('accounting_storage_enforce','unknown')
    rows+=['','<b>Что задаёт Slurm</b>','AccountingStorageEnforce: <code>'+esc(enforce)+'</code>']
    if audit.get('enforcement_enabled') is False:rows+=['Квоты из БД Slurm не включены. Ресурсы ограничивают ноды; численные потолки выше — локальная политика.']
    elif audit.get('enforcement_enabled') is True:rows+=['Квоты БД включены; они дополнительно ограничивают локальный запас.']
    else:rows+=['Применение квот БД не подтверждено; свободный запас только по локальной политике.']
    chosen=set();submits=set()
    for user,account in data.get('user_accounts',{}).items():
        for a in audit.get('associations',[]):
            if a.get('user')==user and a.get('account')==account.get('account'):
                chosen.add(account.get('qos') or a.get('default_qos') or a.get('qos') or '?')
                if a.get('max_submit'):submits.add(a['max_submit'])
    if chosen:rows+=['QoS аккаунтов: <code>'+esc(', '.join(sorted(chosen)))+'</code>.']
    if submits:rows+=['MaxSubmitJobs в БД: '+esc('/'.join(sorted(submits)))+' на пользователя (RUNNING + PENDING).']
    if chosen and 'stud' not in chosen and any(q['name']=='stud' for q in audit.get('qos',[])):
        rows+=['QoS stud существует, но у этих аккаунтов не выбран.']
    rows+=['','Учтены выделения и запросы в Slurm. Внутренняя очередь ресурсов не занимает.',
           'CPU/RAM/GPU — выделенные ресурсы, не измеренная загрузка. Старт зависит от нод, профиля, очереди и действующих квот. Для точной заявки: cdispatch plan.',
           '¹ Ноды считаются по allocations, как в проверке лимитов.']
    return '\n'.join(rows)


def mobile(data,command='status',argument=''):
    def esc(value):return html.escape(str(value))
    tasks=data.get('tasks',{});snapshot=data.get('snapshot') or {};age=time.time()-data.get('heartbeat',0)
    warning='⚠️ <b>Данные устарели</b>\n\n' if age>60 or data.get('error') else ''
    if command=='limits':return limits_mobile(data)
    if command in ('start','help'):
        return '<b>🖥 Монитор кластера</b>\n\nВыберите раздел кнопками ниже.\nОбновление — по нажатию «↻ Обновить».\n\n<code>/why ID</code> — детали задачи.\n<code>/queue 2</code> — следующая страница.\n<code>/alerts</code> — состояние уведомлений о сбоях.\n<code>/alerts on</code> / <code>/alerts off</code> — включить / выключить.'
    if command=='status':
        from .presentation import status
        stale=time.time()-snapshot.get('time',0)>90
        return (warning or ('⚠️ <b>Данные Slurm устарели</b>\n\n' if stale else ''))+status(data)
    if command in ('queue','decisions'):
        from .presentation import label, reason, total, fmt
        active=[t for t in ordered(data) if t['state'] not in TERMINAL];page=max(1,int(argument or 1))
        rows=['<b>📋 Задания</b> · '+str(page)+'/'+str(max(1,(len(active)+5)//6)), 'Выделено: <b>'+fmt(total(data))+'</b>']
        for task in active[(page-1)*6:page*6]:
            rows+=['','<b>'+esc(label(task))+'</b>', STATE_RU.get(task['state'],esc(task['state']))]
            resource=task_resources(task,snapshot)
            rows.extend(esc(line) for line in resource if not line.startswith(('Нод','Общий cd-service')) and 'профиль' not in line)
            why=reason(task.get('reason'))
            if why and task['state']!='RUNNING':rows.append(esc(why))
            if task.get('selection',{}).get('shared'):rows.append('Входит в ресурсы службы')
            if task.get('job_id'):rows.append('Slurm <code>'+esc(task['job_id'])+'</code>')
        if not active:rows.append('Активных заявок нет.')
        return warning+'\n'.join(rows)
    if command=='nodes':
        page=max(1,int(argument or 1));nodes=snapshot.get('nodes',[])
        rows=['<b>🧮 Ноды · занято/всего</b> · '+str(page)+'/'+str(max(1,(len(nodes)+5)//6))]
        for n in nodes[(page-1)*6:page*6]:
            bad=any(word in n.get('State','') for word in ('DOWN','DRAIN','FAIL','INVALID','UNKNOWN','PLANNED'))
            cpu=int(n.get('CPUEfctv',n.get('CPUTot',0)))-int(n.get('CPUAlloc',0));ram=(int(n.get('RealMemory',0))-int(n.get('AllocMem',0)))/1024
            hw=description(n)
            rows+=['','<b>'+('🔴 ' if bad else '🟢 ')+esc(n['NodeName'])+'</b>',esc(hw['cpu'])+' · '+esc(hw['gpu']),esc(n.get('State','')),
                   esc(node_usage(n))]
        return warning+'\n'.join(rows)
    plain=render(data,command,argument)
    lines=plain.splitlines();out='<b>'+esc(lines[0] if lines else command)+'</b>\n'
    for line in lines[1:]:
        escaped=esc(line)
        if len(out)+len(escaped)>3800:out+='\n… Полная версия: cdispatch watch --once';break
        out+='\n'+escaped
    return out


def short_id(tid, tasks):
    for length in range(8, len(tid)+1):
        if sum(key.startswith(tid[:length]) for key in tasks) == 1:
            return tid[:length]
    return tid


def task_lookup(data, prefix):
    if prefix in data.get('tasks',{}):return data['tasks'][prefix]
    found = [t for key,t in data.get('tasks',{}).items() if key.startswith(prefix)]
    if len(found) != 1:
        raise ValueError('Нужен однозначный ID задачи; посмотрите /queue.')
    return found[0]


def ordered(data):
    return sorted(data.get('tasks',{}).values(), key=lambda t:(
        not (t['spec'].get('urgent') or 'urgent' in t['spec'].get('tags',[]) or t['spec'].get('kind')=='interactive'),
        -t['spec']['priority'],t['created']))


def render(data, command='status', argument=''):
    snapshot = data.get('snapshot') or {}
    tasks = data.get('tasks',{})
    age = time.time()-data.get('heartbeat',0)
    if not data:
        return 'Нет снимка диспетчера. Ожидается запуск службы на вычислительной ноде.'
    stale = age > 60 or time.time()-snapshot.get('time',0) > 60 or bool(data.get('error'))
    banner = ('⚠ Снимок устарел или Slurm недоступен. Показаны последние известные данные.\n' if stale else '')
    if command == 'status':
        counts = Counter(t['state'] for t in tasks.values())
        lines = ['Диспетчер кластера', 'Heartbeat: %.0f с; Slurm: %.0f с' % (age,time.time()-snapshot.get('time',0)),
                 'Автозапуск: '+('приостановлен' if data.get('paused') else 'включён'),
                 'Состояния: '+', '.join('%s=%s'%item for item in sorted(counts.items()))]
        if data.get('error'):lines.append('Ошибка: '+data['error'])
        if data.get('circuit'):lines.append('Блокировка: '+data['circuit'])
        from .alerts import warnings
        for message in warnings(data).values():lines.append('⚠ '+message)
        managed={t.get('job_id') for t in tasks.values()}
        for job in snapshot.get('jobs',[]):
            if job['JobId'] not in managed:
                lines.append('Slurm %s %s %s: %s' % (job['JobId'],job.get('JobName',''),job.get('JobState',''),job.get('Reason','')))
        lines+=['','Ноды · занято/всего']+compact_nodes(snapshot)
        return banner+'\n'.join(lines)
    if command in ('queue','decisions'):
        selected = [t for t in ordered(data) if t['state'] not in TERMINAL]
        page = max(1,int(argument or 1));part=selected[(page-1)*8:page*8]
        lines = ['Локальная очередь, страница %d/%d'%(page,max(1,(len(selected)+7)//8))]
        for t in part:
            s=t['spec'];sel=t.get('selection',{});key=short_id(t['id'],tasks)
            lines.append('%s | %s | p=%s | %s'%(key,t['state'],s['priority'],s['name']))
            lines.append('  '+t.get('reason',''))
            if sel:lines.append('  '+sel['profile']['name']+' → '+','.join(sel['nodes']))
            if command=='decisions' and t.get('decision',{}).get('bypassed_waiting_tasks'):
                lines.append('  Проходит перед ожидающими: '+', '.join(t['decision']['bypassed_waiting_tasks']))
        if not selected:lines.append('Активных локальных заявок нет.')
        lines.append('/why ID — ресурсы, причины и ограничения; /queue N — страница.')
        return banner+'\n'.join(lines)
    if command in ('why','task'):
        t=task_lookup(data,argument);s=t['spec'];sel=t.get('selection',{})
        lines=[t['id'],s['name']+' | '+t['state'],'Приоритет: '+str(s['priority']),
               'Slurm: '+str(t.get('job_id','ещё не отправлено')),'Причина: '+t.get('reason',''),
               'Попытки: %d/%d'%(len(t['attempts']),s['max_attempts'])]
        if t.get('pending_advice'):lines.append('Решение по Slurm: '+json.dumps(t['pending_advice'],ensure_ascii=False))
        if t.get('placement_override'):lines.append('Ручной выбор: '+json.dumps(t['placement_override'],ensure_ascii=False))
        for p in s['profiles']:
            lines.append('%s: %s, %d×%d×%d CPU, %.1f GiB/ноду, %d мин'%(p['name'],p['partition'],p['nodes'],p['ranks_per_node'],p['threads'],p['mem_mib']/1024,p['wall_minutes']))
        decision=t.get('decision',{})
        if decision.get('rationale'):lines.append(decision['rationale'])
        for detail in decision.get('suggestions',[]):
            lines.append('Профиль '+detail['profile'])
            for n in detail.get('nodes',[]):
                line='  %s %s: свободно %s CPU, %.1f GiB'%(n['node'],n['state'],n['free_cpu'],n['free_mem_mib']/1024)
                if n.get('reservations'):line+='; резерв Slurm '+','.join(r['job_id']+' с '+r['start'] for r in n['reservations'])
                lines.append(line)
            for user,blocks in detail.get('pending_feedback',{}).items():
                for block in blocks:lines.append('  '+user+': '+block['job_id']+' '+block['reason'])
        return banner+'\n'.join(lines)
    if command == 'nodes':
        lines=['Ноды: выделено/всего (память по Slurm)']
        for n in snapshot.get('nodes',[]):
            hw=description(n)
            lines.append('%s | %s | %s\n  %s · CPU %s/%s; RAM %.1f/%.1f GiB'%(n['NodeName'],hw['cpu'],hw['gpu'],n.get('State',''),n.get('CPUAlloc','?'),n.get('CPUTot','?'),int(n.get('AllocMem',0))/1024,int(n.get('RealMemory',0))/1024))
        return banner+'\n'.join(lines)
    if command == 'limits':
        audit=snapshot.get('limits',{});cfg=data.get('config',{})
        local=cfg.get('local_pool_limits',cfg.get('pool_limits',{}))
        def value(x):return 'нет локального потолка' if x is None else str(x)
        lines=['Действующие ограничения · снимок Slurm',
               'AccountingStorageEnforce='+audit.get('accounting_storage_enforce','неизвестно')+' · JobSubmitPlugins='+audit.get('job_submit_plugins','неизвестно'),
               'Общий пул: jobs '+value(local.get('jobs'))+' · CPU '+value(local.get('cpus'))+' · RAM '+value(local.get('mem_mib'))+' MiB · GPU '+value(local.get('gpus'))+' · ноды '+value(local.get('nodes'))]
        for user,policy in data.get('user_limits',{}).items():
            assocs=[a for a in audit.get('associations',[]) if a.get('user')==user]
            qos=next((a.get('default_qos') or a.get('qos','').split(',')[0] for a in assocs if a.get('default_qos') or a.get('qos')),'неизвестно')
            lines.append(user+': jobs '+value(policy.get('jobs'))+' · CPU '+value(policy.get('cpus'))+' · RAM '+value(policy.get('mem_mib'))+' MiB · GPU '+value(policy.get('gpus'))+' · QoS '+qos)
        for assoc in audit.get('associations',[]):
            if assoc.get('user'):continue
            limits=', '.join(k+'='+str(assoc.get(k)) for k in ('group_tres','group_jobs','group_submit') if assoc.get(k))
            lines.append('Общий scope '+assoc['account']+': '+(limits or 'численных Grp-потолков нет'))
        lines.append('Чужой usage '+('полностью виден' if audit.get('full_usage_visible') else 'скрыт PrivateData; будущие общие квоты требуют полного аудита'))
        lines.append('Детали и свежий опрос: cdispatch limits --refresh --json')
        return banner+'\n'.join(lines)
    if command == 'users':
        from .scheduler import usage, SLURM_ACTIVE
        from .resource_report import report
        totals=report(data);users=data.get('user_limits',{})
        if not users:users={j.get('UserId','').split('(')[0]:{} for j in snapshot.get('jobs',[])}
        def cap(value):return 'без локального потолка' if value is None else str(value)
        total=usage(snapshot.get('jobs',[]))
        pool=data.get('config',{}).get('local_pool_limits',data.get('config',{}).get('pool_limits',{}))
        enforce=snapshot.get('limits',{}).get('accounting_storage_enforce','неизвестно')
        lines=['Аккаунты: выделенные + запрошенные ресурсы / локальный лимит',
               'Общий пул: jobs %s/%s · CPU %s · RAM %.1f GiB · GPU %s'%(total['jobs'],cap(pool.get('jobs')),total['cpus'],total['mem_mib']/1024,total['gpus']),
               'Slurm AccountingStorageEnforce='+enforce+'; административные scopes: cdispatch limits --refresh']
        for user,limits in users.items():
            used=usage(snapshot.get('jobs',[]),user)
            title=data.get('config',{}).get('account_labels',{}).get(user,user)
            lines.append('\n'+title+' · '+user)
            lines.append('  jobs %s/%s (pending %s), CPU %s/%s, RAM %.1f/%s GiB, GPU %s/%s'%(
                used['jobs'],cap(limits.get('jobs')),used['pending'],used['cpus'],cap(limits.get('cpus')),
                used['mem_mib']/1024,round(limits['mem_mib']/1024,1) if limits.get('mem_mib') is not None else 'без локального потолка',used['gpus'],cap(limits.get('gpus'))))
            allocated=usage([j for j in snapshot.get('jobs',[]) if j.get('JobState')=='RUNNING'],user)
            lines.append('  RUNNING: %s jobs · %s CPU · %.1f GiB · %s GPU'%(allocated['jobs'],allocated['cpus'],allocated['mem_mib']/1024,allocated['gpus']))
            for job in snapshot.get('jobs',[]):
                if job.get('UserId','').split('(')[0]!=user or job.get('JobState') not in SLURM_ACTIVE:continue
                resource=usage([job])
                lines.append('  Slurm %s · %s · %s · %s CPU · %.1f GiB · %s GPU'%(job['JobId'],job.get('JobState',''),job.get('NodeList') or job.get('ReqNodeList',''),resource['cpus'],resource['mem_mib']/1024,resource['gpus']))
            rows=[v for k,v in totals.items() if k.startswith(user+' / ')]
            lines.append('  учёт: %.2f CPU·ч, %.2f GPU·ч, %.2f GiB·ч'%(sum(v['allocated_cpu_hours'] for v in rows),sum(v['gpu_hours'] for v in rows),sum(v['allocated_gib_hours'] for v in rows)))
        lines.append('Учёт Slurm: задания в окне с '+str(data.get('accounting_since','начала доступной истории'))+'. CPU·ч = выделенные ядра × время; показана не загрузка процессора.')
        return banner+'\n'.join(lines)
    if command == 'events':
        lines=['Последние события']
        for event in data.get('events',[])[-12:]:
            lines.append(time.strftime('%H:%M:%S',time.localtime(event['time']))+' '+event['kind']+' '+str(event.get('task') or '')+' '+str(event.get('reason') or event.get('error') or event.get('state') or ''))
        return banner+'\n'.join(lines)
    if command == 'report':
        from .resource_report import report
        return banner+'Учёт выделенных ресурсов\n'+json.dumps(report(data),ensure_ascii=False,indent=2)
    return ('Монитор кластера, доступ только владельцу.\n'
            '/status — служба и внешние jobs\n/queue [N] — задачи по приоритету\n'
            '/nodes — CPU, память и состояние нод\n/users — расход и лимиты аккаунтов\n/why ID — объяснение решения\n'
            '/decisions — порядок допуска\n/limits — политика и лимиты\n'
            '/events — последние события\n/report — CPU·ч и память\n'
            '/alerts on|off — уведомления о сбоях. Управление заданиями доступно через cdispatch на кластере.')
