from pathlib import Path
import tempfile
import unittest

from cdispatch.alerts import health, warnings
from cdispatch.telegram import Monitor, TelegramError


class API:
    def __init__(self): self.calls=[]
    def call(self, method, **payload): self.calls.append((method,payload));return {'message_id':len(self.calls)}


class AlertTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.api=API()
        self.settings=dict(state_file=str(self.root/'bot.json'),allowed_user_ids=[12],chat_id=12,notifications=True)
        self.bot=Monitor(self.root,self.settings,self.api)
        self.data=dict(heartbeat=1000,snapshot=dict(time=1000,jobs=[]),tasks={},events=[])

    def task(self,state,**values):
        return dict(id='task',state=state,spec=dict(name='SCF'),reason='dependencies',**values)

    def test_no_alert_for_dependencies_or_manual_hold(self):
        self.data['tasks']={'a':self.task('QUEUED'),'b':self.task('HELD',manual_hold=True)}
        self.bot.notify(self.data,1000);self.assertEqual(self.api.calls,[])

    def test_one_warning_across_restart_and_one_recovery(self):
        self.data['error']='Slurm unavailable'
        self.bot.notify(self.data,1000);self.bot.notify(self.data,1000)
        restarted=Monitor(self.root,self.settings,self.api);restarted.notify(self.data,1000)
        self.assertEqual(len(self.api.calls),1)
        self.data['error']=None;restarted.notify(self.data,1000);restarted.notify(self.data,1000)
        self.assertEqual(len(self.api.calls),2);self.assertEqual(self.api.calls[-1][0],'editMessageText');self.assertIn('восстановлена',self.api.calls[-1][1]['text'])

    def test_activation_skips_old_failures_and_new_failure_is_once(self):
        self.data['events']=[dict(time=999,kind='TERMINAL',task='old',state='FAILED')]
        self.bot.notify(self.data,1000);self.assertEqual(self.api.calls,[])
        self.data['events'].append(dict(time=1001,kind='TERMINAL',task='new',state='FAILED',reason='OOM'))
        self.bot.notify(self.data,1001);self.bot.notify(self.data,1002)
        self.assertEqual(len(self.api.calls),1);self.assertIn('OOM',self.api.calls[0][1]['text'])

    def test_notifications_require_opt_in_and_paired_recipient(self):
        self.data['circuit']='ambiguous submission'
        for changes in (dict(notifications=False),dict(chat_id=99)):
            settings={**self.settings,**changes};Monitor(self.root,settings,self.api).notify(self.data,1000)
        self.assertEqual(self.api.calls,[])

    def test_transport_failure_retries_without_losing_incident(self):
        self.data['error']='RPC error'
        original=self.api.call
        def fail(*_,**__): raise TelegramError('temporary network failure')
        self.api.call=fail
        with self.assertRaises(TelegramError):self.bot.notify(self.data,1000)
        self.api.call=original;self.bot.notify(self.data,1000)
        self.assertEqual(len(self.api.calls),1)

    def test_pending_wait_starts_at_submission_and_survives_restart(self):
        self.data['tasks']={'task':self.task('PENDING',job_id='123')}
        self.bot.notify(self.data,1000)
        self.data['heartbeat']=self.data['snapshot']['time']=2800
        restarted=Monitor(self.root,self.settings,self.api);restarted.notify(self.data,2800)
        self.assertEqual(len(self.api.calls),1);self.assertIn('123',self.api.calls[0][1]['text'])

    def test_failed_probe_cleanup_is_not_external_cancellation_alert(self):
        self.bot.notify(self.data,1000)
        self.data['events']=[dict(time=1001,kind='EXTERNAL_JOB_TERMINAL',name='cd-cpu-quota-test',slurm_state='CANCELLED by 12')]
        self.bot.notify(self.data,1001);self.assertEqual(self.api.calls,[])

    def test_snapshot_controller_and_bot_staleness_are_reported(self):
        result=health(self.data,dict(time=1000,error='proxy unavailable'),dict(time=1000),1001)
        self.assertFalse(result['ok']);self.assertIn('telegram-error',result['warnings'])
        self.assertIn('heartbeat',warnings(self.data,1100))


if __name__=='__main__':unittest.main()
