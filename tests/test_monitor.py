import copy
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from cdispatch.common import atomic
from cdispatch.monitor import mobile,render
from cdispatch.telegram import Monitor,TelegramError
from test_dispatcher import config,node,spec


class API:
    def __init__(self):self.calls=[]
    def call(self,method,**payload):self.calls.append((method,payload));return {}


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.data=dict(heartbeat=time.time(),tasks={},user_limits={'alice':config()['users']['alice']['limits'],'bob':dict(jobs=2,cpus=32,mem_mib=1048576,gpus=2),'carol':dict(jobs=2,cpus=32,mem_mib=1048576,gpus=2)},snapshot=dict(time=time.time(),jobs=[],nodes=[node('node'+str(i),part='IXG6154-AI-common') for i in range(17)]))
        atomic(self.root/'status.json',self.data)
        self.api=API();self.monitor=Monitor(self.root,dict(state_file=str(self.root/'bot.json'),allowed_user_ids=[12],notifications=True),self.api)

    def update(self,callback=False,command='users'):
        msg=dict(message_id=34,chat=dict(id=12,type='private'),text='/'+command,**{'from':dict(id=12)})
        return dict(callback_query=dict(id='query',message=msg,data=command,**{'from':dict(id=12)})) if callback else dict(message=msg)

    def test_zero_usage_students_keep_numeric_limits(self):
        text=mobile(self.data,'users')
        self.assertIn('bob',text);self.assertIn('carol',text)
        self.assertIn('CPU 0/32',text);self.assertIn('RAM 0.0/1024.0 GiB',text);self.assertNotIn('/?',text)

    def test_nodes_paginate_and_status_includes_all_allocations(self):
        text=mobile(self.data,'nodes','2')
        self.assertIn('node6',text);self.assertNotIn('node0<',text);self.assertIn('2/3',text);self.assertLess(len(text),4096)
        text=mobile(self.data,'status');self.assertNotIn('node16',text);self.assertIn('Свободно',text);self.assertIn('0 GPU',text);self.assertLess(len(text),4096)

    def test_uncapped_users_show_real_allocations_and_node_without_none(self):
        self.data['user_limits']['alice']={k:None for k in ('cpus','mem_mib','gpus','nodes')}
        self.data['user_limits']['alice']['jobs']=5
        self.data['snapshot']['jobs']=[dict(JobId='123',UserId='alice',JobState='RUNNING',NodeList='node1',AllocTRES='cpu=64,mem=512G,node=1')]
        text=render(self.data,'users')
        self.assertIn('RUNNING: 1 jobs · 64 CPU',text);self.assertIn('node1',text)
        self.assertIn('без локального потолка',text);self.assertNotIn('/None',text)

    def test_queue_reports_running_and_requested_resources_without_none(self):
        task=dict(id='legacy-1',state='RUNNING',spec=spec(),created=1,attempts=[],job_id='123',reason='None')
        self.data['tasks']={'legacy-1':task}
        self.data['snapshot']['jobs']=[dict(JobId='123',JobState='RUNNING',UserId='alice',AllocTRES='cpu=32,mem=900G,node=1',NodeList='node1')]
        text=mobile(self.data,'queue');self.assertIn('32 CPU · 900.0 GiB',text);self.assertNotIn('None',text)

    def test_refresh_edits_existing_message_and_unauthorized_is_silent(self):
        self.monitor.handle(self.update(True))
        self.assertEqual([c[0] for c in self.api.calls],['answerCallbackQuery','editMessageText'])
        self.assertEqual(self.api.calls[-1][1]['message_id'],34)
        update=self.update();update['message']['from']['id']=99;self.monitor.handle(update)
        self.assertEqual(len(self.api.calls),2)

    def test_expired_callback_does_not_block_refresh(self):
        original=self.api.call
        def call(method,**payload):
            if method=='answerCallbackQuery':raise TelegramError('query is too old',status=400)
            return original(method,**payload)
        self.api.call=call;self.monitor.handle(self.update(True));self.assertEqual(self.api.calls[-1][0],'editMessageText')

    def test_long_html_has_readable_plain_fallback(self):
        self.monitor.send(12,'<b>'+('&amp;'*5000)+'</b>')
        payload=self.api.calls[-1][1];self.assertNotIn('parse_mode',payload);self.assertLess(len(payload['text']),4096)

    def test_polling_changes_never_sends_automatic_messages(self):
        calls=[]
        def call(method,**payload):
            calls.append(method)
            if method=='getMe':return dict(username='fake')
            if method=='getUpdates':self.monitor.stop=True;return []
            return {}
        self.api.call=call
        with patch('cdispatch.telegram.signal.signal'):self.monitor.run()
        self.assertNotIn('sendMessage',calls);self.assertNotIn('editMessageText',calls)


if __name__=='__main__':unittest.main()
