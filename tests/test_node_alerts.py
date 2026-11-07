from pathlib import Path
import tempfile
import unittest
from cdispatch.node_alerts import transitions
from cdispatch.telegram import Monitor, TelegramError

class API:
    def __init__(self):self.calls=[]
    def call(self,method,**kw):self.calls.append((method,kw));return {'message_id':len(self.calls)}

class NodeAlertTests(unittest.TestCase):
    def data(self,state='IDLE',now=1000):
        return {'heartbeat':now,'snapshot':{'time':now,'nodes':[{'NodeName':'n1','State':state,'Reason':'Prolog <error>'}],'jobs':[]},'tasks':{},'events':[]}
    def test_baseline_existing_drain_and_planned_are_not_new_failure(self):
        notices,baseline,_=transitions(self.data('DRAIN'),None,1000)
        self.assertEqual(notices,[])
        self.assertEqual(transitions(self.data('DRAIN'),baseline,1001)[0],[])
        self.assertEqual(transitions(self.data('PLANNED'),None,1000)[1]['n1']['condition'],'healthy')
    def test_drain_down_escalation_and_html(self):
        _,baseline,_=transitions(self.data(),None,1000)
        notices,baseline,_=transitions(self.data('MIXED+DRAIN'),baseline,1001)
        self.assertEqual(notices[0][0],1);self.assertIn('&lt;error&gt;',notices[0][1])
        notices,_,_=transitions(self.data('DOWN+DRAIN'),baseline,1002)
        self.assertEqual(notices[0][0],2);self.assertIn('недоступна',notices[0][1])
    def test_stale_and_missing_nodes_do_not_change_baseline(self):
        _,baseline,_=transitions(self.data(),None,1000)
        notices,new,_=transitions(self.data('DOWN'),baseline,1200)
        self.assertEqual(notices,[]);self.assertEqual(new,baseline)
        data=self.data('DOWN');data['snapshot']['nodes']=[]
        self.assertEqual(transitions(data,baseline,1001)[1],baseline)
    def test_notification_immediate_deduplicated_across_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings={'state_file':str(Path(tmp)/'state.json'),'allowed_user_ids':[12],'chat_id':12,'notifications':True};api=API();bot=Monitor(tmp,settings,api)
            bot.notify(self.data(),1000);bot.notify(self.data('DOWN',1001),1001)
            Monitor(tmp,settings,api).notify(self.data('DOWN',1002),1002)
            self.assertEqual(len(api.calls),1);self.assertIn('n1',api.calls[0][1]['text']);self.assertIn('🔴',api.calls[0][1]['text'])
    def test_delivery_failure_retries_transition(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings={'state_file':str(Path(tmp)/'state.json'),'allowed_user_ids':[12],'chat_id':12,'notifications':True};api=API();bot=Monitor(tmp,settings,api);bot.notify(self.data(),1000)
            call=api.call
            def fail(*a,**k):raise TelegramError('temporary')
            api.call=fail
            with self.assertRaises(TelegramError):bot.notify(self.data('DOWN',1001),1001)
            api.call=call;bot.notify(self.data('DOWN',1002),1002)
            self.assertEqual(len(api.calls),1)
    def test_generic_pending_reason_is_not_node_failure(self):
        data=self.data();data['snapshot']['jobs']=[{'JobId':'1','JobState':'PENDING','Reason':'Nodes_required_for_job_are_DOWN,_DRAINED_or_reserved'}]
        _,baseline,_=transitions(data,None,1000)
        self.assertEqual(transitions(data,baseline,1001)[0],[])

if __name__=='__main__':unittest.main()
