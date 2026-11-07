import json
from pathlib import Path
import tempfile
import unittest
from cdispatch.presentation import total, free, status, label, event_line
from cdispatch.telegram import Monitor

class API:
    def __init__(self):self.calls=[]
    def call(self,method,**kw):self.calls.append((method,kw));return {'message_id':len(self.calls)}

class PresentationTests(unittest.TestCase):
    def test_actual_allocations_exclude_pending_and_shared_double_count(self):
        data={'snapshot':{'jobs':[{'JobId':'1','JobName':'cd-service','JobState':'RUNNING','AllocTRES':'cpu=4,mem=4G'}, {'JobId':'2','JobState':'PENDING','ReqTRES':'cpu=64,mem=128G'}, {'JobId':'1','JobName':'cd-service','JobState':'RUNNING','AllocTRES':'cpu=4,mem=4G'}]},'tasks':{'shared':{'state':'RUNNING','job_id':'1','selection':{'shared':True},'spec':{'name':'codex-node'}}}}
        self.assertEqual(total(data),(4,4096,0));self.assertIn('Служба',status(data))
    def test_status_explains_full_job_slots_despite_free_cpu(self):
        data={'config':{'local_pool_limits':{'jobs':1}},'snapshot':{'jobs':[{'JobId':'1','JobState':'RUNNING','AllocTRES':'cpu=4,mem=4G'}], 'nodes':[{'NodeName':'a','State':'MIXED','CPUTot':128,'CPUAlloc':4,'RealMemory':100000,'AllocMem':4096}]}}
        text=status(data)
        self.assertIn('124 CPU',text);self.assertIn('1/1',text);self.assertIn('ждут освобождения слота',text)

    def test_free_excludes_drain_duplicate_and_unavailable_nodes(self):
        node={'NodeName':'a','State':'IDLE','CPUTot':32,'CPUAlloc':4,'RealMemory':64000,'AllocMem':10000,'CfgTRES':'gres/gpu=4','AllocTRES':'gres/gpu=1'}
        data={'snapshot':{'nodes':[node,node,{**node,'NodeName':'bad','State':'DRAIN'}]}}
        self.assertEqual(free(data),(28,54000,3))
    def test_explicit_display_labels_and_fallback(self):
        self.assertEqual(label({'spec': {'name': 'worker'}}), 'worker')
        self.assertEqual(label({'spec': {'name': 'worker', 'labels': {'display_name': 'Custom worker'}}}), 'Custom worker')

    def test_preemption_authoritative_over_timeout_sidecar(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'test.request.json';Path(str(p)+'.result.json').write_text(json.dumps({'time_budget_exhausted':True}))
            task={'spec':{'name':'SCF','profiles':[{'command':[str(p)]}]}}
            data={'tasks':{'t':task}}
            self.assertIn('PREEMPTED',event_line({'task':'t','reason':'Slurm PREEMPTED'},data)[1])
            self.assertEqual(event_line({'task':'t','reason':'Slurm FAILED'},data)[0],2)
    def test_batch_interruptions_edit_digest_recovery_silent_escalation_visible(self):
        with tempfile.TemporaryDirectory() as tmp:
            api=API();settings={'state_file':str(Path(tmp)/'bot.json'),'allowed_user_ids':[12],'chat_id':12,'notifications':True}
            bot=Monitor(tmp,settings,api)
            data={'heartbeat':1000,'snapshot':{'time':1000},'tasks':{},'events':[]}
            bot.notify(data,1000)
            data['events']=[{'time':1001,'kind':'TERMINAL','state':'FAILED','task':str(i),'reason':'Slurm PREEMPTED','job_id':str(i)} for i in range(3)]
            bot.notify(data,1001);self.assertEqual(len(api.calls),1)
            data['events'].append({'time':1002,'kind':'TERMINAL','state':'FAILED','reason':'Slurm PREEMPTED','task':'new'})
            Monitor(tmp,settings,api).notify(data,1002);self.assertEqual(api.calls[-1][0],'editMessageText')
            data['events'].append({'time':1003,'kind':'TERMINAL','state':'FAILED','reason':'OUT_OF_MEMORY','task':'oom'})
            Monitor(tmp,settings,api).notify(data,1003);self.assertEqual(api.calls[-1][0],'sendMessage');self.assertIn('🔴',api.calls[-1][1]['text'])
            self.assertEqual(len(api.calls),3)
    def test_pending_does_not_send_false_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            api=API();bot=Monitor(tmp,{'state_file':str(Path(tmp)/'bot.json'),'allowed_user_ids':[12],'chat_id':12,'notifications':True,'resource_wait_notice_seconds':600},api)
            t={'state':'PENDING','job_id':'1','spec':{'name':'SCF'},'attempts':[]}
            data={'heartbeat':1000,'snapshot':{'time':1000},'tasks':{'t':t},'events':[]}
            bot.notify(data,1000);data['heartbeat']=data['snapshot']['time']=1600;bot.notify(data,1600)
            self.assertEqual(len(api.calls),1);t['state']='RUNNING';bot.notify(data,1601)
            self.assertEqual(len(api.calls),1)

if __name__=='__main__':unittest.main()
