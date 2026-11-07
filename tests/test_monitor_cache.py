import copy
import io
import json
from pathlib import Path
import tempfile
import unittest

from cdispatch.alerts import warnings
from cdispatch.monitor import mobile
from cdispatch.monitor_cache import JSONStream, read_monitor_cache, task_view


def fixture(payload_size=20000):
    profile=dict(name='cpu',partition='cpu',nodes=1,ranks_per_node=4,threads=2,
                 mem_mib=4096,wall_minutes=60,slowdown=1,gpus_per_node=0,
                 env={'LARGE_PAYLOAD':'x'*payload_size},command=['python','worker','/tmp/point.request.json'])
    spec=dict(name='O56 excited',kind='batch',priority=300,max_attempts=1,urgent=False,
              tags=[],labels={'project':'glass','case':'bond_si3','stage':'excited'},profiles=[profile])
    task=dict(id='geometry-one',spec=spec,submitted_spec=copy.deepcopy(spec),state='FAILED',
              created=10,reason='Slurm FAILED',job_id='123',attempts=[dict(number=1,path='/tmp/run',
              selection={'profile':profile,'nodes':['n1']},result={'huge':'x'*payload_size})],
              selection={'profile':profile,'nodes':['n1']},decision={'reason':'fit','rationale':'matched',
              'selection':{'profile':profile,'nodes':['n1']}})
    return dict(heartbeat=100,snapshot={'time':100,'jobs':[],'nodes':[]},tasks={'geometry-one':task},
                events=[{'time':100,'kind':'TERMINAL','task':'geometry-one','state':'FAILED','job_id':'123'}],
                accounting=[],counters={'FAILED':1})


class MonitorCacheTests(unittest.TestCase):
    def test_tiny_chunks_preserve_unicode_escapes_and_numbers(self):
        data=fixture(40);data['edge']=['кириллица 😀',{'quote':'\\"{}'},1.23e123,1234567890,False,None]
        expected=copy.deepcopy(data);expected['tasks']={k:task_view(v) for k,v in data['tasks'].items()}
        for width in [1,2,7,64,65536]:
            actual=JSONStream(io.StringIO(json.dumps(data,ensure_ascii=False)),chunk_size=width).root()
            self.assertEqual(actual,expected)

    def test_view_retains_monitor_details_and_typed_failure_paths(self):
        data=fixture();original=copy.deepcopy(data);compact=copy.deepcopy(data)
        compact['tasks']={k:task_view(v) for k,v in data['tasks'].items()}
        for command,arg in [('queue',''),('decisions',''),('why','geometry-one'),('events',''),('report','')]:
            self.assertEqual(mobile(data,command,arg),mobile(compact,command,arg))
        self.assertEqual(warnings(data,now=100),warnings(compact,now=100))
        self.assertNotIn('command',compact['tasks']['geometry-one']['spec']['profiles'][0])
        self.assertNotIn('submitted_spec',compact['tasks']['geometry-one'])
        self.assertEqual(data,original)

    def test_large_history_is_read_in_bounded_chunks(self):
        data=fixture();data['tasks']={str(i):dict(data['tasks']['geometry-one'],id=str(i)) for i in range(150)}
        class BoundedReader(io.StringIO):
            def read(self,size=-1):
                if size<0 or size>65536:raise AssertionError('unbounded read')
                return super().read(size)
        raw=json.dumps(data);result=JSONStream(BoundedReader(raw)).root()
        self.assertEqual(len(result['tasks']),150)
        self.assertLess(len(json.dumps(result)),len(raw)//10)

    def test_shared_resource_memory_estimate_is_preserved(self):
        data=fixture(40);task=data['tasks']['geometry-one']
        task['state']='RUNNING';task['selection']['shared']=True
        task['spec']['profiles'][0]['memory_model']={'base_mib':8192,'per_rank_mib':512,'safety_factor':1.2}
        compact=JSONStream(io.StringIO(json.dumps(data))).root()
        self.assertEqual(mobile(data,'queue',''),mobile(compact,'queue',''))

    def test_missing_malformed_truncated_and_oversized_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(read_monitor_cache(Path(directory)/'missing',{}),{})
        for raw in ['{"tasks":', '{"tasks":[]}','{} garbage','{"tasks":{"x":oops}}']:
            with self.assertRaises(ValueError):JSONStream(io.StringIO(raw),chunk_size=3).root()
        with self.assertRaisesRegex(ValueError,'bounded reader limit'):
            JSONStream(io.StringIO('{"long":"'+'x'*100+'"}'),chunk_size=5,max_value_chars=20).root()

if __name__=='__main__':unittest.main()

class IncrementalAccountingTests(unittest.TestCase):
    def test_accounting_array_exceeds_individual_value_limit_without_redecoding_whole_array(self):
        data={'tasks':{},'accounting':[{'id':str(i),'payload':'x'*300} for i in range(200)]}
        stream=JSONStream(io.StringIO(json.dumps(data)),chunk_size=128,max_value_chars=1024,pool_keys=True)
        result=stream.root()
        self.assertEqual(result,data)
        self.assertGreater(len(json.dumps(data['accounting'])),stream.limit)
