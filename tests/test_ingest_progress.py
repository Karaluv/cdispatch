import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from cdispatch.common import atomic,read
from cdispatch.daemon import Daemon
from test_dispatcher import config,Fake,spec
class ProgressTests(unittest.TestCase):
 def test_ingest_yields_after_durable_request(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);d=Daemon(root,config(),Fake())
   for i in range(3):atomic(root/"inbox"/(str(i)+".json"),dict(op="pause-all",id="test"))
   with patch("cdispatch.daemon.time.monotonic",side_effect=[0,8]),patch.object(d,"save") as save:d.ingest()
   self.assertEqual(len(list((root/"inbox").glob("*.json"))),2)
   self.assertTrue(read(root/"receipts/0.json")["ok"]);save.assert_called_once();self.assertTrue(d.state["paused"])
 def test_interactive_request_precedes_older_batch_burst(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);d=Daemon(root,config(),Fake())
   atomic(root/"inbox/batch.json",dict(op="submit",id="batch",spec=spec()))
   atomic(root/"inbox/interactive.json",dict(op="submit",id="interactive",spec=spec(labels=dict(project="interactive",purpose="test"))))
   with patch("cdispatch.daemon.time.monotonic",side_effect=[0,8]),patch.object(d,"save"):d.ingest()
   self.assertIn("interactive",d.state["tasks"]);self.assertNotIn("batch",d.state["tasks"])
 def test_bulk_atomic_preserves_encoding_and_replacement(self):
  with tempfile.TemporaryDirectory() as directory:
   p=Path(directory)/"state.json";data={"unicode":"данные","tasks":[{"a":i} for i in range(100)]}
   atomic(p,data);self.assertEqual(p.read_text(),json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False)+"\n")
   atomic(p,{"next":1});self.assertEqual(read(p),{"next":1})
if __name__=="__main__":unittest.main()
