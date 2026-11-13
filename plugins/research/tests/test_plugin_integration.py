import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from cdispatch import plugins
from cdispatch.scheduler import choose
from support import config, spec, snapshot

class IntegrationTests(unittest.TestCase):
    def test_reviewed_disk_profile_and_digest_are_enforced_by_core(self):
        with tempfile.TemporaryDirectory() as tmp:
            identifier='cp2k/energy-force-excited/cpu/reviewed'
            path=Path(tmp)/identifier/'profile.json';path.parent.mkdir(parents=True)
            record=dict(id=identifier,kind='batch',status='candidate',profile=dict(
                name='cpu8',partition='cpu',nodes=1,ranks_per_node=8,threads=1,
                mem_mib=4096,wall_minutes=10,slowdown=1,command=['ignored-provenance']))
            path.write_text(json.dumps(record))
            digest=hashlib.sha256(path.read_bytes()).hexdigest()
            cfg=config();cfg['plugins']=[dict(module='cdispatch_research.plugin',options=dict(
                profile_library=tmp,application_profile_rules=[dict(name='reviewed-rule',
                authorization='test fixture',labels={'project':'example'},
                profiles=[dict(id=identifier,sha256=digest)])]))]
            request=spec(labels={'project':'example'})
            request['profiles'][0]['command']=list(request['command'])
            original=copy.deepcopy(request)
            selection,_=choose(request,snapshot(),cfg)
            self.assertEqual(selection['profile']['ranks_per_node'],8)
            self.assertEqual(selection['profile']['command'],request['command'])
            self.assertEqual(request,original)
            # No process-lifetime cache may hide changes to pinned bytes.
            path.write_text(path.read_text()+' ')
            with self.assertRaisesRegex(ValueError,'changed'):
                choose(request,snapshot(),cfg)

    def test_default_plugin_cannot_authorize_destructive_victims(self):
        task={'spec':{'labels':{'stage':'excited'}}}
        cfg={'plugins':[{'module':'cdispatch_research.plugin'}]}
        self.assertFalse(plugins.allow_displacement(task,cfg))
        cfg['plugins'][0]['options']={'displacement_stages':['excited']}
        self.assertTrue(plugins.allow_displacement(task,cfg))
