import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cdispatch.common import atomic,validate
from cdispatch.slurm import Slurm
from cdispatch.worker import main as worker_main
from test_dispatcher import config


class PinningTests(unittest.TestCase):
    def test_scheduler_hint_and_worker_environment_match_profile(self):
        c=config();p=dict(name='pinned',partition='cpu',nodes=1,ranks_per_node=2,threads=4,mem_mib=1024,wall_minutes=10,
                          command=['/usr/bin/python3','-c',"import os,json;from pathlib import Path;Path('seen.json').write_text(json.dumps({k:os.getenv(k) for k in ('I_MPI_PIN','I_MPI_PIN_DOMAIN','OMP_NUM_THREADS','OMP_PROC_BIND','OMP_PLACES','MKL_NUM_THREADS','DISPATCH_RANKS')}))"],
                          pinning=dict(slurm_hint='nomultithread',mpi='intel-omp',omp_proc_bind='close',omp_places='cores',mkl_threads=1,application_threads=4))
        with tempfile.TemporaryDirectory() as directory:
            work=Path(directory);(work/'attempt').mkdir()
            task=validate(dict(name='pinned',workdir=str(work),profiles=[p],validate_command=['/usr/bin/true']))
            selection=dict(user='alice',profile=task['profiles'][0],nodes=['node1'],mem_mib=1024,job_name='pin-test')
            slurm=Slurm(c)
            with patch.object(slurm,'user_run',return_value='123') as mock:
                self.assertEqual(slurm.submit('alice',selection,work/'attempt'),'123')
            self.assertIn('--hint=nomultithread',mock.call_args.args[1])
            atomic(work/'attempt'/'spec.json',dict(id='pinned',spec=task,selection=selection,created=0))
            self.assertEqual(worker_main(str(work/'attempt')),0)
            seen=json.loads((work/'seen.json').read_text())
            self.assertEqual(seen,dict(I_MPI_PIN='1',I_MPI_PIN_DOMAIN='omp',OMP_NUM_THREADS='4',
                                       OMP_PROC_BIND='close',OMP_PLACES='cores',MKL_NUM_THREADS='1',DISPATCH_RANKS='2'))
