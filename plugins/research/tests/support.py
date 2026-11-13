import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from cdispatch.common import atomic,read,validate,enqueue,fields,lock
from cdispatch.scheduler import choose,reserve,memory,usage
from cdispatch.daemon import Daemon
from cdispatch.cli import report

BASE=Path(__file__).resolve().parents[1]

def config():
    c=read(BASE/'tests/fixtures/config.json');c['owner']='alice';c['users']={'alice':dict(enabled=True,authorized=True,account='lab',limits=dict(jobs=5,cpus=256,gpus=4,mem_mib=3000000,nodes=8),partitions=['cpu','old','gpu'])}
    c['submit_interval']=0;c['interactive_reserve_jobs']=0
    return c

def spec(**kw):
    s=dict(name='test',workdir='/tmp/test-work',priority=100,labels=dict(project='glass',purpose='test'),
           command=['/bin/true'],validate_command=['/bin/true'],profiles=[dict(name='cpu64',partition='cpu',ranks_per_node=16,threads=4,mem_mib=100000,wall_minutes=10)])
    s.update(kw);return validate(s)

def node(name='n1',part='cpu',cpus=128,mem=1000000,state='IDLE',**kw):
    n=dict(NodeName=name,Partitions=part,CPUTot=str(cpus),CPUEfctv=str(cpus),CPUAlloc='0',RealMemory=str(mem),AllocMem='0',State=state,Gres='(null)',AllocTRES='')
    n.update(kw);return n

def snapshot():
    return dict(nodes=[node()],partitions={k:dict(State='UP',MaxTime='1-00:00:00') for k in ['cpu','gpu','old']},jobs=[],time=time.time())

class Fake:
    def __init__(self):self.snap=snapshot();self.acct=[];self.calls=[];self.error=False
    def snapshot(self):
        if self.error:raise RuntimeError('controller unreachable')
        return copy.deepcopy(self.snap)
    def accounting(self,since,ids=None):return self.acct
    def submit(self,user,sel,a):self.calls.append(sel);return '123'
    def cancel(self,u,j):self.calls.append(('cancel',j))
    def pause(self,u,j):self.calls.append(('pause',j))
