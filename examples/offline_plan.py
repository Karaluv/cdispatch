"""Explore resource-profile selection without Slurm, disk state or submissions."""
import json
import time
from cdispatch.common import validate
from cdispatch.configuration import initial
from cdispatch.scheduler import queue_plan

config = initial('alice', 'lab', ['cpu'])
snapshot = dict(time=time.time(), jobs=[], nodes=[dict(
    NodeName='demo-node', State='IDLE', Partitions='cpu', CPUTot='16',
    CPUAlloc='0', RealMemory='65536', AllocMem='0', Gres='(null)', AllocTRES='')],
    partitions={'cpu': dict(State='UP', MaxTime='1-00:00:00')})
tasks = []
for i in range(2):
    spec = validate(dict(name='analysis-'+str(i), workdir='/example/run-'+str(i),
        priority=100, max_slowdown=1.6, profiles=[dict(
            name='cpu'+str(n), partition='cpu', ranks_per_node=1, threads=n,
            mem_mib=8192, wall_minutes=30, slowdown=slowdown,
            command=['/example/analysis', '--threads', str(n)])
            for n, slowdown in [(16, 1), (8, 1.6)]]))
    tasks.append(dict(id='task-'+str(i), created=i, state='QUEUED', spec=spec))
plan, decisions = queue_plan(tasks, snapshot, config)
print(json.dumps(dict(plan=plan, decisions=decisions), indent=2))
