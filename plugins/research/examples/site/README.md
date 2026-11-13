# Site recipes

`single-owner.json` starts with one owner and a bounded shared service.
`research-pool.json` illustrates a 19-slot pool split 11/4/4, with two delegated
accounts disabled until explicitly authorized and configured. This shape derives
from a real research deployment; all identities, paths and partitions are examples.

Set actual account permissions, node access, service size and local limits.
`null` CPU/RAM/GPU/node ceilings do not bypass Slurm or physical capacity.
The service counts as an allocation; its handlers count as steps.

Copy a recipe outside the repository, set CDISPATCH_CONFIG / CDISPATCH_ROOT /
CDISPATCH_PROFILES, run check-config, then inspect live limits and a service plan.
Never enable fictional delegated identities or submit the example paths unchanged.
