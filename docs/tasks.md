# Task and lifecycle contracts

Required fields: `name`, absolute existing `workdir`, nonempty `profiles`.
Profiles declare partition, nodes, ranks_per_node, threads, mem_mib, wall_minutes,
slowdown and command argv. Optional typed GPU and pinning fields constrain the
same profile. `memory_model` can add base/per-rank/per-thread estimates and a
safety factor; the explicit memory minimum is never reduced.

`priority` is a local number (larger first), not Slurm Priority or Nice.
`urgent` selects a higher importance class. `depends_on` references previously
submitted task IDs and waits for SUCCEEDED. `group` labels a campaign; `parent`
binds child lifetime to its controller. `resource_key` prevents concurrent work
on the same logical output resource; otherwise the workdir is used.

`validate_command` runs after an exit-zero application. `artifact_contract`
provides supported file-level checks. Without acceptance evidence an exit-zero
batch ends FINISHED_UNVALIDATED. Validators must check application-specific
completeness, not merely whether the wrapper ran.

## Pause and resume

Declare all of `preemptible`, `checkpoint_command`,
`checkpoint_validate_command`, `checkpoint_files`, `resume_command`.
Checkpoint files must be complete, stable and hash-verified. `pause` requests an
application hook; it does not promise arbitrary POSIX process suspension.
`resume` uses a new attempt/allocation. Resume across resource layouts requires
explicit `resume_compatible_profiles`; elastic acceleration additionally needs
measured profile `expected_seconds` and migration overhead estimates.

`retry` after failure is explicit, requires `restart_from_scratch_safe` and must
fit `max_attempts`. Failed/external-cancelled jobs do not automatically retry.
`experimental: true` fixes one profile and one attempt without automatic restart
or migration.

## Deliberately destructive displacement

This is separate from checkpoint preemption. An urgent request can enumerate
`force_displace_tasks`; a configured plugin must allow each victim class. Core
requires a lower-priority, nonurgent batch child and excludes shared, protected
and adopted tasks. Partial progress can be lost. Forecasts distinguish `cancel`
from `pause`; the requester waits for confirmed resource release. Do not enable
this for workloads whose outputs cannot safely survive the interruption.

## Python API

```python
from cdispatch.client import Client
client = Client('/shared/site/state')
task_id = client.submit(spec, key='campaign-point-001')
task = client.status(task_id)
```

Keep logical IDs separate from Slurm job IDs. Reusing a key with a different spec
is rejected. `snapshot()` checks heartbeat and snapshot freshness. If the
controller is unavailable, preserve existing IDs and wait rather than resubmitting.
Cancellation receipts confirm intent acceptance; terminal task state confirms
actual completion. `GeometryWorkers` and other scientific orchestration belong
to the companion adapter package, not the generic Client API.
