# cdispatch

**A configurable task queue above Slurm for keeping a resource pool busy.**

You have several workloads competing for a limited pool of CPUs, GPUs, memory
and job slots. Some tasks are urgent, some can wait, and some can safely pause
and resume. A task may also have several reviewed resource profiles: a fast,
large allocation or a smaller alternative that fits the available capacity.

cdispatch keeps these requests in a durable local queue, compares feasible
placements, and admits work to Slurm at a bounded rate. Its goal is better
**resource utilization and responsive scheduling** within your pool. Slurm still
owns allocations, cluster policy and final start times. Utilization gains depend
on workload flexibility and available resources; no benchmark gain is claimed.

## What it does

- **Queue across a resource pool.** Account for CPU, RAM, typed GPUs, nodes, job
  slots and existing external jobs across explicitly authorized accounts.
- **Select resource profiles.** Choose among declared MPI/OMP layouts, memory
  budgets, partitions and commands, with slowdown and placement constraints.
- **Prioritize work.** Urgent classes, numeric priority, dependencies, bounded
  queue-wide packing and optional group CPU fair share.
- **Pause eligible tasks.** Checkpoint-aware preemption and explicit resume;
  optional migration between declared restart-compatible profiles.
- **Explain decisions.** Preview admission with `plan`, inspect constraints with
  `explain`, and follow cached status, events and resource accounting.
- **Recover carefully.** Durable intents, idempotent submissions, unique attempt
  identities, unknown-submission reconciliation and independent attempt logs.
- **Validate results.** Distinguish process completion from an accepted application
  result; downstream tasks wait for validated success.
- **Share service capacity.** Optional small trusted handlers in separately
  limited Slurm steps within the service allocation.
- **Customize the installation.** External site config, resource-profile libraries,
  application commands and validators, and explicitly configured Python plugins.

The project was extracted from a working research-cluster installation. The
portable package and plugin boundary are new; this is an early release, not a
claim of validation on every Slurm installation. The runtime is Python 3.9+
using the standard library. Terminal panels additionally need tmux. Some legacy
monitor/Telegram text is currently Russian; the CLI and public docs are English.

## Install

On a Linux host:

```bash
git clone https://github.com/Karaluv/cdispatch.git
cd cdispatch
```

Install from the checkout:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
cdispatch --help
```

Use an interpreter and installation visible on the compute nodes. The package
is not assumed to be published on PyPI. See [configuration](docs/configuration.md)
for shared-filesystem paths and optional delegated accounts.

## Configure and inspect

```bash
cdispatch init --owner alice --account lab --partition cpu
cdispatch check-config
```

`init` writes a new configuration and refuses to overwrite one. It creates no
Slurm jobs. Edit the generated file for the real pool, partitions and limits.
Use `--config` before the subcommand, or set `CDISPATCH_CONFIG`.

```bash
cdispatch --config /shared/site/config.json --root /shared/site/state check-config
cdispatch --config /shared/site/config.json --root /shared/site/state service-start --plan
```

`service-start --plan` observes Slurm and previews the service placement. On a
configured cluster, `service-start` starts the compute-node service. Running
that command is an actual allocation request. Keep persistent services off
login nodes unless the site's policy explicitly allows them.

## Submit a task

A task provides an isolated existing work directory and explicit resource profiles:

```json
{
  "name": "analysis",
  "workdir": "/shared/runs/analysis-001",
  "priority": 100,
  "max_slowdown": 1.6,
  "profiles": [
    {
      "name": "cpu16", "partition": "cpu", "nodes": 1,
      "ranks_per_node": 1, "threads": 16,
      "mem_mib": 32768, "wall_minutes": 60, "slowdown": 1,
      "command": ["/shared/apps/run-analysis", "--threads", "16"]
    },
    {
      "name": "cpu8", "partition": "cpu", "nodes": 1,
      "ranks_per_node": 1, "threads": 8,
      "mem_mib": 24576, "wall_minutes": 90, "slowdown": 1.6,
      "command": ["/shared/apps/run-analysis", "--threads", "8"]
    }
  ],
  "validate_command": ["/shared/apps/check-analysis", "result.json"]
}
```

These are illustrative commands and resource estimates, not qualified defaults.
Replace them with reviewed application-specific values.

```bash
cdispatch plan task.json
cdispatch submit task.json
cdispatch status
cdispatch explain TASK_ID
cdispatch logs TASK_ID
```

`plan` uses the cached snapshot, creates no allocation and does not guarantee a
start time. A stale snapshot prevents reliable admission. Without a validator or
artifact contract, an exit-zero batch task is `FINISHED_UNVALIDATED`.

## Priority and temporary interruption

`priority` orders work inside its importance class; `urgent: true` requests the
urgent class. Enable `auto_preempt` in site config to consider checkpoint-based
preemption. Eligible victims must declare `preemptible`, `checkpoint_command`,
`checkpoint_validate_command`, `checkpoint_files` and `resume_command`.

The worker runs the checkpoint hook, validates it, stops the application, validates
again, and records file hashes. Resources are treated as released only after
confirmation. `resume` starts another attempt with the verified checkpoint.
Arbitrary processes cannot be safely paused merely by setting a flag. Protected
work and absent compatible capacity can still make urgent tasks wait.

See [task contracts](docs/tasks.md) for dependencies, retries, migration and the
separate, explicitly authorized destructive-displacement mechanism.

## Core, plugins and site data

```text
cdispatch/                  generic Python package and CLI
  scheduler.py              placement and queue planning
  daemon.py                 durable state and action execution
  slurm.py                  Slurm command backend
  plugins.py                explicit versioned extension hooks
external site directory/    config, state, profiles; not inside the package
cdispatch-examples/         separately installed plugins and recipe examples
```

The companion [cdispatch-examples](https://github.com/Karaluv/cdispatch-examples) repository contains the extracted research
adapter, a minimal plugin and an anonymized multi-account site recipe. Core
operation does not require it. There is no automatic plugin discovery from task
files. See [plugins](docs/plugins.md), [profiles](docs/profiles.md),
[architecture](docs/architecture.md) and [migration](docs/migration.md).

## Development

```bash
python -m pip install -e .
python -m unittest discover -s tests -v
```

Try `python examples/offline_plan.py` for an offline two-task placement example.

Tests use temporary directories and simulated Slurm snapshots/backends. They
must not submit live work. See [CONTRIBUTING.md](CONTRIBUTING.md) and
[AGENTS.md](AGENTS.md). Licensed under MIT.
