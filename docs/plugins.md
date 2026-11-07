# Plugin API v1

Install a plugin package into the controller/worker Python environment, then add:

```json
{"plugins": [{"module": "example_plugin", "options": {"displacement_stages": ["worker"]}}]}
```

A module declares `API_VERSION = 1`. All hooks are optional. There is no decorator,
automatic discovery or entry-point registry; only modules named in administrator
configuration are loaded. Plugins are trusted Python code, not sandboxed code.
Plugin names/options must not come from untrusted task submissions.

| Hook | Contract |
|---|---|
| `prepare_spec(spec, options)` | Return a task with reviewed `profiles` / `placement` changes on a private copy. Other task fields remain unchanged. The resulting task is schema-validated. |
| `expected_failure(task, slurm_state, job_id, options)` | Return a bool after checking application evidence. Only FAILED/COMPLETED wrapper outcomes are eligible; OOM, node and walltime failures stay infrastructure failures. |
| `allow_displacement(task, options)` | Return a bool allowing an application-specific victim class. Core still requires exact target IDs, urgent higher-priority requester, batch child, and no protected/adopted/shared victim. Default is false. |
| `display_labels(task, options)` | Return string `display_name` / `display_group` labels. Applied to observer projections, never to persisted submitted specs. |

`prepare_spec` may also return `_application_profile_rule` metadata. A delegated
lane may return `_application_lane_delegated` plus an empty profile list to block
admission, preserving the original task. These legacy metadata names are retained
in v1 for migration; they do not activate any built-in scientific behavior.

Hooks receive copies, execute synchronously, and should be bounded and repeatable.
The planner may call a hook more than once. Resource-profile commands remain part
of the trusted profile contract: a plugin author must preserve scientific inputs,
validators and restart compatibility; schema checks cannot prove scientific
correctness. No hook may call sbatch, scancel or launch work.

Load/version errors fail before normal controller operation. Planning hook errors
prevent scheduling rather than silently skipping the configured policy. Display
hook failures write monitor-error.json while authoritative status survives.
Expected-failure classification changes circuit accounting, not the task's FAILED
state. The raw Slurm outcome remains available.

Core tests run without the companion package. The separate `cdispatch-examples`
repository contains a minimal plugin and the extracted `cdispatch_research`
adapter, with its own tests and cluster-specific options.
