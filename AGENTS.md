# Contributor and coding-agent instructions

## Scope and architecture

Read README.md and the relevant docs before changing behavior. Public documentation
and new user-facing messages are English. Python 3.9+; the core runtime uses only
the standard library. Keep application logic and cluster identities outside the
core. Extensions are explicit trusted plugins; never auto-import task-supplied code.
Prefer small functions for planning; isolate filesystem, clock, process and Slurm
operations. Do not turn a forecast into an execution path.

## Operational invariants

- Never submit, cancel, pause or restart real cluster work as part of tests.
- Persist submission/cancellation intent before external RPC. An ambiguous sbatch
  response is not permission to retry; reconcile the unique identity first.
- A successful process exit is not validated application success.
- Resource profiles are explicit reviewed alternatives. Do not infer GPU support,
  checkpoint compatibility or safe rank changes from available hardware.
- Keep quotas, ownership, health gates and checkpoint validation in force.
- Keep submitted specs immutable and attempts separate. No hidden requeue.
- Observers read caches; observer failure must not stop the scheduler.
- Never cancel the parent service allocation to stop one shared handler.

## Changes and validation

Use an isolated checkout and synthetic snapshots. Preserve existing regression
coverage; add tests for changed lifecycle contracts and plugin boundaries.
Run `python -m unittest discover -s tests -v` after installing the project.
For packaging changes, build and install a wheel in a fresh environment and run
CLI smoke checks from outside the checkout. Run companion-plugin tests when
changing plugin APIs. Report what was actually tested; distinguish simulated
Slurm behavior from live cluster validation.

Do not copy production config, state, logs, credentials, SSH material, research
artifacts or private provenance into this repository. Use fictional fixture
identities. Do not restart a live deployment during a refactor. Prepare a
reviewable migration and use its separately controlled deployment procedure.

## Style and API

Use descriptive names, four-space indentation, explicit exceptions and argv
lists for subprocess calls. Avoid introducing new compressed one-line blocks.
Keep APIs small and documented; version breaking plugin or record contracts.
Do not add dependencies or broad abstractions without a concrete use case.
Commit focused changes and include validation and migration implications.
