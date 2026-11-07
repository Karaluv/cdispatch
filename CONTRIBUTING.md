# Contributing

Use Python 3.9+ and a virtualenv. Install with `python -m pip install -e .` and
run `python -m unittest discover -s tests -v`. Tests require no Slurm service and
must never launch real jobs. The runtime has no third-party Python dependencies.

For lifecycle changes, cover ambiguous submission, cancellation intent,
validation and recovery. For planning changes, use deterministic synthetic
snapshots and confirm that forecasts do not mutate their inputs or submit work.
For plugin changes, run the companion package's tests in the same environment.

Build with `python -m build`; install the wheel into a fresh environment and
check `cdispatch --help`, `init` and `check-config` outside the source tree.
Keep site configuration and scientific integration outside the core. See
AGENTS.md for invariants and docs/migration.md for live deployment boundaries.

Open a focused issue/PR with the use case, minimal sanitized reproduction,
expected behavior and validation. Never attach production credentials or raw
research outputs. Code is MIT licensed; contributions must be compatible.
