# cdispatch examples and research plugins

[![Tests](https://github.com/Karaluv/cdispatch/actions/workflows/research-plugins.yml/badge.svg)](https://github.com/Karaluv/cdispatch/actions/workflows/research-plugins.yml)

Bundled optional recipes for [**cdispatch**](https://github.com/Karaluv/cdispatch), a configurable task queue above Slurm for
improving resource utilization across competing workloads.

This subproject keeps application-specific behavior and site recipes outside the
generic dispatcher. It contains an actual installable plugin package, not only
configuration snippets. Core cdispatch runs without this package.

## Contents

- `cdispatch_example`: a minimal plugin for display labels and opt-in victim
  classification. Useful as a starting point for your own extensions.
- `cdispatch_research`: extracted geometry-worker journaling, electronic-result
  validation, CP2K resource bindings, application-budget classification and
  research-specific display labels.
- `examples/site/`: fictional single-site and multi-account configuration recipes.
- `examples/tasks/`: explicit CPU/GPU resource-shape examples and profile context.
- `docs/`: plugin contracts, profile setup and migration notes.
- `tests/`: independent regression coverage for research contracts.

## Install alongside the core

From the root of the cdispatch checkout:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
python -m pip install ./plugins/research
(cd plugins/research && python -m unittest discover -s tests -v)
```

The core is not assumed to be on PyPI. Both packages and the selected interpreter
must be visible on compute nodes. A second repository is no longer needed.

Enable a plugin explicitly in your private site configuration:

```json
{
  "plugins": [
    {"module": "cdispatch_example", "options": {"displacement_stages": []}}
  ]
}
```

Use `cdispatch_research.plugin` for the scientific integration. Installing the
package alone does not activate policies. Keep production config, state, tokens,
SSH keys, live task IDs and private profile evidence outside this repository.

## Research extension

`GeometryWorkers` journals submissions, preserves logical IDs across recovery and
redelivers unacknowledged results until the optimizer saves its state. The
optimizer must save consumed IDs before acknowledgement. Electronic acceptance
remains an application gate, not merely a Slurm exit code.

The CP2K binding adapter accepts explicitly reviewed CPU energy-force-excited
profiles, validates their SHA-256, and preserves application commands and gates
when replacing resource shapes. It is intentionally domain-specific. Resource
availability is not evidence that different scientific layouts are equivalent.

See [research plugin configuration](docs/research-plugin.md),
[profile recipes](docs/profiles.md) and the core plugin/migration documentation.
The included configuration uses fictional users, hosts, paths and partitions;
it does not claim those profiles are qualified on your hardware.

The project is MIT licensed. Public documentation is English; some extracted
research display labels retain the original Russian terminology.
