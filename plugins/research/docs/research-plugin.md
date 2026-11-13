# Research plugin configuration

```json
{
  "plugins": [{
    "module": "cdispatch_research.plugin",
    "options": {
      "profile_library": "/shared/site/profiles",
      "application_profile_rules": [],
      "application_lane_owners": [],
      "displacement_stages": []
    }
  }],
  "controller_transfer_stages": ["geometry-controller"]
}
```

These options carry former installation-specific rules. Empty rules perform no
resource replacement, and empty displacement stages allow no forced victim
class. Existing checkpoint preemption remains controlled by core contracts.
To retain an explicitly authorized research policy, configure the reviewed stage
list and exact requesting task IDs; naming a stage alone does not cancel work.

A binding rule requires `name`, `authorization`, matching `labels`, and `profiles`
with exact `id` and `sha256` references. It may set `preferred_partitions` and
`preferred_cpu_counts`. Ambiguous rules fail. A changed file with an old digest
fails immediately; the extracted adapter deliberately rechecks bytes instead of
retaining a process-lifetime cache. Profiles must be CPU batch candidate/validated
entries under `cp2k/energy-force-excited/`.

New application code imports `cdispatch_research.geometry.GeometryWorkers` and
uses `python -m cdispatch_research.adapters`. Historical attempt bundles are not
rewritten. The expected-budget adapter recognizes both legacy and new module
names, verifies request hashes and job identity, and never reclassifies Slurm
OOM, node failure or walltime kills as normal application exhaustion.

For effective-mass recipes, set `CDISPATCH_MASS_PARTITIONS`,
`CDISPATCH_RESEARCH_ENV`, `CDISPATCH_MASS_DRIVER` and `CDISPATCH_MASS_PYTHON` to
reviewed site values. The wrapper does not contain a user's home directory,
compiler setup path or campaign-specific driver location.
