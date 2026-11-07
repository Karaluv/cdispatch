# Resource-profile libraries

Site profiles are external data, separate from Python packages and runtime state.
Set `CDISPATCH_PROFILES` or pass `cdispatch profiles --library /shared/profiles`.

An entry has an ID `program/workload/hardware/name`, `profile.json` and companion
`NOTES.md`. It records applicability context, explicit resource shape, provenance
hashes, observations and qualification status: candidate, validated, rejected or
retired. A Slurm COMPLETED record alone does not validate an application profile.

```bash
cdispatch profiles --library /shared/profiles search --program myapp
cdispatch profiles --library /shared/profiles show myapp/workload/cpu/layout
cdispatch profiles --library /shared/profiles validate
```

`import-task` records a candidate from a reviewed task JSON. `record` adds an
observation; promotion requires application validation and matching context.
`build` binds selected shapes to new commands/workdirs. Historical commands are
provenance and are not silently reused. Performance comparisons require matched
input and timing scope. Unmeasured layouts are tied, not claimed equally fast.

The companion repository provides synthetic importable CPU/GPU task templates
and a profile-generation recipe. Real cluster profiles and their private evidence
remain in the site's external library. Copying a profile to another cluster does
not transfer its qualification automatically.
