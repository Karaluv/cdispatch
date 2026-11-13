# Build a separate site profile library

The task templates and `examples/profile-library/` entries are synthetic candidates, not measured profiles.

Run `cdispatch profiles --library examples/profile-library validate` to check the
bundled library. Each entry includes a source hash and a NOTES.md file.
Edit partition, commands, paths and resource requirements for your application.
Then use the core CLI to import a configuration with explicit applicability:

```bash
cdispatch profiles --library /shared/site/profiles import-task examples/tasks/cpu.json --program demo --workload analysis --context examples/tasks/context.json
cdispatch profiles --library /shared/site/profiles validate
cdispatch profiles --library /shared/site/profiles search --program demo
```

The import records source hashes and creates profile.json / NOTES.md pairs.
`record` can later add real observations. Promote only after application-level
validation, and record matching inputs, hardware/software and timing scope.
`build` requires commands for the new isolated run; historical commands are not
implicitly reused. Keep actual profile evidence outside the public examples.
