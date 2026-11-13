# Research plugins now share this repository

The former cdispatch-examples project is included under `plugins/research/`.
It retains the `cdispatch_example` and `cdispatch_research` import names and
its optional `cdispatch-research-examples` distribution name. Core APIs,
installation and explicit plugin activation are unchanged.

From this checkout:

```sh
python -m pip install .
python -m pip install ./plugins/research
(cd plugins/research && python -m unittest discover -s tests -v)
```

Existing environments with the companion package can install the new local
subproject with the same package name. Configuration module names do not change.
No second repository checkout is needed. Plugin CI builds and tests the local
subproject alongside the core; no remote companion checkout is used.

This is a source-layout migration. It does not deploy or restart a cluster
service. Keep deployment configuration, state and live research artifacts outside
the repository. See the [plugin documentation](../plugins/research/README.md).

Original snapshot and import scope are recorded in
[import-provenance.json](../plugins/research/docs/import-provenance.json).
