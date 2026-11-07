# Migrating an existing installation

This extraction is prepared separately from the running service. A source-code
refactor does not authorize killing handlers, changing active attempts or claiming
that the new package is deployed.

1. Preserve the original installation, config, code hashes, state and attempt
   paths. Keep the current controller's exact runtime available for rollback.
2. Install core and needed plugins in a separate shared virtualenv. Set an
   external config/state/profile location. Run unit tests and offline CLI checks.
3. Move application_profile_rules/application_lane_owners into the research
   plugin's options; point profile_library at the unchanged reviewed library.
   Set controller_transfer_stages explicitly for legacy scientific supervisors.
4. Update new controller imports from `cdispatch.geometry` to
   `cdispatch_research.geometry`, and new adapter commands from
   `cdispatch.adapters` to `cdispatch_research.adapters`. Active attempt bundles
   and historical commands remain immutable. The extracted timeout adapter can
   recognize both command identities for historical evidence.
5. Compare old and new forecasts against captured snapshots. Reconcile queued
   legacy commands before cutover; do not silently edit their original specs.
6. Drain shared handlers or schedule a controlled service transition. Stop only
   the old controller when safe; never run two writers against the same state.
   Keep independent batch workers tracked. Verify delegated broker roots and
   worker interpreters before enabling submissions.
7. Start the new service against the intended state, verify heartbeat, task IDs,
   outstanding submissions, lifecycle and profiles, then admit a bounded test
   task. Record actual validation. Preserve a rollback environment and state copy.

This release retains scheduler record v2 and monitor schema v1. The monitor no
longer includes scientific command paths; it uses prepared display labels instead.
The plugin boundary and external paths are new deployment requirements, so a
live upgrade is not automatic or promised seamless.
