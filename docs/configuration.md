# Site configuration

The installation has three independent writable locations:

| Purpose | CLI/environment | Default |
|---|---|---|
| Config | `--config`, `CDISPATCH_CONFIG` | `$XDG_CONFIG_HOME/cdispatch/config.json` or `~/.config/cdispatch/config.json` |
| State | `--root`, `CDISPATCH_ROOT` | `$XDG_STATE_HOME/cdispatch` or `~/.local/state/cdispatch` |
| Profiles | `profiles --library`, `CDISPATCH_PROFILES` | `$XDG_DATA_HOME/cdispatch/profiles` or `~/.local/share/cdispatch/profiles` |

Place all required code, input and runtime paths on storage accessible to the
participating nodes/accounts. Never put mutable state under site-packages.
`cdispatch init --owner USER --account ACCOUNT --partition PARTITION` writes a
conservative single-owner configuration. Multiple `--partition` flags are allowed.
`check-config` performs local basic structure/plugin checks; it is not a live
cluster qualification. `doctor` and `limits --refresh` query Slurm.

## Principal settings

- `users`: enabled/authorized identities, Slurm account, allowed partitions,
  optional QoS, Unix groups and per-user `limits`.
- `pool_limits`: aggregate jobs, CPU, MiB, GPU and node ceilings. `null` resource
  ceilings mean no additional local cap, not unlimited physical capacity. The
  current implementation requires a positive integer local job ceiling.
- `poll_interval`, `account_interval`, `submit_interval`, `max_pending`: observation
  and submission bounds. The example uses one new allocation per ten seconds.
- `account_labels`: optional display titles keyed by Unix account name.
- `node_cost`, `excluded_nodes`, service/interactive partitions and preferred
  nodes: explicit site placement policy. No cluster node names are built in.
- `auto_preempt`, `auto_rebalance`: opt-in checkpoint displacement and compatible
  restart-based acceleration. Application contracts are still mandatory.
- `shared_pool`, `component_steps`, service walltime: bounded service resources.
- `plugins`: explicit module names and plugin-owned options.
- `controller_transfer_stages`: eligible supervisor stage names for an explicit
  parent handoff; defaults to `controller`.
- `telegram_settings`: optional external settings file. Keep it out of Git.

The generated `python` path identifies the creating interpreter. Use a virtualenv
visible to workers, install plugins into that same environment, and verify paths
before deploying. Config and state are read at runtime; changing the file is not
proof that an existing daemon has loaded it.

## Delegated accounts

Delegation is opt-in and requires explicit authorization and filesystem access.
Configure `ssh_identity`, host/key-alias information or a pre-authorized SSH
control socket per account. Restrict the SSH key to the broker entry point.
An administrator-controlled forced-command wrapper must set `CDISPATCH_ROOT` to
the exact shared state root before calling `cdispatch-broker` or
`cdispatch-shared-broker`. Do not accept that root from a remote client.

The broker checks managed attempt paths, selected user and permitted operations.
Existing workdirs are not recursively chmod'ed. Delegation cannot bypass shared
Slurm association/QoS limits. Test with authorized accounts on the target cluster
before enabling it; unit tests cannot establish cluster ACL or QoS correctness.
