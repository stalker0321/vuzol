# Capability registry, installations and local node (WP03)

Status: implemented additively. Three distinct facts stay separate
(ADR-A01): **descriptor** (what the capability means), **installation** (where
it is verifiably available) and **permission grant** (`Capability` enum /
`ProjectConfig.allowed_capabilities`). Discovery never installs and never grants
permissions; the installer remains the backend.

## Descriptor registry (`capability-descriptors.v1`)

`vuzol.projects.descriptors` ships static, versioned descriptors for the
capabilities that already exist: `git`, `python-runtime`, `node-runtime`,
`java-runtime`, `go-toolchain`, `gradle-toolchain` and the actions
`repo.validate`, `repo.apply`, `capability.install`. Each descriptor carries:

- `key`, `version`, `label`, `kind` (`host_tool` / `managed_toolchain` / `action`);
- `effect_class` (`read_only` / `isolated_mutation` / `external_mutation` / `host_privileged`);
- `input_schema` / `output_schema` (`<key>.input.v1`, `<key>.output.v1`);
- `requires` (other capability keys) and `secret_refs` (aliases only);
- `descriptor_hash` (sha256 over the canonical form).

`host_executables()` reproduces the legacy `_EXECUTABLES` map
(`git`→`git`, `python-runtime`→`python3`, `node-runtime`→`node`); the other
keys are managed-toolchain only. A descriptor never contains a permission or a
credential value.

## Local node descriptor (`local-node.v1`)

`local_node_descriptor(settings)` builds the single static local node descriptor
from configuration: `node_id`, `labels`, `trust_class="local"`, the capability
keys the node advertises, the approved `toolchain_root`, and `secret_refs`.
`CapabilityProvisioningSettings` gained `node_id`, `health_ttl_seconds` and
`secret_refs` (`env:`/`file:` references only). Node advertisement is a
candidate set, not a proof of health.

## Installations (`capability_installations`)

New additive table. One row per `(capability_key, node_id)`:

- `version`, `receipt_hash` (sha256 of the toolchain receipt), `environment_hash`;
- `installation_root`, `node_id`;
- `status` (`installed` / `stale` / `failed` / `unknown`), `probe_status`,
  `probed_at`, `health_until`, `detail`.

`probe_toolchain(root, key, confined_roots=...)` is side-effect free: it
re-validates the on-disk receipt and additionally requires every managed
executable to be readable in the confined environment (see E23). A failed or
missing receipt, or an executable outside the approved read roots, yields
`failed`. `record_installation` is idempotent per key/node and stamps
`health_until = now + health_ttl_seconds`. `installation_states(session)` reports
`installed` while the TTL holds, `stale` once it lapses, otherwise the stored
status.

`preflight_capabilities(..., installation_status=..., approved_roots=...)`
excludes `stale`/`failed` installations (→ `NEEDS_SETUP`) and rejects a
PATH-resolved host executable that is not readable under the real confinement
roots. Discovery still performs no install and returns no permission.

## E23 — confined executable verification

An executable resolved with `shutil.which` may live outside the paths the
confined child can read (e.g. a per-user NVM install under `$HOME`), so `which()`
alone is not proof. `vuzol.security.confined_paths` answers "would the confined
process read/execute this path?" against the **actual** ruleset roots:
`landlock.interpreter_read_only()` (system/interpreter paths) plus declared
extras (the preview source root and the approved managed-toolchain root).
`executable_within_roots` uses `os.path.realpath` containment. `$HOME` is never
added; a `.nvm` binary is rejected.

Both consumers use it:

- `runtime_preview` preflight passes `approved_roots` and uses the same check
  before spawning, returning `environment_setup_required` with a clear summary
  instead of a Landlock `PermissionError`.
- `probe_toolchain` requires managed executables to be readable in the confined
  environment.

## Run version pins (`capability_run_pins`)

New additive table. `pin_capability(session, run_id, spec)` records the exact
`(version, receipt_hash)` a run first resolved; `pin_matches` returns `False` if
the on-disk receipt no longer matches. Installing or downgrading a toolchain
therefore cannot silently change how an already-pinned run resolves its
executable. Historical runs have no pins and keep reading the on-disk receipt
(legacy behavior). (True "keep using the old bytes" would need versioned
installation directories — an installer-backend change, out of WP03 scope.)

## How to add a capability on the current backend

1. Add a `CapabilityDescriptor` to `builtin_descriptors()` (or a future
   catalogue) with schemas, effect class and prerequisites.
2. If it is a managed toolchain, add its source to
   `source_catalog.v1.json` (hash/size/executables) so the existing
   `OfflineCapabilityInstaller` can install it under the current source/hash
   bound approval (ADR-A03). No new installer.
3. After install, probe it with `probe_toolchain` (confined roots) and record it
   with `record_installation`; stale/failed then excludes it from preflight.
4. Do not add a permission; grant `Capability`/`allowed_capabilities` through
   the existing policy path.

## Migration note

Revision `a1c5e7b93d20` (parent `e7d2c4a91b06`), single linear head. It creates
`capability_installations` and `capability_run_pins` (+ indexes). There is no row
backfill: the tables are new and historical installs/pins must not be
fabricated. `downgrade()` drops both tables. Approvals, permissions, secrets,
budget and routing semantics are unchanged.
