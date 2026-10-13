# Capability registry, installations and local node (WP03)

## Current capability / dependency policy (2026-10-13)

Projects declare **what they need**, not how to install it or which permissions
to grant. Legacy `provisioning` fields remain readable, but do not determine
installation approval for known tools. Action/secret/external-resource boundaries
are not removed: available != authorized, installed != healthy, declared != verified.

The existing handler resolves node-scoped inventory first, rechecks the receipt
and confined executable paths, refreshes stale health without installation, and
fails closed on recorded failure, missing receipt, changed registered receipt or
root mismatch. A verified existing installation is reused across projects, even
when the installer feature is disabled. Built-in Git/Python host tools retain
their existing compatibility path; a recorded non-healthy host tool is rejected.

Missing reviewed shared toolchains use one hash/source/root/node-bound approval.
Concurrent projects (including overlapping requirement sets) share the pending
installation; consumption wakes the waiters, rejection wakes them to attention.
Only the applier installs shared toolchains. The executor remains read-only on
the shared toolchain root. An approval wait does not spend an install attempt.

After code generation, explicit manifests and Node Makefile recipes can append
new requirements to the existing immutable environment history. No permanent
stack/component is inferred from browser JS. Missing tools add an ordinary
existing privileged `ensure_capabilities` step before the dependency step resumes;
this is not an LLM repair, another planner, or a new workflow type.

Project-local dependencies use the existing bounded manifest/lockfile inspection,
controlled-egress sandbox builder and immutable per-project environments without
a new human approval. Legacy approval envelopes/decisions remain readable; an
explicit rejection is not silently overridden. Custom sources still require
explicit project-scoped registration. Secrets, prod deployment, external writes
and sensitive integrations retain their separate authorization paths.

The default-off provisioning feature flags are deployment readiness controls,
not project-level permission grants; this code change does not enable installation
in production or fabricate owner consent. Node's first shared installation still
requires the real installation approval.

Status: implemented additively. Three distinct facts stay separate
(`docs/contracts/ADR-A01.md`): **descriptor** (what the capability means), **installation** (where
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

`record_installation` is called from the existing post-install probe in
`CapabilityProvisioningHandler.execute` (after `installer.install` + `ready()`):
managed capabilities are probed and recorded with the environment hash; a failed
probe is recorded as `failed` before the step blocks. These records are also
checked/refreshed before reuse under the current policy.

`preflight_capabilities(..., installation_status=..., approved_roots=...)`
excludes `stale`/`failed` installations (→ `NEEDS_SETUP`) and rejects a
PATH-resolved host executable that is not readable under the real confinement
roots. Discovery still performs no install and returns no permission.

## E23 — confined executable verification

An executable resolved with `shutil.which` may live outside the paths the
confined child can read (e.g. a per-user NVM install under `$HOME`), so `which()`
alone is not proof. `vuzol.security.confined_paths` answers "would the confined
process read/execute this path?" against the **actual** ruleset:
`landlock.interpreter_read_only()` (system/interpreter paths) plus the preview
spec's `read_only` root (`_source_root()`). Managed toolchain paths under
`toolchain_root` are **not** part of that ruleset and are therefore rejected —
the gate and the ruleset are aligned exactly, so a path is accepted only if the
child could actually execute it. `executable_within_roots` uses
`os.path.realpath` containment; `$HOME` is never added.

Both consumers use it:

- `runtime_preview` preflight passes `approved_roots` and uses the same check
  before spawning, returning `environment_setup_required` with a clear summary
  instead of a Landlock `PermissionError`.
- `probe_toolchain` requires managed executables to be readable in the confined
  environment (the sandbox mounts `toolchain_root`, so that root is the probe's
  confined root).

## Run version pins (`capability_run_pins`)

New additive table. `pin_capability(session, run_id, spec)` records the exact
`(version, receipt_hash)` a run first resolved; `pin_matches` returns `False` if
the on-disk receipt no longer matches. The resolution paths enforce this:
`execution/codex.py:_managed_artifact_runtime` and
`execution/dependency_build.py:build` call `enforce_run_pins` before mounting a
managed runtime, so installing or downgrading a toolchain **fails closed**
(`CapabilityPinMismatch`) instead of silently changing how an already-pinned run
resolves its executable. Historical runs have no pins and keep reading the
on-disk receipt (legacy behavior). (True "keep using the old bytes" would need
versioned installation directories — an installer-backend change, out of WP03
scope.)

## How to add a capability on the current backend

1. Add a `CapabilityDescriptor` to `builtin_descriptors()` (or a future
   catalogue) with schemas, effect class and prerequisites.
2. If it is a managed toolchain, add its source to
   `source_catalog.v1.json` (hash/size/executables) so the existing
   `OfflineCapabilityInstaller` can install it under the current source/hash
   bound approval (`docs/contracts/ADR-A03.md`). No new installer.
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
