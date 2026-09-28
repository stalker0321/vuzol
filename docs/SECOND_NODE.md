# Second execution node (WP12)

One control plane, local + one remote node, slots/device locks, content-hash
artifact transfer. Explicit non-goals: no autoscaling, no consensus, no
global filesystem, no plugin loader, no network server in this package.

## 1. Protocol version

`node-protocol.v1` (`projects/nodes.py:NODE_PROTOCOL_VERSION`,
`execution/remote.py:TRANSFER_PROTOCOL_VERSION`). A node whose protocol
differs is ineligible; a transfer peer speaking another version is rejected
by configuration, not negotiated.

## 2. Onboarding

1. Operator stages the host (toolchain root, artifact root, credential
   alias — values never leave the operator).
2. `register_node(node_id, trust_class, credential_ref=alias)` inserts the
   row `online` with a fresh heartbeat. `trust_class` is `local` or
   `remote`; `credential_ref` must match the alias pattern
   (`[a-z0-9][a-z0-9_-]{0,63}`) — anything else is rejected, so a pasted
   value cannot travel as a ref.
3. The node proves toolchains via the existing probe path
   (`record_installation`); only then do capability-gated claims flow.
4. Pre-registry deployments are backfilled with the `local` node
   (`online`, provenance `legacy`) by the migration — single-node behavior
   is unchanged.

## 3. Claim boundary

`projects/node_claim.py::claim_node_step` always carries the node filter:
unknown / revoked / offline / stale-heartbeat / protocol-mismatched nodes
get no claims (fail-closed, returns `None`). Capability health narrows the
worker's capability set before delegating to `leasing.claim_step` — a step
requiring an installation that is stale/failed on that node is not eligible
there. Optional `require_trust_class` pins the trust scope explicitly, never
inferred. Step fencing (`lease_generation`) is unchanged: a stale token
completes nothing (`LeaseLost`).

## 4. Slots / device locks

`storage/slots.py` reuses the leasing pattern (one advisory lock
`SLOT_CLAIM_LOCK_KEY = 8_946_527_106`, single-row claim, generation
fencing) — not a second lock mechanism. `claim_slot` takes free slots only;
held slots return `None`. Expired-but-held slots require explicit
`reconcile_slot` (restart path) before any reuse — a new claim alone never
picks up a previous owner's slot. `release_slot` checks owner+generation;
stale holders get `LeaseLost`.

## 5. Artifact transfer

One transport: client-side pull over HTTPS (`execution/remote.py`,
`GET {base}/artifacts/{sha256}`, bounded bytes, no redirects). The receipt
path re-hashes: mismatch → `TransferHashMismatch`, oversize/refusal →
`TransferError`. `ArtifactStore.read_verified(uri, expected_hash)` does the
same check for local receipt. SSH (key management beyond the ref boundary)
and agent channels (no such protocol) were considered and rejected; there
is no server side in this package.

## 6. Revocation

`revoke_node(node_id, detail)` flips status to `revoked` (terminal for
claims; the row survives for audit). Revoked nodes get no claims; held
slots must expire and reconcile; in-flight steps stay fenced by their
lease generation. Re-onboarding is a new explicit `register_node` call.

## 7. Credential injection

Credentials travel as operator-staged aliases (`credential_ref`) resolved
outside this package (existing secret-file/secret-reference machinery).
Values are never stored in `nodes`, never logged, never placed in URLs:
transfer URLs carry only the hex digest.

## 8. Ops recovery

- Node disconnect: `mark_node_offline` stops new claims; in-flight steps
  complete or expire through the existing fencing (`find_expired_leases`,
  `LeaseLost` on stale completion).
- Restart: `find_expired_slots` + `reconcile_slot` frees only expired
  slots; live slots report `held`; unknown slots report `free`.
- Corrupt transfer: rejected at receipt, nothing persisted under a
  mismatched hash; retry is a fresh pull by digest (idempotent).
- Registry loss: `nodes`/`node_slots` rebuild from onboarding + probes;
  `downgrade` drops both tables (migration is additive-only otherwise).

## 9. Logical paths

- Registry: `projects/nodes.py` over `nodes`.
- Slots: `storage/slots.py` over `node_slots`.
- Claim: `projects/node_claim.py` → `storage/leasing.py`.
- Transfer: `execution/remote.py` → `ArtifactStore.read_verified`.
- No global filesystem: artifact bytes move only as verified transfers.
