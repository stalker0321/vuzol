# Retired second-node substrate

WP12 added a registry, claim boundary, slots and an HTTPS artifact pull for one
remote execution node. Nothing in production ever called it, and the design
(pull-only transfer, no remote executor, no ssh) did not match the machines the
owner actually runs, so the cleanup removed it: `projects/nodes.py`,
`projects/node_claim.py`, `execution/remote.py` and `storage/slots.py` with the
`node_slots` table.

What stays: the `nodes` table with its `local` row. Capability installations
reference it through `node_id`, so toolchain probes remain per node.

The forwarded Docker runtime below follows the actual host setup; git history
keeps the retired substrate.

## Forwarded Docker nodes

`execution.nodes` now points at rootless Docker Unix sockets forwarded to this host.
Set a node's `provider_parallelism` above its default of `0` to add coding lanes;
`model_call_parallelism` separately controls model calls. Both share profile limits.
Coding lanes claim only `execute_code` with a resolved pi profile while the socket exists.
Codex OAuth state has one refresh owner and stays local, as do other writable provider states.
Read-only dependency caches stage into volumes; only the task worktree may be writable.
After normal exit (including nonzero), copy-back extracts beside the local worktree,
preserves nested metadata mounts, and swaps it in; interrupted transfers leave it untouched.
`max_copy_back_bytes` caps each tar stream (default 2 GiB); copy-back operations time out after 600 s.
