# Retired second-node substrate

WP12 added a registry, claim boundary, slots and an HTTPS artifact pull for one
remote execution node. Nothing in production ever called it, and the design
(pull-only transfer, no remote executor, no ssh) did not match the machines the
owner actually runs, so the cleanup removed it: `projects/nodes.py`,
`projects/node_claim.py`, `execution/remote.py` and `storage/slots.py` with the
`node_slots` table.

What stays: the `nodes` table with its `local` row. Capability installations
reference it through `node_id`, so toolchain probes remain per node.

Running work on other servers is still planned. It will be designed from the
real setup (ssh access, separate execution and storage roles) rather than
revived from this code; git history keeps the old version.
