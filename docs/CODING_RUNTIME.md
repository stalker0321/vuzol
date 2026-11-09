# Coding runtime

A project lead starts each task of an approved plan. The task compiles into a fixed
workflow (`src/vuzol/workflows/compiler.py`); each step is claimed by the service that
owns its boundary. A running executor alone does not make coding ready: every consumer
below must run. Never remove a privileged step or complete a queued step by hand to make
up for a missing service.

| Step | Consumer | Boundary |
| --- | --- | --- |
| interpret | intake | the compiled, retained task interpretation (completed at creation) |
| ensure_capabilities | applier | detected or approved environment; installation needs its own approval |
| prepare_context, finalize | worker | internal fenced completion |
| prepare_worktree | executor | isolated shallow repository; the source stays read-only |
| execute_code / execute_agent | executor (or a node lane) | routed CLI profile in a rootless sandbox under a lease; `execute_agent` is read-only, for report tasks |
| ensure_dependencies | executor | manifest inspection; installation needs its own approval |
| validate | executor | measured Git and system checks, configured trusted gates in the validation sandbox |
| review | worker or executor review lane | the retained result and validation; independent review by risk level |
| produce_artifacts, build_static | executor | declared artifacts and build, or an explicit skipped receipt (change tasks only) |
| approval | applier | the exact retained result; applies after the owner's approval or the goal's apply grant |
| publish_static | static publisher | configured publication after the result is applied, or skipped |

Report tasks (research, architecture, inspection) stop after review and approval; they
never edit the worktree.

`execute_code` for a Pi profile can run on a remote node (`execution.nodes[].provider_parallelism`,
see [Second node](SECOND_NODE.md)): the worktree is copied there and back, and an interrupted
run leaves the local worktree untouched. Codex profiles always run on this host, because their
OAuth state must have a single refresh owner.

## Readiness

Production readiness checks verify running processes, not handler definitions: worker,
executor, applier, static publisher and Telegram delivery. Their registry, worktree and
artifact paths must refer to the same host state. Check each process identity and filesystem
restrictions before activation, and inspect unsettled effects and pending approvals before
the first applier activation.

Projects created before provisioning rows existed get a conservative baseline on the first
valid `ensure_capabilities` lease, from the import detector. It never picks a framework, runs
repository code, replaces an accepted revision or approves installation.

## Cards

Start, pause, resume, cancel and retry enqueue a fresh project card only on an actual change;
duplicate controls are no-ops. Step start and completion refresh the existing card under the
task revision fence even when the coarse task status does not change.

## Tests

The regression composition uses an isolated test database and temporary Git repositories with
the real capability, worktree, dependency, validation, review, artifact, publication and apply
handlers, and a fixture model edit. It proves that approval waits keep the source unchanged and
that disabled publication finishes with skipped receipts. It does not certify model quality,
invent tests for an unknown project, install dependencies or publish a website.
