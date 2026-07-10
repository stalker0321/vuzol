# ADR-0006 — Per-Task Worktrees and Constrained Sandboxes

## Status

Accepted

## Decision

Use a separate Git worktree per coding task and execute project work in a constrained non-root sandbox by default.

## Reason

Worktrees provide clean diffs, isolation from the primary checkout, safer cancellation, and future parallel execution. A sandbox limits the impact of model or repository mistakes.

## Consequences

- the primary checkout is not edited by executors;
- worktree lifecycle is persisted;
- the Docker socket is not mounted;
- credentials and mounts are scoped;
- host-privileged operations use a separate approval-gated executor path.
