# ADR-0010 — Confine Runtime Previews to a Per-Run Materialized Copy

## Status

Accepted

## Decision

The managed runtime preview never executes project code inside the retained
task worktree. Before publication, the approved `result_commit` is exported
(`git archive`) into a disposable per-run runtime directory under the preview
site root, with bounded extraction (byte and file caps) and traversal-safe
unpacking.

The preview process is spawned through a standalone confinement wrapper that
applies a Linux Landlock domain before exec: the per-run runtime directory is
the only writable path, an explicit read-only allowlist covers interpreter and
system paths, and everything else is denied. The retained worktree therefore
stays read-only for previewed code by construction, not only by mount flags.

Confinement fails closed. If Landlock is unavailable, if the domain cannot be
applied, or if the wrapper exits before exec, the `publish_preview` step
blocks with a dedicated category instead of degrading to an unconstrained
spawn.

## Reason

Project code is untrusted input (ADR-0008). Running it from the retained
worktree coupled preview availability to a directory the publisher user could
not write to (blocking legitimate services that create local state) while the
same directory feeds validation and approval evidence. A buggy or malicious
service must not be able to mutate the retained result, read sibling preview
state, or write published site content.

Materializing the exact approved commit also makes the published preview match
the `source_commit` recorded in the approval evidence.

## Consequences

- preview processes get read-write access only to their own per-run runtime
  directory (`app/`, `home/`, `tmp/`, `preview.log`);
- retained worktrees and repository mirrors are never written to by previewed
  code;
- archive export is bounded by `preview_export_max_bytes` and
  `preview_export_max_files`; exceeding a bound blocks the step;
- replacement previews terminate the previous process and remove its runtime
  directory; publisher startup sweeps orphaned runtime state;
- kernels without Landlock cannot serve runtime previews (fail closed);
- network access of previewed processes is not restricted by this domain; the
  preview listens on loopback behind the gateway.
