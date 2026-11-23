# Engineering backlog

Agreed follow-up work that is important but deferred. Completed work belongs in
`CHANGELOG.md`. Rewritten on 9 November 2026: entries about the pre-lead task pipeline
(work-item discussion, the per-task planner, discussion intake states) and items fixed
since were removed; they remain in Git history. On 23 November 2026 chunked independent
review (`review/partitions.py`) and progress-aware repair (`workflows/recovery_policy.py`)
were found already built and removed here.

## Operations and security

- `codex-b` and `codex-c` point at home directories the executor cannot read. To use them in
  Vuzol, each needs its own login under `/var/lib/vuzol-provider-state/<id>`. Never copy a
  personal Codex home: two refresh owners break the login.
- Codex subscription quota (percent used, reset time) is not accounted. Codex calls are
  recorded at a flat minimum cost.
- Coding steps of Codex profiles cannot run on a remote node: their OAuth home must stay with
  one refresh owner. Pi coding can (`execution.nodes[].provider_parallelism`).

- Git 2.43 (production) refuses a local `ls-remote` of a repository owned by someone else;
  Git 2.49+ no longer checks ownership in upload-pack. The worktree guard test asserts that
  only where the installed Git does. Before a host Git upgrade, decide whether the copy of a
  source repository needs its own ownership check.

## Leads

- Leads have no MCP tools: Pi has no MCP client. Assistant and Codex workers do.

## Product ideas (owner, not scheduled)

- Leads create repositories in the owner's GitHub account and publish there. Access (token
  scope, per-repository, private by default) must be designed with the owner first.
- A Vuzol project whose lead builds connectors that other leads request, with a catalog and a
  way to hand it API keys.
- Assistant v2: proactive, gathers information itself and suggests work.
- Monitor sources that need credentials: X. (Gmail and Calendar are the assistant's,
  `vuzol.cli.google_login`.)

## Review and repair

### Agent efficiency

Port selected behaviors into the harness rather than as an injected skill: batched
reconnaissance, bounded reads, one-shot dependency preflight, stopping once checks are green,
suppressing polling and repeated failed approaches. Instrument duplicate tool work, retries,
tokens and wall time so each change can be compared, not assumed useful.

### Evaluation corpus for leads and planners

About 20 realistic requests (trivial, ambiguous, multi-step, risky, cross-domain) with a reference
decomposition reviewed with the owner. Run the real lead and planner contracts on each and keep
model, prompt revision, structured output, tokens, latency and validation failures, scored
against observable criteria. Runs must be reproducible so cheaper models can be compared with
the current baseline.
