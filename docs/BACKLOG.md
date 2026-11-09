# Engineering backlog

Agreed follow-up work that is important but deferred. Completed work belongs in
`CHANGELOG.md`. Rewritten on 9 November 2026: entries about the pre-lead task pipeline
(work-item discussion, the per-task planner, discussion intake states) and items fixed
since were removed; they remain in Git history.

## Operations and security

- Postgres still uses the default password, and the compose services run with
  `VUZOL_ENVIRONMENT=development`. Changing it needs a coordinated deploy.
- The full suite's coverage is below the 90% gate in `pyproject.toml`.
- `codex-b` and `codex-c` point at home directories the executor cannot read. To use them in
  Vuzol, each needs its own login under `/var/lib/vuzol-provider-state/<id>`. Never copy a
  personal Codex home: two refresh owners break the login.
- Codex subscription quota (percent used, reset time) is not accounted. Codex calls are
  recorded at a flat minimum cost.
- Coding steps of Codex profiles cannot run on a remote node: their OAuth home must stay with
  one refresh owner. Pi coding can (`execution.nodes[].provider_parallelism`).

## Leads

- A pause the lead decides on its own is recorded with the owner's pause reason
  (`owner_pause`).
- Leads have no MCP tools: Pi has no MCP client. Assistant and Codex workers do.

## Product ideas (owner, not scheduled)

- Leads create repositories in the owner's GitHub account and publish there. Access (token
  scope, per-repository, private by default) must be designed with the owner first.
- A Vuzol project whose lead builds connectors that other leads request, with a catalog and a
  way to hand it API keys.
- Assistant v2: proactive, gathers information itself and suggests work.
- Monitor sources that need credentials: Gmail, Calendar, X.

## Review and repair

### Chunked independent review instead of a fixed diff-size failure

The independent reviewer accepts a complete diff up to a character bound and fails closed above
it. A large valid change must not need human recovery only because of its size. A bounded review
pipeline should:

1. run trusted mechanical gates over the complete retained result;
2. separate generated files and lockfiles without hiding them or bypassing mechanical checks;
3. partition the rest on file or coherent logical boundaries and review every partition;
4. aggregate the findings into one verdict, keeping file and line provenance;
5. bound automatic review batches, and ask for a split before involving the owner.

Acceptance: a diff above the old bound completes review automatically, findings from separate
chunks are kept, a blocking finding blocks the verdict, noisy files do not use up the context, and
the batch cap terminates deterministically.

### Progress-aware repair loops

Fingerprint normalized validation failures and reviewer findings, and do not send the worker
through an unchanged failure again unless new evidence, code or instructions can change the
outcome. Keep a high aggregate token ceiling only as an emergency guard. Bound automatic cycles
explicitly; an owner retry opens one new bounded round. Show a compact attempt history on a
blocked task card, collapsing identical outcomes.

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
