# Step 08 — Git Worktrees and Execution Sandbox

## Goal

Execute coding tasks in isolated per-task worktrees with constrained process and filesystem access, supervised timeouts, and complete artifact collection.

## Deliverable

An execution backend that prepares, runs, observes, and cleans coding environments without allowing default host-wide modification.

## Applicable architecture decisions

- ADR-0006 — Per-Task Worktrees and Constrained Sandboxes.
- ADR-0007 — Transactional Delivery and Fenced Leases.
- ADR-0008 — Treat Repository Content and Model Output as Untrusted.

## Worktree lifecycle

For each coding run:

1. validate project and clean base repository assumptions;
2. create a task branch;
3. create a per-task worktree under the configured worktree root;
4. record branch, commit, path, and ownership in PostgreSQL;
5. mount only that worktree and task artifact directory into the sandbox;
6. execute through the selected adapter;
7. collect diff, logs, and status;
8. retain or clean according to workflow outcome and retention policy.

Do not modify the primary checkout directly.

Record the source remote, immutable base commit, task branch, and expected delivery mode. Fetching or updating the base is a separate network operation. A dirty primary checkout does not become task input implicitly.

## Sandbox profile

Default project sandbox:

- non-root user;
- read/write task worktree;
- read/write task artifact directory;
- read-only runtime support files only when necessary;
- no Docker socket;
- no privileged mode;
- resource limits;
- process-count limit;
- timeout;
- working directory fixed to the worktree;
- network disabled by default or controlled per project;
- no unrelated repository mounts;
- only profile-specific credentials required for the task.

Repository contents and build tooling are untrusted. Enforce containment after resolving symlinks, prevent access to host metadata endpoints and unrelated Unix sockets, and do not follow artifact or worktree paths outside their configured roots. Mounts and temporary secret files must not be reachable through user-created links.

Network policy is deny-by-default. When enabled, it declares purpose, destinations, protocols, and whether credentials are present. Provider API egress does not automatically grant general project network access.

## Process supervision

Record:

- command;
- normalized arguments;
- working directory;
- start and end time;
- exit code or signal;
- stdout and stderr artifact references;
- timeout and termination sequence;
- resource-limit failure;
- lease and task IDs.

Avoid `shell=True` for system-controlled commands. Model-proposed commands are untrusted and policy checking is defense in depth; host protection must remain effective even when arbitrary shell syntax cannot be classified correctly. Sensitive operations use typed operations rather than free-form shell.

## Command policy

Classify commands as:

- read-only;
- project-local write;
- network access;
- destructive project-local;
- host privileged;
- prohibited.

The sandbox executor may run only commands allowed by the project and workflow.

Host-privileged commands are not part of this backend and belong to a separate later executor path with approval.

Package installation, Git hooks, build scripts, test plugins, archive extraction, and commands that fetch or execute remote content are explicit risk signals. Git hooks are disabled for system-controlled Git operations unless a reviewed project policy requires them.

## Credential injection

Inject credentials narrowly:

- provider credentials only to the provider process;
- project secrets only when explicitly approved and required;
- no secret values in persisted command strings;
- redact known secrets from logs;
- destroy temporary secret mounts after use.

## Artifacts

Collect at least:

- final Git diff;
- changed-file list;
- current commit and branch;
- command logs;
- model result;
- generated files requested by the task;
- validation inputs for Step 09.

Artifacts use content hashes and retention metadata.

## Cleanup

Safe cleanup conditions:

- task completed and artifacts persisted;
- no running process remains;
- worktree Git state recorded;
- retention deadline reached;
- branch handling follows policy.

Failed or blocked worktrees are retained longer for inspection.

## Git result lifecycle

A coding run ends in one explicit delivery state:

- `worktree_retained` — validated changes remain available for inspection;
- `patch_delivered` — a hash-addressed patch and base commit were delivered;
- `applied` — changes were applied to a configured local target;
- `merged` — a recorded commit was merged into the configured branch;
- `pushed` — the exact commit reached the configured remote ref.

Apply, merge, and push are distinct persisted steps with separate policy and approval. Before each, revalidate target repository identity, base or expected head, clean-state assumptions, diff hash, branch protection policy, and current risk. Base drift or merge conflict blocks for resolution; it does not silently rebase, force push, or report completion. Repeating a delivery step uses an idempotency check against the resulting commit or remote ref.

## Tests

Required:

- separate tasks get separate worktrees;
- primary checkout remains unchanged;
- sandbox runs as non-root;
- Docker socket is absent;
- unrelated repositories are inaccessible;
- resource limit terminates a runaway process;
- timeout records unknown-effect status when appropriate;
- command policy blocks prohibited command;
- symlink and path traversal cannot escape worktree or artifact roots;
- metadata endpoints and unrelated Unix sockets are inaccessible;
- provider-only egress cannot reach arbitrary Internet destinations;
- malicious repository instructions cannot grant capabilities or expose unrelated secrets;
- credentials do not appear in logs;
- diff and logs become artifacts;
- cleanup refuses to delete an active or unrecorded worktree;
- concurrent light tasks do not bypass heavy-slot limit;
- base drift and merge conflict block delivery without changing the target branch;
- repeated apply or push recognizes the already-produced commit or ref;

## Forbidden implementations

- direct editing of the primary repository checkout;
- mounting `/` or the entire home directory;
- mounting the Docker socket;
- running default tasks as root;
- giving every sandbox all credentials;
- executing arbitrary model shell strings without policy checks;
- relying on command parsing or model compliance as the sandbox boundary;
- enabling unrestricted egress because one provider endpoint is required;
- following worktree or artifact symlinks outside configured roots;
- deleting failed worktrees immediately;
- assuming process termination means no side effect occurred.

## Acceptance criteria

- a coding executor can modify only its task worktree;
- the host and unrelated projects remain protected by default;
- process lifecycle and outputs are fully recorded;
- timeout and cancellation behavior is explicit;
- artifacts are persisted before cleanup;
- the final Git delivery state and immutable commit or patch identity are unambiguous;
- one-heavy-task limit is enforced on the current VPS.

## Completion report

Report:

- worktree naming and branch policy;
- sandbox mounts and resource limits;
- command policy implementation;
- credential injection;
- artifact layout;
- timeout and termination behavior;
- security tests performed.
