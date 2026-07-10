# Step 09 — Validation, Review, and Approvals

## Goal

Verify task outcomes with deterministic checks, use independent model review only when justified, and require explicit action-specific approval for sensitive operations.

## Deliverable

Validation profiles, risk-based review policy, runtime risk escalation, single-use approvals, and Telegram decision flows.

## Applicable architecture decisions

- ADR-0007 — Transactional Delivery and Fenced Leases.
- ADR-0008 — Treat Repository Content and Model Output as Untrusted.

## Validation profiles

Each project may configure commands for:

- tests;
- lint;
- type check;
- build;
- smoke tests;
- custom checks.

System-level checks include:

- command exit status;
- `git diff --check`;
- changed-file allowlist;
- prohibited-path detection;
- artifact existence;
- output schema validation;
- clean primary checkout;
- no unexpected secret files.

Validation results are persisted as structured records and artifacts.

## Risk levels

### Low

Examples:

- small project-local code or documentation change;
- no secrets, network side effect, deployment, or config migration.

Required:

- relevant deterministic checks;
- diff summary.

No LLM reviewer by default.

### Medium

Examples:

- multiple files;
- behavior change;
- dependency update;
- uncertain bug fix.

Required:

- tests or targeted checks;
- changed-file and diff inspection;
- optional fresh-context self-review or lightweight reviewer.

### High

Examples:

- authentication;
- database migration;
- CI or deployment configuration;
- security-sensitive code;
- broad architectural change.

Required:

- deterministic checks;
- independent reviewer with requirements, diff, and validator output;
- explicit review verdict and unresolved concerns;
- user approval when applying or deploying.

### Privileged or destructive

Examples:

- host administration;
- firewall, SSH, systemd, production secrets;
- force push or destructive remote action.

Required:

- precise action plan;
- rollback or compensation plan;
- independent review;
- action-specific user approval;
- execution in a separate privileged path.

## Runtime risk escalation

Re-evaluate risk when:

- executor requests a command;
- changed files become known;
- secrets are requested;
- network or deployment is requested;
- tests reveal migration or production impact;
- the diff exceeds expected scope.

Runtime policy may escalate risk and insert approval or review steps.

## Reviewer contract

The reviewer receives:

- original request;
- acceptance criteria;
- final diff;
- changed-file list;
- validation results;
- relevant project policy.

The reviewer does not receive:

- executor hidden reasoning;
- unrestricted repository history;
- production credentials;
- authority to apply changes.

Reviewer output is structured:

- pass;
- pass with warnings;
- changes required;
- blocked;
- findings with severity and file references.

## Approvals

An approval request includes:

- task and step ID;
- exact proposed action;
- target host, repository, branch, or service;
- normalized command or operation;
- reason;
- expected effect;
- rollback or compensation;
- expiry;
- one-time token.

The immutable action envelope additionally contains or hashes:

- operation type and exact executable or typed operation version;
- normalized arguments, working directory, and non-secret environment references;
- resolved target identity and expected current version or commit;
- diff, patch, migration, or configuration hash when applicable;
- sandbox, network, credential-scope, and policy revisions;
- validation and review evidence revisions on which the decision depends.

Approval is not reusable. A changed command requires a new approval.

Any material envelope change, target drift, expiry, policy escalation, or newer superseding approval invalidates the decision. Approval expiry uses database time. Consuming the approval and transitioning the step to executable occurs atomically with compare-and-set state validation. The privileged executor recalculates and compares the envelope hash immediately before execution.

Natural-language messages and semantic-interpreter output never consume privileged or destructive approvals. Only an authenticated explicit callback or equally explicit operator command tied to the persisted approval ID may do so.

Callback processing records the decision but does not perform the action directly.

## Apply semantics

For the MVP, completing a coding task may mean:

- changes remain in task worktree;
- patch or diff is delivered;
- optional local merge/apply is a separate explicit step.

Do not automatically push or deploy unless separately configured and approved.

## Tests

Required:

- low-risk task skips LLM review;
- high-risk task requires independent review;
- changed-file scope can escalate risk;
- validator failure prevents completion;
- reviewer receives no secret values;
- approval is tied to one step and expires;
- duplicate approval callback is harmless;
- altered command invalidates old approval;
- changed target version, diff, environment reference, sandbox, or policy invalidates old approval;
- approval consumption races result in at most one executable transition;
- target drift between approval and execution blocks the operation;
- natural-language text cannot approve a privileged action;
- rejected approval prevents execution;
- Telegram outage preserves pending approval state;
- diff and validation artifacts are available to the user.

## Forbidden implementations

- asking a reviewer whether tests passed instead of checking exit codes;
- letting executor approve its own action;
- one blanket “server access approved” permission;
- reusable approval tokens;
- approvals bound only to a human-readable command string;
- approval expiry based on client or worker time;
- auto-push or auto-deploy by default;
- review of low-risk trivial changes on every task;
- passing hidden chain-of-thought between executor and reviewer;
- treating warnings as pass without policy.

## Acceptance criteria

- validation is deterministic and project-configurable;
- review policy is risk-based;
- runtime risk can escalate;
- privileged action cannot run without precise approval;
- approvals are persisted, expiring, single-use, and idempotent;
- failed validation cannot be reported as completed.

## Completion report

Report:

- validation profile format;
- risk and escalation rules;
- reviewer schema and context;
- approval UX and token design;
- apply or merge behavior;
- security and idempotency tests.
