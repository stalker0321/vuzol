# Handoff: project environment contracts

Updated: 2026-08-20

## Completed in this workstream

- Added typed environment deltas to planner output, immutable plan bodies, hashes and revision
  restoration.
- Added append-only project environment revisions with approval provenance and atomic application at
  plan approval.
- Added a separate compact `Stack` section to the plan card.
- Added conservative import detection and startup reconciliation for older imported projects.
- Added deterministic capability preflight and a `Needs setup` task-card state.
- Added typed artifact expectations for web, Android, CLI, library, bot, MCP, worker and database
  components.
- Added a managed Node.js preview runtime, lifecycle registry, healthcheck and preview URL recording.
- Added a FastAPI preview gateway for static and runtime projects, including streaming proxy support.
- Changed the test Caddy route to the gateway and made publisher shutdown clean up gateway/client and
  child processes.
- Added focused domain, persistence, Telegram, gateway, capability and artifact tests.
- Restored the repository-wide 90% coverage gate with focused fail-closed environment, preview,
  projection and work-package tests. The complete suite now reports `2055 passed, 7 skipped` and
  90.001336% branch coverage; Ruff, format-check and strict mypy are green. The new environment
  migration has also passed a direct downgrade/upgrade round trip.
- Added `coding.v2` trusted artifact production for every non-web component contract. Approved,
  bounded commands run without a shell in the rootless offline validation sandbox; APK/package files
  and protocol/transcript/migration reports are stored privately with hash-bound evidence. The step
  fails closed on missing setup, command/file failures, tracked Git mutation, or malformed approval
  evidence, while `coding.v1` remains available for in-flight compatibility.
- Added artifact types to the result approval card and bound artifact IDs, types, sizes and content
  hashes into the immutable approval envelope.
- Added `coding.v3` with a separate capability-installation approval before project execution.
  The default-off, manifest-driven installer accepts any hash-pinned, operator-staged archive
  toolchain without a per-stack Python adapter. It records a validated immutable receipt and exposes
  only declared commands/environment paths read-only during artifact production, without shell,
  package-manager or network access. Plan approval alone never grants installation permission.
- Added the reviewed online source catalogue for pinned Go, Node, Gradle and Java archives. The
  applier downloads only after approval, verifies exact size and SHA-256, constrains redirects, and
  caches immutable content-addressed bytes.
- Added `coding.v4` with a separate dependency approval after code generation. Python
  `pyproject.toml` and Node `package.json` dependencies resolve only through catalogued HTTPS
  registries in the rootless controlled-proxy sandbox; npm lifecycle scripts and source builds are
  disabled. The resulting project/hash-specific environment is frozen and mounted read-only into
  validation, artifact production and subsequent agent sessions.
- Added user-originated project source trust with `/source add`. Exact Git commits are supported for
  Python/Node and SHA-256-pinned HTTPS artifacts for Python. Sources are scoped to one project,
  ecosystem and package, displayed in the installation approval, and revocable by UUID.

The pre-existing untracked root-owned `package-lock.json` belongs to the user and must remain
untracked and untouched.

## Still required after this revision

1. Add a friendlier multi-step source registration card if the explicit `/source` command proves too
   cumbersome. Do not weaken the exact project/package/pin binding.
2. Replace the free-port probe with a stronger reservation or supervised socket handoff if preview
   concurrency becomes high.
3. Decide whether to add wildcard `*.test.hryshyn.dev`; it removes the absolute-path limitation but
   requires a deliberate DNS/TLS change.
4. Define production delivery for server applications. Do not automatically trust or execute an
   imported `deploy.sh`; model it as a reviewed project-specific adapter.
5. Add persistent preview-process metadata or reconciliation if previews must survive publisher
   restarts. Current processes intentionally live only for the service lifetime.
6. Extend result-card delivery so private stored binary artifacts can be attached or downloaded via
   a short-lived authenticated link.

## Deployment and dogfood checks

Before deployment, run the complete test suite and migration upgrade/downgrade checks. Then deploy
the application, validate/reload Caddy, and verify:

- `GET https://test.hryshyn.dev/health/ready` returns `{"status":"ok"}`;
- the imported `three-body-problem` has a detected Node.js web-service environment revision;
- a newly approved plan shows its Stack delta and creates exactly one environment revision;
- a Falling Worlds task publishes a working `/three-body-problem/` preview, including its streaming
  endpoint;
- stopping/restarting `vuzol-static-publisher-worker.service` leaves no orphan preview process;
- an unsupported capability shows `Needs setup` and remains retryable after configuration.
