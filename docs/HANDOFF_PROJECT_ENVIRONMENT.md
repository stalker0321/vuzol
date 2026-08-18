# Handoff: project environment contracts

Updated: 2026-08-18

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

The pre-existing untracked root-owned `package-lock.json` belongs to the user and must remain
untracked and untouched.

## Still required after this revision

1. Implement trusted artifact producers for Android APK, CLI transcript, library package, bot/MCP
   protocol reports, worker reports and database migration reports. The type mapping exists, but does
   not pretend those artifacts were produced.
2. Add host adapters beyond Node.js. Python/Android are detected by preflight but runtime/build
   provisioning must remain `Needs setup` until an explicit adapter is implemented.
3. Replace the free-port probe with a stronger reservation or supervised socket handoff if preview
   concurrency becomes high.
4. Decide whether to add wildcard `*.test.hryshyn.dev`; it removes the absolute-path limitation but
   requires a deliberate DNS/TLS change.
5. Define production delivery for server applications. Do not automatically trust or execute an
   imported `deploy.sh`; model it as a reviewed project-specific adapter.
6. Add persistent preview-process metadata or reconciliation if previews must survive publisher
   restarts. Current processes intentionally live only for the service lifetime.
7. Extend result-card delivery so private stored binary artifacts can be attached or downloaded via
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
