# Coding runtime coverage

The coding.v4 workflow needs all four consumer lanes below. A running chat
executor alone does not make coding ready. Do not remove privileged steps or
manually complete a queued step to compensate for an absent service.

| Stage | Consumer | Boundary |
| --- | --- | --- |
| interpret | intake/dispatcher | compiled, retained interpretation |
| ensure_capabilities | vuzol-applier | detected/approved environment; separate installation approval |
| prepare_context, finalize | common worker | internal fenced completion |
| prepare_worktree | executor | isolated shallow repository; source read-only |
| execute_code | executor | routed CLI profile; rootless sandbox and lease |
| ensure_dependencies | executor | manifest inspection; separate installation approval |
| validate | executor | measured Git/system checks and configured trusted sandbox gates |
| review | common worker | retained result + validation; risk-based independent review |
| produce_artifacts, build_static | executor | declared artifacts/build, or explicit skipped receipt |
| publish_preview | publisher | supported declared preview, or explicit skipped receipt |
| acceptance | applier (also registered by common worker) | package evidence/promotion gate; standalone pass-through |
| approval | applier | exact retained result, separate user approval before apply |
| publish_static | publisher | configured publication after approved application, or skipped |

Production readiness checks must verify actual processes, not just handler
definitions: common worker, executor, applier, publisher and Telegram delivery.
Their registry, worktree and artifact paths must refer to the same host state.
Check each process identity and filesystem restrictions before activation.
Inspect unsettled effects/pending approvals before first applier activation;
inspect the publisher's runtime cleanup targets and gateway listener too.

Registry-created legacy projects can lack ProjectProvisioning rows. The
capability handler now detects their conservative baseline on its first valid
lease using the existing import detector. It does not choose a framework,
execute repository code, replace an accepted revision, or approve installation.

Start, pause, resume, cancel and retry must enqueue a fresh project card on an
actual change. Duplicate controls remain no-ops. Stage start/completion also
refreshes the existing card using the Task revision fence, even if the coarse
Task status stays unchanged. Before Start the card says "waiting for start",
not the final future step.

The regression composition uses an isolated TEST PostgreSQL DB and temporary
Git source/result repositories, real capability/worktree/dependency/validation/
review/artifact/publication/acceptance/apply handlers and a fixture model edit.
It proves that approval waits prevent source changes and that disabled deployment
finishes with skipped receipts. It does not certify live model quality, invent
tests for an unknown project, install dependencies, or enable a public website.
