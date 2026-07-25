# Configuration

Vuzol separates process settings, non-secret registries, and secret values.

## Format

Process settings use `VUZOL_` environment variables with `__` for nested fields. Static project,
provider-profile, and Telegram-topic registries use TOML. TOML was selected because Python 3.12
parses it without a runtime dependency, it supports typed tables and arrays, and application modules
never need to parse it directly.

See `.env.example` and `config/registries.example.toml`.

## Startup

Set `VUZOL_REGISTRY_FILE` to enable a registry document. Before app or worker startup, Vuzol validates:

- stable IDs and cross-references;
- enabled project paths under `VUZOL_REPOSITORY_ROOT`;
- known capabilities and positive limits;
- fallback references and cycles;
- provider roles, cost classes, fallback compatibility, and API/CLI field separation;
- unique CLI runtime identities and non-overlapping absolute state directories;
- topic-to-project mappings;
- network destination policy;
- required scoped secret references.

Invalid configuration stops the process before it starts accepting work.

`VUZOL_REGISTRY_OVERLAY_FILE` optionally points to the JSON registry fragment owned by the bounded
project provisioner. Static configuration is loaded first and the overlay is appended before the
same cross-registry validation and revision calculation. The provisioner writes the overlay with an
atomic replace only after the new repository and Telegram thread exist. Duplicate IDs, path escapes,
or an invalid inherited project policy prevent activation.

## Registry interfaces

- `ProjectRegistry.get(project_id)` returns normalized immutable project configuration;
- `ProfileRegistry.find_candidates(required_capabilities)` returns enabled compatible profiles;
- `TopicRegistry.resolve(chat_id, message_thread_id)` resolves stable Telegram scope;
- `ScopedSecretResolver.get(reference, consumer_scope)` resolves a secret only for its declared consumer.

Unknown lookups raise `RegistryError`; unavailable or unauthorized secrets raise `SecretResolutionError` without including the secret value.

## Secrets

Registry files contain references such as `env:OPENAI_API_KEY` or `file:openai_api_key`, never values. File references are constrained to `VUZOL_SECRET_FILE_ROOT`. Each provider credential is scoped to `profile:<profile-id>`; system database and Telegram references use their own scopes.

Secret values are validated for presence but are not stored in configuration objects, revisions, string representations, or logs.

## Revisions and reloads

Every validated registry document has a deterministic SHA-256 revision over normalized non-secret content. A run snapshot can retain its project and profile revisions so ordinary display or validation-command changes do not mutate in-progress work.

Security-sensitive changes take effect immediately. Removed or disabled projects and profiles,
capability or role revocation, credential-reference, runtime-identity, state-directory, accounting,
repository, sandbox, network, or delivery-policy changes block an old snapshot until policy
re-evaluates it.

## Hard limits

Typed settings define positive defaults for concurrency, retention, input and artifact sizes,
provider attempts and fallback depth, provider-call/step/task token budgets, step/task/daily cost and
quota units, and task duration. Provider routing reserves these limits atomically before each call.
Known usage reconciles the reservation; unknown usage retains a conservative charge. Budget modes
affect preference but cannot bypass hard limits or security policy.

See [Provider routing](PROVIDERS.md) for profile fields, deterministic precedence, health, quota,
fallback, and Codex isolation rules.

## Retention maintenance

`VUZOL_RETENTION__SWEEP_BATCH_SIZE` bounds the number of cleanup actions in one sweep (default 50);
`VUZOL_RETENTION__SWEEP_LOCK_TIMEOUT_SECONDS` bounds lock acquisition (default 5 seconds). Failed
or blocked worktree retention cannot be shorter than completed-worktree retention.

`vuzol-retention` is dry-run by default. Filesystem and database mutation requires the explicit
`--apply` flag. The checked-in `vuzol-retention.service` and `.timer` files are deployment templates,
not installed or enabled production units; enabling scheduled apply requires a separate operator
decision after an isolated dry-run/apply drill.

## Backup capture foundation

`VUZOL_BACKUP__ENABLED` remains fail-closed and cannot be set to true. Manual capture has a separate
gate, `VUZOL_BACKUP__CAPTURE_CLI_PERMITTED`, which defaults to false. `vuzol-backup capture`
defaults to dry-run and requires both that configuration gate and `--apply` before it can write a
local package.

Optional staging and drill roots must be absolute. Retention counts and RPO/RTO targets are bounded,
and drill database names require the configured suffix (default `_restore`). Capture re-runs the
production-root isolation guards, streams a PostgreSQL custom dump directly into chunked
AES-256-GCM ciphertext, and publishes an explicitly partial manifest. A KEK reference uses only
`env:` or a file beneath the configured secret root.

This B2 slice is manual and local only: it installs no timer, contacts no off-host destination, and
provides no restore operation. A local encrypted package is therefore not evidence of durable
backup or recoverability.

## Workflow runtime

`VUZOL_WORKFLOW__*` settings bound polling, lease and heartbeat timing, retry backoff, recovery batch
size, claim candidates, and graceful-shutdown deadline. Defaults are a 60-second lease, 15-second
heartbeat, 30-second shutdown deadline, 2-120-second retry range, and 15-second recovery interval.
Validation requires heartbeat to remain below one third of the lease.

Control, light, heavy, and privileged concurrency use `VUZOL_CONCURRENCY__*`. Step claims enforce
both the queue-class limit and any assigned provider-profile concurrency limit transactionally.
`VUZOL_INTERPRETATION__AUTOMATIC_EXECUTION_ENABLED=false` materializes interpreted workflows but
leaves them waiting for an authenticated `start` control.

Optional disk low-watermark for **new heavy** work uses `VUZOL_DISK_PRESSURE__MIN_FREE_BYTES`
(default `0` = disabled). When positive, free space is measured with `statvfs` on
`VUZOL_DISK_PRESSURE__PATHS` if set, otherwise on `worktree_root` and `artifact_root`. HEAVY
claims are deferred without leasing; control/light work, recovery, and retention are not gated.
Probe failures fail closed for new heavy work only. Residual race after claim is re-checked before
worktree preparation and requeued as retryable `disk_pressure` (attempt refunded).
