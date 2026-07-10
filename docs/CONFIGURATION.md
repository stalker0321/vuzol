# Configuration

Vuzol separates process settings, non-secret registries, and secret values.

## Format

Process settings use `VUZOL_` environment variables with `__` for nested fields. Project, provider-profile, and Telegram-topic registries use TOML. TOML was selected because Python 3.12 parses it without a runtime dependency, it supports typed tables and arrays, and application modules never need to parse it directly.

See `.env.example` and `config/registries.example.toml`.

## Startup

Set `VUZOL_REGISTRY_FILE` to enable a registry document. Before app or worker startup, Vuzol validates:

- stable IDs and cross-references;
- enabled project paths under `VUZOL_REPOSITORY_ROOT`;
- known capabilities and positive limits;
- fallback references and cycles;
- topic-to-project mappings;
- network destination policy;
- required scoped secret references.

Invalid configuration stops the process before it starts accepting work.

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

Security-sensitive changes take effect immediately. Removed or disabled projects and profiles, capability revocation, credential-reference changes, and repository, sandbox, network, or delivery-policy changes block an old snapshot until policy re-evaluates it.

## Hard limits

Typed settings define positive defaults for concurrency, retention, input and artifact sizes, provider attempts, token budgets, task cost units, and task duration. Later workflow steps enforce these values; budget modes may select lower limits but cannot bypass them.
