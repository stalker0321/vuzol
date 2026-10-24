# Vuzol MCP: opt-in boundary and rollout

Current production: `vuzol:t136-execution-reliability-r5`, schema `e8b5a61fa505`.
ChatGPT has submitted a real request through this endpoint. OAuth metadata and
unauthenticated rejection are verified; submission still requires the owner's
scoped grant. MCP dependencies are bundled in the release image. The old t135
module-overlay workaround below is historical, not the current deployment.

See [execution reliability report](EXECUTION_RELIABILITY_2026_10_22.md) for
provenance, durable request notices, idempotency and live execution checks.

## Contract

- `list_projects`: only explicitly granted, enabled projects.
- `get_project_status(project_id)`: latest owned goal in the configured discussion.
- `get_result(project_id, goal_id)`: owned goal and existing evidence projection.
- `submit_request(work)`: a user-authorized message, project and UUID `request_id`;
  optional `scope`, `criteria` and existing `work_limits`. There is no discussion mode: the
  assistant's own chat is the discussion, and a submission is always for the lead.
  `ProjectApplication` calls the same `ProjectEventService.submit_event` as Telegram.

Submission returns `{status, event_id, goal_id, receipt_id, reason}` after commit.
`status=accepted` acknowledges durable input, not started or finished work; a replay returns
`status=duplicate` with the same event and receipt IDs. Retry an uncertain submission with the
SAME UUID and identical content. Changed content with that UUID is denied. Provenance
records the first authenticated client and request UUID; a renamed client does not invalidate
retries. `allow_submit` and OAuth `vuzol:submit` remain required.

New messages join the live goal, or create a draft conversation when none exists. The lead
answers, clarifies or asks the strong planner for a plan; replies and plan cards arrive in the
project topic. Execution requires the owner's plan approval. A message alone does not resume a paused goal or approve a plan. Poll status/result
later; do not retry denials in a loop. If an accepted event is moved after its original goal
finishes, its receipt still identifies the original admission; `get_project_status` shows the
latest goal.

Existing project discussion sessions remain required in the access grant. No arbitrary SQL,
shell, file reads, project creation, push or deployment approval is exposed. No DDL is needed.

## Configuration

Install optional dependencies with `uv sync --extra mcp` in the intended service
environment (not in an unreviewed production environment). Run with the same
runtime registry/settings as the existing Vuzol services:

```sh
uv run --extra mcp vuzol-mcp --config /path/to/mcp.json
```

Example configuration (replace all placeholder identities):

```json
{
  "resource_url": "https://vuzol.hryshyn.dev/mcp",
  "issuer_url": "https://YOUR-TENANT.example/",
  "jwks_url": "https://YOUR-TENANT.example/.well-known/jwks.json",
  "port": 8093,
  "clients": {"EXACT-VERIFIED-OAUTH-CLIENT-ID": "ChatGPT"},
  "subjects": {
    "EXACT-VERIFIED-OAUTH-SUB": {
      "user_id": 42,
      "projects": {"example-project": "00000000-0000-0000-0000-000000000001"},
      "allow_submit": false
    }
  }
}
```

`user_id` must be an existing allowed Vuzol user; each mapping value is an
existing active project discussion UUID, NOT a Telegram topic number. The
discussion's chat must also be allowed for submissions. Begin read-only and
explicitly enable `allow_submit` only for the isolated pilot project.

The process defaults to 127.0.0.1. Containers use explicit `--host 0.0.0.0`
with a loopback-only published host port. Port 8093 avoids the existing private
Caddy origin at 10.0.0.3:8092. Config is operator-controlled; tool arguments cannot
choose identity, grants, branch or approval state.

## Authentication and publication

External OAuth provider: authorization code + PKCE S256, discovery metadata,
RS256 JWT access tokens, HTTPS JWKS. Tokens must contain `sub`, `iss`, `aud`,
`iat`, `exp`, and a space-separated `scope`. Issuer must exactly match config;
audience must be the full resource URL above. Scope `vuzol:read` is mandatory;
submission additionally requires `vuzol:submit` AND the local allow_submit grant.
Unknown subjects fail closed. No token-supplied JWKS URL is followed.

Use a predefined OAuth client with the exact callback URL shown by ChatGPT.
Do not guess the callback URL, accept wildcards, or paste its client secret in
chat/repository. Provider must map MCP's `resource` parameter to token audience.
Auth0 provides a Resource Parameter Compatibility Profile for this purpose.
Short access-token lifetimes bound revocation delay; removing the local subject
and restarting MCP revokes its access without waiting for JWT expiration.

Before calling the integration complete: verify actual code/PKCE exchange,
scope consent, refresh, JWKS rotation, and a real ChatGPT connection. Local
tests verify signed JWT checks with a substituted JWKS lookup, not a live IdP.
Add ingress rate limiting appropriate to the chosen public edge before rollout.

After those checks, replace the closed Caddy site with:

```caddyfile
vuzol.hryshyn.dev {
    request_body {
        max_size 256KB
    }
    reverse_proxy 127.0.0.1:8093 {
        flush_interval -1
    }
}
```

Proxy the metadata path too, not just `/mcp`. Never expose the upstream port.
Do not log Authorization headers, tokens, or OAuth codes. Validate the complete
Caddy config and reload (not restart); keep a copy for rollback.

## Historical t135 rollout state, 2026-10-22

DNS `vuzol.hryshyn.dev` points to 178.104.34.10. Caddy serves TLS and proxies
to the separate `vuzol-mcp` container, published only at 127.0.0.1:8093.
Public resource metadata returns 200; unauthenticated `/mcp` returns 401 with
the resource metadata challenge. Existing site blocks are unchanged. Backup:
`/etc/caddy/Caddyfile.backup-20261022-before-mcp-proxy` restores the closed
503 placeholder. The earlier `Caddyfile.backup-20261022-before-mcp` predates
the entire MCP site block. Production Vuzol services have not been restarted.

Runtime: `/home/vodkolyan/vuzol-mcp-runtime/{compose.yaml,mcp.json,overlay/,deps/}`.
It uses the existing `vuzol:t135-inference-diagnostics` image (core code 710b98e)
plus only the new MCP/application modules and optional dependency directory.
The core image matches production DB migration d7a4f50e9404. The development
branch additionally contains migration e8b5a61fa505; do NOT deploy that whole
branch against production without the separate migration/modes rollout.
Dependency files are hard-linked copies outside `/tmp`; container restart
does not depend on the temporary test venv. Registry and repository mounts are
read-only; no Docker socket, Telegram token or inference credentials are supplied.
The MCP-only Compose project is `vuzol-mcp`, attached to `repo_default` for DB
connectivity. Updates require resupplying the existing DB DSN; the running
container stores it as an environment value like the other services. Never
print `docker inspect` environment values or rendered Compose output.

### Overlay must mount `service.py`

The overlay adapts the branch MCP/application modules onto the t135 core.
`leadership/access.py` submits with `apply_human(..., uow=...)`, and only the
branch version of `leadership/service.py` accepts that keyword. Mount both:

```yaml
- ./overlay/access.py:/app/src/vuzol/leadership/access.py:ro
- ./overlay/service.py:/app/src/vuzol/leadership/service.py:ro
```

If `service.py` is omitted the container still starts, but submission fails with
`LeadershipCommandService.apply_human() got an unexpected keyword argument 'uow'`.
The branch `service.py` differs from t135 only by the optional `uow` parameter on
`apply`/`apply_human` and reuse of the caller's UnitOfWork; it does not introduce
the branch's migration. After changing any overlay file, recreate (not just
restart) the container so the bind mount and imported module are refreshed.

Verified 2026-10-22 after recreating `vuzol-mcp`: `apply_human` accepts `uow`,
resource metadata returns 200 at both the local and public
`/.well-known/oauth-protected-resource/mcp`, unauthenticated `/mcp` returns 401
with the metadata challenge, and startup logs contain no errors. Auth0 callback
configuration remains the separate open item below.

The user-specified Auth0 subject is explicitly mapped to the existing Vuzol
owner and four active project discussions in the operator-controlled runtime
JSON. Submission requires both the local grant and token `vuzol:submit`.
The application credential file `.env.auth0` is a git-ignored staging file;
the MCP verifier does not consume its client secret. Those client credentials
belong in the ChatGPT OAuth client form, not in the resource server.

Verified on deployment: schema-head preflight, application status reads for all
four granted projects, two integration tests, Ruff and scoped strict mypy.
These status reads did not create goals or invoke providers.

Auth0 discovery advertises PKCE S256 and issuer response binding. A silent
authorization preflight with the configured client currently fails with
`Callback URL mismatch`. The next user action is to allow the exact ChatGPT
redirect URI in the Auth0 application. The stable issuer-bound callback is
`https://chatgpt.com/connector_platform_oauth_redirect`; if ChatGPT shows a
connection-specific redirect, use the exact value it displays instead.
Actual OAuth token issuance, refresh, and ChatGPT tool calls remain unverified.

Rollback: `docker stop vuzol-mcp`, restore the proxy backup above and validate /
reload Caddy. This does not require rolling back any Vuzol worker or DB schema.

## Tests

`tests/integration/leadership/test_mcp_boundary.py` exercises actual SDK HTTP
dispatch with PostgreSQL: discovery, metadata, signed token rejection,
read-only scope, forged identity arguments, host validation, durable submission,
restart/retry deduplication, payload conflicts and cross-project/goal denial.

Run only against a disposable migrated database. Integration fixtures truncate
all public tables and terminate other sessions in that database:

```sh
VUZOL_TEST_DATABASE_DSN=postgresql://USER:PASS@HOST/OWN_DISPOSABLE_DB \
  uv run --extra mcp pytest tests/integration/leadership/test_mcp_boundary.py --no-cov
```

Remaining rollout gates include the existing provider-auth refresh and disk
headroom issues; this MCP patch does not resolve those unrelated operational
debts. A successful MCP receipt alone does not prove worker execution readiness.
