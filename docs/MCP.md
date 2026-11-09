# Vuzol MCP server: an opt-in boundary

The `mcp` container exposes Vuzol to an external MCP client such as ChatGPT. It
runs from the same release image as the other services. OAuth metadata and
unauthenticated rejection are part of the contract; submitting work still
requires the owner's scoped grant.

This is the server side. Remote MCP servers that Vuzol's own agents may call are
configured per role: `assistant.mcp_servers`, `execution.model_call_mcp_servers`
and `execution.worker_mcp_servers` (see [Configuration](CONFIGURATION.md)).

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
  "resource_url": "https://mcp.example.com/mcp",
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

The public edge then proxies the endpoint, for example:

```caddyfile
mcp.example.com {
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
