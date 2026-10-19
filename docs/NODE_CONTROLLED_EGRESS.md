# Node clients and controlled sandbox egress

The sandbox is attached only to its internal per-execution network. Outbound
HTTPS must traverse the exact-host allowlisted CONNECT proxy; direct Internet,
private destinations and unapproved redirect origins remain unavailable.

Controlled-egress fixes:

1. The pinned sandbox's Node 22.23.1 does not enable built-in fetch's HTTP/HTTPS
   proxy environment support by default. Runtime-owned proxy wiring now supplies
   `NODE_USE_ENV_PROXY=1`, after caller environment, only for controlled-egress
   sandboxes. Network-disabled envelopes are unchanged.
2. Node's CONNECT request is `CONNECT hostname:443 HTTP/1.1` with `Host: hostname`.
   The proxy previously required the redundant `:443` in Host. It now accepts
   exactly the matching hostname, optionally followed by `:443`. The validated
   CONNECT authority is still the sole routing target. Duplicate/mismatched Host,
   other ports, IP literals, private resolution and non-allowlisted targets remain
   rejected. No arbitrary Host normalization or new destinations are permitted.

Pi 1.0.3 also installs its own `EnvHttpProxyAgent` in bundled chunks. The Node
opt-in fixes the common execution contract; it is not proof that Pi itself
bypassed the proxy. Pi's confirmed transport failure was the rejected Host form.

[Pinned Node environment-proxy documentation](https://nodejs.org/download/release/v22.23.1/docs/api/cli.html#node_use_env_proxy1).

The proxy is a separate pinned image: updating only application source is not
sufficient for the second fix. Roll out a source-only proxy image and an explicit
digest override with a retained prior digest for rollback. Do not mutate old tags,
credential files, TLS verification, provider models, routing or budget limits.

Regression coverage:

- Sandbox argv tests enforce Node opt-in after caller environment and absence in
  network-disabled envelopes.
- CONNECT parser tests cover both legitimate Host forms and conflicting,
  duplicated, alternative-port and malformed authority variants.
- `tests/integration/execution/test_node_proxy_transport.py` runs actual Node
  fetch through `RootlessDockerRuntime` and `ProxyServiceManager` under the pinned
  sandbox/seccomp and internal-only network. It fetches public OpenCode model
  metadata without credentials/inference, rejects an unapproved fetch origin,
  verifies direct egress remains unavailable and cleans its exact resources.
- The existing native negative network matrix tests forbidden destinations,
  private/IP/metadata/redirect origins, alternate DNS and host/daemon access.

An owner-requested retry must use normal version-bound task control, preserving
all prior attempts, usage, frozen context and the downstream validation/review/
approval gates. A successful public metadata probe alone does not prove that a
real model generation succeeded; verify the actual retried step separately.

The provider-attempt cap follows that same owner-admitted budget epoch. Counting
all historical routing decisions made an explicit Retry permanently unusable
after the first epoch exhausted its attempts. Count selected decisions against
their reservation's epoch (unreserved legacy decisions are epoch zero), while
keeping globally increasing attempt IDs and all receipts. Automatic retries do
not create an epoch. Existing token/cost, daily, lifetime package and explicit
money-ceiling checks still run on every newly admitted call. Exhausting the
attempt cap is safely retryable only through the existing owner/version/unknown-
effects guards; it does not grant automatic unlimited retries.

## Account-bound credential import

Actual Pi calls then reached OpenCode and exposed HTTP 401. Both imported auth
files contained the literal expression `{env:OPENCODE_CONSOLE_TOKEN}`, not a
resolved credential. Comparing that string against the remote config was not a
valid credential check. Resolve the known Console-token reference against the
same signed-in account that supplies `/api/config`. Accept literal keys; reject
unknown references and commands, without arbitrary environment/file expansion.

Use the authenticated config's API transport, model and org headers. These
accounts specify `/inference/go/openai/v1`; a public metadata URL cannot establish
the correct credential-bound inference path. B/C were repaired atomically with
private backups and preserved uid/gid/0600. No tokens were printed or rotated.

Both operator installers share `/home/vodkolyan/vuzol-local/bin/pi_go_binding.py`;
synthetic regressions live in `/home/vodkolyan/vuzol-release-t124/test_pi_go_binding.py`.
An offline pinned-Pi mock verified correct Bearer/org headers. The actual resumed
coding task verified model responses and tools without a separate paid probe.
