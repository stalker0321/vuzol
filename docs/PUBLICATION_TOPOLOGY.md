# Publication with separate edge and worker hosts

The public DNS for hryshyn.dev and test.hryshyn.dev points to the edge
vuzol-main (178.104.227.224 / private 10.0.0.2). The current Vuzol publisher,
worktrees, static releases and loopback preview gateway are on ubuntu-4gb
(178.104.34.10 / private 10.0.0.3). They do not share a localhost or filesystem.

An edge proxy to 127.0.0.1:8091 therefore returns 502 even while the worker's
gateway and local TLS site return 200. A production file_server on the edge
also cannot see a newly approved release written only on the worker.

## Route contract

- Worker Caddy imports `deploy/caddy/vuzol-private-origin.caddy` at top level.
  It binds HTTP **only** to 10.0.0.3:8092. The firewall permits this destination
  only from 10.0.0.2 on the private network interface. No public listener or
  wildcard firewall rule is introduced. Internal HTTP does not depend on
  worker certificates whose public DNS now points to another host.
- The private listener accepts only the preview and production Host names.
  Preview traffic proxies to the loopback 8091 gateway, preserving streaming.
  Production serves only the worker's approved `current` static release.
  Unknown hosts/projects return 404.
- Edge Caddy imports `deploy/caddy/vuzol-edge-routes.caddy` at top level and
  the named preview/projects snippets inside the corresponding existing hosts.
  Preserve explicit unrelated application routes before generic project routes.
- Existing edge-local project index files keep their original file-serving
  route. Projects absent from the edge forward their original URI/Host over the
  private link. The edge's homepage and unrelated applications remain local.
  An intentional move of an existing edge-local project needs a separate
  explicit migration; local files are not silently superseded.
- DNS, public TLS termination, project builds, artifact hashes and approval
  policies remain unchanged. Routing is not permission to publish a result.

## Verification and recovery

Back up both complete Caddyfiles and hash them before installation; do not
overwrite intervening edits. Validate complete configs with each host's Caddy
binary, then reload worker first and edge second. Verify the private socket's
bind address, source-scoped firewall, preview readiness, HTML and referenced
assets through the **public** hostname. A loopback-only probe cannot certify
the external route. Compare public HTML hashes for pre-existing local projects,
the homepage and unrelated applications before/after the edge reload.

Confirm a nonexistent/unapproved production project still returns 404. Resume
the blocked publish_preview step via normal owner/version-bound Retry; its real
probe and workflow evidence must succeed. Never complete a step manually,
approve the result on the owner's behalf or copy a preview into production.

Rollback restores the exact saved Caddyfiles and reloads edge then worker;
remove only the newly added private firewall rule and imported files. Keep
operator backups private because unrelated route configuration may contain
authentication material.
