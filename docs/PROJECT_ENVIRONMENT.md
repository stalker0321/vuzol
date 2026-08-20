# Project environment contracts

Vuzol does not permanently infer a project's stack from its first message. The technical shape is
allowed to emerge during discussion. A generated plan contains two independently readable parts:
the work items and an environment delta. Approving the plan atomically approves both.

## Data model

`project_environment_revisions` is an append-only history per project. A revision contains a
versioned JSON contract, its canonical SHA-256 hash, provenance, its parent, and—when applicable—the
approved plan revision and Telegram user. Git and the current repository remain the source of truth;
the contract declares how Vuzol may validate and deliver it.

The v1 contract has:

- `components`: static sites, HTTP services, Android apps, CLIs, libraries, bots, MCP servers,
  workers, databases, or bounded custom components;
- `capabilities`: named host or external requirements classified as `automatic`,
  `approval_required`, or `external_setup`.

An environment delta can add/update components, remove components, and add requirements. Replanning
can therefore replace Django with Flask or remove a preview component without rewriting history.

Every provisioned project receives a conservative detected baseline (which may be an empty
contract). Detection reads markers only; it does not install packages or execute repository code.
The project provisioner reconciles older completed projects which predate environment revisions.

## Capability preflight

Host capabilities are checked deterministically. Known automatic capabilities are mapped to trusted
host adapters. Unknown automatic capabilities fail closed as `Needs setup`; privileged or external
requirements are never silently installed. A blocked item remains resumable after setup instead of
being reported as a model failure.

Current trusted executable checks cover Node.js, Python, Android SDK and Git. This registry is
intentionally small and should grow only together with a bounded adapter and tests.

## Separate toolchain installation approval

`coding.v3` checks the approved environment before repository work begins. Approving a plan records
the chosen stack, but does not authorize a host/toolchain mutation. If a declared capability is
already present, execution continues without another prompt. If it is absent and a trusted offline
bundle is available, Vuzol creates a second immutable approval containing the capability key, exact
bundle SHA-256, byte size, environment revision/hash, and managed installation root. Rejecting this
approval cancels the task without installing anything.

Capability provisioning is default-off. Installation is manifest-driven rather than hard-coded per
stack. A root-trusted `<capability-key>.json` manifest and its uncompressed tar archive are staged
under the configured read-only bundle root. The v2 manifest declares the exact version, SHA-256,
bounded command-to-executable mappings, and optional sandbox environment roots. Adding Rust, Go,
Flutter, Terraform, or another archive-based toolchain therefore does not require a Vuzol code
release. An optional deployment allowlist can further restrict staged keys; an empty allowlist still
requires a trusted manifest and does not permit model-supplied installers.

Absolute/traversing paths, links, special files, writable manifests, oversized archives, duplicate
commands/environment variables, hash changes, environment changes, and partial target directories
all fail closed. Vuzol never invokes `apt`, a shell, or an online installer, and never accepts
third-party licences on the user's behalf.

After approval, the applier extracts into a same-filesystem temporary directory, verifies every
declared executable and environment path, writes an immutable installation receipt, and atomically
renames it under `/var/lib/vuzol/toolchains/<capability-key>`. Preflight and sandbox construction use
that receipt rather than a Python registry. Artifact commands receive only the toolchains declared
by the current project as a read-only `/toolchains` mount; their commands, `PATH` entries, and
environment roots are derived from the receipt. Normal validation gates do not receive this mount.

Example manifest:

```json
{
  "schema_version": "capability-bundle.v2",
  "capability_key": "rust-toolchain",
  "version": "1.89.0",
  "archive": "rust-toolchain-1.89.0.tar",
  "sha256": "<64 lowercase hex characters>",
  "executables": {
    "cargo": "bin/cargo",
    "rustc": "bin/rustc"
  },
  "environment": {
    "CARGO_HOME": "cargo-home"
  }
}
```

Enabling the adapter requires
`VUZOL_CAPABILITY_PROVISIONING__ENABLED=true`. If provisioning is disabled or the reviewed bundle is
absent, the task remains `Needs setup`; plan approval is never silently promoted into installation
permission. Online discovery/download remains a separate trust boundary: v2 installs only staged,
hash-pinned bytes.

The staging procedure is deliberately administrative: assemble the archive outside Vuzol, review
its provenance and licence, compute its SHA-256, write the matching v2 manifest, and place both as
non-writable regular files in the bundle root. Test `inspect_bundle(<key>)` before enabling a real
task. Replacing either file creates different approval evidence; an already approved request cannot
silently install the replacement.

## Typed result artifacts

Every component kind maps to an acceptance artifact contract:

| Component | Expected result evidence |
| --- | --- |
| static site / HTTP service | reachable preview and HTTP check |
| Android app | APK and install/smoke validation |
| CLI | command transcript and exit status |
| library | package archive and consumer smoke test |
| bot | protocol test report |
| MCP server | initialize/tool-list conformance report |
| worker | bounded job report |
| database | migration/schema report |

The current `coding.v2` workflow adds a trusted artifact-production step after review. For every
non-web component it runs the approved argv-style acceptance command in the pinned, rootless,
network-disabled validation sandbox. Executables are allowlisted, arguments and artifact patterns
are bounded, Git metadata is read-only, and tracked source must still match the retained result after
the command. Android and library artifacts are copied into private artifact storage from declared
patterns; CLI, bot, MCP, worker, database, and custom components store bounded JSON reports with
exit status, output hashes, image identity, and execution duration. Each result also has a separate
evidence artifact.

Missing commands, failed commands, missing declared files, unavailable capabilities, changed source,
or malformed evidence block the workflow. The approval envelope binds stored artifact IDs and hashes
to the retained commit, and the result card names the verified artifact types. Binary attachment or
short-lived authenticated download delivery is still a separate concern.

## HTTP preview runtime

The trusted publisher can launch an approved argv-style Node.js service from the retained result
worktree. It does not use a shell and receives a reduced environment (`PATH`, isolated `HOME`,
loopback `HOST`, assigned `PORT`, and `NODE_ENV=test`). A healthcheck must pass before the target is
published.

`test.hryshyn.dev/<project>/` is served by a gateway on `127.0.0.1:8091`. The gateway serves existing
static releases or reverse-proxies a live runtime and supports streaming responses. Caddy remains the
public TLS boundary. The in-memory registry and child processes belong to the publisher service and
are cleaned up on shutdown.

Known limitation: path-prefix previews rely on the browser Referer for application requests that use
absolute paths. Per-project wildcard subdomains would be cleaner, but require wildcard DNS and TLS
configuration and are deliberately not introduced implicitly.

## Adding a new stack

1. Add or reuse a component kind and artifact expectation.
2. Register required capabilities and their provisioning class.
3. Add an approved, argv-based acceptance command supported by the validation image; do not execute
   an arbitrary repository deploy script.
4. Add preflight, success, failure, cleanup, and Telegram projection tests.
5. Only then allow the adapter to produce verified result evidence.
