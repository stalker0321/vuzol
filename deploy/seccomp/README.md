# Vuzol sandbox seccomp profile

`vuzol-sandbox.json` is based on the Moby `seccomp/v0.2.1` default allowlist:

- source: `https://github.com/moby/profiles/blob/seccomp/v0.2.1/seccomp/default.json`
- upstream SHA-256: `536529b665dd0972c37bfb569f5d4ac8a53592e7b00752bc39ff063ca9864c74`
- rendered Vuzol SHA-256: `bdbf16ca9391fc73e7f7f75d910dce7718c81009fb205fb04529021adaec4efa`

The only policy delta is one allow rule for `clone`, `mount`, `pivot_root`, `setns`, `umount2`,
and `unshare`. These calls let the trusted Codex `bwrap` launcher create its nested user and mount
namespace. The outer container still runs rootless with all capabilities dropped, no new
privileges, a read-only root, resource limits, and controlled networking. Moby's remaining
allowlist and explicit denials, including the `AF_ALG` socket restriction, are preserved.

Deploy this file root-owned and not group/other-writable (normally mode `0444`) at the absolute
path configured by `VUZOL_EXECUTION__SANDBOX_SECCOMP_PROFILE`. Configure the matching SHA-256 in
`VUZOL_EXECUTION__SANDBOX_SECCOMP_PROFILE_SHA256`. The executor verifies the file type, ownership,
mode, resolved path, and digest before every sandbox launch.
