#!/bin/sh
set -eu

# Scan only the task delta. Existing repository history is outside the worker's
# authority and is scanned separately in CI. A temporary file keeps Git failure
# distinct from an empty diff; a pipeline here could otherwise fail open.
diff_file="$(mktemp /tmp/vuzol-secret-diff.XXXXXX)"
trap 'rm -f "$diff_file"' EXIT HUP INT TERM
git -c safe.directory=/workspace diff --no-ext-diff --binary HEAD >"$diff_file"
/usr/local/bin/gitleaks stdin --no-banner --redact --exit-code 1 <"$diff_file"
