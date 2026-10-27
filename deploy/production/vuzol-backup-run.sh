#!/bin/sh
# Daily database backup, run by vuzol-backup.service:
#   1. capture an encrypted Postgres dump into $VUZOL_BACKUP__STAGING_ROOT,
#   2. prove the new package decrypts with the key,
#   3. copy all local packages to every host in $BACKUP_TARGETS.
# Each target accepts this host's key only for `rrsync -wo -no-del` into one
# directory, so a copy can add packages there but never read or delete them.
set -eu

PY=/opt/vuzol/current/.venv/bin/python
KEY_DIR=/etc/vuzol/backup

field() {
  "$PY" -c 'import json, sys; print(json.loads(sys.stdin.read().strip().splitlines()[-1]).get(sys.argv[1]) or "")' "$1"
}

capture=$("$PY" -m vuzol.cli.backup capture --apply --json) || true
run_id=$(printf '%s\n' "$capture" | field run_id)
if [ "$(printf '%s\n' "$capture" | field code)" != captured ] || [ -z "$run_id" ]; then
  echo "backup capture failed: $(printf '%s\n' "$capture" | field code)" >&2
  exit 1
fi

verify=$("$PY" -m vuzol.cli.backup restore --run-id "$run_id" --verify-crypto --json) || true
if [ "$(printf '%s\n' "$verify" | field code)" != crypto_verified ]; then
  echo "backup $run_id does not decrypt: $(printf '%s\n' "$verify" | field code)" >&2
  exit 1
fi
echo "backup $run_id captured and verified"

ssh="ssh -i $KEY_DIR/ssh_key -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes"
ssh="$ssh -o UserKnownHostsFile=$KEY_DIR/known_hosts -o ConnectTimeout=20"
failed=0
for target in ${BACKUP_TARGETS:-}; do
  if rsync -a --timeout=300 -e "$ssh" "$VUZOL_BACKUP__STAGING_ROOT/runs/" "$target:"; then
    echo "backup $run_id copied to $target"
  else
    echo "backup $run_id NOT copied to $target" >&2
    failed=1
  fi
done
exit $failed
