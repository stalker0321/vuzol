#!/bin/sh
# Install and switch production to one Git commit:
#   sudo deploy/production/deploy.sh <commit>
# Rollback is the same command with the previous commit (its release is kept).
set -eu

REPO=${VUZOL_SOURCE_REPO:-/srv/vuzol/repo}
UV=${UV:-/usr/local/bin/uv}
RELEASES=/opt/vuzol/releases
UNITS="vuzol-executor.service vuzol-applier.service vuzol-static-publisher-worker.service"
KEEP=3

[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
[ $# = 1 ] || { echo "usage: $0 <commit>" >&2; exit 1; }
SHA=$(git -C "$REPO" rev-parse --verify "$1^{commit}")
TAG=$(echo "$SHA" | cut -c1-12)
REL=$RELEASES/$TAG
IMAGE=vuzol:$TAG

exec 9>/run/vuzol-deploy.lock
flock -n 9 || { echo "another deploy is running" >&2; exit 1; }

for unit in $UNITS vuzol-opencode-usage.service; do
  if [ -d "/etc/systemd/system/$unit.d" ]; then
    echo "drop-ins found in /etc/systemd/system/$unit.d; fold them into deploy/production/systemd" >&2
    exit 1
  fi
done

for env in vuzol executor applier static-publisher secrets; do
  [ -f "/etc/vuzol/$env.env" ] || { echo "/etc/vuzol/$env.env is missing" >&2; exit 1; }
done

if [ ! -f "$REL/.installed" ]; then
  echo "== building release $TAG"
  rm -rf "$REL"
  mkdir -p "$REL"
  git -C "$REPO" archive "$SHA" | tar -x -C "$REL"
  (cd "$REL" && UV_PYTHON_DOWNLOADS=never UV_CACHE_DIR=/var/cache/vuzol-uv \
    "$UV" sync --frozen --no-dev --extra mcp --python /usr/bin/python3.12)
  docker build -q -t "$IMAGE" --build-arg VUZOL_BUILD_GIT_SHA="$SHA" "$REL" >/dev/null
  echo "$SHA" > "$REL/.installed"
fi

echo "== migrating database"
VUZOL_IMAGE=$IMAGE docker compose -p repo -f "$REL/deploy/production/compose.yaml" \
  --env-file /srv/vuzol/.env run --rm --no-deps worker alembic upgrade head

echo "== switching to $TAG"
echo "VUZOL_IMAGE=$IMAGE" > /opt/vuzol/current.env.new
mv /opt/vuzol/current.env.new /opt/vuzol/current.env
ln -sfn "$REL" /opt/vuzol/current.new
mv -T /opt/vuzol/current.new /opt/vuzol/current
install -m 0755 "$REL/deploy/production/vuzol-compose" /usr/local/bin/vuzol-compose

install -m 0644 "$REL"/deploy/production/systemd/*.service "$REL"/deploy/production/systemd/*.timer \
  /etc/systemd/system/
systemctl daemon-reload
/usr/local/bin/vuzol-compose up -d --remove-orphans --wait postgres
/usr/local/bin/vuzol-compose up -d --remove-orphans
systemctl restart $UNITS

echo "== verifying"
# A service that crashes at startup still reads "active"/"running" between
# restarts, so restart counters are checked too (2026-10-27 outage).
sleep 30
systemctl is-active $UNITS vuzol-opencode-usage.timer
for unit in $UNITS; do
  restarts=$(systemctl show -p NRestarts --value "$unit")
  [ "$restarts" = 0 ] || { echo "$unit restarted $restarts times" >&2; exit 1; }
done
for svc in $(/usr/local/bin/vuzol-compose config --services); do
  state=$(/usr/local/bin/vuzol-compose ps --format '{{.State}}' "$svc")
  image=$(/usr/local/bin/vuzol-compose ps --format '{{.Image}}' "$svc")
  id=$(/usr/local/bin/vuzol-compose ps -q "$svc")
  restarts=$(docker inspect -f '{{.RestartCount}}' "$id")
  echo "$svc $state $image restarts=$restarts"
  [ "$state" = running ] || { echo "container $svc is not running" >&2; exit 1; }
  [ "$restarts" = 0 ] || { echo "container $svc restarted $restarts times" >&2; exit 1; }
done

ls -1dt "$RELEASES"/*/.installed | tail -n +$((KEEP + 1)) | while read -r marker; do
  old=$(dirname "$marker")
  [ "$(readlink -f /opt/vuzol/current)" = "$old" ] && continue
  rm -rf "$old"
  docker rmi "vuzol:$(basename "$old")" >/dev/null 2>&1 || true
done
docker image prune -f >/dev/null
# Every release builds an image; unbounded build cache filled the disk on 2026-10-24.
docker builder prune -f --max-used-space 2gb >/dev/null
echo "== deployed $SHA"
