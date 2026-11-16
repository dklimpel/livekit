#!/usr/bin/env bash
# Regenerates the patch series from a LiveKit checkout containing the
# distributed rooms changes (committed or not).
#
#   patches/distributed-rooms/make-patches.sh <base-tag>     e.g. v1.13.11
set -euo pipefail
BASE=${1:?usage: $0 <base-tag>}
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$(git -C "$HERE" rev-parse --show-toplevel)"

UPSTREAM_HOOKS=(
  pkg/config/config.go
  pkg/routing/redisrouter.go
  pkg/rtc/room.go
  pkg/service/roomallocator.go
  pkg/service/roommanager.go
  pkg/service/signal.go
  pkg/config/distributed.go
  pkg/routing/distributed_hook.go
  pkg/rtc/room_distributed.go
  pkg/service/roommanager_distributed.go
)

declare -A SERIES=(
  [0001-distributed-rooms-upstream-hooks]="${UPSTREAM_HOOKS[*]}"
  [0002-distributed-rooms-implementation]="pkg/distributed"
  [0003-distributed-rooms-tests]="test/distributed"
  [0004-distributed-rooms-docs]="docs/distributed-rooms"
)

# include new, untracked files in the diff without staging their content
untracked=$(git ls-files --others --exclude-standard -- ${SERIES[*]})
[ -n "$untracked" ] && git add -N $untracked
trap '[ -n "$untracked" ] && git reset -q -- $untracked' EXIT

rm -f "$HERE"/*.patch
for name in $(printf '%s\n' "${!SERIES[@]}" | sort); do
  git diff --binary --full-index "$BASE" -- ${SERIES[$name]} > "$HERE/$name.patch"
  printf '%-45s %s\n' "$name.patch" "$(git diff --shortstat "$BASE" -- ${SERIES[$name]})"
done
echo "$BASE" > "$HERE/BASE"
