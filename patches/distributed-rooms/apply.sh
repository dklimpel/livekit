#!/usr/bin/env bash
# Applies the distributed rooms patch series to an upstream LiveKit release.
#
#   patches/distributed-rooms/apply.sh <upstream-tag> [target-dir]
#
# Clones github.com/livekit/livekit into target-dir (or uses an existing
# checkout there), creates the branch distributed-<tag>, applies the patches
# (with 3-way merge), builds and runs the unit tests.
# Set LIVEKIT_REPO to clone from a different remote or a local mirror.
set -euo pipefail
TAG=${1:?usage: $0 <upstream-tag> [target-dir]}
DIR=${2:-livekit-$TAG-distributed}
REPO=${LIVEKIT_REPO:-https://github.com/livekit/livekit.git}
HERE=$(cd "$(dirname "$0")" && pwd)

if [ ! -d "$DIR/.git" ]; then
  git clone --quiet "$REPO" "$DIR"
fi
cd "$DIR"
git fetch --quiet --tags origin || true
# 3-way merges need the blobs of the base release the patches were made from
git fetch --quiet origin "refs/tags/$(cat "$HERE/BASE"):refs/tags/$(cat "$HERE/BASE")" 2>/dev/null || true
git checkout --quiet -B "distributed-$TAG" "$TAG"

for p in "$HERE"/0*.patch; do
  echo "applying $(basename "$p")"
  if ! git apply --3way --whitespace=nowarn "$p"; then
    echo
    echo "conflict in $(basename "$p"), resolve the files listed above (git status),"
    echo "then continue with the remaining patches manually, see docs/distributed-rooms/patch-workflow.md"
    exit 1
  fi
done

echo "building"
go build ./...
echo "unit tests"
go test ./pkg/distributed/ ./pkg/routing/... ./pkg/config/
echo
echo "done: $PWD (branch distributed-$TAG, changes are not committed)"
