#!/usr/bin/env bash
# Kills the LiveKit node with the given relay address (ip:port), used by the
# node failure test (LIVEKIT_STOP_NODE_CMD="./stop-node.sh {node}").
set -euo pipefail
cd "$(dirname "$0")"
ip="${1%%:*}"
case "$ip" in
  172.28.0.11) svc=livekit-1 ;;
  172.28.0.12) svc=livekit-2 ;;
  172.28.0.13) svc=livekit-3 ;;
  *) echo "unknown node $1" >&2; exit 1 ;;
esac
echo "killing $svc ($1)"
docker compose kill -s SIGKILL "$svc" >/dev/null
