#!/usr/bin/env bash
# End-to-end tests of distributed rooms on a local 3 node cluster.
#
#   ./run-tests.sh            all scenarios
#   ./run-tests.sh go         Go e2e tests (needs Go on the host)
#   ./run-tests.sh loadtest   lk load-test across nodes
#   ./run-tests.sh failover   node failure with lk
#
# Exit code 0 when all selected scenarios pass. Set KEEP=1 to keep the
# cluster running afterwards.
set -uo pipefail
cd "$(dirname "$0")"
export BUILDKIT_PROGRESS=plain

API_KEY=devkey
API_SECRET=secretsecretsecretsecretsecretsecret
URLS=(ws://localhost:7880 ws://localhost:7980 ws://localhost:8080)
SERVICES=(livekit-1 livekit-2 livekit-3)
RELAY_ADDRS=(172.28.0.11:7890 172.28.0.12:7890 172.28.0.13:7890)

SCENARIOS=("$@")
[ ${#SCENARIOS[@]} -eq 0 ] && SCENARIOS=(go loadtest failover)

RESULTS=()
FAILED=0

log()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
pass() { RESULTS+=("PASS  $*"); printf '\033[32mPASS\033[0m %s\n' "$*"; }
fail() { RESULTS+=("FAIL  $*"); printf '\033[31mFAIL\033[0m %s\n' "$*"; FAILED=1; }

lk() { docker compose run --rm -T lk "$@"; }
redis() { docker compose exec -T redis redis-cli "$@"; }

# relay address of a node id
node_relay() { redis HGET dist:node_relay "$1" | tr -d '\r'; }
# participants per node of a room, e.g. "livekit-1: 4"
print_distribution() {
  echo "participants per node in $1:"
  for nodeID in $(redis HVALS "dist:participant_node:$1" | tr -d '\r' | sort | uniq); do
    local n; n=$(redis HVALS "dist:participant_node:$1" | tr -d '\r' | grep -cx "$nodeID")
    echo "  $(node_service "$nodeID") ($nodeID): $n"
  done
}

# service name of a node id
node_service() {
  local addr; addr=$(node_relay "$1")
  for i in "${!RELAY_ADDRS[@]}"; do
    [ "${RELAY_ADDRS[$i]}" = "$addr" ] && { echo "${SERVICES[$i]}"; return; }
  done
}

wait_ready() {
  for url in "${URLS[@]}"; do
    local http=${url/ws:/http:}
    for _ in $(seq 1 60); do
      curl -fs "$http" >/dev/null 2>&1 && break
      sleep 1
    done
    curl -fs "$http" >/dev/null 2>&1 || { echo "node $http not ready"; docker compose logs --tail 50; exit 1; }
  done
  # nodes register their stats in redis every few seconds
  sleep 3
}

cluster_up() {
  log "starting cluster (max_participants_per_node=$1)"
  # plain progress: BuildKit fails with "failed to get console" when output is not a terminal
  if ! MAX_PER_NODE=$1 BUILDKIT_PROGRESS=plain docker compose up -d --build --force-recreate \
       redis "${SERVICES[@]}" > /tmp/lk-dist-compose.log 2>&1; then
    tail -n 40 /tmp/lk-dist-compose.log
    echo "starting the cluster failed, full output: /tmp/lk-dist-compose.log"
    exit 1
  fi
  wait_ready
}

# ------------------------------------------------------------------------------

scenario_go() {
  if ! command -v go >/dev/null; then
    echo "go not found on the host, skipping Go e2e tests"
    RESULTS+=("SKIP  go e2e (no Go toolchain)")
    return
  fi
  cluster_up 1
  log "Go e2e tests (participants spread, media, data, leave, node failure)"
  if (cd ../.. && LIVEKIT_DISTRIBUTED_E2E=1 \
      LIVEKIT_NODE_URLS=$(IFS=,; echo "${URLS[*]}") \
      LIVEKIT_NODE_RELAY_ADDRS=$(IFS=,; echo "${RELAY_ADDRS[*]}") \
      LIVEKIT_REDIS_ADDR=localhost:6379 \
      LIVEKIT_API_KEY=$API_KEY LIVEKIT_API_SECRET=$API_SECRET \
      LIVEKIT_STOP_NODE_CMD="$PWD/test/distributed/stop-node.sh {node}" \
      go test ./test/distributed/ -count=1 -v 2>&1 | tee /tmp/lk-dist-go.log | grep -E '^\s*(--- |ok|FAIL|e2e_test.go)'); then
    grep -q '^ok' /tmp/lk-dist-go.log && pass "go e2e" || fail "go e2e (see /tmp/lk-dist-go.log)"
  else
    fail "go e2e (see /tmp/lk-dist-go.log)"
  fi
}

scenario_loadtest() {
  cluster_up 4
  local room=loadtest-$RANDOM
  log "lk load-test in room $room: 3 video + 3 audio publishers (simulcast), 6 subscribers"
  lk load-test --url "${URLS[0]}" --api-key $API_KEY --api-secret $API_SECRET \
     --room "$room" --video-publishers 3 --audio-publishers 3 --subscribers 6 \
     --duration 40s > /tmp/lk-dist-loadtest.log 2>&1 &
  local lkpid=$!

  sleep 25
  # participants are spread over multiple nodes
  local nodes
  nodes=$(redis HVALS "dist:participant_node:$room" | tr -d '\r' | sort -u | grep -c . || true)
  local participants
  participants=$(redis HLEN "dist:participant_node:$room" | tr -d '\r')
  echo "participants: $participants on $nodes nodes"
  [ "${nodes:-0}" -ge 2 ] && pass "loadtest: participants spread over $nodes nodes" \
                          || fail "loadtest: participants on $nodes node(s), expected >= 2"

  wait $lkpid; local rc=$?
  tail -n 30 /tmp/lk-dist-loadtest.log
  [ $rc -eq 0 ] && pass "loadtest: lk load-test exit code 0" || fail "loadtest: lk load-test exit code $rc"

  # summary "<subscribed>/<expected>" tracks, if lk prints it
  local summary
  summary=$(grep -iE 'summary|total' /tmp/lk-dist-loadtest.log | grep -oE '[0-9]+/[0-9]+' | head -1)
  if [ -n "$summary" ]; then
    [ "${summary%/*}" = "${summary#*/}" ] && pass "loadtest: all tracks subscribed ($summary)" \
                                          || fail "loadtest: tracks subscribed $summary"
  else
    echo "could not find a track summary in the lk output, relying on server side checks"
  fi

  # media was relayed between nodes, with all simulcast layers of the video tracks
  local logs; logs=$(docker compose logs "${SERVICES[@]}" 2>/dev/null)
  local relayed received failed
  relayed=$(grep "relaying track to remote node" <<<"$logs" | grep -c "$room")
  received=$(grep "relayed track received" <<<"$logs" | grep -c "$room")
  failed=$(grep -E "could not add relayed receiver|could not relay receiver" <<<"$logs" | grep -c "$room")
  echo "relayed tracks: $relayed, relayed streams received: $received, errors: $failed"
  [ "$relayed" -gt 0 ] && pass "loadtest: tracks relayed between nodes ($relayed)" || fail "loadtest: no track relayed"
  [ "$failed" -eq 0 ] && pass "loadtest: no relay errors" || fail "loadtest: $failed relay errors"
}

scenario_failover() {
  cluster_up 4
  local room=failover-$RANDOM
  log "failover in room $room"
  # capacity: 3 nodes x 4 = 12 slots, 2 x 4 = 8 after the failure. At most 6 load
  # test participants, so that after they reconnected there is still room for
  # the additional participant joining after the failure.
  lk load-test --url "${URLS[0]}" --api-key $API_KEY --api-secret $API_SECRET \
     --room "$room" --video-publishers 1 --audio-publishers 1 --subscribers 4 \
     --duration 90s > /tmp/lk-dist-failover.log 2>&1 &
  local lkpid=$!
  sleep 20

  print_distribution "$room"

  # kill a node hosting participants, but not the node the load tester signals through
  local victim="" victim_svc=""
  for nodeID in $(redis HVALS "dist:participant_node:$room" | tr -d '\r' | sort -u); do
    local svc; svc=$(node_service "$nodeID")
    if [ -n "$svc" ] && [ "$svc" != "${SERVICES[0]}" ]; then victim=$nodeID; victim_svc=$svc; break; fi
  done
  if [ -z "$victim" ]; then
    fail "failover: no participants on livekit-2/livekit-3, cannot test"
    kill $lkpid 2>/dev/null; return
  fi
  echo "killing $victim_svc ($victim)"
  docker compose kill -s SIGKILL "$victim_svc" >/dev/null

  sleep 15
  # the failed node is removed from the room
  if redis HKEYS "dist:room_nodes:$room" | tr -d '\r' | grep -qx "$victim"; then
    fail "failover: failed node still registered for the room"
  else
    pass "failover: failed node removed from the room"
  fi
  # no participant is listed that was hosted on the failed node
  local listed stale=0
  listed=$(lk room participants list "$room" 2>/dev/null)
  echo "$listed" | head -20
  for identity in $(redis HKEYS "dist:participant_node:$room" | tr -d '\r'); do
    [ "$(redis HGET "dist:participant_node:$room" "$identity" | tr -d '\r')" = "$victim" ] && stale=$((stale+1))
  done
  [ $stale -eq 0 ] && pass "failover: no participant assigned to the failed node" \
                   || fail "failover: $stale participants still assigned to the failed node"
  [ -n "$listed" ] && pass "failover: room still alive on the remaining nodes" || fail "failover: room not listed"

  print_distribution "$room"

  # a new participant joins on a live node
  timeout 20 docker compose run --rm -T lk room join --identity rejoiner --publish-demo "$room" > /tmp/lk-dist-rejoin.log 2>&1 &
  sleep 12
  local node; node=$(redis HGET "dist:participant_node:$room" rejoiner | tr -d '\r')
  if [ -n "$node" ] && [ "$node" != "$victim" ]; then
    pass "failover: new participant placed on live node $(node_service "$node")"
  else
    fail "failover: new participant not placed on a live node (node: '${node}')"
    print_distribution "$room"
    grep -E "ERROR|response|could not" /tmp/lk-dist-rejoin.log | head -5
  fi
  wait

  log "restarting $victim_svc"
  docker compose start "$victim_svc" >/dev/null 2>&1
}

# ------------------------------------------------------------------------------

for s in "${SCENARIOS[@]}"; do
  case "$s" in
    go) scenario_go ;;
    loadtest) scenario_loadtest ;;
    failover) scenario_failover ;;
    *) echo "unknown scenario $s"; exit 2 ;;
  esac
done

log "results"
printf '%s\n' "${RESULTS[@]}"
[ "${KEEP:-0}" = "1" ] || docker compose down >/dev/null 2>&1
exit $FAILED
