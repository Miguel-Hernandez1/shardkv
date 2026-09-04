#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f deploy/docker-compose.yml"
NODES=("localhost:8081" "localhost:8082" "localhost:8083")
SERVICES=("node1" "node2" "node3")

SHARD_ID="${1:-0}"
RECOVERY_TIMEOUT="${2:-10}"

green()  { echo -e "\033[0;32m$*\033[0m"; }
yellow() { echo -e "\033[1;33m$*\033[0m"; }
red()    { echo -e "\033[0;31m$*\033[0m"; }

usage() {
  echo "Usage: $0 [shard-index] [recovery-timeout-seconds]"
  echo "  Pauses the current leader of the given shard (default: shard 0),"
  echo "  waits for that shard specifically to re-elect, then unpauses."
  echo "  Note: with the default deployment (every node replicates every"
  echo "  shard), node1 leads every shard until something disturbs it, so"
  echo "  targeting whichever shard node1 currently leads will pause every"
  echo "  shard's leader at once, not just the one you asked for. Run this"
  echo "  against a shard that's already failed over once (its leader"
  echo "  isn't node1 anymore) to actually see just that one shard react."
}

# find_shard_leader_idx prints the index into NODES/SERVICES of the node
# currently leading the given shard, or -1 if none is found.
find_shard_leader_idx() {
  local shard_id="$1"
  for i in "${!NODES[@]}"; do
    state=$(curl -sf "http://${NODES[$i]}/v1/status" 2>/dev/null \
      | python3 -c "
import sys, json
d = json.load(sys.stdin)
for s in d['shards']:
    if s['shard_id'] == $shard_id:
        print(s['raft_state'])
        break
" 2>/dev/null || true)
    if [[ "$state" == "Leader" ]]; then
      echo "$i"
      return
    fi
  done
  echo "-1"
}

wait_for_new_shard_leader() {
  local shard_id="$1" exclude_idx="$2" timeout="$3"
  local start=$SECONDS
  while (( SECONDS - start < timeout )); do
    for i in "${!NODES[@]}"; do
      [[ "$i" == "$exclude_idx" ]] && continue
      state=$(curl -sf "http://${NODES[$i]}/v1/status" 2>/dev/null \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
for s in d['shards']:
    if s['shard_id'] == $shard_id:
        print(s['raft_state'])
        break
" 2>/dev/null || true)
      if [[ "$state" == "Leader" ]]; then
        echo "$i"
        return 0
      fi
    done
    sleep 0.2
  done
  echo "-1"
}

shard_state_line() {
  local addr="$1" shard_id="$2"
  curl -sf "http://$addr/v1/status" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
for s in d['shards']:
    if s['shard_id'] == $shard_id:
        print(s['raft_state'], 'commit='+str(s['commit_index']))
        break
" 2>/dev/null || echo "UNREACHABLE"
}

yellow "Finding current leader of shard $SHARD_ID..."
TARGET_IDX=$(find_shard_leader_idx "$SHARD_ID")
if [[ "$TARGET_IDX" == "-1" ]]; then
  red "No leader found for shard $SHARD_ID. Is the cluster running?"
  usage
  exit 1
fi

TARGET_ADDR="${NODES[$TARGET_IDX]}"
TARGET_SERVICE="${SERVICES[$TARGET_IDX]}"

yellow "Pausing $TARGET_SERVICE ($TARGET_ADDR), leader of shard $SHARD_ID"
$COMPOSE pause "$TARGET_SERVICE"

T_PAUSE=$SECONDS
NEW_LEADER_IDX=$(wait_for_new_shard_leader "$SHARD_ID" "$TARGET_IDX" "$RECOVERY_TIMEOUT")

if [[ "$NEW_LEADER_IDX" == "-1" ]]; then
  red "No new leader elected for shard $SHARD_ID within ${RECOVERY_TIMEOUT}s"
  $COMPOSE unpause "$TARGET_SERVICE"
  exit 1
fi

ELAPSED=$(( SECONDS - T_PAUSE ))
NEW_LEADER_ADDR="${NODES[$NEW_LEADER_IDX]}"
green "New leader for shard $SHARD_ID: ${SERVICES[$NEW_LEADER_IDX]} ($NEW_LEADER_ADDR), elected in ~${ELAPSED}s"

yellow "Resuming $TARGET_SERVICE..."
$COMPOSE unpause "$TARGET_SERVICE"

sleep 2
green "Node resumed. Shard $SHARD_ID state across the cluster:"
for i in "${!NODES[@]}"; do
  echo "  ${SERVICES[$i]}: $(shard_state_line "${NODES[$i]}" "$SHARD_ID")"
done
