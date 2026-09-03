#!/usr/bin/env bash
set -euo pipefail

NODES=("localhost:8081" "localhost:8082" "localhost:8083")
CONTAINERS=("shardkv-node1-1" "shardkv-node2-1" "shardkv-node3-1")

SHARD_ID="${1:-0}"
RECOVERY_TIMEOUT="${2:-10}"

green()  { echo -e "\033[0;32m$*\033[0m"; }
yellow() { echo -e "\033[1;33m$*\033[0m"; }
red()    { echo -e "\033[0;31m$*\033[0m"; }

usage() {
  echo "Usage: $0 [shard-index] [recovery-timeout-seconds]"
  echo "  Pauses the current leader of the given shard (default: shard 0),"
  echo "  waits for that shard specifically to re-elect, then unpauses."
  echo "  Other shards are left untouched so you can confirm they are"
  echo "  unaffected while checking the paused shard recovers."
}

# find_shard_leader_idx prints the index into NODES/CONTAINERS of the node
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
TARGET_CONTAINER="${CONTAINERS[$TARGET_IDX]}"

yellow "Pausing $TARGET_CONTAINER ($TARGET_ADDR) — leader of shard $SHARD_ID"
docker pause "$TARGET_CONTAINER"

T_PAUSE=$SECONDS
NEW_LEADER_IDX=$(wait_for_new_shard_leader "$SHARD_ID" "$TARGET_IDX" "$RECOVERY_TIMEOUT")

if [[ "$NEW_LEADER_IDX" == "-1" ]]; then
  red "No new leader elected for shard $SHARD_ID within ${RECOVERY_TIMEOUT}s"
  docker unpause "$TARGET_CONTAINER"
  exit 1
fi

ELAPSED=$(( SECONDS - T_PAUSE ))
NEW_LEADER_ADDR="${NODES[$NEW_LEADER_IDX]}"
green "New leader for shard $SHARD_ID: ${CONTAINERS[$NEW_LEADER_IDX]} ($NEW_LEADER_ADDR) — elected in ~${ELAPSED}s"

yellow "Resuming $TARGET_CONTAINER..."
docker unpause "$TARGET_CONTAINER"

sleep 2
green "Node resumed. Shard $SHARD_ID state across the cluster:"
for i in "${!NODES[@]}"; do
  echo "  ${CONTAINERS[$i]}: $(shard_state_line "${NODES[$i]}" "$SHARD_ID")"
done
