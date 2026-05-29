#!/usr/bin/env bash
set -euo pipefail

NODES=("localhost:8081" "localhost:8082" "localhost:8083")
CONTAINERS=("shardkv-node1-1" "shardkv-node2-1" "shardkv-node3-1")

PAUSE_NODE="${1:-}"
RECOVERY_TIMEOUT="${2:-10}"

green()  { echo -e "\033[0;32m$*\033[0m"; }
yellow() { echo -e "\033[1;33m$*\033[0m"; }
red()    { echo -e "\033[0;31m$*\033[0m"; }

usage() {
  echo "Usage: $0 [node-index (0|1|2)] [recovery-timeout-seconds]"
  echo "  Pauses a node, waits for re-election, then unpauses."
  echo "  If node-index is omitted, the current leader is paused."
}

find_leader_idx() {
  for i in "${!NODES[@]}"; do
    state=$(curl -sf "http://${NODES[$i]}/v1/status" 2>/dev/null \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['raft_state'])" 2>/dev/null || true)
    if [[ "$state" == "Leader" ]]; then
      echo "$i"
      return
    fi
  done
  echo "-1"
}

wait_for_new_leader() {
  local exclude_idx="$1"
  local timeout="$2"
  local start=$SECONDS
  while (( SECONDS - start < timeout )); do
    for i in "${!NODES[@]}"; do
      [[ "$i" == "$exclude_idx" ]] && continue
      state=$(curl -sf "http://${NODES[$i]}/v1/status" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['raft_state'])" 2>/dev/null || true)
      if [[ "$state" == "Leader" ]]; then
        echo "$i"
        return 0
      fi
    done
    sleep 0.2
  done
  echo "-1"
}

# Determine which node to pause.
if [[ -n "$PAUSE_NODE" ]]; then
  TARGET_IDX="$PAUSE_NODE"
else
  yellow "Finding current leader..."
  TARGET_IDX=$(find_leader_idx)
  if [[ "$TARGET_IDX" == "-1" ]]; then
    red "No leader found. Is the cluster running?"
    exit 1
  fi
fi

TARGET_ADDR="${NODES[$TARGET_IDX]}"
TARGET_CONTAINER="${CONTAINERS[$TARGET_IDX]}"

STATUS=$(curl -sf "http://$TARGET_ADDR/v1/status" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['raft_state'])")
yellow "Pausing $TARGET_CONTAINER ($TARGET_ADDR) — current state: $STATUS"

docker pause "$TARGET_CONTAINER"

T_PAUSE=$SECONDS
NEW_LEADER_IDX=$(wait_for_new_leader "$TARGET_IDX" "$RECOVERY_TIMEOUT")

if [[ "$NEW_LEADER_IDX" == "-1" ]]; then
  red "No new leader elected within ${RECOVERY_TIMEOUT}s"
  docker unpause "$TARGET_CONTAINER"
  exit 1
fi

ELAPSED=$(( SECONDS - T_PAUSE ))
NEW_LEADER_ADDR="${NODES[$NEW_LEADER_IDX]}"
green "New leader: ${CONTAINERS[$NEW_LEADER_IDX]} ($NEW_LEADER_ADDR) — elected in ~${ELAPSED}s"

yellow "Resuming $TARGET_CONTAINER..."
docker unpause "$TARGET_CONTAINER"

sleep 2
green "Node resumed. Cluster state:"
for i in "${!NODES[@]}"; do
  state=$(curl -sf "http://${NODES[$i]}/v1/status" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['raft_state'], 'commit='+str(d['commit_index']))" 2>/dev/null \
    || echo "UNREACHABLE")
  echo "  ${CONTAINERS[$i]}: $state"
done
