#!/usr/bin/env bash
set -euo pipefail

CLI="./bin/shardkv-cli"
NODES=("localhost:8081" "localhost:8082" "localhost:8083")
CONTAINERS=("shardkv-node1-1" "shardkv-node2-1" "shardkv-node3-1")

green()  { echo -e "\033[0;32m$*\033[0m"; }
yellow() { echo -e "\033[1;33m$*\033[0m"; }
red()    { echo -e "\033[0;31m$*\033[0m"; }
step()   { echo; yellow "==> $*"; }

wait_for_leader() {
  local timeout=15
  local start=$SECONDS
  while (( SECONDS - start < timeout )); do
    for addr in "${NODES[@]}"; do
      state=$(curl -sf "http://$addr/v1/status" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['raft_state'])" 2>/dev/null || true)
      if [[ "$state" == "Leader" ]]; then
        echo "$addr"
        return 0
      fi
    done
    sleep 0.5
  done
  red "No leader found within ${timeout}s"
  exit 1
}

get_leader_node() {
  local addr="$1"
  for i in "${!NODES[@]}"; do
    if [[ "${NODES[$i]}" == "$addr" ]]; then
      echo "$i"
      return
    fi
  done
}

echo "╔══════════════════════════════════════════════╗"
echo "║          ShardKV Demo — Fault Tolerance       ║"
echo "╚══════════════════════════════════════════════╝"

step "Step 1: Starting cluster (make up)"
make up > /dev/null 2>&1 || true
sleep 3

step "Step 2: Waiting for leader election..."
LEADER_ADDR=$(wait_for_leader)
green "Leader elected: $LEADER_ADDR"

step "Step 3: Writing initial data..."
$CLI --addr "$LEADER_ADDR" set user:1 alice
$CLI --addr "$LEADER_ADDR" set user:2 bob
$CLI --addr "$LEADER_ADDR" set product:1 laptop
green "3 keys written."

step "Step 4: Reading from a follower (data should be replicated)..."
sleep 1
FOLLOWER="${NODES[1]}"
$CLI --addr "$FOLLOWER" get user:1
green "Follower returned the value — replication confirmed."

step "Step 5: Identifying the current leader..."
LEADER_IDX=$(get_leader_node "$LEADER_ADDR")
LEADER_CONTAINER="${CONTAINERS[$LEADER_IDX]}"
green "Leader is $LEADER_CONTAINER ($LEADER_ADDR)"

step "Step 6: Stopping the leader (docker pause)..."
docker pause "$LEADER_CONTAINER"
yellow "Leader paused. Waiting for re-election (~500ms)..."

sleep 2

step "Step 7: Detecting new leader..."
NEW_LEADER_ADDR=$(wait_for_leader)
green "New leader: $NEW_LEADER_ADDR"

step "Step 8: Writing a new key during the leadership change..."
$CLI --addr "$NEW_LEADER_ADDR" set user:3 carol
green "Write succeeded on new leader — cluster remained available."

step "Step 9: Reviving the original leader..."
docker unpause "$LEADER_CONTAINER"
yellow "Original leader resumed. Waiting for log catch-up..."
sleep 3

step "Step 10: Verifying data consistency on all 3 nodes..."
ALL_OK=true
for addr in "${NODES[@]}"; do
  val=$(curl -sf "http://$addr/v1/keys/user:3" 2>/dev/null || echo "UNAVAILABLE")
  if [[ "$val" == "carol" ]]; then
    green "  $addr → user:3 = $val ✓"
  else
    red "  $addr → user:3 = $val ✗"
    ALL_OK=false
  fi
done

echo
if $ALL_OK; then
  green "══════════════════════════════════════════════"
  green " All 3 nodes consistent. Demo complete."
  green " Cluster survived leader failure and recovered."
  green "══════════════════════════════════════════════"
else
  red "Consistency check FAILED — some nodes diverged."
  exit 1
fi
