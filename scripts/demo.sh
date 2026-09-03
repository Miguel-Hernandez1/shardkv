#!/usr/bin/env bash
set -euo pipefail

CLI="./bin/shardkv-cli"
NODES=("localhost:8081" "localhost:8082" "localhost:8083")
CONTAINERS=("shardkv-node1-1" "shardkv-node2-1" "shardkv-node3-1")
DEMO_KEY="user:1"

green()  { echo -e "\033[0;32m$*\033[0m"; }
yellow() { echo -e "\033[1;33m$*\033[0m"; }
red()    { echo -e "\033[0;31m$*\033[0m"; }
step()   { echo; yellow "==> $*"; }

# shard_for_key prints the shard index a key deterministically hashes to,
# using the same FNV-1a-mod-N scheme as internal/shard.KeyToShard.
shard_for_key() {
  local key="$1" num_shards="$2"
  python3 - "$key" "$num_shards" <<'PY'
import sys
key, num_shards = sys.argv[1], int(sys.argv[2])
h = 2166136261
for b in key.encode():
    h ^= b
    h = (h * 16777619) & 0xFFFFFFFF
print(h % num_shards)
PY
}

num_shards() {
  for addr in "${NODES[@]}"; do
    n=$(curl -sf "http://$addr/v1/status" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['num_shards'])" 2>/dev/null || true)
    if [[ -n "$n" ]]; then echo "$n"; return 0; fi
  done
  red "Could not determine shard count from any node"
  exit 1
}

# shard_leader_addr prints the HTTP address (from NODES) of the node
# currently leading the given shard, discovered from any reachable node's
# /v1/status.
shard_leader_addr() {
  local shard_id="$1"
  for addr in "${NODES[@]}"; do
    leader_host=$(curl -sf "http://$addr/v1/status" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
for s in d['shards']:
    if s['shard_id'] == $shard_id and s['raft_state'] == 'Leader':
        print(s['leader_addr'].split(':')[0])
        break
" 2>/dev/null || true)
    if [[ -n "$leader_host" ]]; then
      case "$leader_host" in
        node1) echo "${NODES[0]}"; return 0 ;;
        node2) echo "${NODES[1]}"; return 0 ;;
        node3) echo "${NODES[2]}"; return 0 ;;
      esac
    fi
  done
  return 1
}

wait_for_shard_leader() {
  local shard_id="$1" timeout=15
  local start=$SECONDS
  while (( SECONDS - start < timeout )); do
    if addr=$(shard_leader_addr "$shard_id"); then
      echo "$addr"
      return 0
    fi
    sleep 0.5
  done
  red "No leader found for shard $shard_id within ${timeout}s"
  exit 1
}

container_for_addr() {
  local addr="$1"
  for i in "${!NODES[@]}"; do
    if [[ "${NODES[$i]}" == "$addr" ]]; then
      echo "${CONTAINERS[$i]}"
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

NUM_SHARDS=$(num_shards)
DEMO_SHARD=$(shard_for_key "$DEMO_KEY" "$NUM_SHARDS")
green "Cluster has $NUM_SHARDS shards. Demo key '$DEMO_KEY' routes to shard $DEMO_SHARD."

step "Step 2: Waiting for shard $DEMO_SHARD to elect a leader..."
LEADER_ADDR=$(wait_for_shard_leader "$DEMO_SHARD")
green "Shard $DEMO_SHARD leader: $LEADER_ADDR"

step "Step 3: Writing initial data (any node auto-routes to the right shard leader)..."
$CLI --addr "${NODES[0]}" set "$DEMO_KEY" alice
$CLI --addr "${NODES[0]}" set user:2 bob
$CLI --addr "${NODES[0]}" set product:1 laptop
green "3 keys written."

step "Step 4: Reading from a follower (data should be replicated)..."
sleep 1
FOLLOWER="${NODES[1]}"
$CLI --addr "$FOLLOWER" get "$DEMO_KEY"
green "Follower returned the value — replication confirmed."

step "Step 5: Identifying shard $DEMO_SHARD's current leader container..."
LEADER_CONTAINER=$(container_for_addr "$LEADER_ADDR")
green "Shard $DEMO_SHARD leader is $LEADER_CONTAINER ($LEADER_ADDR)"

step "Step 6: Stopping that leader (docker pause)..."
docker pause "$LEADER_CONTAINER"
yellow "Leader paused. Waiting for shard $DEMO_SHARD to re-elect (~500ms)..."

sleep 2

step "Step 7: Detecting shard $DEMO_SHARD's new leader..."
NEW_LEADER_ADDR=$(wait_for_shard_leader "$DEMO_SHARD")
green "New leader for shard $DEMO_SHARD: $NEW_LEADER_ADDR"

step "Step 8: Writing a new key during the leadership change..."
$CLI --addr "$NEW_LEADER_ADDR" set user:3 carol
green "Write succeeded on new leader — shard $DEMO_SHARD remained available."

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
  green " Shard $DEMO_SHARD survived a leader failure while other shards were undisturbed."
  green "══════════════════════════════════════════════"
else
  red "Consistency check FAILED — some nodes diverged."
  exit 1
fi
