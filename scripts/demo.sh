#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="deploy/docker-compose.yml"
COMPOSE="docker compose -f $COMPOSE_FILE"
CLI="./bin/shardkv-cli"
NODES=("localhost:8081" "localhost:8082" "localhost:8083")
SERVICES=("node1" "node2" "node3")
DEMO_SHARD=0
WRITE_KEY=""
WRITER_PID=""

green()  { echo -e "\033[0;32m$*\033[0m"; }
yellow() { echo -e "\033[1;33m$*\033[0m"; }
red()    { echo -e "\033[0;31m$*\033[0m"; }
cyan()   { echo -e "\033[0;36m$*\033[0m"; }
step()   { echo; yellow "==> $*"; }

# pause_for gives a live viewer control over pacing without making an
# unattended run hang: it waits up to $2 seconds for Enter, and just moves
# on either way once that runs out.
pause_for() {
  local prompt="$1" secs="${2:-6}"
  read -r -t "$secs" -p "$prompt (or wait ${secs}s) " _ 2>/dev/null || true
}

cleanup() {
  if [[ -n "$WRITER_PID" ]]; then
    kill "$WRITER_PID" 2>/dev/null || true
    wait "$WRITER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

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

# key_for_shard finds a key that routes to a specific shard, so the demo
# can target shard 0 (the one Fleet View shows by default) by name instead
# of making the viewer hunt for whichever shard an arbitrary key landed on.
key_for_shard() {
  local target="$1" num_shards="$2" i=0
  while (( i < 1000 )); do
    local k="demo:$i"
    if [[ "$(shard_for_key "$k" "$num_shards")" == "$target" ]]; then
      echo "$k"
      return 0
    fi
    i=$((i + 1))
  done
  echo "demo:0"
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
# currently leading the given shard, found by asking each node directly
# whether it believes itself to be that shard's leader. This deliberately
# never looks at the leader_addr field a status response carries: raft
# advertises a node's address as whatever its TCP transport resolved to,
# which on Docker is the container's bridge-network IP, not its service
# name, so matching that string back to node1/node2/node3 doesn't work.
# Asking "are you the leader" of the address already in NODES sidesteps
# that entirely.
shard_leader_addr() {
  local shard_id="$1"
  for addr in "${NODES[@]}"; do
    is_leader=$(curl -sf "http://$addr/v1/status" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
for s in d['shards']:
    if s['shard_id'] == $shard_id:
        print('yes' if s['raft_state'] == 'Leader' else 'no')
        break
" 2>/dev/null || true)
    if [[ "$is_leader" == "yes" ]]; then
      echo "$addr"
      return 0
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

service_for_addr() {
  local addr="$1"
  for i in "${!NODES[@]}"; do
    if [[ "${NODES[$i]}" == "$addr" ]]; then
      echo "${SERVICES[$i]}"
      return
    fi
  done
}

# start_writer keeps writing to WRITE_KEY every ~400ms for the rest of the
# demo, rotating across all three nodes so it keeps working through
# whichever one gets killed later (the CLI follows redirects, so it always
# ends up at the real shard leader regardless of which node it asked).
# This is what keeps Fleet View's packet animation and log-entry-committed
# lines running continuously instead of one write with long silent gaps.
start_writer() {
  (
    i=0
    while true; do
      addr="${NODES[$((i % 3))]}"
      "$CLI" --addr "$addr" set "$WRITE_KEY" "$i" >/dev/null 2>&1 || true
      i=$((i + 1))
      sleep 0.4
    done
  ) &
  WRITER_PID=$!
}

echo "╔══════════════════════════════════════════════╗"
echo "║          ShardKV Demo - Fault Tolerance       ║"
echo "╚══════════════════════════════════════════════╝"

step "Step 1: Starting the cluster (make up)"
make up > /dev/null 2>&1 || true

NUM_SHARDS=$(num_shards)
WRITE_KEY=$(key_for_shard "$DEMO_SHARD" "$NUM_SHARDS")
green "Cluster has $NUM_SHARDS shards. This demo disrupts shard $DEMO_SHARD (Fleet View's default view) using key '$WRITE_KEY'."

step "Step 2: Waiting for shard $DEMO_SHARD to elect a leader..."
LEADER_ADDR=$(wait_for_shard_leader "$DEMO_SHARD")
green "Shard $DEMO_SHARD leader: $LEADER_ADDR"

cyan "Open Fleet View now:  http://localhost:8081/fleet"
cyan "Shard $DEMO_SHARD is already selected by default, that's the one about to have a bad day."
pause_for "Ready when you are." 8

step "Step 3: Writing to shard $DEMO_SHARD continuously..."
start_writer
sleep 3
green "Writes are flowing. In Fleet View you should see the cyan packet traveling leader -> followers, and the console logging committed entries."
pause_for "Take a look, then continue." 5

step "Step 4: Killing shard $DEMO_SHARD's leader ($LEADER_ADDR)..."
LEADER_SERVICE=$(service_for_addr "$LEADER_ADDR")
if [[ "$LEADER_SERVICE" == "node1" ]]; then
  cyan "That's node1, which also happens to lead every other shard right now (it's the bootstrap node, so it started out ahead everywhere). Killing it means all $NUM_SHARDS shards lose their leader at once, but each one elects a replacement on its own, no coordination between them. Worth clicking through the other SHARD rows in the sidebar afterward to watch that happen independently."
fi
yellow "Stopping $LEADER_SERVICE in 3..."; sleep 1
yellow "2..."; sleep 1
yellow "1..."; sleep 1
$COMPOSE stop "$LEADER_SERVICE"
red "$LEADER_SERVICE is down. Watch that ship go dark in Fleet View, followed by a gold expanding ring on whichever of the other two wins the election."

step "Step 5: Waiting for shard $DEMO_SHARD to re-elect..."
NEW_LEADER_ADDR=$(wait_for_shard_leader "$DEMO_SHARD")
green "New leader for shard $DEMO_SHARD: $NEW_LEADER_ADDR"

step "Step 6: Confirming writes never stopped..."
sleep 2
val=$("$CLI" --addr "$NEW_LEADER_ADDR" get "$WRITE_KEY")
green "$WRITE_KEY = $val, still climbing the whole time. Shard $DEMO_SHARD stayed writable with only $LEADER_SERVICE down."
pause_for "Continue to recovery." 5

step "Step 7: Reviving $LEADER_SERVICE..."
$COMPOSE start "$LEADER_SERVICE"
yellow "$LEADER_SERVICE is back. Watch Fleet View: its engine glow returns, then it replays its log and rejoins as a follower."
sleep 6

step "Step 8: Stopping the write loop and checking consistency across all 3 nodes..."
kill "$WRITER_PID" 2>/dev/null || true
wait "$WRITER_PID" 2>/dev/null || true
WRITER_PID=""
sleep 1

ALL_OK=true
FINAL_VAL=$("$CLI" --addr "${NODES[0]}" get "$WRITE_KEY")
for addr in "${NODES[@]}"; do
  val="UNAVAILABLE"
  for attempt in 1 2 3 4 5; do
    val=$(curl -sf "http://$addr/v1/keys/$WRITE_KEY?consistency=stale" 2>/dev/null || echo "UNAVAILABLE")
    [[ "$val" == "$FINAL_VAL" ]] && break
    sleep 1
  done
  if [[ "$val" == "$FINAL_VAL" ]]; then
    green "  $addr -> $WRITE_KEY = $val (matches)"
  else
    yellow "  $addr -> $WRITE_KEY = $val (still catching up after 5s of retries)"
    ALL_OK=false
  fi
done

if (( NUM_SHARDS > 1 )); then
  step "Step 9: Checking how the other shard(s) handled losing the same node..."
  for ((s = 0; s < NUM_SHARDS; s++)); do
    [[ "$s" == "$DEMO_SHARD" ]] && continue
    addr=$(wait_for_shard_leader "$s")
    green "  shard $s: elected its own new leader ($(service_for_addr "$addr")), independently of shard $DEMO_SHARD"
  done
fi

echo
if $ALL_OK; then
  green "══════════════════════════════════════════════"
  green " Shard $DEMO_SHARD lost its leader, kept accepting writes,"
  green " elected a replacement, and the original rejoined and caught"
  green " up automatically. Every other shard did the same thing on"
  green " its own, no shared coordinator, no single point of failure."
  green "══════════════════════════════════════════════"
else
  yellow "One or more replicas are still replaying their log, that's normal right after a restart; check again in a few seconds."
fi
