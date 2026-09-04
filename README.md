# ShardKV

A distributed, sharded key-value store written in Go.

Nine independent Raft groups. Deterministic routing. Persistent storage. Per-shard leader election. Fault tolerant. Observable.

---

## What it is

ShardKV is a CP distributed key-value store built around real horizontal sharding. Keys are hashed to one of several shards, and each shard is replicated by its own independent Raft consensus group. A default deployment runs 3 physical nodes and 3 shards, so every node hosts one replica of every shard: 9 Raft groups in total, each with its own leader, log, and failure domain.

Every write is replicated across a shard's Raft group before being acknowledged. If a shard's leader crashes, that shard's surviving replicas elect a new leader independently of every other shard, and continue accepting writes. When the dead replica comes back, it replays its own Raft log and catches up automatically. Losing a majority of one shard's replicas takes that shard offline; it does not affect any other shard. All of this is visible in real time on a browser dashboard.

## Why it matters

Building this requires holding the full distributed systems stack in your head at once, for more than one Raft group at a time:

- **Sharding** - deterministic key-to-shard routing (FNV-1a hash mod shard count), one independent Raft group per shard, transparent per-key request routing
- **Consensus** - Raft leader election and log replication per shard (via `hashicorp/raft`, the same library used by Consul, Vault, and Nomad)
- **Persistence** - a BoltDB-backed Raft log store and stable store per shard replica; file-based snapshots for log compaction
- **Crash recovery** - a node that restarts replays each shard's log or restores from a snapshot independently; no data loss
- **Read consistency** - linearizable reads by default (redirect to the shard leader and confirm with `raft.Barrier()`), with an explicit `stale` mode to trade that guarantee for a faster local read
- **Fault isolation** - one shard losing quorum does not take down any other shard; the cluster survives the loss of any one node
- **Observability** - Prometheus metrics, Grafana dashboard, and a real-time browser visualization, all labeled per shard

---

## Screenshots

### Fleet View - Real-time cluster visualization

> Open `http://localhost:8081/fleet` after `make up`

<img width="800" alt="image" src="https://github.com/user-attachments/assets/0fa4b608-c3fb-4aaf-bbfe-cdb2750024ba" />

*The animated ship view shows one selected shard at a time (gold ship = that shard's Raft leader, blue ships = followers). This screenshot predates sharding; the current build adds a shard membership sidebar next to this view, listing every shard's leader, per-node replica health, commit index, and replication lag, with a click to switch which shard the animated view follows.*

### Grafana Dashboard

> Open `http://localhost:3000` after `make up` (no login required)
>
<img width="800" alt="image" src="https://github.com/user-attachments/assets/7ec6fd43-1ce6-4535-8ac1-b6b9274e7b7a" />

*Live metrics: operations per second, request latency (p50/p99), Raft state per node and shard, commit vs applied index, replication lag.*

### CLI - Leader failover demo

> Run `make demo` after `make up`

<img width="800" alt="image" src="https://github.com/user-attachments/assets/4623a874-067f-4759-b087-dd47fc24e74c" />

*Automated failover: write to a shard's leader, pause that leader, new election within that shard, write to the new leader, revive the original, verify consistency across all nodes.*

---

## Architecture

```
  +------------------------------------------------------------+
  |                     Client Layer                            |
  |         shardkv-cli  /  curl  /  bench                      |
  +---------------------------+----------------------------------+
                              | HTTP  :8081 / :8082 / :8083
  +---------------------------v----------------------------------+
  |                 3-Node Cluster, 3 Shards Each                |
  |                                                               |
  |   node1                node2                node3            |
  |   HTTP :8081            HTTP :8082            HTTP :8083      |
  |                                                               |
  |   shard 0 replica  <->  shard 0 replica  <->  shard 0 replica |
  |     Raft TCP :9081        Raft TCP :9082        Raft TCP :9083|
  |   shard 1 replica  <->  shard 1 replica  <->  shard 1 replica |
  |     Raft TCP :9181        Raft TCP :9182        Raft TCP :9183|
  |   shard 2 replica  <->  shard 2 replica  <->  shard 2 replica |
  |     Raft TCP :9281        Raft TCP :9282        Raft TCP :9283|
  |                                                               |
  |   Each replica: own BoltDB LogStore + StableStore,           |
  |   own filesystem snapshot directory, own Raft leader.        |
  +---------------------------+----------------------------------+
                              |
  +---------------------------v----------------------------------+
  |              Observability Stack                            |
  |         Prometheus :9090     Grafana :3000                  |
  +----------------------------------------------------------------+
```

**Routing:** every key hashes deterministically to one of `SHARDKV_NUM_SHARDS` shards (`internal/shard.KeyToShard`, FNV-1a mod N). Any node can receive any request; it computes the target shard locally and either serves it (if it holds a replica, which every node does in the default topology) or forwards writes to that shard's current leader.

**Write path:** client -> any node -> node computes the key's shard -> if not that shard's leader, `307` to that shard's leader -> leader applies to that shard's Raft log -> quorum commits -> FSM applies -> `200 OK`

**Read path (linearizable, the default for GET):** client -> any node -> node computes the key's shard -> if not that shard's leader, `307` to that shard's leader -> leader confirms it is current with `raft.Barrier()` -> reads the local FSM -> returns value. **Read path (`?consistency=stale`):** the receiving node answers immediately from its local shard replica's FSM, no redirect, no wait. A prefix scan fans out across every shard and merges the results, since a key belongs to exactly one shard; see [Read Consistency](#read-consistency) for how `SCAN`'s default differs from `GET`'s.

---

## Quickstart

### Prerequisites

- Go 1.22+
- Docker + Docker Compose

### Build

```bash
git clone https://github.com/mighdz/shardkv
cd shardkv
make build
```

### Run the cluster

```bash
make up
```

Docker Compose starts 3 ShardKV nodes (3 shards each), Prometheus, and Grafana. Node1 bootstraps every shard's Raft group and starts as leader of all of them. Nodes 2 and 3 join automatically; shard leadership then moves independently per shard as elections happen.

Wait ~10 seconds, then:

```bash
./bin/shardkv-cli status
# Node ID:    node1
# Num Shards: 3
#
# SHARD  STATE      LEADER                     COMMIT  APPLIED   TERM
# 0      Leader     172.19.0.2:9081                 4        4      2
# 1      Leader     172.19.0.2:9181                 4        4      2
# 2      Leader     172.19.0.2:9281                 4        4      2
```

Open **Grafana** at `http://localhost:3000` (no login needed) and you'll land on Grafana's own home page, not the ShardKV dashboard itself, so don't worry if it looks empty at first. The dashboard is already provisioned and sitting there waiting for you, you just need to click through to it: go to Dashboards in the left sidebar and open "ShardKV", or skip that step entirely and jump straight to `http://localhost:3000/d/shardkv-main`.

Open **Fleet View** at `http://localhost:8081/fleet`, a real-time browser visualization of every shard's Raft state.

---

## Fleet View

Fleet View is a browser-based dashboard for the sharded cluster. It polls every node's `/v1/status` every 1.5 seconds (now a per-shard array, not a single Raft state) and reflects all shards' current state without requiring WebSockets or a separate server.

```
http://localhost:8081/fleet
```

**Shard Membership sidebar** (left): one row per shard, showing its current leader, a health dot per replica (gold = leader, blue = follower, purple = candidate, red = unreachable), the shard's commit index, and its replication lag. Click a row to switch which shard the animated view on the right follows.

**Animated fleet view** (center, for the selected shard):

| Visual element | Meaning |
|---|---|
| Large angular ship with gold stripe | That shard's Raft leader |
| Smaller ship with blue nav lights | That shard's follower |
| Pulsing purple ring | Candidate (election in progress) |
| Dimmed / greyscale ship | Replica unreachable |
| Cyan packet traveling between ships | Log entry replicating to followers |
| Gold expanding rings | New leader elected for this shard |

**Shard HUD** (top left) shows, for the selected shard: current leader, Raft term, commit index, online replica count, replication lag.

**Cluster badge** (top right) reflects the whole cluster: healthy only if every shard has a leader and every node is reachable.

**Engineering Console** (bottom) streams events for the selected shard in real time, prefixed with the shard number.

One thing worth knowing going in: right after `make up`, before you've written anything to the cluster, Fleet View will look mostly calm. The ships still glow and the orbit rings still turn, that part never stops, but the packet traveling between ships and the expanding ring on a new election only show up when something actually happens on the selected shard, a write landing or a leader changing. An idle cluster has neither, so it just sits there looking healthy and still. That's expected, not a sign anything's wrong, it just means there's nothing to show yet.

A related thing people notice: node1 shows as the leader of every shard and just stays that way, no matter how long you leave the cluster running. That's expected too, and it's not the same thing as the cluster being idle. Node1 is the bootstrap node, the very first one to exist, so it became the leader of every shard before node2 and node3 even showed up to vote on anything. Raft doesn't rotate leadership around on its own once things are stable; a leader keeps the job for as long as it's healthy and sending heartbeats, since there's no reason to force an election otherwise. So node1 just keeps winning by default, forever, until something actually takes it down.

To watch a real election happen, you have to be the thing that takes it down. Stop node1's container by hand:

```bash
docker stop deploy-node1-1
```

Watch Fleet View for a few seconds and one of node2 or node3 should get elected, that's the gold expanding ring. Bring node1 back with `docker start deploy-node1-1` and it rejoins as a follower; it does not automatically reclaim leadership just because it's the original bootstrap node, whoever's currently leading stays leading. Or skip the manual steps entirely and just run `make demo`, which does this exact sequence for you and narrates each step as it happens.

**To see it in action:**
1. `make up`, start the cluster
2. Open `http://localhost:8081/fleet`
3. `make demo` in another terminal, watch a specific shard's leader election, cargo packet animations, and recovery while the sidebar shows the other shards are undisturbed

---

## CLI

```bash
# Write a key (routed automatically to the correct shard's leader)
./bin/shardkv-cli set user:1 miguel

# Read from any node (routed automatically to the correct shard; linearizable
# by default, so a non-leader redirects to that shard's leader just like a write)
./bin/shardkv-cli get user:1                        # from node1
./bin/shardkv-cli --addr localhost:8082 get user:1  # from node2
./bin/shardkv-cli --addr localhost:8083 get user:1  # from node3

# Fast local read instead, with no freshness guarantee
./bin/shardkv-cli --addr localhost:8082 get user:1 --consistency stale

# Delete
./bin/shardkv-cli delete user:1

# Scan by prefix (merged across every shard; stale by default, add
# --consistency linearizable for a slower but confirmed-current scan)
./bin/shardkv-cli scan --prefix user:

# Node status across all shards
./bin/shardkv-cli --addr localhost:8082 status
```

The CLI has no notion of shards; it always talks to whichever node address you give it. That node computes the key's shard itself and forwards writes with an HTTP `307` if it isn't that shard's leader. The CLI follows the redirect automatically.

---

## Verified Demo: Per-Shard Leader Failover

This is the exact sequence verified against a running cluster (see `scripts/demo.sh`).

```bash
# 1. Write a key; it is routed to whichever shard it hashes to
./bin/shardkv-cli set user:1 miguel
# -> OK

# 2. Confirm the value is readable from all nodes
./bin/shardkv-cli --addr localhost:8081 get user:1  # -> miguel
./bin/shardkv-cli --addr localhost:8082 get user:1  # -> miguel
./bin/shardkv-cli --addr localhost:8083 get user:1  # -> miguel

# 3. Identify and stop the current leader of user:1's shard
docker pause shardkv-node1-1   # (whichever container currently leads that shard)

# 4. A surviving replica of that specific shard is elected leader (~150-500ms)
#    Other shards are untouched: their leaders do not change.

# 5. Write a new key to the new leader
./bin/shardkv-cli --addr localhost:8082 set user:2 amazon
# -> OK

# 6. Restart the original leader
docker unpause shardkv-node1-1

# 7. Wait a few seconds for log catch-up, then verify both keys everywhere
./bin/shardkv-cli --addr localhost:8081 get user:1  # -> miguel
./bin/shardkv-cli --addr localhost:8081 get user:2  # -> amazon
```

Or run the automated version:

```bash
make demo
```

To chaos-test one shard directly and confirm the others are unaffected:

```bash
./scripts/chaos.sh 1   # pause shard 1's leader, wait for shard 1 to recover, then resume it
```

---

## HTTP API

All endpoints are available on every node. Requests are routed to the correct shard internally; clients never specify a shard.

| Method | Path | Body | Response | Notes |
|---|---|---|---|---|
| `PUT` | `/v1/keys/{key}` | raw bytes | `200` | Write. Routed to the key's shard; redirects `307` to that shard's leader if needed. |
| `GET` | `/v1/keys/{key}` | - | raw bytes | Read. Default consistency is linearizable (redirects `307` to the shard leader if needed); `?consistency=stale` reads locally instead. `404` if missing. |
| `DELETE` | `/v1/keys/{key}` | - | `200` | Write. Routed and redirected the same way as PUT. |
| `GET` | `/v1/keys?prefix={p}` | - | JSON array | Scan. Default consistency is stale (merges every shard's local replica); `?consistency=linearizable` fetches each shard's contribution from that shard's actual leader instead. |
| `GET` | `/v1/status` | - | JSON | Per-shard Raft state, leader address, log indices, term. |
| `POST` | `/v1/cluster/join` | JSON | `200` | Internal, used at startup by node2/node3. Adds the joining node as a voter to every shard's Raft group. |

**Status response:**
```json
{
  "node_id": "node1",
  "num_shards": 3,
  "shards": [
    {"shard_id": 0, "raft_state": "Leader",   "leader_addr": "172.19.0.2:9081", "commit_index": 14, "applied_index": 14, "term": 3},
    {"shard_id": 1, "raft_state": "Follower", "leader_addr": "172.19.0.3:9182", "commit_index": 9,  "applied_index": 9,  "term": 2},
    {"shard_id": 2, "raft_state": "Leader",   "leader_addr": "172.19.0.2:9281", "commit_index": 11, "applied_index": 11, "term": 3}
  ]
}
```

`leader_addr` is the shard's Raft transport address as `hashicorp/raft` advertises it, which is a resolved IP rather than the container's hostname. Fleet View resolves it back to a node ID for display; `shardkv-cli status` and the raw API show it as-is.

---

## Sharding

Shards are a fixed count, `SHARDKV_NUM_SHARDS` (default 3), set the same way on every node in the cluster. A key's shard is `FNV-1a(key) mod NUM_SHARDS` (`internal/shard.KeyToShard`), computed independently on every node from the key alone, so routing is deterministic and requires no coordination or lookup table.

Each shard is a fully independent Raft group: its own `raft.Raft` instance, its own BoltDB log store and stable store (under `<data-dir>/shard-{i}/`), its own filesystem snapshot directory, and its own Raft TCP transport port. On a physical node with base Raft port `P`, shard `i`'s Raft transport listens on `P + i*100` (`internal/cluster.ShardPortOffset`); the HTTP and metrics ports are shared by every shard on that node. In the default 3-node, 3-shard deployment, every node replicates every shard, so each shard has 3 replicas spread across the 3 nodes, and each of the 9 replicas runs its own Raft loop, election timer, and log.

This means:
- Each shard elects its own leader independently. At any moment, different shards can be led by different nodes.
- A shard's leader failing triggers an election only within that shard; other shards are unaffected.
- Losing a majority of one shard's replicas takes only that shard offline (no leader, writes return `503`); other shards keep serving normally.
- A node crash and restart replays or restores each shard's Raft state independently from that shard's own BoltDB files and snapshots.

---

## Read Consistency

Reads take an explicit `?consistency=` parameter with two modes:

- **`linearizable`** (the default for `GET`): only the key's shard leader answers. A non-leader replica redirects `307` to the leader, exactly like a write does. The leader confirms it is current with a `raft.Barrier()` (a no-op log entry committed and applied through the normal Raft path) before answering, so the result reflects every write acknowledged before the read arrived. `SCAN` also supports this mode: for any shard the receiving node doesn't lead, it fetches that shard's contribution from the shard's actual leader over an internal endpoint (`/v1/internal/shards/{id}/scan`) and merges it in, rather than settling for a partial or stale local answer.
- **`stale`** (the default for `SCAN`): served immediately from whatever is in the local FSM, on whichever replica received the request, leader or follower, with no wait and no confirmation that it reflects the latest committed write. A replica that has fallen behind, or is on the wrong side of a network partition, can answer with data older than a caller might expect.

`SCAN` defaults to stale rather than linearizable because it already fans out across every shard; making that linearizable by default would turn one bulk read into a multi-way, leader-bound round trip on every call. `GET` defaults to linearizable because a single-key read has no such fan-out cost, and it matches the CP guarantee the rest of this README claims, correctness by default, speed as an opt-in trade a caller makes deliberately.

This replaces an earlier design where every read (`GET` and `SCAN` alike) polled a follower's `AppliedIndex >= CommitIndex` and served locally after up to a 1-second wait, falling back to whatever was available even if that wait expired. That scheme is not linearizable: it proves a replica has applied everything *it currently knows* is committed, but doesn't rule out that replica being stale or partitioned and simply unaware of a newer commit. `stale` mode names that tradeoff explicitly instead of applying it silently as the only option.

---

## Tests

```bash
# Full suite with race detector (recommended)
make test

# Unit tests only
go test ./internal/...

# Integration tests, in-process multi-node clusters, no Docker required
go test ./integration/... -v -timeout 120s
```

`integration/cluster_test.go` exercises the single-Raft-group primitive every shard is built from: leader election, replication, failover, deletion, scan, and a 50-key write/read check.

`integration/shard_cluster_test.go` exercises the sharded cluster on top of that:

- deterministic key-to-shard placement, agreeing across every node
- replication within a single shard, across all of that shard's replicas
- independent per-shard leader election (forcing one shard's leader to step down does not change any other shard's leader)
- per-shard leader failover, with writes resuming against the new leader
- cross-shard consistency for a key set that spans every shard
- full-cluster crash recovery, restarting every node and verifying every shard's data survives from its BoltDB log/snapshot
- one shard losing quorum (2 of 3 replicas killed) without affecting reads or writes on any other shard
- prefix scan and delete merged correctly across shards

`integration/consistency_test.go` spins up a real cluster of `server.Server` instances (not just the lower-level `node.Node` type) to test the HTTP layer's read consistency behavior specifically:

- a default `GET` against a replica that isn't the key's shard leader gets a `307` to the leader, and following it returns the correct value
- `?consistency=stale` is served locally by a non-leader with no redirect
- an invalid `?consistency=` value is rejected with `400`
- a linearizable scan issued against a node that doesn't lead every shard touched by the result still returns complete, correct data, proving the internal per-shard fan-out to each shard's actual leader works end to end

---

## Benchmark

```bash
make bench

# Custom parameters:
./bin/bench --addr localhost:8081,localhost:8082,localhost:8083 --ops 50000 --concurrency 64 --ratio 0.8 --consistency linearizable
```

The benchmark tool no longer assumes one cluster-wide leader. It spreads requests across every node address you give it; a write, or a linearizable read, that lands on a node that is not the leader for that key's shard is transparently redirected by the server to the correct shard leader (the same `307` mechanism the CLI uses), so the tool works correctly regardless of which node currently leads which shard. `--consistency` selects which mode the read fraction of the workload uses; it defaults to `linearizable`, matching the server's own default.

**Methodology:** 3 ShardKV processes (`node1`/`node2`/`node3`), 3 shards each, all run as plain OS processes on one machine (no Docker; loopback networking with hostname aliases for `node1`/`node2`/`node3`). Each run performs 50,000 operations (80% reads, 20% writes) at 64 concurrent workers, pre-seeded with 100 keys, requests spread across all 3 node addresses. Reported numbers are from 3 consecutive runs per mode against a freshly started cluster; no shard leader changed during any run.

**Test environment:** 4-core x86_64 (Intel Xeon, 2.80GHz), 15 GiB RAM, Linux, Go 1.24.

**Results (3 runs per mode, 150,000 total operations per mode, 0 errors):**

```
                  linearizable (default)       stale
Ops/sec:          2,846 - 3,162                6,371 - 6,789
Latency p50:      19.1 - 21.3 ms                5.9 - 6.4 ms
Latency p99:      43.9 - 47.3 ms                42.2 - 44.1 ms
Latency p999:     58.6 - 107.2 ms               58.3 - 61.7 ms
Errors:           0                             0
```

That gap is the consistency/latency tradeoff made concrete: `linearizable` reads pay for a redirect to the shard leader (when the request didn't already land there) plus a `raft.Barrier()` round trip before answering; `stale` reads never wait or redirect at all, they just return whatever the receiving replica has. Neither number is comparable to, or reuses, any previously published benchmark for this project; both are fresh measurements of the current code on this machine, taken together specifically to show the tradeoff rather than a single headline number.

---

## Observability

Prometheus scrapes all 3 nodes every 5 seconds, and Grafana has the Prometheus datasource and the ShardKV dashboard both auto-provisioned, so there's genuinely nothing to configure by hand. Open `http://localhost:3000` (no login) and go to Dashboards -> ShardKV, or jump directly to `http://localhost:3000/d/shardkv-main`.

If you'd rather poke at the raw metrics yourself, Prometheus's own UI is at `http://localhost:9090`. It opens on an empty query box that just says "No data queried yet", which is normal and not a sign anything's broken, it's simply waiting for you to type something. Try `up` and hit Execute to confirm all 3 nodes are being scraped, or `shardkv_raft_state` to see each shard's current Raft role.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `shardkv_operations_total` | Counter | `op`, `status`, `shard` | Total operations. `shard` is `all` for scans, which span every shard. |
| `shardkv_operation_duration_seconds` | Histogram | `op`, `shard` | Request latency per operation type and shard. |
| `shardkv_raft_state` | Gauge | `shard` | Per-shard Raft state on this node: `0`=Follower, `1`=Candidate, `2`=Leader, `3`=Shutdown. |
| `shardkv_raft_commit_index` | Gauge | `shard` | Last committed log index, per shard. |
| `shardkv_raft_applied_index` | Gauge | `shard` | Last applied log index, per shard. |
| `shardkv_shard_replication_lag` | Gauge | `shard` | Commit index minus applied index, per shard, on this node. |

Prometheus's own target relabeling adds a `node` label identifying which physical node each series came from, so every panel can be sliced by node and shard together.

---

## Development

```bash
make build   # compile server, shardkv-cli, bench
make test    # go test -race -timeout 120s ./...
make up      # docker compose up -d --build
make down    # docker compose down
make logs    # docker compose logs -f
make bench   # run load generator against localhost:8081/8082/8083
make demo    # automated per-shard leader-failover demonstration
```

---

## Docker Compose ports

| Service | HTTP | Raft (shard 0 / 1 / 2) | Metrics |
|---|---|---|---|
| node1 | 8081 | 9081 / 9181 / 9281 | 10081 |
| node2 | 8082 | 9082 / 9182 / 9282 | 10082 |
| node3 | 8083 | 9083 / 9183 / 9283 | 10083 |
| Prometheus | 9090 | - | - |
| Grafana | 3000 | - | - |

Shard `i`'s Raft port is always the node's base Raft port plus `i*100`.

---

## Design decisions

**Why `hashicorp/raft` instead of building from scratch?**
The interesting engineering here is the sharding layer, the storage layer, the HTTP API, the observability, and the end-to-end system behavior across multiple independent consensus groups, not reimplementing a consensus algorithm that already has a production-proven form. `hashicorp/raft` is the same library Consul uses. Running 9 independent instances of it correctly, with per-shard storage, transport, and failure isolation, is itself non-trivial. Building Raft from scratch is planned as a later milestone.

**Why hash-mod sharding instead of consistent hashing?**
With a fixed shard count known to every node at startup, `hash(key) mod N` gives fully deterministic routing with no coordination, no lookup table, and no extra network hop. Consistent hashing earns its complexity when shards are added or removed at runtime; this phase does not support live resharding, so the simpler scheme is the right one for what exists today.

**Why full replication of every shard on every node, instead of assigning shards to a subset of nodes?**
It keeps the deployment and the mental model simple: every node can answer any read locally, and the join/bootstrap logic is the same for every shard. The tradeoff is that this topology does not let you add capacity by adding nodes without also adding shards; a node-subset assignment scheme is a natural next step once resharding exists.

**Why BoltDB instead of a custom WAL?**
BoltDB gives ACID transactions, a clean Go API, and no external process. Each shard replica gets its own BoltDB log store and stable store, so shards never contend on file locks or storage state. The `LogStore` interface is swappable; replacing it with BadgerDB or a custom LSM tree is a single file change.

**Why HTTP instead of gRPC for the client API?**
HTTP is easy to benchmark with standard tools, easy to test with `curl`, and simple to demonstrate. The internal Raft transport uses `hashicorp/raft`'s TCP transport directly, once per shard. gRPC for the client API is a future milestone.

**Why linearizable reads redirect to the leader instead of using a follower read protocol**
`raft.Barrier()` submits a log entry and blocks until it commits, which requires leader status; `hashicorp/raft` has no ReadIndex-style primitive for a follower to serve a confirmed-current read without one. Redirecting a linearizable read to the leader, the same way a write already redirects, is the honest option that fits the library, rather than approximating a read-index protocol with a follower's own bookkeeping. See [Read Consistency](#read-consistency) above for the full explanation, including the `stale` mode that skips this entirely.

**CAP theorem, per shard**
Each shard is independently **CP**: consistent and partition-tolerant. During a majority partition within one shard (2+ of its 3 replicas unreachable), that shard's remaining minority stops accepting writes rather than risk split-brain, while every other shard continues operating normally.

---

## Roadmap

See [`docs/ROADMAP.md`](docs/ROADMAP.md) for the full plan.

| Version | Description |
|---|---|
| **v0.1.0** | 3-node Raft cluster, persistence, CLI, Docker Compose, benchmarks (done) |
| **v0.2.0** | Fleet View, real-time spaceship visualization of cluster state (done) |
| **v0.3.0** | Multi-shard architecture: deterministic routing, independent Raft groups per shard, per-shard failover, shard-aware Fleet View and metrics (done) |
| v0.4.0 | Raft from scratch (replaces `hashicorp/raft`) |
| v0.5.0 | Live resharding: adding/removing shards and nodes without downtime |

---

## License

MIT, see [LICENSE](LICENSE).
