# ShardKV

A distributed key-value store written in Go.

Three nodes. Raft consensus. Persistent storage. Leader election. Fault tolerant. Observable.

---

## What it is

ShardKV is a CP distributed key-value store that replicates every write across a 3-node Raft cluster before acknowledging success. If the leader crashes, the surviving nodes elect a new leader in ~150–500 ms and continue accepting writes. When the dead node comes back, it replays the Raft log and catches up automatically. All of this is visible in real time on a browser dashboard.

The name reflects the intended architecture. The current version ships one shard with full replication. Consistent hashing and multiple Raft groups are on the roadmap.

## Why it matters

Building this requires holding the full distributed systems stack in your head at once:

- **Consensus** — Raft leader election and log replication (via `hashicorp/raft`, the same library used by Consul, Vault, and Nomad)
- **Persistence** — BoltDB-backed Raft log store and stable store; file-based snapshots for log compaction
- **Crash recovery** — a node that restarts replays the log or restores from a snapshot; no data loss
- **Read consistency** — leader uses `raft.Barrier()` before reads; followers poll until `AppliedIndex ≥ CommitIndex`
- **Fault tolerance** — the cluster survives the loss of any one node and self-heals when it returns
- **Observability** — Prometheus metrics, Grafana dashboard, and a real-time browser visualization

---

## Screenshots

### Fleet View — Real-time cluster visualization

> Open `http://localhost:8081/fleet` after `make up`

![Fleet View](docs/screenshots/fleet-view.png)

*Three-node cluster in healthy state. Gold ship = Raft leader. Blue ships = followers. Cyan packets = log entries replicating. Gold rings = election animation on leader change.*

### Grafana Dashboard

> Open `http://localhost:3000` after `make up` (no login required)

<img width="800" alt="image" src="https://github.com/user-attachments/assets/0fa4b608-c3fb-4aaf-bbfe-cdb2750024ba" />

*Live metrics: operations per second, request latency (p50/p99), Raft state per node, commit vs applied index.*

### CLI — Leader failover demo

> Run `make demo` after `make up`

<img width="800" alt="image" src="https://github.com/user-attachments/assets/4623a874-067f-4759-b087-dd47fc24e74c" />

*Automated failover: write to leader → pause leader → new election → write to new leader → revive original → verify consistency across all nodes.*

---

## Architecture

```
  ┌──────────────────────────────────────────────────────────┐
  │                     Client Layer                          │
  │         shardkv-cli  /  curl  /  bench                    │
  └───────────────────────────┬──────────────────────────────┘
                              │ HTTP  :8081 / :8082 / :8083
  ┌───────────────────────────▼──────────────────────────────┐
  │                   3-Node Raft Cluster                     │
  │                                                           │
  │   ┌──────────────────────────────────────────────────┐   │
  │   │  node1 (Leader)                                  │   │
  │   │  HTTP :8081  │  Raft TCP :9081  │  Metrics :10081│   │
  │   │  FSM: map[string][]byte                          │   │
  │   │  LogStore: BoltDB   StableStore: BoltDB          │   │
  │   │  Snapshots: filesystem                           │   │
  │   └──────────────┬──────────────────┬───────────────┘   │
  │                  │    Raft RPC       │                    │
  │   ┌──────────────▼──────┐  ┌────────▼──────────────┐    │
  │   │  node2 (Follower)   │  │  node3 (Follower)     │    │
  │   │  :8082 / :9082      │  │  :8083 / :9083        │    │
  │   └─────────────────────┘  └───────────────────────┘    │
  └──────────────────────────────────────────────────────────┘
                              │
  ┌───────────────────────────▼──────────────────────────────┐
  │              Observability Stack                          │
  │         Prometheus :9090     Grafana :3000                │
  └──────────────────────────────────────────────────────────┘
```

**Write path:** client → any node → follower returns `307` to leader → leader applies to Raft log → quorum commits → FSM applies → `200 OK`

**Read path:** client → any node → node waits until `AppliedIndex ≥ CommitIndex` → reads local FSM → returns value

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

Docker Compose starts 3 ShardKV nodes, Prometheus, and Grafana. Node1 bootstraps the cluster and is elected leader. Nodes 2 and 3 join automatically.

Wait ~10 seconds, then:

```bash
./bin/shardkv-cli status
# Node ID:       node1
# State:         Leader
# Leader:        node1:9081
# Commit Index:  4
# Applied Index: 4
```

Open **Grafana** at `http://localhost:3000` — dashboard loads automatically, no login required.

Open **Fleet View** at `http://localhost:8081/fleet` — real-time browser visualization of the Raft cluster.

---

## Fleet View

Fleet View is a browser-based visualization that renders the three-node cluster as a fleet of spaceships. It polls each node's `/v1/status` endpoint every 1.5 seconds and reflects the current Raft state without requiring WebSockets or a separate server.

```
http://localhost:8081/fleet
```

| Visual element | Meaning |
|---|---|
| Large angular ship with gold stripe | Raft leader |
| Smaller ship with blue nav lights | Raft follower |
| Pulsing purple ring | Candidate (election in progress) |
| Dimmed / greyscale ship | Node unreachable |
| Cyan packet traveling between ships | Log entry replicating to followers |
| Gold expanding rings | New leader elected |

**Cluster HUD** (top left) shows: current leader, Raft term, commit index, online node count, replication lag.

**Engineering Console** (bottom) streams cluster events in real time:

| Event | Color |
|---|---|
| Leader elected | Gold |
| Node recovered | Green |
| Node unreachable | Red |
| Log entry committed | Cyan |
| Term advance / state change | Muted blue |

**To see it in action:**
1. `make up` — start the cluster
2. Open `http://localhost:8081/fleet`
3. `make demo` in another terminal — watch leader election, cargo packet animations, and recovery

---

## CLI

```bash
# Write a key (automatically routed to leader)
./bin/shardkv-cli set user:1 miguel

# Read from any node
./bin/shardkv-cli get user:1                        # from node1 (leader)
./bin/shardkv-cli --addr localhost:8082 get user:1  # from node2 (follower)
./bin/shardkv-cli --addr localhost:8083 get user:1  # from node3 (follower)

# Delete
./bin/shardkv-cli delete user:1

# Scan by prefix
./bin/shardkv-cli scan --prefix user:

# Node status
./bin/shardkv-cli --addr localhost:8082 status
```

Writes sent to a follower receive an HTTP 307 redirect to the leader. The CLI follows it automatically.

---

## Verified Demo: Leader Failover

This is the exact sequence verified against the running cluster.

```bash
# 1. Write a key to the leader
./bin/shardkv-cli set user:1 miguel
# → OK

# 2. Confirm the value is readable from all nodes
./bin/shardkv-cli --addr localhost:8081 get user:1  # → miguel
./bin/shardkv-cli --addr localhost:8082 get user:1  # → miguel
./bin/shardkv-cli --addr localhost:8083 get user:1  # → miguel

# 3. Stop node1 (the current leader)
docker pause shardkv-node1-1

# 4. node2 or node3 is elected the new leader (~300ms)
./bin/shardkv-cli --addr localhost:8082 status
# → State: Leader

# 5. Write a new key to the new leader
./bin/shardkv-cli --addr localhost:8082 set user:2 amazon
# → OK

# 6. Restart the original leader
docker unpause shardkv-node1-1

# 7. Wait a few seconds for log catch-up, then verify both keys
./bin/shardkv-cli --addr localhost:8081 get user:1  # → miguel
./bin/shardkv-cli --addr localhost:8081 get user:2  # → amazon
```

Or run the automated version:

```bash
make demo
```

---

## HTTP API

All endpoints are available on every node.

| Method | Path | Body | Response | Notes |
|---|---|---|---|---|
| `PUT` | `/v1/keys/{key}` | raw bytes | `200` | Write. Followers return `307` to leader. |
| `GET` | `/v1/keys/{key}` | — | raw bytes | Read from any node. `404` if missing. |
| `DELETE` | `/v1/keys/{key}` | — | `200` | Write. Followers return `307` to leader. |
| `GET` | `/v1/keys?prefix={p}` | — | JSON array | Scan. Reads from any node. |
| `GET` | `/v1/status` | — | JSON | Node state, leader address, log indices, term. |
| `POST` | `/v1/cluster/join` | JSON | `200` | Internal — used at startup by node2/node3. |

**Status response:**
```json
{
  "node_id":       "node1",
  "raft_state":    "Leader",
  "leader_addr":   "node1:9081",
  "commit_index":  14,
  "applied_index": 14,
  "term":          3
}
```

---

## Tests

```bash
# Full suite with race detector (recommended)
make test

# Unit tests only
go test ./internal/...

# Integration tests — in-process 3-node cluster, no Docker required
go test ./integration/... -v -timeout 120s
```

Integration tests cover:

- Leader election (exactly one leader after bootstrap)
- Basic replication (write on leader, read from all nodes)
- Leader failover (shut down leader, elect new one, write, verify)
- Key deletion
- Prefix scan
- 50-key write and full cluster read verification

---

## Benchmark

```bash
make bench

# Custom parameters:
./bin/bench --ops 50000 --concurrency 64 --ratio 0.8
```

The benchmark tool auto-discovers the leader via `/v1/status` before starting.

Sample output (MacBook Pro M3, Docker Desktop):

```
Leader: localhost:8081

=== Benchmark Results ===
Operations:   50000
Duration:     8.1s
Ops/sec:      6172
Errors:       0
Latency p50:  3.9ms
Latency p99:  16.2ms
Latency p999: 38.5ms
```

---

## Observability

Prometheus scrapes all 3 nodes every 5 seconds. Grafana auto-provisions the datasource and dashboard. Open `http://localhost:3000` — no login, no manual configuration.

| Metric | Type | Description |
|---|---|---|
| `shardkv_operations_total` | Counter | Total operations, labeled by `op` and `status` |
| `shardkv_operation_duration_seconds` | Histogram | Request latency per operation type |
| `shardkv_raft_state` | Gauge | Per-node Raft state: `0`=Follower, `1`=Candidate, `2`=Leader |
| `shardkv_raft_commit_index` | Gauge | Last committed log index |
| `shardkv_raft_applied_index` | Gauge | Last applied log index |

---

## Development

```bash
make build   # compile server, shardkv-cli, bench
make test    # go test -race -timeout 120s ./...
make up      # docker compose up -d --build
make down    # docker compose down
make logs    # docker compose logs -f
make bench   # run load generator against localhost:8081
make demo    # automated leader-failover demonstration
```

---

## Docker Compose ports

| Service | HTTP | Raft | Metrics |
|---|---|---|---|
| node1 | 8081 | 9081 | 10081 |
| node2 | 8082 | 9082 | 10082 |
| node3 | 8083 | 9083 | 10083 |
| Prometheus | 9090 | — | — |
| Grafana | 3000 | — | — |

---

## Design decisions

**Why `hashicorp/raft` instead of building from scratch?**
The interesting engineering here is the storage layer, the HTTP API, the observability, and the end-to-end system behavior — not reimplementing a consensus algorithm that already has a production-proven form. `hashicorp/raft` is the same library Consul uses. Wiring it correctly with custom LogStore, StableStore, FSM, and transport is non-trivial. Building Raft from scratch is planned as a later milestone.

**Why BoltDB instead of a custom WAL?**
BoltDB gives ACID transactions, a clean Go API, and no external process. The `LogStore` interface is swappable — replacing it with BadgerDB or a custom LSM tree is a single file change.

**Why HTTP instead of gRPC for the client API?**
HTTP is easy to benchmark with standard tools, easy to test with `curl`, and simple to demonstrate. The internal Raft transport uses `hashicorp/raft`'s TCP transport directly. gRPC for the client API is a future milestone.

**Read consistency on followers**
`raft.Barrier()` submits a log entry and blocks until it commits — it requires leader status. On followers, reads are served after polling until `AppliedIndex ≥ CommitIndex`, which guarantees every locally-known committed entry has been applied to the FSM.

**CAP theorem**
ShardKV is **CP**: consistent and partition-tolerant. During a majority partition (2+ nodes unreachable), the remaining minority stops accepting writes rather than risk split-brain. Stale or conflicting data is worse than temporary unavailability.

---

## Roadmap

See [`ROADMAP.md`](ROADMAP.md) for the full plan.

| Version | Description |
|---|---|
| **v0.1.0** | 3-node Raft cluster, persistence, CLI, Docker Compose, benchmarks ✓ |
| **v0.2.0** | Fleet View — real-time spaceship visualization of cluster state ✓ |
| v0.3.0 | Raft from scratch (replaces `hashicorp/raft`) |
| v0.4.0 | Sharding — consistent hash ring, multiple Raft groups, router layer |

---

## License

MIT — see [LICENSE](LICENSE).
