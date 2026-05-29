# ShardKV Architecture

## Overview

ShardKV is a 3-node distributed key-value store built with [Raft consensus](https://raft.github.io/). It prioritizes strong consistency and fault tolerance: the cluster can lose one node and continue serving reads and writes. Data is persisted to disk and survives full cluster restarts.

```
         ┌──────────────────────────────────────────┐
         │               Client Layer                │
         │   shardkv-cli / curl / bench              │
         └───────────────┬──────────────────────────┘
                         │ HTTP (port 8081/8082/8083)
         ┌───────────────▼──────────────────────────┐
         │             3-Node Cluster                │
         │                                           │
         │  ┌──────────────────────────────────┐    │
         │  │         Node-1 (Leader)           │    │
         │  │  HTTP API      :8081              │    │
         │  │  Raft TCP      :9081              │    │
         │  │  Metrics       :10081/metrics     │    │
         │  │  FSM           map[string][]byte  │    │
         │  │  LogStore      BoltDB (log.db)    │    │
         │  │  StableStore   BoltDB (stable.db) │    │
         │  │  SnapshotStore filesystem         │    │
         │  └────────┬─────────────┬────────────┘    │
         │           │  Raft TCP   │                  │
         │  ┌────────▼──────┐  ┌──▼─────────────┐   │
         │  │  Node-2       │  │  Node-3         │   │
         │  │  (Follower)   │  │  (Follower)     │   │
         │  │  :8082/:9082  │  │  :8083/:9083    │   │
         │  └───────────────┘  └─────────────────┘   │
         └──────────────────────────────────────────┘
                         │
         ┌───────────────▼──────────────────────────┐
         │          Observability Stack              │
         │  Prometheus :9090    Grafana :3000         │
         └──────────────────────────────────────────┘
```

## Consensus: Raft

ShardKV uses [`hashicorp/raft`](https://github.com/hashicorp/raft), the same library used by HashiCorp Consul, Vault, and Nomad.

**Leader election**: When a node's election timer fires without hearing from a leader, it becomes a candidate and requests votes. A node wins if it receives votes from a majority (2 of 3). This guarantees only one leader per term.

**Log replication**: All writes go to the leader. The leader appends the command to its log and sends `AppendEntries` RPCs to followers. Once a majority acknowledges, the entry is committed and applied to the FSM.

**Safety**: Raft ensures that only a node with the most up-to-date log can be elected leader. This prevents a lagging node from overwriting committed entries.

**Snapshotting**: When the log grows beyond 64 entries, the leader snapshots the full FSM state to disk and truncates the old log. Followers that fall too far behind receive a snapshot instead of individual log entries.

## Storage Layer

Each node writes to a local directory (`/data/{node-id}/`):

| File | Purpose |
|---|---|
| `log.db` | BoltDB database; stores Raft log entries |
| `stable.db` | BoltDB database; stores current term and last vote |
| `snapshots/` | Directory managed by `FileSnapshotStore` |

BoltDB was chosen for its simplicity: single-file, no external process, ACID transactions, good Go API.

The `LogStore` and `StableStore` implement interfaces defined by `hashicorp/raft`, so the storage backend is cleanly swappable.

## State Machine (FSM)

The FSM is an in-memory `map[string][]byte` protected by a `sync.RWMutex`. All mutations (SET, DELETE) flow through the Raft log. Reads use `raft.Barrier()` to ensure the local node's applied index is current before reading.

Snapshots encode the entire map to JSON. Restore decodes the JSON into a fresh map. This is correct because snapshots only happen after a quorum has acknowledged all entries up to the snapshot index.

## HTTP API

Client-facing API on port 8081/8082/8083. Writes arrive at any node; followers return HTTP 307 to the leader's address. The CLI follows the redirect automatically.

A `X-ShardKV-Forwarded: 1` header prevents redirect loops during elections: any request carrying this header gets a `503 Service Unavailable` instead of a second redirect.

## CAP Theorem

ShardKV is **CP** (Consistent + Partition Tolerant):

- **Consistent**: All reads go through Raft barriers; no stale reads. All writes are linearizable.
- **Partition Tolerant**: The cluster survives a minority partition (1 of 3 nodes).
- **Not Available during majority partition**: If 2+ nodes are unreachable, the remaining node cannot form a quorum and stops accepting writes. This is a deliberate safety tradeoff — a split cluster returning inconsistent data would be worse.

## Failure Modes

| Scenario | Behavior |
|---|---|
| Leader crashes | Followers detect timeout, elect new leader. Writes resume in ~150–500ms. |
| Follower crashes | Cluster continues normally with 2 nodes. Dead follower catches up on restart. |
| 2 nodes crash | No quorum; writes return 503. Cluster recovers when a second node rejoins. |
| Full cluster restart | All nodes replay their logs or restore from snapshot. Leader re-elected. Data intact. |
| Network partition (1 node isolated) | Isolated node cannot win election (no quorum). Cluster elects from remaining 2. |

## Design Decisions

**Why `hashicorp/raft` instead of building from scratch?**
Raft bugs are subtle and hard to catch without weeks of chaos testing. `hashicorp/raft` is battle-tested in production. The interesting work — storage backends, FSM, API, observability — is still 100% custom. V0.3.0 plans to replace the library with a hand-written Raft as an educational exercise.

**Why BoltDB instead of a WAL?**
BoltDB gives ACID guarantees without managing file offsets, checksums, and corruption recovery manually. The tradeoff is slightly higher write amplification, which is acceptable at this scale. The `LogStore` interface means swapping to a custom WAL or BadgerDB is a single implementation change.

**Why HTTP instead of gRPC for the client API?**
HTTP is easier to demo, easier to benchmark with standard tools, and simpler to test. The internal Raft transport uses TCP directly (via `hashicorp/raft`'s `TCPTransport`). gRPC for the client API is planned for V0.4.0.

**Why reads through Barrier instead of leader-only?**
`raft.Barrier()` allows any node to serve reads while still guaranteeing linearizability. This avoids all read traffic concentrating on the leader, even though all 3 nodes are on the same machine in the Docker demo.

## Port Conventions

| Service | HTTP | Raft TCP | Metrics |
|---|---|---|---|
| node1 | 8081 | 9081 | 10081 |
| node2 | 8082 | 9082 | 10082 |
| node3 | 8083 | 9083 | 10083 |
| Prometheus | 9090 | — | — |
| Grafana | 3000 | — | — |

HTTP port = Raft port − 1000. Metrics port = Raft port + 1000. This convention is used for leader redirect URL construction.
