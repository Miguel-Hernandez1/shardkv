# ShardKV Architecture

## Overview

ShardKV is a distributed, sharded key-value store built with [Raft consensus](https://raft.github.io/). Keys are partitioned across a fixed number of shards by a deterministic hash, and each shard is replicated by its own independent Raft group. The default deployment runs 3 physical nodes and 3 shards, so every node hosts a replica of every shard: 9 Raft groups in total, each with its own leader, log, and failure domain. The cluster can lose any one node, or a minority of any single shard's replicas, and continue serving. Data is persisted to disk and survives full cluster restarts.

```
         +--------------------------------------------+
         |               Client Layer                  |
         |   shardkv-cli / curl / bench                |
         +---------------+------------------------------+
                         | HTTP (port 8081/8082/8083)
         +---------------v------------------------------+
         |         3-Node Cluster, 3 Shards Each         |
         |                                               |
         |  node1              node2              node3  |
         |  HTTP :8081         HTTP :8082         HTTP :8083 |
         |                                               |
         |  shard 0 replica <-> shard 0 replica <-> shard 0 replica |
         |    Raft :9081         Raft :9082         Raft :9083     |
         |  shard 1 replica <-> shard 1 replica <-> shard 1 replica |
         |    Raft :9181         Raft :9182         Raft :9183     |
         |  shard 2 replica <-> shard 2 replica <-> shard 2 replica |
         |    Raft :9281         Raft :9282         Raft :9283     |
         |                                               |
         |  Each replica: own BoltDB LogStore + StableStore,       |
         |  own snapshot directory, own FSM, own Raft leader.      |
         +---------------+------------------------------+
                         |
         +---------------v------------------------------+
         |          Observability Stack                  |
         |  Prometheus :9090    Grafana :3000             |
         +------------------------------------------------+
```

## Sharding

`internal/shard.KeyToShard(key, numShards)` hashes a key with FNV-1a and takes it mod the shard count. Every node runs with the same `SHARDKV_NUM_SHARDS`, so this computation is deterministic and identical on every node: no coordination, no lookup table, no extra network hop to find out which shard a key belongs to.

`internal/cluster.Manager` is the per-process owner of every shard replica this node hosts. On startup it creates one `node.Node` (one Raft group replica) per shard, each with:

- its own `raft.Raft` instance and election timer
- its own BoltDB log store and stable store, under `<data-dir>/shard-{i}/log.db` and `stable.db`
- its own filesystem snapshot directory, `<data-dir>/shard-{i}/`
- its own Raft TCP transport, listening on `base_raft_port + i * 100` (`internal/cluster.ShardPortOffset`)

Because each shard's Raft group is a fully separate `raft.Raft` instance with its own goroutines, storage, and transport, one shard's election, failure, or slow follower has no effect on any other shard's Raft loop.

HTTP and metrics ports are shared across every shard on a node; there is exactly one HTTP listener and one `/metrics` listener per physical process, regardless of shard count.

## Consensus: Raft, per shard

ShardKV uses [`hashicorp/raft`](https://github.com/hashicorp/raft), the same library used by HashiCorp Consul, Vault, and Nomad, once per shard.

**Leader election**: each shard's replicas run independent election timers. When a shard replica's timer fires without hearing from that shard's leader, it becomes a candidate and requests votes from the other replicas of that same shard. A replica wins if it receives votes from a majority of that shard's replicas (2 of 3 in the default topology). This guarantees only one leader per shard per term, with no coordination between shards.

**Log replication**: a write for a key goes to that key's shard's leader. The leader appends the command to that shard's log and sends `AppendEntries` RPCs to that shard's followers. Once a majority of that shard's replicas acknowledge, the entry is committed and applied to that shard's FSM.

**Safety**: within each shard, Raft ensures that only a replica with the most up-to-date log for that shard can be elected its leader.

**Snapshotting**: when a shard's log grows beyond 64 entries, that shard's leader snapshots its FSM state to disk and truncates its own log. A replica of that shard that falls too far behind receives a snapshot instead of individual log entries. Snapshotting for one shard has no effect on any other shard's log.

## Storage Layer

Each shard replica writes to its own subdirectory, `<data-dir>/shard-{i}/`:

| File | Purpose |
|---|---|
| `log.db` | BoltDB database; stores this shard's Raft log entries |
| `stable.db` | BoltDB database; stores this shard's current term and last vote |
| snapshot files | Managed by `raft.FileSnapshotStore`, one set per shard |

BoltDB was chosen for its simplicity: single-file, no external process, ACID transactions, good Go API. Giving each shard its own BoltDB files means shards never contend on file locks or storage state, and a shard's data is fully self-contained on disk.

`Node.Shutdown` closes both the log store and the stable store after shutting down Raft. This matters beyond tidiness: bbolt takes an exclusive file lock on open, so a process that restarts a shard replica against the same data directory without the previous handle being closed would block forever waiting for a lock nobody released.

The `LogStore` and `StableStore` implement interfaces defined by `hashicorp/raft`, so the storage backend is cleanly swappable, per shard if desired.

## State Machine (FSM)

Each shard replica's FSM is an in-memory `map[string][]byte` protected by a `sync.RWMutex`, scoped to that shard's key space. All mutations (SET, DELETE) flow through that shard's Raft log. Reads use `raft.Barrier()` on that shard's leader to ensure the local applied index is current before reading, or poll `AppliedIndex >= CommitIndex` on a follower.

Snapshots encode a shard's map to JSON. Restore decodes the JSON into a fresh map. This is correct because snapshots only happen after a quorum of that shard's replicas has acknowledged all entries up to the snapshot index.

## HTTP API and Routing

Client-facing API on port 8081/8082/8083, shared by every shard on a node. On each request, the server computes `shard.KeyToShard(key, numShards)` to find the target shard, then either serves it against the local replica of that shard or, for writes, checks whether the local replica is that shard's leader. If not, it returns an HTTP `307` to that shard's leader specifically, not to a single cluster-wide leader.

A `X-ShardKV-Forwarded: 1` header prevents redirect loops during elections: any request carrying this header gets a `503 Service Unavailable` instead of a second redirect.

Prefix scans fan out across every shard replica hosted locally and merge the results. Since a key belongs to exactly one shard, there is no possibility of a key appearing in more than one shard's results.

Cluster join (`POST /v1/cluster/join`) adds a new physical node as a voter to every shard's Raft group in one request: `cluster.Manager.AddVoter` loops over every shard and calls `AddVoter` on that shard's leader replica.

## CAP Theorem, per shard

Each shard is independently **CP** (Consistent + Partition Tolerant):

- **Consistent**: reads on a shard go through that shard's Raft barrier or applied-index check; no stale reads. Writes to a shard are linearizable within that shard.
- **Partition Tolerant**: a shard survives the loss of a minority of its own replicas (1 of 3, in the default topology).
- **Not available during a majority partition of one shard**: if 2 or more of a shard's 3 replicas are unreachable, that shard cannot form a quorum and stops accepting writes, returning `503`. Every other shard is unaffected and continues operating normally.

## Failure Modes

| Scenario | Behavior |
|---|---|
| One shard's leader crashes | That shard's surviving replicas detect the timeout and elect a new leader. Writes to that shard resume in ~150-500ms. Other shards are unaffected. |
| One shard's follower crashes | That shard continues normally with 2 replicas. The dead replica catches up on restart. |
| A majority of one shard's replicas crash | That shard has no quorum; writes to it return 503. Other shards keep serving. The shard recovers once a second of its replicas rejoins. |
| A physical node crashes | Every shard hosted on that node loses one replica. Each affected shard's remaining replicas continue independently; a shard only becomes unavailable if it loses a majority. |
| Full cluster restart | Every node replays or restores each of its shard replicas from that shard's own BoltDB log and snapshot files. Every shard re-elects a leader independently. No data loss. |
| Network partition (1 node isolated) | Every shard replica on the isolated node cannot win its shard's election (no quorum for that shard from one replica). Each shard elects a leader from its remaining reachable replicas. |

## Design Decisions

**Why `hashicorp/raft` instead of building from scratch?**
Raft bugs are subtle and hard to catch without weeks of chaos testing, and this design runs 9 independent instances of it. The interesting work, sharding, storage backends per shard, the FSM, the API, the observability, is still 100% custom. A future milestone plans to replace the library with a hand-written Raft as an educational exercise.

**Why hash-mod sharding instead of consistent hashing?**
The shard count is fixed at startup and identical on every node, so `hash(key) mod N` gives fully deterministic, coordination-free routing. Consistent hashing is worth its complexity once shards can be added or removed at runtime; that is a planned future milestone (live resharding), not something this phase supports.

**Why BoltDB instead of a WAL?**
BoltDB gives ACID guarantees without managing file offsets, checksums, and corruption recovery manually. Giving each shard its own BoltDB files keeps shards' storage fully isolated from one another. The tradeoff is slightly higher write amplification per shard, which is acceptable at this scale. The `LogStore` interface means swapping to a custom WAL or BadgerDB is a single implementation change.

**Why HTTP instead of gRPC for the client API?**
HTTP is easier to demo, easier to benchmark with standard tools, and simpler to test. The internal Raft transport uses TCP directly (via `hashicorp/raft`'s `TCPTransport`), once per shard. gRPC for the client API is a future milestone.

**Why reads through Barrier instead of leader-only?**
`raft.Barrier()` allows any replica of a shard to serve reads while still guaranteeing linearizability within that shard. This avoids all read traffic for a shard concentrating on that shard's leader.

## Port Conventions

| Service | HTTP | Raft TCP (shard 0 / 1 / 2) | Metrics |
|---|---|---|---|
| node1 | 8081 | 9081 / 9181 / 9281 | 10081 |
| node2 | 8082 | 9082 / 9182 / 9282 | 10082 |
| node3 | 8083 | 9083 / 9183 / 9283 | 10083 |
| Prometheus | 9090 | - | - |
| Grafana | 3000 | - | - |

HTTP port = shard 0's Raft port minus 1000. Metrics port = shard 0's Raft port plus 1000. Shard `i`'s Raft port = shard 0's Raft port plus `i * 100`. This convention is used for leader redirect URL construction: given a shard leader's Raft address and the shard index, the server subtracts `i * 100` to recover the leading node's base Raft port, then subtracts 1000 to get that node's HTTP port.
