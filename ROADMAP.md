# ShardKV Roadmap

## v0.1.0 - Replicated KV Store (done)
- [x] 3-node Raft cluster with leader election and log replication
- [x] Persistent storage via BoltDB LogStore and StableStore
- [x] Snapshots for log compaction (threshold 64 entries, retain 2)
- [x] HTTP API: GET / PUT / DELETE / SCAN / STATUS
- [x] HTTP 307 leader forwarding with loop guard (X-ShardKV-Forwarded)
- [x] Read consistency: Barrier on leader, AppliedIndex poll on followers
- [x] CLI client (shardkv-cli): get / set / delete / scan / status
- [x] Docker Compose cluster (3 nodes + Prometheus + Grafana)
- [x] Prometheus metrics (operations, latency, Raft state, log indices)
- [x] Grafana dashboard (auto-provisioned, no login required)
- [x] Integration tests (in-process 3-node cluster, no Docker required)
- [x] Benchmark tool with p50/p99/p999 latency reporting
- [x] Automated failover demo script

## v0.2.0 - Fleet View (done)
- [x] Browser visualization served at `/fleet` by the existing Go server
- [x] Leader shown as command ship (gold stripe, larger); followers as support ships
- [x] Cargo packet animation for replicated log entries (commit index advance)
- [x] Leader election animation (gold expanding rings on new leader)
- [x] Node failure and recovery animation (dim ship, engine power-up on recovery)
- [x] Engineering Console streaming real-time cluster events with color coding
- [x] Cluster HUD: leader ID, term, commit index, online count, replication lag
- [x] Formation lines between leader and followers
- [x] Host-facing address resolution so browser can poll all nodes from localhost
- [x] Human-readable node IDs in display (resolves Docker internal IPs)
- [x] CORS middleware enabling cross-origin status polling

## v0.3.0 - Sharding (done)
- [x] Deterministic key-space partitioning: FNV-1a hash mod shard count (`internal/shard`)
- [x] Multiple independent Raft groups, one per shard, each with its own BoltDB
      log/stable store, snapshot directory, and Raft TCP transport port
- [x] Router layer: every node computes a key's shard locally and either
      serves it or redirects writes to that shard's current leader
- [x] Prefix scan merged across every shard replica on a node
- [x] Independent per-shard leader election and failover, verified by
      integration tests that force one shard's leader down and confirm
      other shards' leaders are unaffected
- [x] Crash recovery verified per shard: a full-cluster restart replays or
      restores each shard's Raft state from its own BoltDB files
- [x] Shard isolation verified: a shard losing quorum (2 of 3 replicas down)
      does not affect reads or writes on any other shard
- [x] Fleet View rebuilt around a shard membership matrix (per-shard leader,
      replica health, commit index, replication lag) plus an animated view
      for a selectable shard
- [x] Prometheus metrics labeled by shard: Raft state, commit/applied index,
      replication lag, operation counts and latency
- [x] Benchmark tool updated to spread load across all nodes and shard
      leaders instead of assuming one cluster-wide leader

## v0.3.1 - Linearizable Reads (done)
- [x] Get/Scan take an explicit consistency: Linearizable (leader-only,
      confirmed with raft.Barrier()) or Stale (immediate local read, no
      wait, no freshness guarantee)
- [x] GET defaults to linearizable and redirects to the shard leader with
      the same 307 mechanism a write uses; `?consistency=stale` opts out
- [x] SCAN defaults to stale (it already fans out across every shard);
      `?consistency=linearizable` fetches each shard's contribution from
      that shard's actual leader over a new internal endpoint instead of
      settling for a partial or stale local answer
- [x] Replaces the old scheme (follower polls AppliedIndex >= CommitIndex,
      serves local state anyway after a 1s timeout even if not caught up),
      which was not actually linearizable
- [x] shardkv-cli and bench expose --consistency to exercise both modes
- [x] Benchmarked both modes head to head to make the latency/consistency
      tradeoff concrete rather than theoretical

## v0.4.0 - Raft From Scratch
- [ ] Replace `hashicorp/raft` with a hand-written Raft implementation
- [ ] Leader election, log replication, snapshotting; no membership changes
- [ ] Full test coverage including chaos testing

## v0.5.0 - Live Resharding
- [ ] Add or remove shards without downtime
- [ ] Move a shard's replicas between nodes to rebalance
- [ ] Consistent hashing (or an explicit shard map) once shard placement
      needs to change at runtime instead of being fixed at startup

## Future
- Cluster membership changes (joint consensus)
- gRPC client API
- TLS between nodes
- True Raft ReadIndex optimization (linearizable reads without committing
  a log entry via Barrier; hashicorp/raft has no built-in primitive for
  this, so it would mean tracking leadership confirmations manually)
- TTL / key expiry
- Watch / subscribe
