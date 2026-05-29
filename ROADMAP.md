# ShardKV Roadmap

## v0.1.0 — Replicated KV Store ✓
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

## v0.2.0 — Fleet View ✓
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

## v0.3.0 — Raft From Scratch
- [ ] Replace `hashicorp/raft` with a hand-written Raft implementation
- [ ] Leader election, log replication, snapshotting — no membership changes
- [ ] Full test coverage including chaos testing

## v0.4.0 — Sharding
- [ ] Static key-space partitioning (consistent hash ring)
- [ ] Multiple Raft groups (one per shard)
- [ ] Router layer: routes client requests to the correct shard
- [ ] Cross-shard scan

## Future
- Cluster membership changes (joint consensus)
- gRPC client API
- TLS between nodes
- Read-index optimization (linearizable reads without Raft log entry)
- TTL / key expiry
- Watch / subscribe
