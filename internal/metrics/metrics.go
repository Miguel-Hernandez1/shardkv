package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	OperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shardkv_operations_total",
		Help: "Total number of KV operations, labeled by shard.",
	}, []string{"op", "status", "shard"})

	OperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "shardkv_operation_duration_seconds",
		Help:    "Latency of KV operations, labeled by shard.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"op", "shard"})

	// RaftState: 0=Follower, 1=Candidate, 2=Leader, 3=Shutdown. Labeled per
	// shard since each shard is an independent Raft group that can be in a
	// different state at the same time.
	RaftState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shardkv_raft_state",
		Help: "Current Raft state per shard: 0=Follower 1=Candidate 2=Leader 3=Shutdown.",
	}, []string{"shard"})

	RaftCommitIndex = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shardkv_raft_commit_index",
		Help: "Last committed Raft log index per shard.",
	}, []string{"shard"})

	RaftAppliedIndex = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shardkv_raft_applied_index",
		Help: "Last applied Raft log index per shard.",
	}, []string{"shard"})

	// ShardReplicationLag is the leader's commit index minus this node's
	// applied index for a shard. Zero on the leader; near zero on healthy
	// followers.
	ShardReplicationLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shardkv_shard_replication_lag",
		Help: "Commit index minus applied index, per shard, on this node.",
	}, []string{"shard"})
)
