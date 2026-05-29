package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	OperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shardkv_operations_total",
		Help: "Total number of KV operations.",
	}, []string{"op", "status"})

	OperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "shardkv_operation_duration_seconds",
		Help:    "Latency of KV operations.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"op"})

	// RaftState: 0=Follower, 1=Candidate, 2=Leader, 3=Shutdown
	RaftState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shardkv_raft_state",
		Help: "Current Raft state: 0=Follower 1=Candidate 2=Leader 3=Shutdown.",
	})

	RaftCommitIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shardkv_raft_commit_index",
		Help: "Last committed Raft log index.",
	})

	RaftAppliedIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shardkv_raft_applied_index",
		Help: "Last applied Raft log index.",
	})
)
