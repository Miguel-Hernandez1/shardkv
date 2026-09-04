// Package configgroup implements the cluster's shard placement: which
// physical nodes replicate which shard. It is itself a small, independent
// Raft group (the "config group") so every node agrees on the current
// assignment and on when it changes, the same way the KV shards agree on
// their own committed state.
package configgroup

import (
	"fmt"
	"sort"
)

// Assignment maps a shard index to the node IDs that replicate it.
type Assignment map[int][]string

// Compute deterministically assigns every shard in [0, numShards) to
// replicationFactor distinct nodes, given the current node list. It does
// not attempt a provably optimal balance, just a simple, reproducible
// spread: shard i's replicas start at node index (i*replicationFactor)
// mod len(nodes) and take replicationFactor consecutive nodes from there,
// wrapping around. In practice this keeps every node's shard count within
// one of every other node's for the node counts and replication factors
// this project targets.
//
// nodes is sorted before use, so the result does not depend on the order
// the caller happened to build the slice in; the same set of node IDs
// always produces the same assignment.
func Compute(nodes []string, numShards, replicationFactor int) (Assignment, error) {
	if numShards <= 0 {
		return nil, fmt.Errorf("numShards must be > 0, got %d", numShards)
	}
	if replicationFactor <= 0 {
		return nil, fmt.Errorf("replicationFactor must be > 0, got %d", replicationFactor)
	}
	if len(nodes) < replicationFactor {
		return nil, fmt.Errorf("not enough nodes (%d) to satisfy replication factor %d", len(nodes), replicationFactor)
	}

	sorted := append([]string(nil), nodes...)
	sort.Strings(sorted)
	n := len(sorted)

	assignment := make(Assignment, numShards)
	for shard := 0; shard < numShards; shard++ {
		start := (shard * replicationFactor) % n
		replicas := make([]string, 0, replicationFactor)
		for r := 0; r < replicationFactor; r++ {
			replicas = append(replicas, sorted[(start+r)%n])
		}
		assignment[shard] = replicas
	}
	return assignment, nil
}

// Replicas returns the node IDs assigned to shard, or nil if the shard
// isn't in the assignment.
func (a Assignment) Replicas(shard int) []string {
	return a[shard]
}

// Hosts reports whether nodeID is one of shard's replicas.
func (a Assignment) Hosts(shard int, nodeID string) bool {
	for _, id := range a[shard] {
		if id == nodeID {
			return true
		}
	}
	return false
}

// ShardsFor returns every shard index nodeID replicates, in ascending
// order.
func (a Assignment) ShardsFor(nodeID string) []int {
	var shards []int
	for shard, replicas := range a {
		for _, id := range replicas {
			if id == nodeID {
				shards = append(shards, shard)
				break
			}
		}
	}
	sort.Ints(shards)
	return shards
}
