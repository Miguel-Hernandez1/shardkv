// Package shard implements deterministic key-to-shard routing.
package shard

import "hash/fnv"

// DefaultCount is the number of shards used when a node's configuration
// does not override it.
const DefaultCount = 3

// KeyToShard deterministically maps a key to a shard index in [0, numShards).
// The mapping depends only on the key bytes and numShards, so it is stable
// across processes, restarts, and shard replicas as long as numShards is
// unchanged.
func KeyToShard(key string, numShards int) int {
	if numShards <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(numShards))
}
