package shard

import (
	"strconv"
	"testing"
)

func TestKeyToShardDeterministic(t *testing.T) {
	keys := []string{"user:1", "user:2", "product:1", "", "a-very-long-key-with-lots-of-characters-in-it"}
	for _, k := range keys {
		first := KeyToShard(k, 5)
		for i := 0; i < 100; i++ {
			if got := KeyToShard(k, 5); got != first {
				t.Fatalf("KeyToShard(%q, 5) not deterministic: got %d and %d", k, first, got)
			}
		}
	}
}

func TestKeyToShardInRange(t *testing.T) {
	for _, numShards := range []int{1, 2, 3, 5, 16} {
		for i := 0; i < 1000; i++ {
			key := randKey(i)
			s := KeyToShard(key, numShards)
			if s < 0 || s >= numShards {
				t.Fatalf("KeyToShard(%q, %d) = %d, out of range", key, numShards, s)
			}
		}
	}
}

func TestKeyToShardSingleShard(t *testing.T) {
	for i := 0; i < 100; i++ {
		key := randKey(i)
		if got := KeyToShard(key, 1); got != 0 {
			t.Fatalf("KeyToShard(%q, 1) = %d, want 0", key, got)
		}
	}
}

func TestKeyToShardDistribution(t *testing.T) {
	const numShards = 4
	counts := make([]int, numShards)
	const n = 10000
	for i := 0; i < n; i++ {
		counts[KeyToShard(randKey(i), numShards)]++
	}
	// Not a strict statistical test — just guards against a degenerate
	// mapping that dumps everything into one shard.
	for shardID, c := range counts {
		if c < n/numShards/4 {
			t.Fatalf("shard %d got only %d of %d keys, distribution looks broken", shardID, c, n)
		}
	}
}

func randKey(i int) string {
	return "bench:key:" + strconv.Itoa(i)
}
