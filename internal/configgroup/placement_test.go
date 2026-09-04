package configgroup

import (
	"fmt"
	"testing"
)

func nodeList(n int) []string {
	nodes := make([]string, n)
	for i := range nodes {
		nodes[i] = fmt.Sprintf("node%d", i+1)
	}
	return nodes
}

func TestComputeEveryShardHasDistinctReplicas(t *testing.T) {
	cases := []struct{ numNodes, numShards, rf int }{
		{3, 3, 3}, {3, 6, 2}, {4, 3, 2}, {5, 3, 3}, {5, 8, 2}, {10, 16, 3},
	}
	for _, c := range cases {
		a, err := Compute(nodeList(c.numNodes), c.numShards, c.rf)
		if err != nil {
			t.Fatalf("Compute(%d nodes, %d shards, rf %d): %v", c.numNodes, c.numShards, c.rf, err)
		}
		for shard := 0; shard < c.numShards; shard++ {
			replicas := a.Replicas(shard)
			if len(replicas) != c.rf {
				t.Fatalf("shard %d has %d replicas, want %d", shard, len(replicas), c.rf)
			}
			seen := make(map[string]bool, c.rf)
			for _, id := range replicas {
				if seen[id] {
					t.Fatalf("shard %d assigns node %s twice: %v", shard, id, replicas)
				}
				seen[id] = true
			}
		}
	}
}

func TestComputeIsDeterministicRegardlessOfInputOrder(t *testing.T) {
	nodes := []string{"node3", "node1", "node2", "node5", "node4"}
	shuffled := []string{"node5", "node2", "node4", "node1", "node3"}

	a1, err := Compute(nodes, 4, 2)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	a2, err := Compute(shuffled, 4, 2)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}

	for shard := 0; shard < 4; shard++ {
		r1 := fmt.Sprint(a1.Replicas(shard))
		r2 := fmt.Sprint(a2.Replicas(shard))
		if r1 != r2 {
			t.Fatalf("shard %d: %s from first input order, %s from shuffled order", shard, r1, r2)
		}
	}
}

func TestComputeBalancesShardCountAcrossNodes(t *testing.T) {
	cases := []struct{ numNodes, numShards, rf int }{
		{3, 3, 3}, {4, 3, 2}, {5, 3, 3}, {5, 8, 2}, {6, 10, 3}, {7, 20, 2},
	}
	for _, c := range cases {
		a, err := Compute(nodeList(c.numNodes), c.numShards, c.rf)
		if err != nil {
			t.Fatalf("Compute: %v", err)
		}

		counts := make(map[string]int, c.numNodes)
		for _, id := range nodeList(c.numNodes) {
			counts[id] = len(a.ShardsFor(id))
		}

		min, max := c.numShards, 0
		for _, cnt := range counts {
			if cnt < min {
				min = cnt
			}
			if cnt > max {
				max = cnt
			}
		}
		if max-min > 1 {
			t.Fatalf("%d nodes/%d shards/rf %d: shard counts range from %d to %d, want a spread of at most 1 (%v)",
				c.numNodes, c.numShards, c.rf, min, max, counts)
		}
	}
}

func TestComputeRejectsInsufficientNodes(t *testing.T) {
	if _, err := Compute(nodeList(2), 3, 3); err == nil {
		t.Fatal("expected an error when replicationFactor exceeds the number of nodes")
	}
}

func TestComputeRejectsInvalidParams(t *testing.T) {
	if _, err := Compute(nodeList(3), 0, 1); err == nil {
		t.Fatal("expected an error for numShards <= 0")
	}
	if _, err := Compute(nodeList(3), 3, 0); err == nil {
		t.Fatal("expected an error for replicationFactor <= 0")
	}
}

func TestHostsAndShardsForAgree(t *testing.T) {
	a, err := Compute(nodeList(5), 8, 2)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	for _, id := range nodeList(5) {
		for _, shard := range a.ShardsFor(id) {
			if !a.Hosts(shard, id) {
				t.Fatalf("ShardsFor(%s) includes shard %d but Hosts(%d, %s) is false", id, shard, shard, id)
			}
		}
	}
}
