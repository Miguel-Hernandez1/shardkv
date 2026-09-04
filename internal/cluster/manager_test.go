package cluster

import (
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// newSingleManager builds a one-node, multi-shard Manager for unit tests
// that don't need real replication. A single node's own shard replicas
// don't require any network round trip to create (it's always the first,
// and only, replica of every shard it's assigned), so EnsureAssignedShards
// never actually uses the client it's given here.
func newSingleManager(t *testing.T, numShards int) *Manager {
	t.Helper()
	raftAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg := Config{
		NodeID:            "node1",
		RaftAddr:          raftAddr,
		DataDir:           t.TempDir(),
		Peers:             []Peer{{NodeID: "node1", RaftAddr: raftAddr}},
		Bootstrap:         true,
		NumShards:         numShards,
		ReplicationFactor: 1,
	}
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { m.Shutdown() })

	if err := m.EnsureAssignedShards(&http.Client{}); err != nil {
		t.Fatalf("EnsureAssignedShards: %v", err)
	}
	return m
}

func TestManagerCreatesOneReplicaPerShard(t *testing.T) {
	m := newSingleManager(t, 3)

	ids := m.ShardIDs()
	if len(ids) != 3 {
		t.Fatalf("expected 3 shards, got %d", len(ids))
	}
	for _, id := range ids {
		if m.Shard(id) == nil {
			t.Fatalf("shard %d has no replica", id)
		}
	}

	for _, id := range ids {
		shardID := id
		waitFor(t, 5*time.Second, func() bool { return m.Shard(shardID).IsLeader() })
	}
}

func TestManagerShardsUseDistinctPorts(t *testing.T) {
	m := newSingleManager(t, 3)

	seen := map[string]int{}
	for _, id := range m.ShardIDs() {
		addr := m.Shard(id).RaftAddr()
		if prev, ok := seen[addr]; ok {
			t.Fatalf("shard %d and shard %d share raft address %s", prev, id, addr)
		}
		seen[addr] = id
	}
}

func TestManagerShardForIsDeterministic(t *testing.T) {
	m := newSingleManager(t, 4)
	for _, key := range []string{"a", "user:1", "product:99"} {
		first := m.ShardFor(key)
		for i := 0; i < 20; i++ {
			if got := m.ShardFor(key); got != first {
				t.Fatalf("ShardFor(%q) not stable: %d vs %d", key, first, got)
			}
		}
	}
}
