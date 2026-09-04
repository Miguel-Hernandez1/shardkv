package configgroup

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/mighdz/shardkv/internal/raftutil"
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

// newCluster bootstraps an n-replica config group, mirroring how a real
// cluster's physical nodes each run one replica of it.
func newCluster(t *testing.T, n int) []*Group {
	t.Helper()
	if n < 1 {
		t.Fatal("cluster size must be >= 1")
	}

	raftAddrs := make([]string, n)
	for i := range raftAddrs {
		raftAddrs[i] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	}

	groups := make([]*Group, n)
	g0, err := New(raftutil.Config{
		NodeID:    "node1",
		RaftAddr:  raftAddrs[0],
		DataDir:   t.TempDir(),
		Peers:     []string{raftAddrs[0]},
		Bootstrap: true,
	})
	if err != nil {
		t.Fatalf("create node1: %v", err)
	}
	groups[0] = g0

	waitFor(t, 10*time.Second, g0.IsLeader)

	for i := 1; i < n; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		g, err := New(raftutil.Config{
			NodeID:    nodeID,
			RaftAddr:  raftAddrs[i],
			DataDir:   t.TempDir(),
			Bootstrap: false,
		})
		if err != nil {
			t.Fatalf("create %s: %v", nodeID, err)
		}
		groups[i] = g

		if err := g0.AddVoter(nodeID, raftAddrs[i]); err != nil {
			t.Fatalf("add voter %s: %v", nodeID, err)
		}
	}

	t.Cleanup(func() {
		for _, g := range groups {
			if g != nil {
				g.Shutdown()
			}
		}
	})
	return groups
}

func leaderOf(groups []*Group) *Group {
	for _, g := range groups {
		if g.IsLeader() {
			return g
		}
	}
	return nil
}

func TestSetConfigReplicatesToEveryNode(t *testing.T) {
	t.Parallel()
	groups := newCluster(t, 3)
	leader := leaderOf(groups)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	nodes := []string{"node1", "node2", "node3"}
	if err := leader.SetConfig(nodes, 4, 2); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}

	for i, g := range groups {
		waitFor(t, 5*time.Second, func() bool { return g.State().Version == 1 })
		state := g.State()
		if state.NumShards != 4 || state.ReplicationFactor != 2 {
			t.Fatalf("replica %d: got numShards=%d rf=%d, want 4/2", i, state.NumShards, state.ReplicationFactor)
		}
		if len(state.Assignment) != 4 {
			t.Fatalf("replica %d: assignment has %d shards, want 4", i, len(state.Assignment))
		}
		for shard := 0; shard < 4; shard++ {
			if len(state.Assignment.Replicas(shard)) != 2 {
				t.Fatalf("replica %d: shard %d has %d replicas, want 2", i, shard, len(state.Assignment.Replicas(shard)))
			}
		}
	}
}

func TestSetConfigOnFollowerReturnsErrNotLeader(t *testing.T) {
	t.Parallel()
	groups := newCluster(t, 3)
	leader := leaderOf(groups)

	for _, g := range groups {
		if g == leader {
			continue
		}
		err := g.SetConfig([]string{"node1", "node2", "node3"}, 3, 2)
		if err != ErrNotLeader {
			t.Fatalf("SetConfig on follower: got %v, want ErrNotLeader", err)
		}
	}
}

func TestSetConfigRejectsInvalidPlacement(t *testing.T) {
	t.Parallel()
	groups := newCluster(t, 3)
	leader := leaderOf(groups)

	// Replication factor greater than the node count can't be satisfied.
	err := leader.SetConfig([]string{"node1", "node2"}, 3, 5)
	if err == nil {
		t.Fatal("expected an error proposing a config with rf > available nodes")
	}

	// The rejected proposal must not have changed the committed config.
	if leader.State().Version != 0 {
		t.Fatalf("version = %d after a rejected proposal, want 0", leader.State().Version)
	}
}

func TestConfigGroupLeaderFailover(t *testing.T) {
	t.Parallel()
	groups := newCluster(t, 3)
	leader := leaderOf(groups)

	if err := leader.SetConfig([]string{"node1", "node2", "node3"}, 3, 2); err != nil {
		t.Fatalf("initial SetConfig: %v", err)
	}

	if err := leader.Shutdown(); err != nil {
		t.Logf("shutdown leader: %v", err)
	}

	var newLeader *Group
	waitFor(t, 10*time.Second, func() bool {
		for _, g := range groups {
			if g != leader && g.IsLeader() {
				newLeader = g
				return true
			}
		}
		return false
	})

	if err := newLeader.SetConfig([]string{"node1", "node2", "node3"}, 5, 2); err != nil {
		t.Fatalf("SetConfig on new leader: %v", err)
	}

	for _, g := range groups {
		if g == leader {
			continue
		}
		waitFor(t, 5*time.Second, func() bool { return g.State().NumShards == 5 })
	}
}
