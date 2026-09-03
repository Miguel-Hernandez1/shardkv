package testutil

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/mighdz/shardkv/internal/node"
)

// Cluster is an in-process multi-node ShardKV cluster for integration tests.
type Cluster struct {
	t     *testing.T
	nodes []*node.Node
}

// NewCluster creates and starts an n-node cluster using temp dirs and free ports.
// Node0 bootstraps as a single-voter cluster and becomes leader, then adds the
// remaining nodes via AddVoter. This matches the production join flow and is more
// reliable than bootstrapping all nodes simultaneously.
func NewCluster(t *testing.T, n int) *Cluster {
	t.Helper()

	if n < 1 {
		t.Fatal("cluster size must be >= 1")
	}

	raftAddrs := make([]string, n)
	for i := range raftAddrs {
		raftAddrs[i] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	}

	nodes := make([]*node.Node, n)

	// Bootstrap node0 as a single-server cluster.
	cfg0 := node.Config{
		NodeID:    "node1",
		RaftAddr:  raftAddrs[0],
		DataDir:   t.TempDir(),
		Peers:     []string{raftAddrs[0]},
		Bootstrap: true,
	}
	nd0, err := node.New(cfg0)
	if err != nil {
		t.Fatalf("create node1: %v", err)
	}
	nodes[0] = nd0

	// Wait for node0 to become leader (guaranteed quickly as a single-voter cluster).
	waitForCondition(t, 10*time.Second, 50*time.Millisecond, func() bool {
		return nd0.IsLeader()
	})
	if !nd0.IsLeader() {
		t.Fatal("node1 did not become leader within timeout")
	}

	// Start remaining nodes and register them with the leader.
	for i := 1; i < n; i++ {
		cfg := node.Config{
			NodeID:    fmt.Sprintf("node%d", i+1),
			RaftAddr:  raftAddrs[i],
			DataDir:   t.TempDir(),
			Bootstrap: false,
		}
		nd, err := node.New(cfg)
		if err != nil {
			t.Fatalf("create node%d: %v", i+1, err)
		}
		nodes[i] = nd

		if err := nd0.AddVoter(cfg.NodeID, cfg.RaftAddr); err != nil {
			t.Fatalf("add voter node%d: %v", i+1, err)
		}
	}

	c := &Cluster{t: t, nodes: nodes}
	t.Cleanup(c.Shutdown)
	return c
}

// Leader returns the node that is currently the Raft leader.
// Polls until a leader is found or the 10s timeout expires.
func (c *Cluster) Leader() *node.Node {
	c.t.Helper()
	var leader *node.Node
	waitForCondition(c.t, 10*time.Second, 100*time.Millisecond, func() bool {
		for _, n := range c.nodes {
			if n != nil && n.IsLeader() {
				leader = n
				return true
			}
		}
		return false
	})
	if leader == nil {
		c.t.Fatal("no leader elected within timeout")
	}
	return leader
}

// WaitForApplied blocks until all running nodes have applied at least index.
func (c *Cluster) WaitForApplied(index uint64) {
	c.t.Helper()
	waitForCondition(c.t, 10*time.Second, 100*time.Millisecond, func() bool {
		for _, n := range c.nodes {
			if n != nil && n.AppliedIndex() < index {
				return false
			}
		}
		return true
	})
}

// Node returns the node at position i (0-indexed).
func (c *Cluster) Node(i int) *node.Node {
	return c.nodes[i]
}

// Nodes returns all nodes (including any that have been shut down as nil).
func (c *Cluster) Nodes() []*node.Node {
	return c.nodes
}

// Shutdown gracefully shuts down all running nodes. Safe to call multiple times.
func (c *Cluster) Shutdown() {
	for i, n := range c.nodes {
		if n == nil {
			continue
		}
		if err := n.Shutdown(); err != nil {
			c.t.Logf("shutdown node%d: %v", i+1, err)
		}
		c.nodes[i] = nil
	}
}

// waitForCondition polls fn every interval until it returns true or timeout expires.
func waitForCondition(t *testing.T, timeout, interval time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(interval)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
