package testutil

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/mighdz/shardkv/internal/cluster"
	"github.com/mighdz/shardkv/internal/node"
)

// ShardCluster is an in-process multi-node cluster where every node
// replicates every shard, matching the production topology. It is used by
// integration tests that exercise shard routing, per-shard failover, and
// cross-shard behavior.
type ShardCluster struct {
	t         *testing.T
	numShards int
	managers  []*cluster.Manager
	dataDirs  []string
	raftAddrs []string
}

// NewShardCluster creates and starts a numNodes-node cluster, each hosting
// numShards independent shard replicas. Node 0 bootstraps every shard as a
// single-voter group and becomes its initial leader; the remaining nodes
// join afterward via cluster.Manager.AddVoter, mirroring the production
// join flow.
func NewShardCluster(t *testing.T, numNodes, numShards int) *ShardCluster {
	t.Helper()
	if numNodes < 1 {
		t.Fatal("cluster size must be >= 1")
	}

	raftAddrs := make([]string, numNodes)
	for i := range raftAddrs {
		raftAddrs[i] = fmt.Sprintf("127.0.0.1:%d", freeBasePort(t, numShards))
	}

	dataDirs := make([]string, numNodes)
	for i := range dataDirs {
		dataDirs[i] = t.TempDir()
	}

	managers := make([]*cluster.Manager, numNodes)

	m0, err := cluster.New(cluster.Config{
		NodeID:    "node1",
		RaftAddr:  raftAddrs[0],
		DataDir:   dataDirs[0],
		Peers:     []string{raftAddrs[0]},
		Bootstrap: true,
		NumShards: numShards,
	})
	if err != nil {
		t.Fatalf("create node1: %v", err)
	}
	managers[0] = m0

	waitForCondition(t, 10*time.Second, 50*time.Millisecond, func() bool {
		for _, id := range m0.ShardIDs() {
			if !m0.Shard(id).IsLeader() {
				return false
			}
		}
		return true
	})

	for i := 1; i < numNodes; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		m, err := cluster.New(cluster.Config{
			NodeID:    nodeID,
			RaftAddr:  raftAddrs[i],
			DataDir:   dataDirs[i],
			Bootstrap: false,
			NumShards: numShards,
		})
		if err != nil {
			t.Fatalf("create %s: %v", nodeID, err)
		}
		managers[i] = m

		if err := m0.AddVoter(nodeID, raftAddrs[i]); err != nil {
			t.Fatalf("add voter %s: %v", nodeID, err)
		}
	}

	c := &ShardCluster{
		t: t, numShards: numShards,
		managers: managers, dataDirs: dataDirs, raftAddrs: raftAddrs,
	}
	t.Cleanup(c.Shutdown)
	return c
}

// NumShards returns the shard count every node in the cluster hosts.
func (c *ShardCluster) NumShards() int { return c.numShards }

// Manager returns the physical node's cluster.Manager at index i, or nil
// after that node has been shut down via RestartNode's stop phase.
func (c *ShardCluster) Manager(i int) *cluster.Manager { return c.managers[i] }

// Managers returns every physical node's cluster.Manager.
func (c *ShardCluster) Managers() []*cluster.Manager { return c.managers }

// ShardLeader polls until shard shardID has an elected leader among the
// running managers and returns that leader's replica.
func (c *ShardCluster) ShardLeader(shardID int) *node.Node {
	c.t.Helper()
	var leader *node.Node
	waitForCondition(c.t, 10*time.Second, 100*time.Millisecond, func() bool {
		for _, m := range c.managers {
			if m == nil {
				continue
			}
			n := m.Shard(shardID)
			if n != nil && n.IsLeader() {
				leader = n
				return true
			}
		}
		return false
	})
	if leader == nil {
		c.t.Fatalf("no leader elected for shard %d within timeout", shardID)
	}
	return leader
}

// WaitForShardApplied blocks until every running node has applied at least
// index on the given shard.
func (c *ShardCluster) WaitForShardApplied(shardID int, index uint64) {
	c.t.Helper()
	waitForCondition(c.t, 10*time.Second, 100*time.Millisecond, func() bool {
		for _, m := range c.managers {
			if m == nil {
				continue
			}
			n := m.Shard(shardID)
			if n != nil && n.AppliedIndex() < index {
				return false
			}
		}
		return true
	})
}

// RestartNode shuts down the manager at index i and recreates it from the
// same Raft address and data directory, simulating a process crash and
// restart. The node rejoins via its persisted Raft state; it does not
// re-run the join handshake.
func (c *ShardCluster) RestartNode(i int) {
	c.t.Helper()
	if m := c.managers[i]; m != nil {
		_ = m.Shutdown()
	}

	nodeID := "node1"
	bootstrap := i == 0
	if !bootstrap {
		nodeID = fmt.Sprintf("node%d", i+1)
	}

	m, err := cluster.New(cluster.Config{
		NodeID:    nodeID,
		RaftAddr:  c.raftAddrs[i],
		DataDir:   c.dataDirs[i],
		Bootstrap: bootstrap,
		NumShards: c.numShards,
	})
	if err != nil {
		c.t.Fatalf("restart node%d: %v", i+1, err)
	}
	c.managers[i] = m
}

// Shutdown gracefully shuts down every running node. Safe to call multiple
// times.
func (c *ShardCluster) Shutdown() {
	for i, m := range c.managers {
		if m == nil {
			continue
		}
		if err := m.Shutdown(); err != nil {
			c.t.Logf("shutdown node%d: %v", i+1, err)
		}
		c.managers[i] = nil
	}
}

// freeBasePort finds a base port such that it and every port derived from
// it by cluster's shard-offset arithmetic (base + shardID*ShardPortOffset)
// are simultaneously free, avoiding collisions between shards or with
// other tests running in parallel.
func freeBasePort(t *testing.T, numShards int) int {
	t.Helper()
	for attempt := 0; attempt < 20; attempt++ {
		base := freePort(t)
		var listeners []net.Listener
		ok := true
		for i := 1; i < numShards; i++ {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i*cluster.ShardPortOffset))
			if err != nil {
				ok = false
				break
			}
			listeners = append(listeners, l)
		}
		for _, l := range listeners {
			l.Close()
		}
		if ok {
			return base
		}
	}
	t.Fatal("could not find a free block of ports for shard cluster")
	return 0
}
