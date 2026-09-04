package testutil

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/mighdz/shardkv/internal/cluster"
	"github.com/mighdz/shardkv/internal/node"
	"github.com/mighdz/shardkv/internal/server"
)

// ShardCluster is an in-process multi-node cluster. Each shard is
// replicated on replicationFactor of the cluster's nodes, per the config
// group's computed placement; NewShardCluster defaults that to every
// node (full replication, the topology used before per-shard placement
// existed). It is used by integration tests that exercise shard routing,
// per-shard failover, and cross-shard behavior.
//
// Joining, both the config group and individual shards, happens over
// real HTTP, the same as in production, so ShardCluster starts an actual
// server.Server for every node.
type ShardCluster struct {
	t                 *testing.T
	numShards         int
	replicationFactor int
	managers          []*cluster.Manager
	servers           []*server.Server
	dataDirs          []string
	raftAddrs         []string
	httpAddrs         []string
	peers             []cluster.Peer
	client            *http.Client
}

// NewShardCluster creates and starts a numNodes-node cluster where every
// node replicates every shard.
func NewShardCluster(t *testing.T, numNodes, numShards int) *ShardCluster {
	return NewShardClusterWithReplication(t, numNodes, numShards, numNodes)
}

// NewShardClusterWithReplication creates and starts a numNodes-node
// cluster where each shard is replicated on exactly replicationFactor of
// those nodes. Node 0 bootstraps the config group and proposes the
// cluster's initial placement; the remaining nodes join the config group
// over HTTP, learn their own assignment from it, and then join (or
// bootstrap) whichever shards they were assigned, mirroring the
// production startup sequence.
func NewShardClusterWithReplication(t *testing.T, numNodes, numShards, replicationFactor int) *ShardCluster {
	t.Helper()
	if numNodes < 1 {
		t.Fatal("cluster size must be >= 1")
	}

	raftAddrs := make([]string, numNodes)
	httpAddrs := make([]string, numNodes)
	for i := range raftAddrs {
		base := freeBasePort(t, numShards)
		raftAddrs[i] = fmt.Sprintf("127.0.0.1:%d", base)
		httpAddrs[i] = fmt.Sprintf("127.0.0.1:%d", base-1000)
	}

	dataDirs := make([]string, numNodes)
	for i := range dataDirs {
		dataDirs[i] = t.TempDir()
	}

	peers := make([]cluster.Peer, numNodes)
	for i, addr := range raftAddrs {
		peers[i] = cluster.Peer{NodeID: fmt.Sprintf("node%d", i+1), RaftAddr: addr}
	}

	c := &ShardCluster{
		t: t, numShards: numShards, replicationFactor: replicationFactor,
		managers: make([]*cluster.Manager, numNodes),
		servers:  make([]*server.Server, numNodes),
		dataDirs: dataDirs, raftAddrs: raftAddrs, httpAddrs: httpAddrs, peers: peers,
		client: &http.Client{Timeout: 5 * time.Second},
	}
	t.Cleanup(c.Shutdown)

	m0, err := cluster.New(cluster.Config{
		NodeID:            "node1",
		RaftAddr:          raftAddrs[0],
		DataDir:           dataDirs[0],
		Peers:             peers,
		Bootstrap:         true,
		NumShards:         numShards,
		ReplicationFactor: replicationFactor,
	})
	if err != nil {
		t.Fatalf("create node1: %v", err)
	}
	c.managers[0] = m0
	c.servers[0] = startHTTPServer(t, m0, httpAddrs[0])

	// Under partial placement, a node isn't necessarily the first
	// replica of every shard it's assigned: it may need to join a shard
	// whose first replica is a different node that hasn't started yet.
	// In production each node is a separate process, so that resolves
	// naturally as the other process comes up and its retries succeed.
	// Mirroring that here means every node's join-and-ensure sequence
	// has to run concurrently rather than one at a time, or the first
	// node in the loop can block waiting for a node the loop hasn't
	// created yet.
	var wg sync.WaitGroup
	errCh := make(chan error, numNodes)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m0.EnsureAssignedShards(c.client); err != nil {
			errCh <- fmt.Errorf("node1: ensure assigned shards: %w", err)
		}
	}()

	for i := 1; i < numNodes; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		m, err := cluster.New(cluster.Config{
			NodeID:            nodeID,
			RaftAddr:          raftAddrs[i],
			DataDir:           dataDirs[i],
			Peers:             peers,
			Bootstrap:         false,
			NumShards:         numShards,
			ReplicationFactor: replicationFactor,
		})
		if err != nil {
			t.Fatalf("create %s: %v", nodeID, err)
		}
		c.managers[i] = m
		c.servers[i] = startHTTPServer(t, m, httpAddrs[i])

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.JoinConfigGroup(c.client, raftAddrs[0]); err != nil {
				errCh <- fmt.Errorf("%s: join config group: %w", nodeID, err)
				return
			}
			if err := m.EnsureAssignedShards(c.client); err != nil {
				errCh <- fmt.Errorf("%s: ensure assigned shards: %w", nodeID, err)
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	return c
}

// startHTTPServer starts a real server.Server for m on httpAddr and waits
// for it to accept connections. Join, both for the config group and for
// individual shards, is an HTTP call, so a manager isn't usable until its
// server is actually listening.
func startHTTPServer(t *testing.T, m *cluster.Manager, httpAddr string) *server.Server {
	t.Helper()
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	srv := server.New(m, httpAddr, metricsAddr)
	go srv.Start()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", httpAddr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return srv
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("HTTP server at %s never came up", httpAddr)
	return nil
}

// NumShards returns the cluster-wide shard count.
func (c *ShardCluster) NumShards() int { return c.numShards }

// Manager returns the physical node's cluster.Manager at index i, or nil
// after that node has been shut down via RestartNode's stop phase.
func (c *ShardCluster) Manager(i int) *cluster.Manager { return c.managers[i] }

// Managers returns every physical node's cluster.Manager.
func (c *ShardCluster) Managers() []*cluster.Manager { return c.managers }

// HTTPAddr returns the HTTP address node i is currently listening on.
// Tests that need to exercise the HTTP layer directly (redirects, the
// internal per-shard endpoints) use this instead of starting their own
// servers.
func (c *ShardCluster) HTTPAddr(i int) string { return c.httpAddrs[i] }

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

// WaitForShardApplied blocks until every running node that hosts shardID
// has applied at least index.
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

// RestartNode shuts down the manager and HTTP server at index i and
// recreates both from the same Raft addresses, HTTP address, and data
// directory, simulating a process crash and restart. Every Raft group
// (the config group and each assigned shard) resumes from its persisted
// state rather than re-bootstrapping, since HasExistingState is true for
// each of them.
//
// The HTTP listener has to actually stop, not just get a fresh port:
// every redirect in this package (leader redirects, the config group
// join redirect) derives a peer's HTTP address from its Raft address by
// a fixed port arithmetic that only holds if a node's HTTP address never
// changes across restarts, exactly as it doesn't for a real process. A
// restart that kept the old listener alive on a stale port, answering
// through the cluster.Manager it was built with even after that Manager
// had itself been shut down, would leave a redirect that resolves to
// this node able to reach that dead Manager forever, since it can never
// report itself a leader of anything again.
func (c *ShardCluster) RestartNode(i int) {
	c.t.Helper()
	if srv := c.servers[i]; srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := srv.Shutdown(ctx); err != nil {
			c.t.Logf("shut down http server node%d: %v", i+1, err)
		}
		cancel()
	}
	if m := c.managers[i]; m != nil {
		_ = m.Shutdown()
	}

	nodeID := "node1"
	bootstrap := i == 0
	if !bootstrap {
		nodeID = fmt.Sprintf("node%d", i+1)
	}

	m, err := cluster.New(cluster.Config{
		NodeID:            nodeID,
		RaftAddr:          c.raftAddrs[i],
		DataDir:           c.dataDirs[i],
		Peers:             c.peers,
		Bootstrap:         bootstrap,
		NumShards:         c.numShards,
		ReplicationFactor: c.replicationFactor,
	})
	if err != nil {
		c.t.Fatalf("restart node%d: %v", i+1, err)
	}
	c.managers[i] = m
	c.servers[i] = startHTTPServer(c.t, m, c.httpAddrs[i])

	// A non-bootstrap node's config group replica replays its persisted
	// log asynchronously after cluster.New returns, so its local state
	// may not have caught up yet. JoinConfigGroup already waits for that
	// (its join is idempotent against an existing voter), which mirrors
	// what a real process does on every restart: cfg.Bootstrap stays
	// false, so main.go calls JoinConfigGroup again unconditionally.
	if !bootstrap {
		if err := m.JoinConfigGroup(c.client, c.raftAddrs[0]); err != nil {
			c.t.Fatalf("restart node%d: join config group: %v", i+1, err)
		}
	}

	if err := m.EnsureAssignedShards(c.client); err != nil {
		c.t.Fatalf("restart node%d: ensure assigned shards: %v", i+1, err)
	}
}

// Shutdown gracefully shuts down every running node. Safe to call multiple
// times.
func (c *ShardCluster) Shutdown() {
	for i, srv := range c.servers {
		if srv == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := srv.Shutdown(ctx); err != nil {
			c.t.Logf("shutdown http server node%d: %v", i+1, err)
		}
		cancel()
		c.servers[i] = nil
	}
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
// it by cluster's shard-offset and config-group-offset arithmetic (base +
// shardID*ShardPortOffset, base + ConfigGroupPortOffset), as well as the
// HTTP port derived from it (base - 1000), are simultaneously free,
// avoiding collisions between shards, the config group, HTTP listeners,
// or with other tests running in parallel.
func freeBasePort(t *testing.T, numShards int) int {
	t.Helper()
	for attempt := 0; attempt < 20; attempt++ {
		base := freePort(t)
		var listeners []net.Listener
		ok := true

		tryListen := func(port int) {
			if !ok {
				return
			}
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
			if err != nil {
				ok = false
				return
			}
			listeners = append(listeners, l)
		}

		for i := 1; i < numShards; i++ {
			tryListen(base + i*cluster.ShardPortOffset)
		}
		tryListen(base + cluster.ConfigGroupPortOffset)
		tryListen(base - 1000)

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
