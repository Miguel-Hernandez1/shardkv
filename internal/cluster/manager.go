// Package cluster wires together the independent per-shard Raft groups
// that make up one physical ShardKV process.
package cluster

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/mighdz/shardkv/internal/node"
	"github.com/mighdz/shardkv/internal/shard"
)

// Manager owns one node.Node (one Raft group replica) per shard on this
// physical process. Every physical node in the cluster runs a Manager with
// the same shard count and replicates every shard, so shard leadership and
// failover are independent per shard while reads stay available locally.
type Manager struct {
	cfg    Config
	shards map[int]*node.Node
}

// New creates and starts a Raft replica for every shard.
func New(cfg Config) (*Manager, error) {
	if cfg.NumShards <= 0 {
		return nil, fmt.Errorf("numShards must be > 0, got %d", cfg.NumShards)
	}

	m := &Manager{cfg: cfg, shards: make(map[int]*node.Node, cfg.NumShards)}

	for i := 0; i < cfg.NumShards; i++ {
		raftAddr, err := shardRaftAddr(cfg.RaftAddr, i)
		if err != nil {
			m.Shutdown()
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}

		peers := make([]string, len(cfg.Peers))
		for j, p := range cfg.Peers {
			peerAddr, err := shardRaftAddr(p, i)
			if err != nil {
				m.Shutdown()
				return nil, fmt.Errorf("shard %d peer %d: %w", i, j, err)
			}
			peers[j] = peerAddr
		}

		n, err := node.New(node.Config{
			NodeID:    cfg.NodeID,
			RaftAddr:  raftAddr,
			DataDir:   filepath.Join(cfg.DataDir, fmt.Sprintf("shard-%d", i)),
			Peers:     peers,
			Bootstrap: cfg.Bootstrap,
		})
		if err != nil {
			m.Shutdown()
			return nil, fmt.Errorf("create shard %d: %w", i, err)
		}
		m.shards[i] = n
	}

	return m, nil
}

// NumShards returns the number of shards this process participates in.
func (m *Manager) NumShards() int { return m.cfg.NumShards }

// NodeID returns this physical node's ID.
func (m *Manager) NodeID() string { return m.cfg.NodeID }

// RaftAddr returns this physical node's base Raft address (shard 0's port).
func (m *Manager) RaftAddr() string { return m.cfg.RaftAddr }

// ShardFor returns the shard index a key deterministically routes to.
func (m *Manager) ShardFor(key string) int {
	return shard.KeyToShard(key, m.cfg.NumShards)
}

// Shard returns the local Raft replica for the given shard, or nil if the
// shard index is out of range.
func (m *Manager) Shard(id int) *node.Node {
	return m.shards[id]
}

// ShardIDs returns every shard index this process hosts, in ascending order.
func (m *Manager) ShardIDs() []int {
	ids := make([]int, 0, len(m.shards))
	for id := range m.shards {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

// AddVoter registers a new physical node as a voter across every shard's
// Raft group. It must be called on a node that currently leads every shard
// group, which holds true for the bootstrap node immediately after startup.
func (m *Manager) AddVoter(nodeID, raftAddr string) error {
	for _, id := range m.ShardIDs() {
		addr, err := shardRaftAddr(raftAddr, id)
		if err != nil {
			return fmt.Errorf("shard %d: %w", id, err)
		}
		if err := m.shards[id].AddVoter(nodeID, addr); err != nil {
			return fmt.Errorf("shard %d: add voter %s: %w", id, nodeID, err)
		}
	}
	return nil
}

// Shutdown gracefully shuts down every shard replica. It returns the first
// error encountered, if any, but always attempts every shard.
func (m *Manager) Shutdown() error {
	var firstErr error
	for _, id := range m.ShardIDs() {
		n := m.shards[id]
		if n == nil {
			continue
		}
		if err := n.Shutdown(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("shard %d: %w", id, err)
		}
	}
	return firstErr
}
