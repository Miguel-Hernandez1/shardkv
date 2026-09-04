// Package cluster wires together the config group and the independent
// per-shard Raft groups that make up one physical ShardKV process.
package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/mighdz/shardkv/internal/configgroup"
	"github.com/mighdz/shardkv/internal/node"
	"github.com/mighdz/shardkv/internal/raftutil"
	"github.com/mighdz/shardkv/internal/shard"
)

// Manager owns this physical process's replica of the cluster's config
// group, plus one node.Node per shard this node is currently assigned to
// host. The config group is always fully replicated across every node in
// Config.Peers; which shards a given node hosts is decided by the config
// group's committed placement (see internal/configgroup), computed once
// from the full node list at cluster startup. This phase does not move
// shard data when placement changes, so a node's assigned shards, once
// created, are not revisited.
type Manager struct {
	cfg         Config
	configGroup *configgroup.Group

	// shardsMu guards shards. In production EnsureAssignedShards always
	// finishes populating it before the HTTP server starts serving, but
	// a test harness that starts serving first (to let one node's join
	// request reach another node whose own EnsureAssignedShards hasn't
	// returned yet) can otherwise race a shard join handler's read
	// against EnsureAssignedShards's writes.
	shardsMu sync.RWMutex
	shards   map[int]*node.Node
}

// New starts this node's config group replica, bootstrapped single-voter
// (self only) on first boot. On the bootstrap node (cfg.Bootstrap), if
// this is genuinely a first boot, it also proposes the cluster's initial
// placement, every peer in cfg.Peers, cfg.NumShards, and
// cfg.ReplicationFactor, as soon as it becomes the config group's leader
// (near-instant, since single-voter bootstrap wins its own election
// immediately). If instead this node is restarting with persisted state
// from an earlier run, New waits for that state to replay locally
// instead: the config is already committed, and a node restarting
// alongside other voters may never win its own election again. It does
// not create any shard replicas yet: call JoinConfigGroup (non-bootstrap
// nodes only) and then EnsureAssignedShards to do that once the config is
// known locally.
func New(cfg Config) (*Manager, error) {
	if cfg.NumShards <= 0 {
		return nil, fmt.Errorf("numShards must be > 0, got %d", cfg.NumShards)
	}
	if cfg.ReplicationFactor <= 0 {
		return nil, fmt.Errorf("replicationFactor must be > 0, got %d", cfg.ReplicationFactor)
	}

	cgAddr, err := configGroupRaftAddr(cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("config group: %w", err)
	}

	cg, err := configgroup.New(raftutil.Config{
		NodeID:    cfg.NodeID,
		RaftAddr:  cgAddr,
		DataDir:   filepath.Join(cfg.DataDir, "config"),
		Peers:     []string{cgAddr},
		Bootstrap: cfg.Bootstrap,
	})
	if err != nil {
		return nil, fmt.Errorf("config group: %w", err)
	}

	m := &Manager{cfg: cfg, configGroup: cg, shards: make(map[int]*node.Node)}

	if cfg.Bootstrap && cg.Resumed() {
		// This node proposed the initial placement on some earlier run
		// and is now restarting, not booting for the first time: the
		// config is already committed to its persisted log, and with
		// other voters in the group it may never win an election by
		// itself again, so waiting to become leader here could hang
		// forever instead of just being redundant. Wait for the local
		// replica to replay what it already has on disk instead, the
		// same thing JoinConfigGroup waits for on a non-bootstrap node.
		deadline := time.Now().Add(raftutil.Timeout)
		for cg.State().Version < 1 {
			if time.Now().After(deadline) {
				m.Shutdown()
				return nil, fmt.Errorf("config group: persisted placement did not replay within %s", raftutil.Timeout)
			}
			time.Sleep(10 * time.Millisecond)
		}
	} else if cfg.Bootstrap {
		deadline := time.Now().Add(raftutil.Timeout)
		for !cg.IsLeader() {
			if time.Now().After(deadline) {
				m.Shutdown()
				return nil, fmt.Errorf("config group: did not become leader within %s", raftutil.Timeout)
			}
			time.Sleep(10 * time.Millisecond)
		}

		nodeIDs := make([]string, len(cfg.Peers))
		for i, p := range cfg.Peers {
			nodeIDs[i] = p.NodeID
		}
		if err := cg.SetConfig(nodeIDs, cfg.NumShards, cfg.ReplicationFactor); err != nil {
			m.Shutdown()
			return nil, fmt.Errorf("propose initial placement: %w", err)
		}
	}

	return m, nil
}

// httpAddrForPeer converts a peer's base Raft address to its HTTP
// address, using the same base-Raft-port-minus-1000 convention the
// client-facing leader redirect uses.
func httpAddrForPeer(raftAddr string) (string, error) {
	host, portStr, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", fmt.Errorf("invalid raft address %q: %w", raftAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", fmt.Errorf("invalid port in %q: %w", raftAddr, err)
	}
	return fmt.Sprintf("%s:%d", host, port-1000), nil
}

// JoinConfigGroup sends a join request to the cluster's bootstrap peer
// (cfg.Peers[0]) so this node becomes a voter of the config group, then
// waits for the resulting placement to replicate to this node's local
// replica. Call this on every non-bootstrap node before
// EnsureAssignedShards; the bootstrap node never needs it, since it
// proposed the initial config itself in New.
func (m *Manager) JoinConfigGroup(client *http.Client, bootstrapRaftAddr string) error {
	httpAddr, err := httpAddrForPeer(bootstrapRaftAddr)
	if err != nil {
		return fmt.Errorf("join config group: %w", err)
	}

	payload, err := json.Marshal(map[string]string{"node_id": m.cfg.NodeID, "raft_addr": m.cfg.RaftAddr})
	if err != nil {
		return fmt.Errorf("join config group: %w", err)
	}
	url := fmt.Sprintf("http://%s/v1/cluster/join", httpAddr)
	if err := retryPost(client, url, payload); err != nil {
		return fmt.Errorf("join config group: %w", err)
	}

	deadline := time.Now().Add(raftutil.Timeout)
	for m.configGroup.State().Version < 1 {
		if time.Now().After(deadline) {
			return fmt.Errorf("join config group: placement did not replicate within %s", raftutil.Timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

// EnsureAssignedShards creates a local Raft replica for every shard this
// node is currently assigned, per the config group's committed
// placement. Every shard replica bootstraps single-voter (self only),
// exactly like the config group does, and the same way: the shard's
// first assigned replica bootstraps it, and every other assigned replica
// starts empty and sends a join request carrying its real node ID,
// retrying until the first replica has actually created and led the
// shard's Raft group. This never needs to guess another node's identity
// from its address the way bootstrapping with a full peer list up front
// would. Requires the config group to already have a committed placement
// locally (true immediately on the bootstrap node; true on any other
// node after JoinConfigGroup returns).
func (m *Manager) EnsureAssignedShards(client *http.Client) error {
	state := m.configGroup.State()
	if state.Version < 1 {
		return fmt.Errorf("ensure assigned shards: no committed placement yet")
	}

	raftAddrByNodeID := make(map[string]string, len(m.cfg.Peers))
	for _, p := range m.cfg.Peers {
		raftAddrByNodeID[p.NodeID] = p.RaftAddr
	}

	for _, shardID := range state.Assignment.ShardsFor(m.cfg.NodeID) {
		m.shardsMu.RLock()
		_, exists := m.shards[shardID]
		m.shardsMu.RUnlock()
		if exists {
			continue
		}

		replicas := state.Assignment.Replicas(shardID)
		selfAddr, err := ShardRaftAddr(m.cfg.RaftAddr, shardID)
		if err != nil {
			return fmt.Errorf("shard %d: %w", shardID, err)
		}

		if replicas[0] == m.cfg.NodeID {
			n, err := node.New(node.Config{
				NodeID:    m.cfg.NodeID,
				RaftAddr:  selfAddr,
				DataDir:   filepath.Join(m.cfg.DataDir, fmt.Sprintf("shard-%d", shardID)),
				Peers:     []string{selfAddr},
				Bootstrap: true,
			})
			if err != nil {
				return fmt.Errorf("bootstrap shard %d: %w", shardID, err)
			}
			m.shardsMu.Lock()
			m.shards[shardID] = n
			m.shardsMu.Unlock()
			continue
		}

		n, err := node.New(node.Config{
			NodeID:   m.cfg.NodeID,
			RaftAddr: selfAddr,
			DataDir:  filepath.Join(m.cfg.DataDir, fmt.Sprintf("shard-%d", shardID)),
		})
		if err != nil {
			return fmt.Errorf("create shard %d: %w", shardID, err)
		}
		m.shardsMu.Lock()
		m.shards[shardID] = n
		m.shardsMu.Unlock()

		// A replica that already has persisted state is resuming after a
		// restart, not joining for the first time: it's already a voter
		// in this shard's Raft group from before, so there's nothing to
		// ask the first replica for, and that replica may not even be up
		// yet if the whole cluster restarted at once.
		if n.Resumed() {
			continue
		}

		firstReplicaHTTPAddr, err := httpAddrForPeer(raftAddrByNodeID[replicas[0]])
		if err != nil {
			return fmt.Errorf("shard %d: %w", shardID, err)
		}
		payload, err := json.Marshal(map[string]string{"node_id": m.cfg.NodeID, "raft_addr": m.cfg.RaftAddr})
		if err != nil {
			return fmt.Errorf("shard %d: %w", shardID, err)
		}
		url := fmt.Sprintf("http://%s/v1/internal/shards/%d/join", firstReplicaHTTPAddr, shardID)
		if err := retryPost(client, url, payload); err != nil {
			return fmt.Errorf("join shard %d: %w", shardID, err)
		}
	}

	return nil
}

// retryPost POSTs payload to url, retrying with a capped linear backoff
// until it gets a 200, or gives up after about 30 attempts (matched to
// how long a peer might take to finish its own startup, including
// bootstrapping the Raft group this request targets).
func retryPost(client *http.Client, url string, payload []byte) error {
	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		resp, err := client.Post(url, "application/json", bytes.NewReader(payload))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			lastErr = err
		}

		wait := time.Duration(attempt) * time.Second
		if wait > 10*time.Second {
			wait = 10 * time.Second
		}
		time.Sleep(wait)
	}
	return fmt.Errorf("gave up after 30 attempts: %w", lastErr)
}

// Peers returns every physical node in the cluster, including self,
// regardless of which shards each one hosts. This is the full node list
// Fleet View and similar cluster-wide views need; a specific shard's own
// replica list (a subset under partial placement) is not a substitute
// for it. Unlike deriving a node's identity from its Raft address's
// hostname, this carries each node's real ID, which is the only thing
// that's reliable once nodes can share a host and differ only by port.
func (m *Manager) Peers() []Peer {
	peers := make([]Peer, len(m.cfg.Peers))
	copy(peers, m.cfg.Peers)
	return peers
}

// NumShards returns the cluster-wide shard count from the locally-known
// config, or the value this node was configured with if no config has
// replicated yet.
func (m *Manager) NumShards() int {
	if state := m.configGroup.State(); state.Version > 0 {
		return state.NumShards
	}
	return m.cfg.NumShards
}

// NodeID returns this physical node's ID.
func (m *Manager) NodeID() string { return m.cfg.NodeID }

// RaftAddr returns this physical node's base Raft address (shard 0's port).
func (m *Manager) RaftAddr() string { return m.cfg.RaftAddr }

// ShardFor returns the shard index a key deterministically routes to.
func (m *Manager) ShardFor(key string) int {
	return shard.KeyToShard(key, m.NumShards())
}

// Shard returns the local Raft replica for the given shard, or nil if
// this node does not currently host that shard.
func (m *Manager) Shard(id int) *node.Node {
	m.shardsMu.RLock()
	defer m.shardsMu.RUnlock()
	return m.shards[id]
}

// ShardIDs returns every shard index this process actually hosts a
// replica of, in ascending order. Under partial placement this can be a
// subset of every shard in the cluster.
func (m *Manager) ShardIDs() []int {
	m.shardsMu.RLock()
	defer m.shardsMu.RUnlock()
	ids := make([]int, 0, len(m.shards))
	for id := range m.shards {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

// Assignment returns the locally-known shard placement. See
// configgroup.FSM.State for why this is a plain local read.
func (m *Manager) Assignment() configgroup.Assignment {
	return m.configGroup.State().Assignment
}

// ReplicasFor returns the node IDs assigned to replicate shardID, per the
// locally-known placement.
func (m *Manager) ReplicasFor(shardID int) []string {
	return m.configGroup.State().Assignment.Replicas(shardID)
}

// PeerHTTPAddr returns the HTTP address of the physical node identified
// by nodeID, or an error if nodeID isn't one of this node's configured
// peers.
func (m *Manager) PeerHTTPAddr(nodeID string) (string, error) {
	for _, p := range m.cfg.Peers {
		if p.NodeID == nodeID {
			return httpAddrForPeer(p.RaftAddr)
		}
	}
	return "", fmt.Errorf("unknown node %q", nodeID)
}

// AddConfigGroupVoter adds a new physical node as a voter of the config
// group. Must be called on the config group's leader.
func (m *Manager) AddConfigGroupVoter(nodeID, raftAddr string) error {
	addr, err := configGroupRaftAddr(raftAddr)
	if err != nil {
		return fmt.Errorf("config group: %w", err)
	}
	return m.configGroup.AddVoter(nodeID, addr)
}

// ConfigGroupIsLeader returns true if this node's config group replica is
// the current leader.
func (m *Manager) ConfigGroupIsLeader() bool {
	return m.configGroup.IsLeader()
}

// ConfigGroupLeaderAddr returns the Raft address of the config group's
// current leader.
func (m *Manager) ConfigGroupLeaderAddr() string {
	return m.configGroup.LeaderAddr()
}

// Shutdown gracefully shuts down the config group replica and every
// shard replica this node hosts. It returns the first error encountered,
// if any, but always attempts every replica.
func (m *Manager) Shutdown() error {
	var firstErr error
	for _, id := range m.ShardIDs() {
		n := m.Shard(id)
		if n == nil {
			continue
		}
		if err := n.Shutdown(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("shard %d: %w", id, err)
		}
	}
	if m.configGroup != nil {
		if err := m.configGroup.Shutdown(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("config group: %w", err)
		}
	}
	return firstErr
}
