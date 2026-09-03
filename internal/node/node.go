package node

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mighdz/shardkv/internal/fsm"
	"github.com/mighdz/shardkv/internal/store"
)

const (
	snapshotThreshold = 64
	snapshotRetain    = 2
	raftTimeout       = 10 * time.Second
)

// Node wraps a Raft instance and the KV state machine.
type Node struct {
	cfg         Config
	raft        *raft.Raft
	fsm         *fsm.KVStore
	logStore    *store.BoltLogStore
	stableStore *store.BoltStableStore
}

func New(cfg Config) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	kvFSM := fsm.New()

	logStore, err := store.NewBoltLogStore(filepath.Join(cfg.DataDir, "log.db"))
	if err != nil {
		return nil, fmt.Errorf("log store: %w", err)
	}

	stableStore, err := store.NewBoltStableStore(filepath.Join(cfg.DataDir, "stable.db"))
	if err != nil {
		return nil, fmt.Errorf("stable store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, snapshotRetain, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, raftTimeout, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.SnapshotThreshold = snapshotThreshold
	raftCfg.SnapshotInterval = 30 * time.Second

	r, err := raft.NewRaft(raftCfg, kvFSM, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	if cfg.Bootstrap {
		hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
		if err != nil {
			return nil, fmt.Errorf("check existing state: %w", err)
		}
		if !hasState {
			servers := buildBootstrapServers(cfg)
			future := r.BootstrapCluster(raft.Configuration{Servers: servers})
			if err := future.Error(); err != nil {
				return nil, fmt.Errorf("bootstrap cluster: %w", err)
			}
		}
	}

	return &Node{cfg: cfg, raft: r, fsm: kvFSM, logStore: logStore, stableStore: stableStore}, nil
}

// Apply submits a command to the Raft cluster. Blocks until committed or timeout.
func (n *Node) Apply(cmd fsm.Command) error {
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	data, err := fsm.EncodeCommand(cmd)
	if err != nil {
		return fmt.Errorf("encode command: %w", err)
	}

	future := n.raft.Apply(data, raftTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return ErrNotLeader
		}
		return err
	}

	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}
	return nil
}

// ensureReadConsistency ensures the local FSM reflects all committed log entries
// before a read is served.
//
// On the leader: calls raft.Barrier(), which submits a no-op log entry and
// blocks until it is applied, the definitive guarantee that the FSM is current.
//
// On followers: raft.Barrier() is unavailable (returns ErrNotLeader). Instead
// we poll until AppliedIndex >= CommitIndex, which means every entry the node
// currently knows is committed has been applied to the FSM. This prevents reads
// of uncommitted or unapplied state without requiring leader status.
// The normal apply lag is <1 ms in-process; we allow 1 s before giving up and
// serving the best-available local state anyway.
func (n *Node) ensureReadConsistency() error {
	if n.raft.State() == raft.Leader {
		return n.raft.Barrier(raftTimeout).Error()
	}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if n.raft.AppliedIndex() >= n.raft.CommitIndex() {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

// Get reads a key from the local FSM after ensuring read consistency.
func (n *Node) Get(key string) ([]byte, bool, error) {
	if err := n.ensureReadConsistency(); err != nil {
		return nil, false, err
	}
	v, ok := n.fsm.Get(key)
	return v, ok, nil
}

// Scan returns all keys whose names start with prefix, after ensuring read consistency.
func (n *Node) Scan(prefix string) (map[string][]byte, error) {
	if err := n.ensureReadConsistency(); err != nil {
		return nil, err
	}
	return n.fsm.Scan(prefix), nil
}

// AddVoter adds a new peer to the cluster. Must be called on the leader.
func (n *Node) AddVoter(nodeID, raftAddr string) error {
	future := n.raft.AddVoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(raftAddr),
		0, raftTimeout,
	)
	return future.Error()
}

// State returns the current Raft state as a string.
func (n *Node) State() string {
	return n.raft.State().String()
}

// LeaderAddr returns the Raft address of the current leader.
func (n *Node) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// CommitIndex returns the last committed log index.
func (n *Node) CommitIndex() uint64 {
	return n.raft.CommitIndex()
}

// AppliedIndex returns the last applied log index.
func (n *Node) AppliedIndex() uint64 {
	return n.raft.AppliedIndex()
}

// NodeID returns this node's ID.
func (n *Node) NodeID() string {
	return n.cfg.NodeID
}

// RaftAddr returns this node's Raft address.
func (n *Node) RaftAddr() string {
	return n.cfg.RaftAddr
}

// PeerRaftAddrs returns all configured Raft addresses (including self).
func (n *Node) PeerRaftAddrs() []string {
	return n.cfg.Peers
}

// Term returns the current Raft term number.
func (n *Node) Term() uint64 {
	if t, err := strconv.ParseUint(n.raft.Stats()["term"], 10, 64); err == nil {
		return t
	}
	return 0
}

// IsLeader returns true if this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// Shutdown gracefully shuts down the Raft node and closes its underlying
// BoltDB log and stable stores. Closing the stores is required, not just
// tidy: bbolt takes an exclusive file lock on open, so a process that
// restarts this node against the same data directory (a crash-recovery
// restart, or a test simulating one) would otherwise block forever
// waiting for a lock this process never released.
func (n *Node) Shutdown() error {
	shutdownErr := n.raft.Shutdown().Error()

	var closeErr error
	if err := n.logStore.Close(); err != nil {
		closeErr = fmt.Errorf("close log store: %w", err)
	}
	if err := n.stableStore.Close(); err != nil && closeErr == nil {
		closeErr = fmt.Errorf("close stable store: %w", err)
	}

	if shutdownErr != nil {
		return shutdownErr
	}
	return closeErr
}

// ErrNotLeader is returned when a write arrives at a non-leader node.
var ErrNotLeader = errors.New("not leader")

// buildBootstrapServers constructs the initial cluster configuration for BootstrapCluster.
// The local node uses cfg.NodeID as its ServerID. For peers, we derive the ServerID from
// the hostname portion of the Raft address (works for Docker service names like "node2:9082").
// If the peer address matches cfg.RaftAddr, we always use cfg.NodeID.
func buildBootstrapServers(cfg Config) []raft.Server {
	if len(cfg.Peers) == 0 {
		return []raft.Server{{
			ID:      raft.ServerID(cfg.NodeID),
			Address: raft.ServerAddress(cfg.RaftAddr),
		}}
	}

	servers := make([]raft.Server, 0, len(cfg.Peers))
	for _, peerAddr := range cfg.Peers {
		id := nodeIDFromRaftAddr(peerAddr)
		if peerAddr == cfg.RaftAddr {
			id = cfg.NodeID // always use the explicit NodeID for self
		}
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(id),
			Address: raft.ServerAddress(peerAddr),
		})
	}
	return servers
}

// nodeIDFromRaftAddr extracts the hostname from a host:port Raft address.
// For Docker service names (node1:9081) this returns "node1".
// For IP addresses (127.0.0.1:54321) this returns "127.0.0.1".
func nodeIDFromRaftAddr(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
