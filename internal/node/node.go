package node

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/hashicorp/raft"
	"github.com/mighdz/shardkv/internal/fsm"
	"github.com/mighdz/shardkv/internal/raftutil"
)

// Node wraps a Raft instance and the KV state machine.
type Node struct {
	cfg   Config
	group *raftutil.Group
	fsm   *fsm.KVStore
}

func New(cfg Config) (*Node, error) {
	kvFSM := fsm.New()

	group, err := raftutil.New(raftutil.Config{
		NodeID:    cfg.NodeID,
		RaftAddr:  cfg.RaftAddr,
		DataDir:   cfg.DataDir,
		Peers:     cfg.Peers,
		Bootstrap: cfg.Bootstrap,
	}, kvFSM)
	if err != nil {
		return nil, err
	}

	return &Node{cfg: cfg, group: group, fsm: kvFSM}, nil
}

// Apply submits a command to the Raft cluster. Blocks until committed or timeout.
func (n *Node) Apply(cmd fsm.Command) error {
	if n.group.Raft.State() != raft.Leader {
		return ErrNotLeader
	}

	data, err := fsm.EncodeCommand(cmd)
	if err != nil {
		return fmt.Errorf("encode command: %w", err)
	}

	future := n.group.Raft.Apply(data, raftutil.Timeout)
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

// Consistency selects the guarantee a read is served under.
type Consistency int

const (
	// Linearizable reads are only served by the shard's leader, after a
	// raft.Barrier() confirms the leader's FSM reflects every entry
	// committed as of the moment the read arrived. This is the only mode
	// that rules out a stale or partitioned replica answering a read with
	// data that predates a write the rest of the cluster already
	// considers committed.
	Linearizable Consistency = iota

	// Stale reads are served immediately from whatever is in the local
	// FSM, on any replica, leader or follower, with no confirmation that
	// it reflects the latest committed writes. A replica that has fallen
	// behind, or is on the wrong side of a network partition, can return
	// data older than the caller might expect. Callers trade this
	// guarantee away deliberately, in return for lower latency and the
	// ability to read from any replica without hitting the leader.
	Stale
)

func (c Consistency) String() string {
	if c == Stale {
		return "stale"
	}
	return "linearizable"
}

// requireLeaderAndBarrier enforces linearizable read semantics: it fails
// unless this replica is currently the shard's leader, and then blocks
// until raft.Barrier() confirms the leader's FSM is caught up to every
// entry committed as of this call. Barrier works by committing a no-op
// log entry through the normal Raft path and waiting for it to apply,
// which is only possible on the leader; a stale or former leader that
// has lost contact with a majority will time out here rather than
// silently answering with outdated state.
func (n *Node) requireLeaderAndBarrier() error {
	if n.group.Raft.State() != raft.Leader {
		return ErrNotLeader
	}
	return n.group.Raft.Barrier(raftutil.Timeout).Error()
}

// Get reads a key from the local FSM under the requested consistency.
// Under Linearizable, it returns ErrNotLeader if this replica is not the
// shard's leader; callers (the HTTP layer) redirect to the leader the same
// way a write would.
func (n *Node) Get(key string, c Consistency) ([]byte, bool, error) {
	if c == Linearizable {
		if err := n.requireLeaderAndBarrier(); err != nil {
			return nil, false, err
		}
	}
	v, ok := n.fsm.Get(key)
	return v, ok, nil
}

// Scan returns all keys whose names start with prefix, under the requested
// consistency. Under Linearizable, it returns ErrNotLeader if this replica
// is not the shard's leader.
func (n *Node) Scan(prefix string, c Consistency) (map[string][]byte, error) {
	if c == Linearizable {
		if err := n.requireLeaderAndBarrier(); err != nil {
			return nil, err
		}
	}
	return n.fsm.Scan(prefix), nil
}

// AddVoter adds a new peer to the cluster. Must be called on the leader.
func (n *Node) AddVoter(nodeID, raftAddr string) error {
	future := n.group.Raft.AddVoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(raftAddr),
		0, raftutil.Timeout,
	)
	return future.Error()
}

// State returns the current Raft state as a string.
func (n *Node) State() string {
	return n.group.Raft.State().String()
}

// LeaderAddr returns the Raft address of the current leader.
func (n *Node) LeaderAddr() string {
	addr, _ := n.group.Raft.LeaderWithID()
	return string(addr)
}

// CommitIndex returns the last committed log index.
func (n *Node) CommitIndex() uint64 {
	return n.group.Raft.CommitIndex()
}

// AppliedIndex returns the last applied log index.
func (n *Node) AppliedIndex() uint64 {
	return n.group.Raft.AppliedIndex()
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
	if t, err := strconv.ParseUint(n.group.Raft.Stats()["term"], 10, 64); err == nil {
		return t
	}
	return 0
}

// IsLeader returns true if this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	return n.group.Raft.State() == raft.Leader
}

// Shutdown gracefully shuts down the Raft node and closes its underlying
// BoltDB log and stable stores.
func (n *Node) Shutdown() error {
	return n.group.Shutdown()
}

// ErrNotLeader is returned when a write arrives at a non-leader node.
var ErrNotLeader = errors.New("not leader")
