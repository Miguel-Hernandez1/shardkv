package configgroup

import (
	"errors"
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/mighdz/shardkv/internal/raftutil"
)

// ErrNotLeader is returned when a config change is proposed to a replica
// that isn't the config group's current leader.
var ErrNotLeader = raftutil.ErrNotLeader

// Group is the cluster's config group: a small Raft group, independent of
// every shard's own Raft group, whose replicated log is shard-assignment
// changes. Every physical node runs one replica of it, the same way every
// node runs a replica of every shard in the current (non-migrating)
// placement model.
type Group struct {
	cfg raftutil.Config
	rg  *raftutil.Group
	fsm *FSM
}

// New creates and starts a config group replica.
func New(cfg raftutil.Config) (*Group, error) {
	f := NewFSM()
	rg, err := raftutil.New(cfg, f)
	if err != nil {
		return nil, err
	}
	return &Group{cfg: cfg, rg: rg, fsm: f}, nil
}

// State returns the current config as known locally. See FSM.State for
// why this is a plain local read rather than a linearizable one.
func (g *Group) State() State {
	return g.fsm.State()
}

// SetConfig proposes a new node list, shard count, and replication
// factor, replacing the previous config in one committed log entry. Must
// be called on the config group's leader.
func (g *Group) SetConfig(nodes []string, numShards, replicationFactor int) error {
	if g.rg.Raft.State() != raft.Leader {
		return ErrNotLeader
	}

	data, err := encodeCommand(Command{
		Op:                OpSetConfig,
		Nodes:             nodes,
		NumShards:         numShards,
		ReplicationFactor: replicationFactor,
	})
	if err != nil {
		return fmt.Errorf("encode command: %w", err)
	}

	future := g.rg.Raft.Apply(data, raftutil.Timeout)
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

// AddVoter adds a new physical node as a voter in the config group.
// Must be called on the leader.
func (g *Group) AddVoter(nodeID, raftAddr string) error {
	future := g.rg.Raft.AddVoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(raftAddr),
		0, raftutil.Timeout,
	)
	return future.Error()
}

// IsLeader returns true if this replica is the config group's current
// leader.
func (g *Group) IsLeader() bool {
	return g.rg.Raft.State() == raft.Leader
}

// Resumed returns true if this replica already had persisted state (from
// a previous run) when it started, rather than starting from nothing.
func (g *Group) Resumed() bool {
	return g.rg.Resumed
}

// LeaderAddr returns the Raft address of the config group's current
// leader.
func (g *Group) LeaderAddr() string {
	addr, _ := g.rg.Raft.LeaderWithID()
	return string(addr)
}

// NodeID returns this replica's node ID.
func (g *Group) NodeID() string {
	return g.cfg.NodeID
}

// RaftAddr returns this replica's Raft address.
func (g *Group) RaftAddr() string {
	return g.cfg.RaftAddr
}

// Shutdown gracefully shuts down the config group replica.
func (g *Group) Shutdown() error {
	return g.rg.Shutdown()
}
