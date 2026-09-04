// Package raftutil wires up a single hashicorp/raft group: BoltDB log and
// stable stores, a filesystem snapshot store, a TCP transport, and
// first-boot cluster bootstrap. Both a KV shard replica (internal/node)
// and the cluster's config group (internal/configgroup) are one Raft
// group each; this package is the plumbing they share so that a fix here,
// like closing the BoltDB stores on shutdown, only has to happen once.
package raftutil

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mighdz/shardkv/internal/store"
)

const (
	SnapshotThreshold = 64
	SnapshotRetain    = 2
	Timeout           = 10 * time.Second
)

// Config holds the configuration for one Raft group replica.
type Config struct {
	NodeID    string
	RaftAddr  string   // host:port for the Raft TCP transport
	DataDir   string   // directory for BoltDB and snapshots
	Peers     []string // every replica's Raft address, including self (bootstrap only)
	Bootstrap bool     // if true, this replica bootstraps a new Raft group
}

// Group is a running Raft group and the pieces of it that must be closed
// on shutdown.
type Group struct {
	Raft        *raft.Raft
	LogStore    *store.BoltLogStore
	StableStore *store.BoltStableStore
	// Resumed is true if this replica already had persisted Raft state
	// (log entries, a snapshot, or stable-store data) before New ran, so
	// raft.NewRaft resumed it rather than starting from nothing. A
	// caller that only knows how to bootstrap a brand new group, such as
	// the config group's initial-placement proposal in
	// internal/cluster.Manager.New, uses this to skip that step on
	// restart: a resumed replica already has its committed state on
	// disk and, for a replica that isn't alone any more, may never win
	// an election by itself, so waiting for it to become leader again
	// would either be redundant or hang.
	Resumed bool
}

// ErrNotLeader is returned when an operation that requires leadership
// arrives at a non-leader replica.
var ErrNotLeader = errors.New("not leader")

// New creates and starts a Raft group replica backed by fsm.
func New(cfg Config, fsm raft.FSM) (*Group, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	logStore, err := store.NewBoltLogStore(filepath.Join(cfg.DataDir, "log.db"))
	if err != nil {
		return nil, fmt.Errorf("log store: %w", err)
	}

	stableStore, err := store.NewBoltStableStore(filepath.Join(cfg.DataDir, "stable.db"))
	if err != nil {
		return nil, fmt.Errorf("stable store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, SnapshotRetain, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, Timeout, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.SnapshotThreshold = SnapshotThreshold
	raftCfg.SnapshotInterval = 30 * time.Second

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return nil, fmt.Errorf("check existing state: %w", err)
	}

	r, err := raft.NewRaft(raftCfg, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	if cfg.Bootstrap && !hasState {
		servers := BuildBootstrapServers(cfg)
		future := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := future.Error(); err != nil {
			return nil, fmt.Errorf("bootstrap cluster: %w", err)
		}
	}

	return &Group{Raft: r, LogStore: logStore, StableStore: stableStore, Resumed: hasState}, nil
}

// Shutdown gracefully shuts down the Raft group and closes its underlying
// BoltDB log and stable stores. Closing the stores is required, not just
// tidy: bbolt takes an exclusive file lock on open, so a process that
// restarts this group against the same data directory would otherwise
// block forever waiting for a lock this process never released.
func (g *Group) Shutdown() error {
	shutdownErr := g.Raft.Shutdown().Error()

	var closeErr error
	if err := g.LogStore.Close(); err != nil {
		closeErr = fmt.Errorf("close log store: %w", err)
	}
	if err := g.StableStore.Close(); err != nil && closeErr == nil {
		closeErr = fmt.Errorf("close stable store: %w", err)
	}

	if shutdownErr != nil {
		return shutdownErr
	}
	return closeErr
}

// BuildBootstrapServers constructs the initial cluster configuration for
// BootstrapCluster. The local replica uses cfg.NodeID as its ServerID.
// For peers, the ServerID is derived from the hostname portion of the
// Raft address (works for Docker service names like "node2:9082"). If a
// peer address matches cfg.RaftAddr, cfg.NodeID is used for it instead.
func BuildBootstrapServers(cfg Config) []raft.Server {
	if len(cfg.Peers) == 0 {
		return []raft.Server{{
			ID:      raft.ServerID(cfg.NodeID),
			Address: raft.ServerAddress(cfg.RaftAddr),
		}}
	}

	servers := make([]raft.Server, 0, len(cfg.Peers))
	for _, peerAddr := range cfg.Peers {
		id := NodeIDFromRaftAddr(peerAddr)
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

// NodeIDFromRaftAddr extracts the hostname from a host:port Raft address.
// For Docker service names (node1:9081) this returns "node1". For IP
// addresses (127.0.0.1:54321) this returns "127.0.0.1".
func NodeIDFromRaftAddr(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
