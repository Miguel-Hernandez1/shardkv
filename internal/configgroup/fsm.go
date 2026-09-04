package configgroup

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// OpType identifies a config group command.
type OpType string

// OpSetConfig replaces the cluster's node list, shard count, and
// replication factor in one step, and recomputes the resulting
// Assignment as part of applying the same log entry. There is
// deliberately no separate "add node" or "remove node" command yet: this
// phase does not move any shard data when the assignment changes, so a
// caller is expected to submit a complete desired membership rather than
// an incremental diff. Live migration (v0.5) is what makes an
// incremental API meaningful.
const OpSetConfig OpType = "SET_CONFIG"

// Command is the unit written to the config group's Raft log.
type Command struct {
	Op                OpType   `json:"op"`
	Nodes             []string `json:"nodes"`
	NumShards         int      `json:"num_shards"`
	ReplicationFactor int      `json:"replication_factor"`
}

func encodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func decodeCommand(data []byte) (Command, error) {
	var cmd Command
	return cmd, json.Unmarshal(data, &cmd)
}

// State is the config group's replicated state: the cluster's current
// node list, shard count, replication factor, and the resulting
// assignment. Version increments on every applied command, so callers
// can tell whether the assignment they're holding is still current.
type State struct {
	Nodes             []string
	NumShards         int
	ReplicationFactor int
	Assignment        Assignment
	Version           uint64
}

// FSM is the config group's Raft state machine.
type FSM struct {
	mu    sync.RWMutex
	state State
}

func NewFSM() *FSM {
	return &FSM{}
}

// Apply is called by Raft after a log entry commits. Must be deterministic.
func (f *FSM) Apply(l *raft.Log) interface{} {
	cmd, err := decodeCommand(l.Data)
	if err != nil {
		return fmt.Errorf("decode command: %w", err)
	}

	switch cmd.Op {
	case OpSetConfig:
		assignment, err := Compute(cmd.Nodes, cmd.NumShards, cmd.ReplicationFactor)
		if err != nil {
			return fmt.Errorf("compute assignment: %w", err)
		}

		f.mu.Lock()
		defer f.mu.Unlock()
		f.state = State{
			Nodes:             append([]string(nil), cmd.Nodes...),
			NumShards:         cmd.NumShards,
			ReplicationFactor: cmd.ReplicationFactor,
			Assignment:        assignment,
			Version:           f.state.Version + 1,
		}
		return nil
	default:
		return fmt.Errorf("unknown op: %s", cmd.Op)
	}
}

// State returns a snapshot of the current config state, read from the
// local FSM with no leadership requirement. Every node needs to know its
// own shard assignment, not just the config group's leader, so this is
// intentionally a plain local read rather than a linearizable one:
// config changes are rare (cluster membership events), so a config
// replica lagging by a few hundred milliseconds after such a change is
// an acceptable, temporary condition, unlike KV reads which default to
// linearizable because they can be arbitrarily frequent and the leader
// hop is cheap relative to correctness.
func (f *FSM) State() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state
}

// Snapshot returns a point-in-time copy of the FSM state.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &fsmSnapshot{state: f.state}, nil
}

// Restore replaces the FSM state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var state State
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = state
	return nil
}

type fsmSnapshot struct {
	state State
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.state); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
