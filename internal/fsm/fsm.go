package fsm

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

// KVStore is the Raft FSM. It holds the authoritative KV state.
type KVStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func New() *KVStore {
	return &KVStore{data: make(map[string][]byte)}
}

// Apply is called by Raft after a log entry commits. Must be deterministic.
func (k *KVStore) Apply(l *raft.Log) interface{} {
	cmd, err := DecodeCommand(l.Data)
	if err != nil {
		return fmt.Errorf("decode command: %w", err)
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	switch cmd.Op {
	case OpSet:
		k.data[cmd.Key] = cmd.Value
		return nil
	case OpDelete:
		delete(k.data, cmd.Key)
		return nil
	default:
		return fmt.Errorf("unknown op: %s", cmd.Op)
	}
}

// Snapshot returns a point-in-time copy of the FSM state.
func (k *KVStore) Snapshot() (raft.FSMSnapshot, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	snapshot := make(map[string][]byte, len(k.data))
	for key, val := range k.data {
		v := make([]byte, len(val))
		copy(v, val)
		snapshot[key] = v
	}
	return &fsmSnapshot{data: snapshot}, nil
}

// Restore replaces the FSM state from a snapshot.
func (k *KVStore) Restore(rc io.ReadCloser) error {
	data, err := decodeSnapshot(rc)
	if err != nil {
		return err
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	k.data = data
	return nil
}

// Get returns the value for key and whether it exists.
func (k *KVStore) Get(key string) ([]byte, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	v, ok := k.data[key]
	return v, ok
}

// Scan returns all key-value pairs whose key starts with prefix.
// An empty prefix returns all keys.
func (k *KVStore) Scan(prefix string) map[string][]byte {
	k.mu.RLock()
	defer k.mu.RUnlock()

	result := make(map[string][]byte)
	for key, val := range k.data {
		if strings.HasPrefix(key, prefix) {
			v := make([]byte, len(val))
			copy(v, val)
			result[key] = v
		}
	}
	return result
}
