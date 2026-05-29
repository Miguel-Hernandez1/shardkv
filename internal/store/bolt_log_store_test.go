package store

import (
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

func newTestLogStore(t *testing.T) *BoltLogStore {
	t.Helper()
	s, err := NewBoltLogStore(filepath.Join(t.TempDir(), "log.db"))
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestBoltLogStore_EmptyIndices(t *testing.T) {
	s := newTestLogStore(t)
	first, err := s.FirstIndex()
	if err != nil || first != 0 {
		t.Fatalf("expected first=0, got %d err=%v", first, err)
	}
	last, err := s.LastIndex()
	if err != nil || last != 0 {
		t.Fatalf("expected last=0, got %d err=%v", last, err)
	}
}

func TestBoltLogStore_StoreAndRetrieve(t *testing.T) {
	s := newTestLogStore(t)

	logs := make([]*raft.Log, 10)
	for i := range logs {
		logs[i] = &raft.Log{
			Index: uint64(i + 1),
			Term:  1,
			Type:  raft.LogCommand,
			Data:  []byte("data"),
		}
	}
	if err := s.StoreLogs(logs); err != nil {
		t.Fatalf("store: %v", err)
	}

	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()
	if first != 1 || last != 10 {
		t.Fatalf("expected first=1 last=10, got %d %d", first, last)
	}

	var out raft.Log
	if err := s.GetLog(5, &out); err != nil {
		t.Fatalf("get log 5: %v", err)
	}
	if out.Index != 5 {
		t.Fatalf("expected index 5, got %d", out.Index)
	}
}

func TestBoltLogStore_DeleteRange(t *testing.T) {
	s := newTestLogStore(t)

	logs := make([]*raft.Log, 20)
	for i := range logs {
		logs[i] = &raft.Log{Index: uint64(i + 1), Term: 1, Data: []byte("x")}
	}
	s.StoreLogs(logs)

	if err := s.DeleteRange(5, 15); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	// Indices 5–15 should be gone.
	for i := 5; i <= 15; i++ {
		var out raft.Log
		if err := s.GetLog(uint64(i), &out); err != raft.ErrLogNotFound {
			t.Fatalf("expected ErrLogNotFound for index %d, got %v", i, err)
		}
	}

	// Indices outside the range should remain.
	for _, idx := range []uint64{1, 2, 3, 4, 16, 17, 18, 19, 20} {
		var out raft.Log
		if err := s.GetLog(idx, &out); err != nil {
			t.Fatalf("index %d should still exist: %v", idx, err)
		}
	}
}
