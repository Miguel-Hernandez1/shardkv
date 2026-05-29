package store

import (
	"path/filepath"
	"testing"
)

func newTestStableStore(t *testing.T) *BoltStableStore {
	t.Helper()
	s, err := NewBoltStableStore(filepath.Join(t.TempDir(), "stable.db"))
	if err != nil {
		t.Fatalf("new stable store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestBoltStableStore_SetGetUint64(t *testing.T) {
	s := newTestStableStore(t)

	key := []byte("CurrentTerm")
	if err := s.SetUint64(key, 42); err != nil {
		t.Fatalf("set: %v", err)
	}
	val, err := s.GetUint64(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestBoltStableStore_SetGetBytes(t *testing.T) {
	s := newTestStableStore(t)

	key := []byte("LastVoteCand")
	want := []byte("node1")
	if err := s.Set(key, want); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := s.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestBoltStableStore_MissingKey(t *testing.T) {
	s := newTestStableStore(t)
	_, err := s.Get([]byte("nonexistent"))
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestBoltStableStore_OverwriteUint64(t *testing.T) {
	s := newTestStableStore(t)
	key := []byte("term")
	s.SetUint64(key, 1)
	s.SetUint64(key, 99)
	val, _ := s.GetUint64(key)
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
}
