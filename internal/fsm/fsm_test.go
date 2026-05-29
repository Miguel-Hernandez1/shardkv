package fsm

import (
	"bytes"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func applyCmd(t *testing.T, k *KVStore, cmd Command) {
	t.Helper()
	data, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}
	result := k.Apply(&raft.Log{Data: data})
	if err, ok := result.(error); ok {
		t.Fatalf("apply returned error: %v", err)
	}
}

func TestFSM_Apply_Set(t *testing.T) {
	k := New()
	applyCmd(t, k, Command{Op: OpSet, Key: "foo", Value: []byte("bar")})

	val, ok := k.Get("foo")
	if !ok {
		t.Fatal("key not found after set")
	}
	if string(val) != "bar" {
		t.Fatalf("expected 'bar', got %q", val)
	}
}

func TestFSM_Apply_Delete(t *testing.T) {
	k := New()
	applyCmd(t, k, Command{Op: OpSet, Key: "foo", Value: []byte("bar")})
	applyCmd(t, k, Command{Op: OpDelete, Key: "foo"})

	_, ok := k.Get("foo")
	if ok {
		t.Fatal("key still exists after delete")
	}
}

func TestFSM_Snapshot_RoundTrip(t *testing.T) {
	k := New()

	for i := 0; i < 50; i++ {
		key := "k" + string(rune('a'+i%26))
		applyCmd(t, k, Command{Op: OpSet, Key: key, Value: []byte("val")})
	}

	snap, err := k.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	var buf bytes.Buffer
	sink := &testSink{buf: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}

	k2 := New()
	if err := k2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify all keys are present in the restored FSM.
	orig := k.Scan("")
	restored := k2.Scan("")
	if len(orig) != len(restored) {
		t.Fatalf("original has %d keys, restored has %d", len(orig), len(restored))
	}
	for key, val := range orig {
		rv, ok := restored[key]
		if !ok {
			t.Fatalf("key %q missing from restored FSM", key)
		}
		if !bytes.Equal(val, rv) {
			t.Fatalf("key %q: original %q, restored %q", key, val, rv)
		}
	}
}

func TestFSM_UnknownOp(t *testing.T) {
	k := New()
	data, _ := EncodeCommand(Command{Op: "UNKNOWN", Key: "k"})
	result := k.Apply(&raft.Log{Data: data})
	if result == nil {
		t.Fatal("expected error for unknown op")
	}
	if _, ok := result.(error); !ok {
		t.Fatal("expected error type")
	}
}

// testSink implements raft.SnapshotSink for testing.
type testSink struct {
	buf *bytes.Buffer
}

func (s *testSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *testSink) Close() error                { return nil }
func (s *testSink) ID() string                  { return "test" }
func (s *testSink) Cancel() error               { return nil }
