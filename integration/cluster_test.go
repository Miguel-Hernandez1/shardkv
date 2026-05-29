package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/mighdz/shardkv/integration/testutil"
	"github.com/mighdz/shardkv/internal/fsm"
	"github.com/mighdz/shardkv/internal/node"
)

func TestLeaderElection(t *testing.T) {
	t.Parallel()
	c := testutil.NewCluster(t, 3)

	leader := c.Leader()
	if leader == nil {
		t.Fatal("expected a leader")
	}

	leaderCount := 0
	for _, n := range c.Nodes() {
		if n.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}
}

func TestBasicReplication(t *testing.T) {
	t.Parallel()
	c := testutil.NewCluster(t, 3)

	leader := c.Leader()

	if err := leader.Apply(fsm.Command{Op: fsm.OpSet, Key: "foo", Value: []byte("bar")}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	c.WaitForApplied(leader.CommitIndex())

	for i, n := range c.Nodes() {
		val, ok, err := n.Get("foo")
		if err != nil {
			t.Fatalf("node %d get: %v", i, err)
		}
		if !ok {
			t.Fatalf("node %d: key 'foo' not found", i)
		}
		if string(val) != "bar" {
			t.Fatalf("node %d: expected 'bar', got %q", i, val)
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	t.Parallel()
	c := testutil.NewCluster(t, 3)

	leader := c.Leader()

	if err := leader.Apply(fsm.Command{Op: fsm.OpSet, Key: "before", Value: []byte("yes")}); err != nil {
		t.Fatalf("pre-failure write: %v", err)
	}

	if err := leader.Shutdown(); err != nil {
		t.Logf("shutdown leader: %v", err)
	}

	var newLeader *node.Node
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.Nodes() {
			if n != leader && n.IsLeader() {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader elected after killing original leader")
	}

	if err := newLeader.Apply(fsm.Command{Op: fsm.OpSet, Key: "after", Value: []byte("yes")}); err != nil {
		t.Fatalf("post-failover write: %v", err)
	}

	// Wait only on surviving (non-stopped) nodes.
	targetIdx := newLeader.CommitIndex()
	deadline2 := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline2) {
		allApplied := true
		for _, n := range c.Nodes() {
			if n == nil || n == leader {
				continue
			}
			if n.AppliedIndex() < targetIdx {
				allApplied = false
				break
			}
		}
		if allApplied {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	for _, n := range c.Nodes() {
		if n == nil || n == leader {
			continue
		}
		val, ok, err := n.Get("after")
		if err != nil {
			t.Fatalf("get after failover: %v", err)
		}
		if !ok {
			t.Fatal("key 'after' not found on surviving node")
		}
		if string(val) != "yes" {
			t.Fatalf("expected 'yes', got %q", val)
		}
	}
}

func TestDeleteKey(t *testing.T) {
	t.Parallel()
	c := testutil.NewCluster(t, 3)

	leader := c.Leader()

	if err := leader.Apply(fsm.Command{Op: fsm.OpSet, Key: "del", Value: []byte("temporary")}); err != nil {
		t.Fatalf("set: %v", err)
	}
	c.WaitForApplied(leader.CommitIndex())

	if err := leader.Apply(fsm.Command{Op: fsm.OpDelete, Key: "del"}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	c.WaitForApplied(leader.CommitIndex())

	_, ok, err := leader.Get("del")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestScan(t *testing.T) {
	t.Parallel()
	c := testutil.NewCluster(t, 3)

	leader := c.Leader()

	keys := []string{"user:1", "user:2", "product:1"}
	for _, k := range keys {
		if err := leader.Apply(fsm.Command{Op: fsm.OpSet, Key: k, Value: []byte("v")}); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}
	c.WaitForApplied(leader.CommitIndex())

	result, err := leader.Scan("user:")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 user: keys, got %d", len(result))
	}
	if _, ok := result["user:1"]; !ok {
		t.Fatal("user:1 not in scan result")
	}
	if _, ok := result["user:2"]; !ok {
		t.Fatal("user:2 not in scan result")
	}
}

func TestManyKeys(t *testing.T) {
	t.Parallel()
	c := testutil.NewCluster(t, 3)

	leader := c.Leader()

	const n = 50
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key:%04d", i)
		if err := leader.Apply(fsm.Command{Op: fsm.OpSet, Key: key, Value: []byte("v")}); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
	}
	c.WaitForApplied(leader.CommitIndex())

	// Verify all keys on all nodes.
	for ni, nd := range c.Nodes() {
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("key:%04d", i)
			_, ok, err := nd.Get(key)
			if err != nil {
				t.Fatalf("node %d get %s: %v", ni, key, err)
			}
			if !ok {
				t.Fatalf("node %d: key %s not found", ni, key)
			}
		}
	}
}
