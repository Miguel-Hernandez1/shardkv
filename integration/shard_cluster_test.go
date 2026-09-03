package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/mighdz/shardkv/integration/testutil"
	"github.com/mighdz/shardkv/internal/fsm"
	"github.com/mighdz/shardkv/internal/shard"
)

// TestDeterministicShardPlacement verifies that every node in the cluster
// computes the same shard for the same key, and that repeated calls are
// stable. Placement must be predictable for routing to work at all.
func TestDeterministicShardPlacement(t *testing.T) {
	t.Parallel()
	c := testutil.NewShardCluster(t, 3, 3)

	keys := []string{"user:1", "user:2", "product:1", "order:42", "a", "zzz"}
	for _, key := range keys {
		want := shard.KeyToShard(key, c.NumShards())
		for _, m := range c.Managers() {
			if got := m.ShardFor(key); got != want {
				t.Fatalf("node %s: ShardFor(%q) = %d, want %d (mismatched routing across nodes)", m.NodeID(), key, got, want)
			}
		}
		// Stable across repeated calls too.
		for i := 0; i < 10; i++ {
			if got := shard.KeyToShard(key, c.NumShards()); got != want {
				t.Fatalf("KeyToShard(%q) unstable: %d vs %d", key, want, got)
			}
		}
	}
}

// TestShardReplicationWithinShard writes through one shard's leader and
// confirms the value replicates to every replica of that specific shard.
func TestShardReplicationWithinShard(t *testing.T) {
	t.Parallel()
	c := testutil.NewShardCluster(t, 3, 3)

	const shardID = 1
	leader := c.ShardLeader(shardID)
	if err := leader.Apply(fsm.Command{Op: fsm.OpSet, Key: "foo", Value: []byte("bar")}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	c.WaitForShardApplied(shardID, leader.CommitIndex())

	for i, m := range c.Managers() {
		val, ok, err := m.Shard(shardID).Get("foo")
		if err != nil {
			t.Fatalf("node %d get: %v", i, err)
		}
		if !ok || string(val) != "bar" {
			t.Fatalf("node %d: expected 'bar', got ok=%v val=%q", i, ok, val)
		}
	}
}

// TestIndependentShardLeaderElection confirms that each shard elects its
// own leader independently: forcing an election in one shard must not
// change leadership in another shard.
func TestIndependentShardLeaderElection(t *testing.T) {
	t.Parallel()
	c := testutil.NewShardCluster(t, 3, 3)

	leaders := make(map[int]string, c.NumShards())
	for id := 0; id < c.NumShards(); id++ {
		leaders[id] = c.ShardLeader(id).NodeID()
	}

	// Force shard 0's leader to step down; shards 1 and 2 must be
	// unaffected.
	shard0Leader := c.ShardLeader(0)
	if err := shard0Leader.Shutdown(); err != nil {
		t.Fatalf("shutdown shard 0 leader: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	var newShard0Leader string
	for time.Now().Before(deadline) {
		for _, m := range c.Managers() {
			n := m.Shard(0)
			if n != nil && n.State() != "Shutdown" && n.IsLeader() {
				newShard0Leader = m.NodeID()
			}
		}
		if newShard0Leader != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if newShard0Leader == "" {
		t.Fatal("no new leader elected for shard 0 after shutdown")
	}
	if newShard0Leader == leaders[0] {
		t.Fatalf("shard 0 leader did not change after shutdown (still %s)", leaders[0])
	}

	for id := 1; id < c.NumShards(); id++ {
		if got := c.ShardLeader(id).NodeID(); got != leaders[id] {
			t.Fatalf("shard %d leader changed from %s to %s after shard 0's leader was shut down; shards must be independent", id, leaders[id], got)
		}
	}
}

// TestShardLeaderFailover verifies that killing a shard's leader triggers
// a new election among that shard's surviving replicas and that writes
// resume against the new leader.
func TestShardLeaderFailover(t *testing.T) {
	t.Parallel()
	c := testutil.NewShardCluster(t, 3, 3)

	const shardID = 2
	leader := c.ShardLeader(shardID)
	if err := leader.Apply(fsm.Command{Op: fsm.OpSet, Key: "before", Value: []byte("yes")}); err != nil {
		t.Fatalf("pre-failure write: %v", err)
	}

	if err := leader.Shutdown(); err != nil {
		t.Logf("shutdown leader: %v", err)
	}

	newLeader := c.ShardLeader(shardID)
	if newLeader == leader {
		t.Fatal("shard leader did not change after shutdown")
	}

	if err := newLeader.Apply(fsm.Command{Op: fsm.OpSet, Key: "after", Value: []byte("yes")}); err != nil {
		t.Fatalf("post-failover write: %v", err)
	}
	c.WaitForShardApplied(shardID, newLeader.CommitIndex())

	for _, m := range c.Managers() {
		n := m.Shard(shardID)
		if n == leader {
			continue
		}
		val, ok, err := n.Get("after")
		if err != nil {
			t.Fatalf("get after failover: %v", err)
		}
		if !ok || string(val) != "yes" {
			t.Fatalf("expected 'yes', got ok=%v val=%q", ok, val)
		}
	}
}

// TestCrossShardConsistency writes many keys that hash across every shard
// and confirms each key lands in its predicted shard and is consistent
// across every replica of that shard.
func TestCrossShardConsistency(t *testing.T) {
	t.Parallel()
	c := testutil.NewShardCluster(t, 3, 3)

	const n = 60
	targetIndex := make(map[int]uint64, c.NumShards())
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key:%04d", i)
		shardID := c.Managers()[0].ShardFor(key)
		leader := c.ShardLeader(shardID)
		if err := leader.Apply(fsm.Command{Op: fsm.OpSet, Key: key, Value: []byte(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatalf("write %s to shard %d: %v", key, shardID, err)
		}
		targetIndex[shardID] = leader.CommitIndex()
	}
	for shardID, idx := range targetIndex {
		c.WaitForShardApplied(shardID, idx)
	}

	seenShards := map[int]bool{}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key:%04d", i)
		want := shard.KeyToShard(key, c.NumShards())
		seenShards[want] = true

		for ni, m := range c.Managers() {
			val, ok, err := m.Shard(want).Get(key)
			if err != nil {
				t.Fatalf("node %d get %s: %v", ni, key, err)
			}
			if !ok {
				t.Fatalf("node %d: key %s not found on predicted shard %d", ni, key, want)
			}
			if string(val) != fmt.Sprintf("v%d", i) {
				t.Fatalf("node %d: key %s = %q, want v%d", ni, key, val, i)
			}
		}
	}

	if len(seenShards) < 2 {
		t.Fatalf("test key set only landed on %d shard(s); increase n or check routing", len(seenShards))
	}
}

// TestShardCrashRecovery stops an entire cluster and restarts every node
// from disk, verifying BoltDB-backed Raft state and the FSM snapshot
// recover correctly for every shard.
func TestShardCrashRecovery(t *testing.T) {
	t.Parallel()
	c := testutil.NewShardCluster(t, 3, 2)

	written := map[string]string{}
	targetIndex := make(map[int]uint64, c.NumShards())
	for shardID := 0; shardID < c.NumShards(); shardID++ {
		key := fmt.Sprintf("shard-%d-key", shardID)
		val := fmt.Sprintf("shard-%d-val", shardID)
		leader := c.ShardLeader(shardID)
		if err := leader.Apply(fsm.Command{Op: fsm.OpSet, Key: key, Value: []byte(val)}); err != nil {
			t.Fatalf("write to shard %d: %v", shardID, err)
		}
		c.WaitForShardApplied(shardID, leader.CommitIndex())
		written[key] = val
		targetIndex[shardID] = leader.CommitIndex()
	}

	// Stop the entire cluster, then bring every node back up from disk.
	for i := 0; i < 3; i++ {
		c.RestartNode(i)
	}

	// Wait for every shard to re-elect a leader, then for every replica's
	// FSM to finish replaying its persisted log before reading. Raft
	// restores committed entries into the FSM asynchronously after
	// NewRaft returns, so AppliedIndex can briefly lag CommitIndex right
	// after a restart.
	for shardID := 0; shardID < c.NumShards(); shardID++ {
		c.ShardLeader(shardID)
		c.WaitForShardApplied(shardID, targetIndex[shardID])
	}

	for key, want := range written {
		shardID := c.Managers()[0].ShardFor(key)
		for ni, m := range c.Managers() {
			val, ok, err := m.Shard(shardID).Get(key)
			if err != nil {
				t.Fatalf("node %d get %s after restart: %v", ni, key, err)
			}
			if !ok || string(val) != want {
				t.Fatalf("node %d: key %s after restart = ok=%v val=%q, want %q", ni, key, ok, val, want)
			}
		}
	}

	// The cluster must still be writable after recovery.
	for shardID := 0; shardID < c.NumShards(); shardID++ {
		leader := c.ShardLeader(shardID)
		if err := leader.Apply(fsm.Command{Op: fsm.OpSet, Key: "post-recovery", Value: []byte("ok")}); err != nil {
			t.Fatalf("write after recovery on shard %d: %v", shardID, err)
		}
	}
}

// TestShardIsolationOnFailure takes down a majority of one shard's
// replicas and confirms that shard loses its leader while every other
// shard keeps serving reads and writes normally.
func TestShardIsolationOnFailure(t *testing.T) {
	t.Parallel()
	c := testutil.NewShardCluster(t, 3, 3)

	const brokenShard = 0
	const healthyShard = 1

	// Confirm the healthy shard works before breaking anything.
	healthyLeader := c.ShardLeader(healthyShard)
	if err := healthyLeader.Apply(fsm.Command{Op: fsm.OpSet, Key: "before", Value: []byte("ok")}); err != nil {
		t.Fatalf("pre-failure write to healthy shard: %v", err)
	}
	c.WaitForShardApplied(healthyShard, healthyLeader.CommitIndex())

	// Kill 2 of the 3 replicas of brokenShard, leaving it without a quorum,
	// while leaving every other shard's replicas untouched.
	killed := 0
	for _, m := range c.Managers() {
		if killed >= 2 {
			break
		}
		if err := m.Shard(brokenShard).Shutdown(); err != nil {
			t.Fatalf("shutdown replica of broken shard: %v", err)
		}
		killed++
	}

	// The broken shard should lose its leader and stop accepting writes.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		hasLeader := false
		for _, m := range c.Managers() {
			n := m.Shard(brokenShard)
			if n != nil && n.State() != "Shutdown" && n.IsLeader() {
				hasLeader = true
			}
		}
		if !hasLeader {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The healthy shard must be completely unaffected: it still has a
	// leader and still accepts writes.
	stillLeader := c.ShardLeader(healthyShard)
	if err := stillLeader.Apply(fsm.Command{Op: fsm.OpSet, Key: "after", Value: []byte("still-ok")}); err != nil {
		t.Fatalf("healthy shard stopped accepting writes after unrelated shard failure: %v", err)
	}
	c.WaitForShardApplied(healthyShard, stillLeader.CommitIndex())

	val, ok, err := stillLeader.Get("after")
	if err != nil || !ok || string(val) != "still-ok" {
		t.Fatalf("healthy shard read failed after unrelated shard failure: ok=%v err=%v val=%q", ok, err, val)
	}

	// Other untouched shards (2) must also be unaffected.
	otherLeader := c.ShardLeader(2)
	if err := otherLeader.Apply(fsm.Command{Op: fsm.OpSet, Key: "untouched", Value: []byte("fine")}); err != nil {
		t.Fatalf("shard 2 stopped accepting writes after shard 0 failure: %v", err)
	}
}

// TestScanAndDeleteAcrossShards writes keys spread across shards, deletes
// one, and confirms a prefix scan aggregated across every local shard
// replica returns the correct merged result on every node.
func TestScanAndDeleteAcrossShards(t *testing.T) {
	t.Parallel()
	c := testutil.NewShardCluster(t, 3, 3)

	keys := []string{"user:1", "user:2", "user:3", "product:1", "product:2"}
	for _, key := range keys {
		shardID := c.Managers()[0].ShardFor(key)
		leader := c.ShardLeader(shardID)
		if err := leader.Apply(fsm.Command{Op: fsm.OpSet, Key: key, Value: []byte("v")}); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
		c.WaitForShardApplied(shardID, leader.CommitIndex())
	}

	// Delete one "user:" key.
	delShard := c.Managers()[0].ShardFor("user:2")
	delLeader := c.ShardLeader(delShard)
	if err := delLeader.Apply(fsm.Command{Op: fsm.OpDelete, Key: "user:2"}); err != nil {
		t.Fatalf("delete user:2: %v", err)
	}
	c.WaitForShardApplied(delShard, delLeader.CommitIndex())

	for ni, m := range c.Managers() {
		merged := map[string][]byte{}
		for _, id := range m.ShardIDs() {
			result, err := m.Shard(id).Scan("user:")
			if err != nil {
				t.Fatalf("node %d scan shard %d: %v", ni, id, err)
			}
			for k, v := range result {
				merged[k] = v
			}
		}
		if len(merged) != 2 {
			t.Fatalf("node %d: expected 2 user: keys after delete, got %d (%v)", ni, len(merged), merged)
		}
		if _, ok := merged["user:2"]; ok {
			t.Fatalf("node %d: deleted key user:2 still present in scan", ni)
		}
		if _, ok := merged["user:1"]; !ok {
			t.Fatalf("node %d: user:1 missing from scan", ni)
		}
		if _, ok := merged["user:3"]; !ok {
			t.Fatalf("node %d: user:3 missing from scan", ni)
		}
	}
}
