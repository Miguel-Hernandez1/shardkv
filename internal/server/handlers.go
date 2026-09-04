package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/mighdz/shardkv/internal/cluster"
	"github.com/mighdz/shardkv/internal/fsm"
	"github.com/mighdz/shardkv/internal/metrics"
	"github.com/mighdz/shardkv/internal/node"
)

// internalClient is used for server-to-server calls this node makes on a
// caller's behalf (fetching one shard's scan contribution from a replica
// that actually hosts it, joining a shard this node was just assigned).
// Kept short: this is an internal hop within the cluster, not a
// client-facing request. Its default redirect following (up to Go's
// standard 10-hop cap) is what keeps a client-facing request that needs
// two internal hops (which replica hosts this shard, then which of that
// shard's replicas is its leader) from needing any custom loop-guarding
// of its own.
var internalClient = &http.Client{Timeout: 3 * time.Second}

// parseConsistency reads the ?consistency= query parameter, defaulting to
// def when absent. "linearizable" and "stale" are the only valid values.
func parseConsistency(r *http.Request, def node.Consistency) (node.Consistency, error) {
	switch v := r.URL.Query().Get("consistency"); v {
	case "":
		return def, nil
	case "linearizable":
		return node.Linearizable, nil
	case "stale":
		return node.Stale, nil
	default:
		return 0, fmt.Errorf("invalid consistency %q: must be \"linearizable\" or \"stale\"", v)
	}
}

// handleGet defaults to linearizable reads: if this node doesn't host the
// key's shard at all, or hosts it but isn't its leader, it redirects
// toward the leader, the same way a write would, rather than silently
// answering from a replica that might not have seen the latest committed
// write. ?consistency=stale opts into a fast local read on any replica
// instead, with no such guarantee, and requires this node to host the
// shard (stale mode has no reason to leave the node that received the
// request, so a node that doesn't host the shard still redirects even
// under stale).
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	shardID := s.cluster.ShardFor(key)
	n := s.cluster.Shard(shardID)
	start := time.Now()

	consistency, err := parseConsistency(r, node.Linearizable)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if n == nil {
		s.redirectToAssignedReplica(w, r, shardID)
		return
	}

	val, ok, err := n.Get(key, consistency)
	if err == node.ErrNotLeader {
		s.redirectToLeader(w, r, shardID, n)
		return
	}
	if err != nil {
		recordOp("get", "error", shardID, start)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !ok {
		recordOp("get", "not_found", shardID, start)
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	recordOp("get", "ok", shardID, start)
	w.WriteHeader(http.StatusOK)
	w.Write(val)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	shardID := s.cluster.ShardFor(key)
	n := s.cluster.Shard(shardID)
	start := time.Now()

	if n == nil {
		s.redirectToAssignedReplica(w, r, shardID)
		return
	}
	if !n.IsLeader() {
		s.redirectToLeader(w, r, shardID, n)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MB limit
	if err != nil {
		recordOp("set", "error", shardID, start)
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	err = n.Apply(fsm.Command{Op: fsm.OpSet, Key: key, Value: body})
	if err == node.ErrNotLeader {
		s.redirectToLeader(w, r, shardID, n)
		return
	}
	if err != nil {
		recordOp("set", "error", shardID, start)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	recordOp("set", "ok", shardID, start)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	shardID := s.cluster.ShardFor(key)
	n := s.cluster.Shard(shardID)
	start := time.Now()

	if n == nil {
		s.redirectToAssignedReplica(w, r, shardID)
		return
	}
	if !n.IsLeader() {
		s.redirectToLeader(w, r, shardID, n)
		return
	}

	err := n.Apply(fsm.Command{Op: fsm.OpDelete, Key: key})
	if err == node.ErrNotLeader {
		s.redirectToLeader(w, r, shardID, n)
		return
	}
	if err != nil {
		recordOp("delete", "error", shardID, start)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	recordOp("delete", "ok", shardID, start)
	w.WriteHeader(http.StatusOK)
}

type scanEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// handleScan fans out across every shard in the cluster (not just the
// ones this node happens to host) and merges the results. Since a key
// belongs to exactly one shard, there is no possibility of duplicate
// keys across shards.
//
// It defaults to stale reads: a scan already touches every shard, and
// forcing every one of those through its own leader by default would turn
// one bulk read into a multi-way, leader-bound fan-out on every call. A
// caller that needs a linearizable scan can opt in with
// ?consistency=linearizable; for any shard this replica doesn't lead
// (including one it doesn't host at all), that mode fetches the shard's
// contribution from a replica that does host it, which itself either
// answers directly (if it's the leader) or redirects toward the real
// leader (see handleInternalShardScan) instead of settling for a
// possibly-stale or partial local answer.
func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	start := time.Now()

	consistency, err := parseConsistency(r, node.Stale)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	merged := make(map[string][]byte)
	for shardID := 0; shardID < s.cluster.NumShards(); shardID++ {
		n := s.cluster.Shard(shardID)

		if n != nil && (consistency == node.Stale || n.IsLeader()) {
			result, err := n.Scan(prefix, consistency)
			if err != nil {
				recordOp("scan", "error", shardID, start)
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			for k, v := range result {
				merged[k] = v
			}
			continue
		}

		result, err := s.fetchShardScan(shardID, consistency, prefix)
		if err != nil {
			recordOp("scan", "error", shardID, start)
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		for k, v := range result {
			merged[k] = v
		}
	}

	entries := make([]scanEntry, 0, len(merged))
	for k, v := range merged {
		entries = append(entries, scanEntry{Key: k, Value: string(v)})
	}

	recordOp("scan", "ok", -1, start)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

// fetchShardScan asks shardID's replicas, in order, for their
// contribution to a scan, stopping at the first one that answers
// successfully. A replica that isn't shardID's leader redirects the
// request toward whichever replica is (see handleInternalShardScan), so
// trying just one usually resolves in at most two hops regardless of
// which replica happens to be first.
func (s *Server) fetchShardScan(shardID int, consistency node.Consistency, prefix string) (map[string][]byte, error) {
	replicas := s.cluster.ReplicasFor(shardID)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("no known replicas for shard %d", shardID)
	}

	var lastErr error
	for _, nodeID := range replicas {
		httpAddr, err := s.cluster.PeerHTTPAddr(nodeID)
		if err != nil {
			lastErr = err
			continue
		}

		targetURL := fmt.Sprintf("http://%s/v1/internal/shards/%d/scan?prefix=%s&consistency=%s",
			httpAddr, shardID, url.QueryEscape(prefix), consistency)

		result, err := func() (map[string][]byte, error) {
			resp, err := internalClient.Get(targetURL)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("status %d", resp.StatusCode)
			}
			var entries []scanEntry
			if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
				return nil, err
			}
			result := make(map[string][]byte, len(entries))
			for _, e := range entries {
				result[e.Key] = []byte(e.Value)
			}
			return result, nil
		}()
		if err != nil {
			lastErr = fmt.Errorf("replica %s: %w", nodeID, err)
			continue
		}
		return result, nil
	}
	return nil, fmt.Errorf("shard %d: every replica failed, last error: %w", shardID, lastErr)
}

// handleInternalShardScan serves a scan of exactly one local shard, for
// another node's fetchShardScan to call when it needs this shard's
// contribution. It is not part of the client-facing API. Under
// linearizable consistency, a replica that isn't the shard's leader
// redirects toward the leader instead of answering, the same way the
// client-facing GET/PUT handlers do.
func (s *Server) handleInternalShardScan(w http.ResponseWriter, r *http.Request) {
	shardID, err := strconv.Atoi(r.PathValue("shard"))
	if err != nil {
		http.Error(w, "invalid shard id", http.StatusBadRequest)
		return
	}
	n := s.cluster.Shard(shardID)
	if n == nil {
		http.Error(w, "shard not hosted here", http.StatusNotFound)
		return
	}

	consistency, err := parseConsistency(r, node.Linearizable)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if consistency == node.Linearizable && !n.IsLeader() {
		s.redirectToLeader(w, r, shardID, n)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	result, err := n.Scan(prefix, consistency)
	if err == node.ErrNotLeader {
		http.Error(w, "not leader for shard", http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	entries := make([]scanEntry, 0, len(result))
	for k, v := range result {
		entries = append(entries, scanEntry{Key: k, Value: string(v)})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

// handleInternalShardJoin adds a node as a voter of exactly one local
// shard's Raft group, for a node that was just assigned that shard and
// needs to join it. Not part of the client-facing API. Must run on that
// shard's leader; a non-leader redirects toward the leader.
func (s *Server) handleInternalShardJoin(w http.ResponseWriter, r *http.Request) {
	shardID, err := strconv.Atoi(r.PathValue("shard"))
	if err != nil {
		http.Error(w, "invalid shard id", http.StatusBadRequest)
		return
	}
	n := s.cluster.Shard(shardID)
	if n == nil {
		http.Error(w, "shard not hosted here", http.StatusNotFound)
		return
	}
	if !n.IsLeader() {
		s.redirectToLeader(w, r, shardID, n)
		return
	}

	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.NodeID == "" || req.RaftAddr == "" {
		http.Error(w, "node_id and raft_addr required", http.StatusBadRequest)
		return
	}

	joinAddr, err := cluster.ShardRaftAddr(req.RaftAddr, shardID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := n.AddVoter(req.NodeID, joinAddr); err != nil {
		log.Printf("shard %d: AddVoter %s: %v", shardID, req.NodeID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "node %s joined shard %d\n", req.NodeID, shardID)
}

type shardStatus struct {
	ShardID      int    `json:"shard_id"`
	RaftState    string `json:"raft_state"`
	LeaderAddr   string `json:"leader_addr"`
	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
	Term         uint64 `json:"term"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	shardIDs := s.cluster.ShardIDs()
	shards := make([]shardStatus, 0, len(shardIDs))
	for _, id := range shardIDs {
		n := s.cluster.Shard(id)
		shards = append(shards, shardStatus{
			ShardID:      id,
			RaftState:    n.State(),
			LeaderAddr:   n.LeaderAddr(),
			CommitIndex:  n.CommitIndex(),
			AppliedIndex: n.AppliedIndex(),
			Term:         n.Term(),
		})
	}

	resp := map[string]interface{}{
		"node_id":    s.cluster.NodeID(),
		"num_shards": s.cluster.NumShards(),
		"shards":     shards,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleJoin registers a new physical node as a voter of the config
// group. It must run on the config group's leader, which is true for the
// bootstrap node during the startup join window. Joining the config
// group is how a new node learns its own shard assignment; joining the
// shards it's assigned happens afterward, against each shard's own
// leader (see handleInternalShardJoin), since the config group's leader
// and a given shard's leader are not necessarily the same node under
// partial placement.
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if !s.cluster.ConfigGroupIsLeader() {
		s.redirectToConfigGroupLeader(w, r)
		return
	}

	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.NodeID == "" || req.RaftAddr == "" {
		http.Error(w, "node_id and raft_addr required", http.StatusBadRequest)
		return
	}

	if err := s.cluster.AddConfigGroupVoter(req.NodeID, req.RaftAddr); err != nil {
		log.Printf("AddConfigGroupVoter %s: %v", req.NodeID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "node %s joined config group\n", req.NodeID)
}

// redirectToLeader redirects toward the Raft address n reports as
// shardID's current leader.
func (s *Server) redirectToLeader(w http.ResponseWriter, r *http.Request, shardID int, n *node.Node) {
	leaderRaftAddr := n.LeaderAddr()
	if leaderRaftAddr == "" {
		// Election in progress or shard unavailable.
		http.Error(w, "no leader elected for shard", http.StatusServiceUnavailable)
		return
	}

	targetURL := s.leaderRedirectURL(r, shardID, leaderRaftAddr)
	if targetURL == "" {
		http.Error(w, "could not resolve leader address", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Location", targetURL)
	w.WriteHeader(http.StatusTemporaryRedirect)
}

// redirectToAssignedReplica redirects toward one of shardID's assigned
// replicas, for a node that doesn't host that shard at all. The target
// may or may not be that shard's current leader; if it isn't, it
// redirects again toward the real leader, so a client following
// redirects (as the CLI and bench tool do) still converges, just in up
// to one extra hop.
func (s *Server) redirectToAssignedReplica(w http.ResponseWriter, r *http.Request, shardID int) {
	replicas := s.cluster.ReplicasFor(shardID)
	if len(replicas) == 0 {
		http.Error(w, "no known replicas for shard", http.StatusServiceUnavailable)
		return
	}

	httpAddr, err := s.cluster.PeerHTTPAddr(replicas[0])
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	_, portStr, err := net.SplitHostPort(httpAddr)
	if err != nil {
		http.Error(w, "could not resolve replica address", http.StatusServiceUnavailable)
		return
	}

	clientHost, _, err := parseHostPort(r.Host)
	if err != nil {
		clientHost = r.Host
	}

	w.Header().Set("Location", fmt.Sprintf("http://%s:%s%s", clientHost, portStr, r.URL.RequestURI()))
	w.WriteHeader(http.StatusTemporaryRedirect)
}

// redirectToConfigGroupLeader redirects toward the config group's current
// leader, for a join request that arrived at a non-leader replica.
func (s *Server) redirectToConfigGroupLeader(w http.ResponseWriter, r *http.Request) {
	leaderRaftAddr := s.cluster.ConfigGroupLeaderAddr()
	if leaderRaftAddr == "" {
		http.Error(w, "no leader elected for config group", http.StatusServiceUnavailable)
		return
	}
	_, portStr, err := net.SplitHostPort(leaderRaftAddr)
	if err != nil {
		http.Error(w, "could not resolve config group leader address", http.StatusServiceUnavailable)
		return
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		http.Error(w, "could not resolve config group leader address", http.StatusServiceUnavailable)
		return
	}

	clientHost, _, err := parseHostPort(r.Host)
	if err != nil {
		clientHost = r.Host
	}
	httpPort := cluster.BaseRaftPortFromConfigGroupPort(port) - 1000

	w.Header().Set("Location", fmt.Sprintf("http://%s:%d%s", clientHost, httpPort, r.URL.RequestURI()))
	w.WriteHeader(http.StatusTemporaryRedirect)
}

func recordOp(op, status string, shardID int, start time.Time) {
	shardLabel := "all"
	if shardID >= 0 {
		shardLabel = strconv.Itoa(shardID)
	}
	metrics.OperationsTotal.WithLabelValues(op, status, shardLabel).Inc()
	metrics.OperationDuration.WithLabelValues(op, shardLabel).Observe(time.Since(start).Seconds())
}
