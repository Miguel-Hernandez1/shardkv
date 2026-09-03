package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/mighdz/shardkv/internal/cluster"
	"github.com/mighdz/shardkv/internal/fsm"
	"github.com/mighdz/shardkv/internal/metrics"
	"github.com/mighdz/shardkv/internal/node"
)

const forwardedHeader = "X-ShardKV-Forwarded"

// internalClient is used for server-to-server calls this node makes on a
// caller's behalf (fetching one shard's scan contribution from that
// shard's actual leader). Kept short: this is an internal hop within the
// cluster, not a client-facing request.
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

// handleGet defaults to linearizable reads: if this replica isn't the
// key's shard leader, it redirects to that shard's leader the same way a
// write would, rather than silently answering from a replica that might
// not have seen the latest committed write. ?consistency=stale opts into
// a fast local read on any replica instead, with no such guarantee.
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

// handleScan fans out across every shard and merges the results. Since a
// key belongs to exactly one shard, there is no possibility of duplicate
// keys across shards.
//
// It defaults to stale reads: a scan already touches every shard, and
// forcing every one of those through its own leader by default would turn
// one bulk read into a multi-way, leader-bound fan-out on every call. A
// caller that needs a linearizable scan can opt in with
// ?consistency=linearizable; for any shard this replica doesn't lead, that
// mode fetches the shard's contribution from its actual leader over an
// internal RPC (see handleInternalShardScan) instead of settling for a
// possibly-stale local answer.
func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	start := time.Now()

	consistency, err := parseConsistency(r, node.Stale)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	merged := make(map[string][]byte)
	for _, shardID := range s.cluster.ShardIDs() {
		n := s.cluster.Shard(shardID)

		if consistency == node.Stale || n.IsLeader() {
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

		result, err := s.fetchShardScanFromLeader(shardID, n.LeaderAddr(), prefix)
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

// fetchShardScanFromLeader asks shardID's actual leader for a linearizable
// scan of just that shard, over an internal HTTP hop. leaderRaftAddr is
// that leader's Raft transport address; the HTTP port is derived from it
// with the same base-port arithmetic used for client redirects.
func (s *Server) fetchShardScanFromLeader(shardID int, leaderRaftAddr, prefix string) (map[string][]byte, error) {
	if leaderRaftAddr == "" {
		return nil, fmt.Errorf("no leader elected for shard %d", shardID)
	}
	leaderHost, leaderRaftPort, err := parseHostPort(leaderRaftAddr)
	if err != nil {
		return nil, fmt.Errorf("parse leader address for shard %d: %w", shardID, err)
	}
	leaderHTTPPort := cluster.BaseRaftPort(leaderRaftPort, shardID) - 1000

	targetURL := fmt.Sprintf("http://%s:%d/v1/internal/shards/%d/scan?prefix=%s",
		leaderHost, leaderHTTPPort, shardID, url.QueryEscape(prefix))

	resp, err := internalClient.Get(targetURL)
	if err != nil {
		return nil, fmt.Errorf("fetch shard %d from leader: %w", shardID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("shard %d leader returned status %d", shardID, resp.StatusCode)
	}

	var entries []scanEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("decode shard %d response: %w", shardID, err)
	}

	result := make(map[string][]byte, len(entries))
	for _, e := range entries {
		result[e.Key] = []byte(e.Value)
	}
	return result, nil
}

// handleInternalShardScan serves a linearizable scan of exactly one local
// shard, for another node's handleScan to call when it needs this shard's
// contribution and this node is that shard's leader. It is not part of the
// client-facing API.
func (s *Server) handleInternalShardScan(w http.ResponseWriter, r *http.Request) {
	shardID, err := strconv.Atoi(r.PathValue("shard"))
	if err != nil {
		http.Error(w, "invalid shard id", http.StatusBadRequest)
		return
	}
	n := s.cluster.Shard(shardID)
	if n == nil {
		http.Error(w, "unknown shard", http.StatusNotFound)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	result, err := n.Scan(prefix, node.Linearizable)
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

// handleJoin registers a new physical node as a voter across every shard's
// Raft group. It must run on the node that currently leads every shard,
// which is true for the bootstrap node during the startup join window.
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	leaderShard := s.cluster.Shard(0)
	if !leaderShard.IsLeader() {
		s.redirectToLeader(w, r, 0, leaderShard)
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

	if err := s.cluster.AddVoter(req.NodeID, req.RaftAddr); err != nil {
		log.Printf("AddVoter %s: %v", req.NodeID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "node %s joined\n", req.NodeID)
}

func (s *Server) redirectToLeader(w http.ResponseWriter, r *http.Request, shardID int, n *node.Node) {
	if r.Header.Get(forwardedHeader) != "" {
		// Already forwarded once, no leader available right now.
		http.Error(w, "no leader available", http.StatusServiceUnavailable)
		return
	}

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

func recordOp(op, status string, shardID int, start time.Time) {
	shardLabel := "all"
	if shardID >= 0 {
		shardLabel = strconv.Itoa(shardID)
	}
	metrics.OperationsTotal.WithLabelValues(op, status, shardLabel).Inc()
	metrics.OperationDuration.WithLabelValues(op, shardLabel).Observe(time.Since(start).Seconds())
}
