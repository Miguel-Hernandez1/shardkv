package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/mighdz/shardkv/internal/fsm"
	"github.com/mighdz/shardkv/internal/metrics"
	"github.com/mighdz/shardkv/internal/node"
)

const forwardedHeader = "X-ShardKV-Forwarded"

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	shardID := s.cluster.ShardFor(key)
	n := s.cluster.Shard(shardID)
	start := time.Now()

	val, ok, err := n.Get(key)
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

// handleScan fans out across every shard replica hosted locally and merges
// the results. Since a key belongs to exactly one shard, there is no
// possibility of duplicate keys across shards.
func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	start := time.Now()

	merged := make(map[string][]byte)
	for _, shardID := range s.cluster.ShardIDs() {
		result, err := s.cluster.Shard(shardID).Scan(prefix)
		if err != nil {
			recordOp("scan", "error", shardID, start)
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		for k, v := range result {
			merged[k] = v
		}
	}

	type entry struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	entries := make([]entry, 0, len(merged))
	for k, v := range merged {
		entries = append(entries, entry{Key: k, Value: string(v)})
	}

	recordOp("scan", "ok", -1, start)
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
