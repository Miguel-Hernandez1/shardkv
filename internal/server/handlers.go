package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/mighdz/shardkv/internal/fsm"
	"github.com/mighdz/shardkv/internal/metrics"
	"github.com/mighdz/shardkv/internal/node"
)

const forwardedHeader = "X-ShardKV-Forwarded"

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	start := time.Now()

	val, ok, err := s.node.Get(key)
	if err != nil {
		recordOp("get", "error", start)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !ok {
		recordOp("get", "not_found", start)
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	recordOp("get", "ok", start)
	w.WriteHeader(http.StatusOK)
	w.Write(val)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	start := time.Now()

	if !s.node.IsLeader() {
		s.redirectToLeader(w, r)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MB limit
	if err != nil {
		recordOp("set", "error", start)
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	err = s.node.Apply(fsm.Command{Op: fsm.OpSet, Key: key, Value: body})
	if err == node.ErrNotLeader {
		s.redirectToLeader(w, r)
		return
	}
	if err != nil {
		recordOp("set", "error", start)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	recordOp("set", "ok", start)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	start := time.Now()

	if !s.node.IsLeader() {
		s.redirectToLeader(w, r)
		return
	}

	err := s.node.Apply(fsm.Command{Op: fsm.OpDelete, Key: key})
	if err == node.ErrNotLeader {
		s.redirectToLeader(w, r)
		return
	}
	if err != nil {
		recordOp("delete", "error", start)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	recordOp("delete", "ok", start)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	start := time.Now()

	result, err := s.node.Scan(prefix)
	if err != nil {
		recordOp("scan", "error", start)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	type entry struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	entries := make([]entry, 0, len(result))
	for k, v := range result {
		entries = append(entries, entry{Key: k, Value: string(v)})
	}

	recordOp("scan", "ok", start)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{
		"node_id":       s.node.NodeID(),
		"raft_state":    s.node.State(),
		"leader_addr":   s.node.LeaderAddr(),
		"commit_index":  s.node.CommitIndex(),
		"applied_index": s.node.AppliedIndex(),
		"term":          s.node.Term(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if !s.node.IsLeader() {
		s.redirectToLeader(w, r)
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

	if err := s.node.AddVoter(req.NodeID, req.RaftAddr); err != nil {
		log.Printf("AddVoter %s: %v", req.NodeID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "node %s joined\n", req.NodeID)
}

func (s *Server) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(forwardedHeader) != "" {
		// Already forwarded once — no leader available right now.
		http.Error(w, "no leader available", http.StatusServiceUnavailable)
		return
	}

	leaderRaftAddr := s.node.LeaderAddr()
	if leaderRaftAddr == "" {
		// Election in progress or cluster unavailable.
		http.Error(w, "no leader elected", http.StatusServiceUnavailable)
		return
	}

	targetURL := s.leaderRedirectURL(r, leaderRaftAddr)
	if targetURL == "" {
		http.Error(w, "could not resolve leader address", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Location", targetURL)
	w.WriteHeader(http.StatusTemporaryRedirect)
}

func recordOp(op, status string, start time.Time) {
	metrics.OperationsTotal.WithLabelValues(op, status).Inc()
	metrics.OperationDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
}
