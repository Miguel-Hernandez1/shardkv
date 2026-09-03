package server

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mighdz/shardkv/internal/cluster"
	"github.com/mighdz/shardkv/internal/metrics"
)

// Server is the HTTP API server for a ShardKV node. It fronts every shard
// replica the node's cluster.Manager hosts.
type Server struct {
	cluster     *cluster.Manager
	httpAddr    string
	metricsAddr string
}

func New(m *cluster.Manager, httpAddr, metricsAddr string) *Server {
	return &Server{cluster: m, httpAddr: httpAddr, metricsAddr: metricsAddr}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/keys/{key}", s.handleGet)
	mux.HandleFunc("PUT /v1/keys/{key}", s.handlePut)
	mux.HandleFunc("DELETE /v1/keys/{key}", s.handleDelete)
	mux.HandleFunc("GET /v1/keys", s.handleScan)
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("POST /v1/cluster/join", s.handleJoin)
	mux.HandleFunc("GET /v1/internal/shards/{shard}/scan", s.handleInternalShardScan)
	mux.HandleFunc("GET /fleet", s.handleFleet)
	mux.HandleFunc("GET /fleet/nodes", s.handleFleetNodes)

	go s.startMetricsServer()
	go s.pollRaftMetrics()

	log.Printf("HTTP API listening on %s", s.httpAddr)
	return http.ListenAndServe(s.httpAddr, corsMiddleware(mux))
}

// corsMiddleware adds permissive CORS headers so the Fleet View page (served on
// one node's port) can poll /v1/status on the other nodes' ports.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-ShardKV-Forwarded")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) startMetricsServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	log.Printf("Metrics listening on %s", s.metricsAddr)
	if err := http.ListenAndServe(s.metricsAddr, mux); err != nil {
		log.Printf("metrics server error: %v", err)
	}
}

func (s *Server) pollRaftMetrics() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for _, id := range s.cluster.ShardIDs() {
			n := s.cluster.Shard(id)
			label := strconv.Itoa(id)

			switch n.State() {
			case "Follower":
				metrics.RaftState.WithLabelValues(label).Set(0)
			case "Candidate":
				metrics.RaftState.WithLabelValues(label).Set(1)
			case "Leader":
				metrics.RaftState.WithLabelValues(label).Set(2)
			default:
				metrics.RaftState.WithLabelValues(label).Set(3)
			}

			commit := n.CommitIndex()
			applied := n.AppliedIndex()
			metrics.RaftCommitIndex.WithLabelValues(label).Set(float64(commit))
			metrics.RaftAppliedIndex.WithLabelValues(label).Set(float64(applied))

			lag := float64(0)
			if commit > applied {
				lag = float64(commit - applied)
			}
			metrics.ShardReplicationLag.WithLabelValues(label).Set(lag)
		}
	}
}

// leaderRedirectURL builds a redirect URL pointing to the leader of a
// specific shard.
//
// Port convention: each shard's Raft port is the node's base Raft port
// plus shardID*cluster.ShardPortOffset; HTTP port = base Raft port - 1000.
// We use the *client's* hostname (from r.Host) so that redirects work
// whether the client connects via "localhost" (from the host machine) or
// the internal Docker service name (e.g. "node2"). Both cases resolve
// correctly because the port arithmetic is the same regardless of hostname.
func (s *Server) leaderRedirectURL(r *http.Request, shardID int, leaderRaftAddr string) string {
	_, leaderRaftPort, err := parseHostPort(leaderRaftAddr)
	if err != nil {
		return ""
	}
	baseRaftPort := cluster.BaseRaftPort(leaderRaftPort, shardID)
	leaderHTTPPort := baseRaftPort - 1000

	clientHost, _, err := parseHostPort(r.Host)
	if err != nil {
		clientHost = r.Host
	}

	return fmt.Sprintf("http://%s:%d%s", clientHost, leaderHTTPPort, r.URL.RequestURI())
}
