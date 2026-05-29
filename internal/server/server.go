package server

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mighdz/shardkv/internal/metrics"
	"github.com/mighdz/shardkv/internal/node"
)

// Server is the HTTP API server for a ShardKV node.
type Server struct {
	node        *node.Node
	httpAddr    string
	metricsAddr string
}

func New(n *node.Node, httpAddr, metricsAddr string) *Server {
	return &Server{node: n, httpAddr: httpAddr, metricsAddr: metricsAddr}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/keys/{key}", s.handleGet)
	mux.HandleFunc("PUT /v1/keys/{key}", s.handlePut)
	mux.HandleFunc("DELETE /v1/keys/{key}", s.handleDelete)
	mux.HandleFunc("GET /v1/keys", s.handleScan)
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("POST /v1/cluster/join", s.handleJoin)
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
		state := s.node.State()
		switch state {
		case "Follower":
			metrics.RaftState.Set(0)
		case "Candidate":
			metrics.RaftState.Set(1)
		case "Leader":
			metrics.RaftState.Set(2)
		default:
			metrics.RaftState.Set(3)
		}
		metrics.RaftCommitIndex.Set(float64(s.node.CommitIndex()))
		metrics.RaftAppliedIndex.Set(float64(s.node.AppliedIndex()))
	}
}

// leaderRedirectURL builds a redirect URL pointing to the leader.
//
// Port convention: HTTP port = Raft port - 1000 (e.g. Raft 9081 → HTTP 8081).
// We use the *client's* hostname (from r.Host) so that redirects work whether
// the client connects via "localhost" (from the host machine) or the internal
// Docker service name (e.g. "node2"). Both cases resolve correctly because the
// port offset is the same regardless of hostname.
func (s *Server) leaderRedirectURL(r *http.Request, leaderRaftAddr string) string {
	_, leaderRaftPort, err := parseHostPort(leaderRaftAddr)
	if err != nil {
		return ""
	}
	leaderHTTPPort := leaderRaftPort - 1000

	clientHost, _, err := parseHostPort(r.Host)
	if err != nil {
		clientHost = r.Host
	}

	return fmt.Sprintf("http://%s:%d%s", clientHost, leaderHTTPPort, r.URL.RequestURI())
}
