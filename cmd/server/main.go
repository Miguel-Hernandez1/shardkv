package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mighdz/shardkv/internal/node"
	"github.com/mighdz/shardkv/internal/server"
)

func main() {
	cfg := node.ConfigFromEnv()

	log.Printf("Starting ShardKV node %s | raft=%s http=%s data=%s bootstrap=%v",
		cfg.NodeID, cfg.RaftAddr, cfg.HTTPAddr, cfg.DataDir, cfg.Bootstrap)

	n, err := node.New(cfg)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	if !cfg.Bootstrap && len(cfg.Peers) > 0 {
		if err := joinCluster(cfg, cfg.Peers[0]); err != nil {
			log.Fatalf("join cluster: %v", err)
		}
	}

	srv := server.New(n, cfg.HTTPAddr, cfg.MetricsAddr)

	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Printf("Shutting down node %s", cfg.NodeID)
	if err := n.Shutdown(); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

// joinCluster sends a join request to the bootstrap node with exponential backoff.
func joinCluster(cfg node.Config, bootstrapHTTPAddr string) error {
	payload, _ := json.Marshal(map[string]string{
		"node_id":   cfg.NodeID,
		"raft_addr": cfg.RaftAddr,
	})

	// Convert Raft addr to HTTP addr for the bootstrap peer.
	// The bootstrap node's HTTP addr is derived by convention: raft port - 1000.
	httpAddr := raftToHTTP(bootstrapHTTPAddr)

	url := fmt.Sprintf("http://%s/v1/cluster/join", httpAddr)
	log.Printf("Joining cluster at %s", url)

	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				log.Printf("Successfully joined cluster")
				return nil
			}
		}
		lastErr = err
		wait := time.Duration(attempt) * time.Second
		if wait > 10*time.Second {
			wait = 10 * time.Second
		}
		log.Printf("Join attempt %d failed (%v), retrying in %s", attempt, err, wait)
		time.Sleep(wait)
	}
	return fmt.Errorf("could not join cluster after 30 attempts: %v", lastErr)
}

// raftToHTTP converts a Raft address to an HTTP address.
// Assumes: raft port N → HTTP port N-1000 (e.g. node1:9081 → node1:8081).
func raftToHTTP(raftAddr string) string {
	for i := len(raftAddr) - 1; i >= 0; i-- {
		if raftAddr[i] == ':' {
			host := raftAddr[:i]
			var port int
			fmt.Sscanf(raftAddr[i+1:], "%d", &port)
			return fmt.Sprintf("%s:%d", host, port-1000)
		}
	}
	return raftAddr
}
