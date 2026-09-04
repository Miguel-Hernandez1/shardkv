package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mighdz/shardkv/internal/cluster"
	"github.com/mighdz/shardkv/internal/server"
)

func main() {
	cfg := cluster.ConfigFromEnv()

	log.Printf("Starting ShardKV node %s | raft=%s http=%s data=%s shards=%d rf=%d bootstrap=%v",
		cfg.NodeID, cfg.RaftAddr, cfg.HTTPAddr, cfg.DataDir, cfg.NumShards, cfg.ReplicationFactor, cfg.Bootstrap)

	m, err := cluster.New(cfg)
	if err != nil {
		log.Fatalf("create cluster manager: %v", err)
	}

	joinClient := &http.Client{Timeout: 5 * time.Second}

	if !cfg.Bootstrap && len(cfg.Peers) > 0 {
		log.Printf("Joining config group via %s", cfg.Peers[0])
		if err := m.JoinConfigGroup(joinClient, cfg.Peers[0].RaftAddr); err != nil {
			log.Fatalf("join config group: %v", err)
		}
	}

	if err := m.EnsureAssignedShards(joinClient); err != nil {
		log.Fatalf("ensure assigned shards: %v", err)
	}

	srv := server.New(m, cfg.HTTPAddr, cfg.MetricsAddr)

	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Printf("Shutting down node %s", cfg.NodeID)
	if err := m.Shutdown(); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
