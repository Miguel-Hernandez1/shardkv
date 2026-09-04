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

	srv := server.New(m, cfg.HTTPAddr, cfg.MetricsAddr)

	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	// Joining the config group and picking up this node's shard
	// assignments both need real contact with enough of the other nodes
	// to form a quorum, which might not exist yet at this exact moment,
	// most obviously right after the whole cluster restarts together. A
	// deployment where other nodes wait for this one's health check
	// before they themselves start (so they can join through it) would
	// deadlock if that health check waited on this too, so it doesn't:
	// the HTTP server above is already serving, and this runs in the
	// background, retrying patiently until the cluster is actually able
	// to make progress.
	go func() {
		joinClient := &http.Client{Timeout: 5 * time.Second}

		if !cfg.Bootstrap && len(cfg.Peers) > 0 {
			log.Printf("Joining config group via %s", cfg.Peers[0])
			for {
				if err := m.JoinConfigGroup(joinClient, cfg.Peers[0].RaftAddr); err == nil {
					break
				} else {
					log.Printf("join config group: %v, retrying", err)
					time.Sleep(2 * time.Second)
				}
			}
		}

		for {
			if err := m.EnsureAssignedShards(joinClient); err == nil {
				break
			} else {
				log.Printf("ensure assigned shards: %v, retrying", err)
				time.Sleep(2 * time.Second)
			}
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
