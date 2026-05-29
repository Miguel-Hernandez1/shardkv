package node

import (
	"os"
	"strings"
)

// Config holds all configuration for a ShardKV node.
type Config struct {
	NodeID      string
	RaftAddr    string   // host:port for Raft TCP transport
	HTTPAddr    string   // :port for HTTP API
	MetricsAddr string   // :port for Prometheus /metrics
	DataDir     string   // directory for BoltDB and snapshots
	Peers       []string // all node Raft addresses (including self)
	Bootstrap   bool     // if true, this node bootstraps a new cluster
}

func ConfigFromEnv() Config {
	cfg := Config{
		NodeID:      getenv("SHARDKV_NODE_ID", "node1"),
		RaftAddr:    getenv("SHARDKV_RAFT_ADDR", "localhost:9081"),
		HTTPAddr:    getenv("SHARDKV_HTTP_ADDR", ":8081"),
		MetricsAddr: getenv("SHARDKV_METRICS_ADDR", ":10081"),
		DataDir:     getenv("SHARDKV_DATA_DIR", "/tmp/shardkv"),
		Bootstrap:   getenv("SHARDKV_BOOTSTRAP", "false") == "true",
	}

	if peers := os.Getenv("SHARDKV_PEERS"); peers != "" {
		cfg.Peers = strings.Split(peers, ",")
	}

	return cfg
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
