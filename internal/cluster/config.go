package cluster

import (
	"os"
	"strconv"
	"strings"

	"github.com/mighdz/shardkv/internal/shard"
)

// Config holds the configuration for a physical ShardKV process. A process
// hosts one independent Raft replica per shard, all sharing the same HTTP
// and metrics listeners.
type Config struct {
	NodeID      string
	RaftAddr    string   // this node's base Raft address; shard i listens on port+i*shardPortOffset
	HTTPAddr    string   // :port for the HTTP API, shared by every shard
	MetricsAddr string   // :port for Prometheus /metrics
	DataDir     string   // root directory; each shard gets its own subdirectory
	Peers       []string // base Raft addresses of every physical node, including self
	Bootstrap   bool     // if true, this node bootstraps every shard's Raft group
	NumShards   int
}

func ConfigFromEnv() Config {
	cfg := Config{
		NodeID:      getenv("SHARDKV_NODE_ID", "node1"),
		RaftAddr:    getenv("SHARDKV_RAFT_ADDR", "localhost:9081"),
		HTTPAddr:    getenv("SHARDKV_HTTP_ADDR", ":8081"),
		MetricsAddr: getenv("SHARDKV_METRICS_ADDR", ":10081"),
		DataDir:     getenv("SHARDKV_DATA_DIR", "/tmp/shardkv"),
		Bootstrap:   getenv("SHARDKV_BOOTSTRAP", "false") == "true",
		NumShards:   shard.DefaultCount,
	}

	if peers := os.Getenv("SHARDKV_PEERS"); peers != "" {
		cfg.Peers = strings.Split(peers, ",")
	}
	if n := os.Getenv("SHARDKV_NUM_SHARDS"); n != "" {
		if v, err := strconv.Atoi(n); err == nil && v > 0 {
			cfg.NumShards = v
		}
	}

	return cfg
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
