package cluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/mighdz/shardkv/internal/shard"
)

// Peer identifies one physical node in the cluster by both its node ID
// and its base Raft address. Placement needs the real node ID up front,
// before that node has joined anything: a node's ID can't be reliably
// guessed from its address's hostname when nodes share a host and differ
// only by port, which every same-host deployment (including every
// integration test in this repo) does.
type Peer struct {
	NodeID   string
	RaftAddr string
}

// Config holds the configuration for a physical ShardKV process. A process
// hosts one Raft replica of the config group (always fully replicated)
// plus one independent Raft replica of each shard it is assigned, all
// sharing the same HTTP and metrics listeners.
type Config struct {
	NodeID            string
	RaftAddr          string // this node's base Raft address; shard i listens on port+i*shardPortOffset
	HTTPAddr          string // :port for the HTTP API, shared by every shard
	MetricsAddr       string // :port for Prometheus /metrics
	DataDir           string // root directory; each shard gets its own subdirectory
	Peers             []Peer // every physical node in the cluster, including self
	Bootstrap         bool   // if true, this node bootstraps the config group and proposes the initial placement
	NumShards         int
	ReplicationFactor int // shard replicas per shard; must be <= len(Peers)
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
		parsed, err := ParsePeers(peers)
		if err != nil {
			panic(fmt.Sprintf("SHARDKV_PEERS: %v", err))
		}
		cfg.Peers = parsed
	}
	if n := os.Getenv("SHARDKV_NUM_SHARDS"); n != "" {
		if v, err := strconv.Atoi(n); err == nil && v > 0 {
			cfg.NumShards = v
		}
	}

	// Default replication factor is "every node replicates every shard",
	// matching the behavior before per-shard placement existed.
	cfg.ReplicationFactor = len(cfg.Peers)
	if rf := os.Getenv("SHARDKV_REPLICATION_FACTOR"); rf != "" {
		if v, err := strconv.Atoi(rf); err == nil && v > 0 {
			cfg.ReplicationFactor = v
		}
	}

	return cfg
}

// ParsePeers parses a comma-separated peer list in "nodeID@host:port"
// form, e.g. "node1@node1:9081,node2@node2:9082,node3@node3:9083".
func ParsePeers(spec string) ([]Peer, error) {
	entries := strings.Split(spec, ",")
	peers := make([]Peer, 0, len(entries))
	for _, e := range entries {
		e = strings.TrimSpace(e)
		at := strings.Index(e, "@")
		if at <= 0 || at == len(e)-1 {
			return nil, fmt.Errorf("invalid peer %q: expected \"nodeID@host:port\"", e)
		}
		peers = append(peers, Peer{NodeID: e[:at], RaftAddr: e[at+1:]})
	}
	return peers, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
