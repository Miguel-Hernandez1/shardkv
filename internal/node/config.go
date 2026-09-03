package node

// Config holds the configuration for a single Raft group: one shard
// replica running on one physical node.
type Config struct {
	NodeID    string
	RaftAddr  string   // host:port for Raft TCP transport
	DataDir   string   // directory for BoltDB and snapshots
	Peers     []string // all replica Raft addresses for this shard (including self)
	Bootstrap bool     // if true, this replica bootstraps a new Raft group
}
