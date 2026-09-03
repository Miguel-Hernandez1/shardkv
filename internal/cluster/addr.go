package cluster

import (
	"fmt"
	"net"
	"strconv"
)

// shardPortOffset spaces out each shard's Raft TCP port from a physical
// node's base Raft port. With a base of 9081 and this offset, shard 0
// listens on 9081, shard 1 on 9181, shard 2 on 9281, and so on.
const shardPortOffset = 100

// shardRaftAddr derives the Raft transport address a given shard uses on a
// physical node from that node's base Raft address.
func shardRaftAddr(baseAddr string, shardID int) (string, error) {
	host, portStr, err := net.SplitHostPort(baseAddr)
	if err != nil {
		return "", fmt.Errorf("invalid raft address %q: %w", baseAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", fmt.Errorf("invalid port in %q: %w", baseAddr, err)
	}
	return net.JoinHostPort(host, strconv.Itoa(port+shardID*shardPortOffset)), nil
}
