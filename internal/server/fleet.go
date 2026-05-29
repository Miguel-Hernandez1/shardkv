package server

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

//go:embed static/fleet.html
var fleetHTML []byte

func (s *Server) handleFleet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(fleetHTML)
}

// handleFleetNodes returns host-facing addresses for each cluster node so the
// browser can poll each node's /v1/status directly. It uses the hostname from
// the incoming request (r.Host) so the addresses work whether the cluster is
// accessed via localhost, a remote IP, or a hostname.
func (s *Server) handleFleetNodes(w http.ResponseWriter, r *http.Request) {
	type nodeInfo struct {
		ID           string `json:"id"`
		HostHTTPAddr string `json:"host_http_addr"`
		RaftAddr     string `json:"raft_addr"`
	}

	clientHost, _, err := parseHostPort(r.Host)
	if err != nil {
		// r.Host may lack a port (e.g. "localhost") — use it as-is.
		clientHost = r.Host
	}

	peers := s.node.PeerRaftAddrs()

	// Fall back to self if no peers configured.
	if len(peers) == 0 {
		_, selfPort, err := parseHostPort(s.node.RaftAddr())
		if err == nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode([]nodeInfo{{
				ID:           s.node.NodeID(),
				HostHTTPAddr: fmt.Sprintf("%s:%d", clientHost, selfPort-1000),
				RaftAddr:     s.node.RaftAddr(),
			}})
			return
		}
	}

	nodes := make([]nodeInfo, 0, len(peers))
	for _, raftAddr := range peers {
		host, port, err := parseHostPort(raftAddr)
		if err != nil {
			continue
		}
		id := host
		if strings.Contains(host, ".") || host == "localhost" {
			if raftAddr == s.node.RaftAddr() {
				id = s.node.NodeID()
			} else {
				id = host
			}
		}
		nodes = append(nodes, nodeInfo{
			ID:           id,
			HostHTTPAddr: fmt.Sprintf("%s:%d", clientHost, port-1000),
			RaftAddr:     raftAddr,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}
