package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mighdz/shardkv/integration/testutil"
	"github.com/mighdz/shardkv/internal/node"
	"github.com/mighdz/shardkv/internal/server"
)

// httpCluster wraps a ShardCluster with a real HTTP server per node, for
// tests that exercise behavior that only exists at the HTTP layer: leader
// redirects and the internal per-shard scan fan-out.
type httpCluster struct {
	t        *testing.T
	sc       *testutil.ShardCluster
	httpAddr []string
}

func newHTTPCluster(t *testing.T, numNodes, numShards int) *httpCluster {
	t.Helper()
	sc := testutil.NewShardCluster(t, numNodes, numShards)

	hc := &httpCluster{t: t, sc: sc, httpAddr: make([]string, numNodes)}
	for i, m := range sc.Managers() {
		// Leader redirects and the internal scan fan-out both derive a
		// node's HTTP port from its shard 0 Raft port (base - 1000), the
		// same convention cmd/server and docker-compose use. The HTTP
		// listener has to actually follow that convention for redirects
		// to resolve to a real, reachable address.
		host, raftPort, err := net.SplitHostPort(m.RaftAddr())
		if err != nil {
			t.Fatalf("parse raft addr %q: %v", m.RaftAddr(), err)
		}
		port, err := strconv.Atoi(raftPort)
		if err != nil {
			t.Fatalf("parse raft port %q: %v", raftPort, err)
		}
		httpAddr := net.JoinHostPort(host, strconv.Itoa(port-1000))
		metricsAddr := fmt.Sprintf("127.0.0.1:%d", freeTCPPort(t))
		hc.httpAddr[i] = httpAddr

		srv := server.New(m, httpAddr, metricsAddr)
		go srv.Start()
	}
	for _, addr := range hc.httpAddr {
		waitForHTTP(t, addr)
	}
	return hc
}

// noRedirectClient never follows redirects, so callers can distinguish a
// 307 (would-have-redirected) from a 200 (served locally).
var noRedirectClient = &http.Client{
	Timeout: 3 * time.Second,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// redirectFollowingClient follows the server's 307 redirects, like the CLI
// and bench tool do.
var redirectFollowingClient = &http.Client{Timeout: 3 * time.Second}

func waitForHTTP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("HTTP server at %s never came up", addr)
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// put writes a key through node index ni's HTTP address, following any
// redirect to the correct shard leader, and fails the test on error.
func (hc *httpCluster) put(ni int, key, value string) {
	hc.t.Helper()
	req, err := http.NewRequest(http.MethodPut,
		fmt.Sprintf("http://%s/v1/keys/%s", hc.httpAddr[ni], key), strings.NewReader(value))
	if err != nil {
		hc.t.Fatalf("build PUT request: %v", err)
	}
	resp, err := redirectFollowingClient.Do(req)
	if err != nil {
		hc.t.Fatalf("PUT %s: %v", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		hc.t.Fatalf("PUT %s: status %d", key, resp.StatusCode)
	}
}

// nonLeaderIndexForKey returns the index of a node that is not currently
// the leader of key's shard.
func (hc *httpCluster) nonLeaderIndexForKey(key string) int {
	hc.t.Helper()
	shardID := hc.sc.Managers()[0].ShardFor(key)
	for i, m := range hc.sc.Managers() {
		if !m.Shard(shardID).IsLeader() {
			return i
		}
	}
	hc.t.Fatalf("every node claims to lead shard %d for key %q", shardID, key)
	return -1
}

func (hc *httpCluster) waitForKeyApplied(key string) {
	hc.t.Helper()
	shardID := hc.sc.Managers()[0].ShardFor(key)
	leader := hc.sc.ShardLeader(shardID)
	hc.sc.WaitForShardApplied(shardID, leader.CommitIndex())
}

// TestGetDefaultsToLinearizableAndRedirects confirms that, with no
// ?consistency= parameter, a GET to a node that is not the key's shard
// leader gets a 307 to that leader, exactly like a write would, instead of
// silently being served from a replica that might not be caught up.
func TestGetDefaultsToLinearizableAndRedirects(t *testing.T) {
	t.Parallel()
	hc := newHTTPCluster(t, 3, 3)

	const key = "user:1"
	hc.put(0, key, "alice")
	hc.waitForKeyApplied(key)

	nonLeader := hc.nonLeaderIndexForKey(key)

	resp, err := noRedirectClient.Get(fmt.Sprintf("http://%s/v1/keys/%s", hc.httpAddr[nonLeader], key))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("default GET on a non-leader replica: status = %d, want %d (redirect)", resp.StatusCode, http.StatusTemporaryRedirect)
	}
	if resp.Header.Get("Location") == "" {
		t.Fatal("307 response missing Location header")
	}

	// Following the redirect should land on the correct value.
	resp2, err := redirectFollowingClient.Get(fmt.Sprintf("http://%s/v1/keys/%s", hc.httpAddr[nonLeader], key))
	if err != nil {
		t.Fatalf("GET (following redirect): %v", err)
	}
	defer resp2.Body.Close()
	body := readAll(t, resp2)
	if resp2.StatusCode != http.StatusOK || body != "alice" {
		t.Fatalf("after following redirect: status=%d body=%q, want 200 \"alice\"", resp2.StatusCode, body)
	}
}

// TestGetStaleServesLocallyWithoutRedirect confirms that
// ?consistency=stale is served directly by whichever replica receives it,
// leader or not, with no redirect.
func TestGetStaleServesLocallyWithoutRedirect(t *testing.T) {
	t.Parallel()
	hc := newHTTPCluster(t, 3, 3)

	const key = "user:2"
	hc.put(0, key, "bob")
	hc.waitForKeyApplied(key)

	nonLeader := hc.nonLeaderIndexForKey(key)

	resp, err := noRedirectClient.Get(fmt.Sprintf("http://%s/v1/keys/%s?consistency=stale", hc.httpAddr[nonLeader], key))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	body := readAll(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stale GET on a non-leader replica: status = %d, want 200 (no redirect)", resp.StatusCode)
	}
	if body != "bob" {
		t.Fatalf("stale GET returned %q, want \"bob\"", body)
	}
}

// TestGetInvalidConsistencyRejected confirms an unrecognized ?consistency=
// value is a client error, not a silent fallback.
func TestGetInvalidConsistencyRejected(t *testing.T) {
	t.Parallel()
	hc := newHTTPCluster(t, 3, 1)

	resp, err := http.Get(fmt.Sprintf("http://%s/v1/keys/anything?consistency=eventual", hc.httpAddr[0]))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid consistency value: status = %d, want 400", resp.StatusCode)
	}
}

// TestScanLinearizableFansOutToShardLeaders writes keys that land on
// different shards, then requests a linearizable scan from a node that
// does not lead every one of those shards, and confirms the result is
// still complete: the server must fetch each shard's contribution from
// that shard's actual leader rather than settling for a partial local
// answer.
func TestScanLinearizableFansOutToShardLeaders(t *testing.T) {
	t.Parallel()
	hc := newHTTPCluster(t, 3, 3)

	keys := map[string]string{
		"scan:1": "v1",
		"scan:2": "v2",
		"scan:3": "v3",
		"scan:4": "v4",
		"scan:5": "v5",
	}
	shardsSeen := map[int]bool{}
	for k, v := range keys {
		hc.put(0, k, v)
		hc.waitForKeyApplied(k)
		shardsSeen[hc.sc.Managers()[0].ShardFor(k)] = true
	}
	if len(shardsSeen) < 2 {
		t.Fatalf("test keys only landed on %d shard(s); can't exercise cross-shard fan-out", len(shardsSeen))
	}

	// Find a node that does not lead every shard our keys touched, so a
	// linearizable scan from it must fan out internally.
	queryFrom := -1
	for i, m := range hc.sc.Managers() {
		leadsAll := true
		for shardID := range shardsSeen {
			if !m.Shard(shardID).IsLeader() {
				leadsAll = false
				break
			}
		}
		if !leadsAll {
			queryFrom = i
			break
		}
	}
	if queryFrom == -1 {
		t.Fatal("every node leads every touched shard; can't exercise the fan-out path")
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/v1/keys?prefix=scan:&consistency=linearizable", hc.httpAddr[queryFrom]))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("linearizable scan: status = %d", resp.StatusCode)
	}

	var entries []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		t.Fatalf("decode scan response: %v", err)
	}

	got := make(map[string]string, len(entries))
	for _, e := range entries {
		got[e.Key] = e.Value
	}
	for k, want := range keys {
		if got[k] != want {
			t.Fatalf("linearizable scan from a non-all-leading node: key %s = %q, want %q (missing shards were not fanned out to their leader)", k, got[k], want)
		}
	}
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return string(body)
}

// Sanity check that node.Consistency's zero value is Linearizable, the
// safe default, not Stale, so a caller that forgets to specify one gets
// the strong guarantee rather than accidentally the weak one.
func TestConsistencyZeroValueIsLinearizable(t *testing.T) {
	var c node.Consistency
	if c != node.Linearizable {
		t.Fatalf("zero value of node.Consistency is %v, want Linearizable", c)
	}
}
