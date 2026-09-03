package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

func main() {
	var (
		addrs       string
		ops         int
		concurrency int
		ratio       float64
		keySpace    int
		consistency string
	)

	cmd := &cobra.Command{
		Use:   "bench",
		Short: "ShardKV load generator",
		RunE: func(cmd *cobra.Command, args []string) error {
			if consistency != "linearizable" && consistency != "stale" {
				return fmt.Errorf("--consistency must be \"linearizable\" or \"stale\", got %q", consistency)
			}
			return run(strings.Split(addrs, ","), ops, concurrency, ratio, keySpace, consistency)
		},
	}

	cmd.Flags().StringVar(&addrs, "addr", "localhost:8081,localhost:8082,localhost:8083",
		"Comma-separated node HTTP addresses. Each worker picks one at random per "+
			"request; a write, or a linearizable read, that lands on a non-leader "+
			"replica for its shard follows the server's redirect to that shard's leader.")
	cmd.Flags().IntVar(&ops, "ops", 10000, "Total number of operations")
	cmd.Flags().IntVar(&concurrency, "concurrency", 32, "Number of concurrent workers")
	cmd.Flags().Float64Var(&ratio, "ratio", 0.8, "Fraction of operations that are reads (0.0-1.0)")
	cmd.Flags().IntVar(&keySpace, "key-space", 1000, "Number of distinct keys")
	cmd.Flags().StringVar(&consistency, "consistency", "linearizable",
		"Read consistency for the read fraction of the workload: linearizable "+
			"(reads redirect to the shard leader, like the server's default) or "+
			"stale (reads are served locally by whichever node was picked, faster "+
			"but without a freshness guarantee).")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(addrs []string, totalOps, concurrency int, readRatio float64, keySpace int, consistency string) error {
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	printShardLayout(addrs[0])
	fmt.Printf("Read consistency: %s\n\n", consistency)

	seedAddr := addrs[0]
	if err := seedKeys(seedAddr, clamp(keySpace, 100)); err != nil {
		return fmt.Errorf("seed keys: %w", err)
	}

	latencies := make([]int64, 0, totalOps)
	var mu sync.Mutex
	var errors atomic.Int64

	work := make(chan int, totalOps)
	for i := 0; i < totalOps; i++ {
		work <- i
	}
	close(work)

	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(rng *rand.Rand) {
			defer wg.Done()
			c := &http.Client{Timeout: 5 * time.Second}
			for range work {
				addr := addrs[rng.Intn(len(addrs))]
				key := fmt.Sprintf("bench:key:%d", rng.Intn(keySpace))
				t0 := time.Now()

				var err error
				if rng.Float64() < readRatio {
					err = doGet(c, addr, key, consistency)
				} else {
					err = doPut(c, addr, key, "value-"+key)
				}

				if err != nil {
					errors.Add(1)
				}
				mu.Lock()
				latencies = append(latencies, time.Since(t0).Microseconds())
				mu.Unlock()
			}
		}(rand.New(rand.NewSource(time.Now().UnixNano() + int64(w))))
	}

	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	fmt.Printf("\n=== Benchmark Results ===\n")
	fmt.Printf("Operations:   %d\n", totalOps)
	fmt.Printf("Duration:     %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Ops/sec:      %.0f\n", float64(totalOps)/elapsed.Seconds())
	fmt.Printf("Errors:       %d\n", errors.Load())
	fmt.Printf("Latency p50:  %s\n", microsToString(percentile(latencies, 0.50)))
	fmt.Printf("Latency p99:  %s\n", microsToString(percentile(latencies, 0.99)))
	fmt.Printf("Latency p999: %s\n", microsToString(percentile(latencies, 0.999)))
	return nil
}

// printShardLayout queries one node's status and prints which node leads
// each shard, purely for operator visibility before the run starts. It has
// no effect on request routing: every request still goes to a randomly
// chosen node address and relies on that node's own redirect if it isn't
// the leader for the key's shard.
func printShardLayout(addr string) {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/status", addr))
	if err != nil {
		fmt.Printf("shard layout unavailable: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var status struct {
		NodeID string `json:"node_id"`
		Shards []struct {
			ShardID    int    `json:"shard_id"`
			RaftState  string `json:"raft_state"`
			LeaderAddr string `json:"leader_addr"`
		} `json:"shards"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		fmt.Printf("shard layout unavailable: %v\n", err)
		return
	}

	fmt.Printf("Shard layout (from %s):\n", addr)
	for _, sh := range status.Shards {
		fmt.Printf("  shard %d leader=%s\n", sh.ShardID, sh.LeaderAddr)
	}
	fmt.Println()
}

func seedKeys(addr string, n int) error {
	c := &http.Client{Timeout: 5 * time.Second}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("bench:key:%d", i)
		if err := doPut(c, addr, key, "seed"); err != nil {
			return err
		}
	}
	return nil
}

func doGet(c *http.Client, addr, key, consistency string) error {
	url := fmt.Sprintf("http://%s/v1/keys/%s?consistency=%s", addr, key, consistency)
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

func doPut(c *http.Client, addr, key, value string) error {
	req, _ := http.NewRequest(http.MethodPut,
		fmt.Sprintf("http://%s/v1/keys/%s", addr, key),
		strings.NewReader(value))
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func microsToString(us int64) string {
	if us < 1000 {
		return fmt.Sprintf("%dµs", us)
	}
	return fmt.Sprintf("%.2fms", float64(us)/1000)
}

func clamp(a, b int) int {
	if a < b {
		return a
	}
	return b
}
