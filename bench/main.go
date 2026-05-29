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
		addr        string
		ops         int
		concurrency int
		ratio       float64
		keySpace    int
	)

	cmd := &cobra.Command{
		Use:   "bench",
		Short: "ShardKV load generator",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(addr, ops, concurrency, ratio, keySpace)
		},
	}

	cmd.Flags().StringVar(&addr, "addr", "localhost:8081", "Node address (will auto-discover leader)")
	cmd.Flags().IntVar(&ops, "ops", 10000, "Total number of operations")
	cmd.Flags().IntVar(&concurrency, "concurrency", 32, "Number of concurrent workers")
	cmd.Flags().Float64Var(&ratio, "ratio", 0.8, "Fraction of operations that are reads (0.0–1.0)")
	cmd.Flags().IntVar(&keySpace, "key-space", 1000, "Number of distinct keys")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(addr string, totalOps, concurrency int, readRatio float64, keySpace int) error {
	leaderAddr, err := discoverLeader(addr)
	if err != nil {
		return fmt.Errorf("discover leader: %w", err)
	}
	fmt.Printf("Leader: %s\n", leaderAddr)

	// Pre-seed some keys so reads don't all 404.
	if err := seedKeys(leaderAddr, clamp(keySpace, 100)); err != nil {
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
		go func() {
			defer wg.Done()
			c := &http.Client{Timeout: 5 * time.Second}
			for range work {
				key := fmt.Sprintf("bench:key:%d", rand.Intn(keySpace))
				t0 := time.Now()

				var err error
				if rand.Float64() < readRatio {
					err = doGet(c, leaderAddr, key)
				} else {
					err = doPut(c, leaderAddr, key, "value-"+key)
				}

				if err != nil {
					errors.Add(1)
				}
				mu.Lock()
				latencies = append(latencies, time.Since(t0).Microseconds())
				mu.Unlock()
			}
		}()
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

func discoverLeader(addr string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/status", addr))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var status struct {
		RaftState  string `json:"raft_state"`
		LeaderAddr string `json:"leader_addr"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return "", err
	}

	if status.RaftState == "Leader" {
		return addr, nil
	}

	// Convert Raft addr to HTTP addr.
	if status.LeaderAddr != "" {
		return raftToHTTP(status.LeaderAddr), nil
	}
	return addr, nil
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

func doGet(c *http.Client, addr, key string) error {
	resp, err := c.Get(fmt.Sprintf("http://%s/v1/keys/%s", addr, key))
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

func raftToHTTP(raftAddr string) string {
	for i := len(raftAddr) - 1; i >= 0; i-- {
		if raftAddr[i] == ':' {
			host := raftAddr[:i]
			var port int
			fmt.Sscanf(raftAddr[i+1:], "%d", &port)
			return fmt.Sprintf("%s:%d", host, port-1000)
		}
	}
	return raftAddr
}

func clamp(a, b int) int {
	if a < b {
		return a
	}
	return b
}
