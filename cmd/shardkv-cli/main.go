package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var (
	addr    string
	timeout int
	asJSON  bool
)

func main() {
	root := &cobra.Command{
		Use:   "shardkv-cli",
		Short: "ShardKV CLI client",
	}

	root.PersistentFlags().StringVar(&addr, "addr", "localhost:8081", "Node HTTP address")
	root.PersistentFlags().IntVar(&timeout, "timeout", 5, "Request timeout in seconds")
	root.PersistentFlags().BoolVar(&asJSON, "json", false, "Output as JSON")

	root.AddCommand(
		cmdGet(),
		cmdSet(),
		cmdDelete(),
		cmdScan(),
		cmdStatus(),
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func cmdGet() *cobra.Command {
	return &cobra.Command{
		Use:   "get <key>",
		Short: "Get the value for a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().Get(fmt.Sprintf("http://%s/v1/keys/%s", addr, args[0]))
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)

			if resp.StatusCode == http.StatusNotFound {
				fmt.Fprintln(os.Stderr, "key not found")
				os.Exit(1)
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("server error %d: %s", resp.StatusCode, body)
			}

			fmt.Println(string(body))
			return nil
		},
	}
}

func cmdSet() *cobra.Command {
	return &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a key to a value",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			req, _ := http.NewRequest(http.MethodPut,
				fmt.Sprintf("http://%s/v1/keys/%s", addr, args[0]),
				strings.NewReader(args[1]))

			resp, err := client().Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("server error %d: %s", resp.StatusCode, body)
			}

			fmt.Println("OK")
			return nil
		},
	}
}

func cmdDelete() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req, _ := http.NewRequest(http.MethodDelete,
				fmt.Sprintf("http://%s/v1/keys/%s", addr, args[0]),
				nil)

			resp, err := client().Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("server error %d: %s", resp.StatusCode, body)
			}

			fmt.Println("OK")
			return nil
		},
	}
}

func cmdScan() *cobra.Command {
	var prefix string
	cmd := &cobra.Command{
		Use:   "scan",
		Short: "List all keys (optionally filtered by prefix)",
		RunE: func(cmd *cobra.Command, args []string) error {
			url := fmt.Sprintf("http://%s/v1/keys", addr)
			if prefix != "" {
				url += "?prefix=" + prefix
			}

			resp, err := client().Get(url)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			var entries []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
				return fmt.Errorf("decode response: %w", err)
			}

			sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

			if asJSON {
				return json.NewEncoder(os.Stdout).Encode(entries)
			}

			if len(entries) == 0 {
				fmt.Println("(empty)")
				return nil
			}
			for _, e := range entries {
				fmt.Printf("%-40s  %s\n", e.Key, e.Value)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&prefix, "prefix", "", "Filter keys by prefix")
	return cmd
}

func cmdStatus() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show node status",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().Get(fmt.Sprintf("http://%s/v1/status", addr))
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			var s struct {
				NodeID       string `json:"node_id"`
				RaftState    string `json:"raft_state"`
				LeaderAddr   string `json:"leader_addr"`
				CommitIndex  uint64 `json:"commit_index"`
				AppliedIndex uint64 `json:"applied_index"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
				return fmt.Errorf("decode status: %w", err)
			}

			if asJSON {
				return json.NewEncoder(os.Stdout).Encode(s)
			}

			fmt.Printf("Node ID:       %s\n", s.NodeID)
			fmt.Printf("State:         %s\n", s.RaftState)
			fmt.Printf("Leader:        %s\n", s.LeaderAddr)
			fmt.Printf("Commit Index:  %d\n", s.CommitIndex)
			fmt.Printf("Applied Index: %d\n", s.AppliedIndex)
			return nil
		},
	}
}

func client() *http.Client {
	return &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Follow redirects automatically (for leader forwarding).
			if len(via) > 2 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}
}
