package server

import (
	"fmt"
	"strconv"
	"strings"
)

func parseHostPort(addr string) (string, int, error) {
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return "", 0, fmt.Errorf("invalid address: %s", addr)
	}
	host := addr[:idx]
	portStr := addr[idx+1:]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port in %s: %w", addr, err)
	}
	return host, port, nil
}
