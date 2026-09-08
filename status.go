package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func limitsFromEnv() (callLimits, error) {
	limits := defaultCallLimits()
	for _, setting := range []struct {
		name   string
		target *int
	}{
		{"GOPLS_MANAGER_MAX_OUTSTANDING", &limits.Session},
		{"GOPLS_MANAGER_MAX_OUTSTANDING_PER_LANE", &limits.PerLane},
	} {
		if raw := os.Getenv(setting.name); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value <= 0 {
				return limits, fmt.Errorf("%s must be a positive integer", setting.name)
			}
			*setting.target = value
		}
	}
	if raw := os.Getenv("GOPLS_MANAGER_EXECUTION_TIMEOUT"); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value < 0 {
			return limits, fmt.Errorf("GOPLS_MANAGER_EXECUTION_TIMEOUT must be a nonnegative duration")
		}
		limits.Execution = value
	}
	return limits, nil
}

type serverUsage struct {
	record
	RSSKiB   *int64
	Identity string
	// Null is deliberate: process existence is not proof of client activity.
	ActiveClients *int
}

// status is observational: unlike list it never sweeps, signals or writes the
// registry. RSS is shown only when the command line matches our recorded server.
func (m *manager) status(ctx context.Context, w io.Writer) error {
	records, intact, err := readMap(m.mapPath)
	if err != nil {
		return err
	}
	servers := make([]serverUsage, 0, len(records))
	for _, r := range records {
		usage := serverUsage{record: r, Identity: "unknown"}
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		out, err := exec.CommandContext(ctx, "ps", "-ww", "-o", "rss=,command=", "-p", strconv.Itoa(r.PID)).Output()
		cancel()
		if err == nil {
			fields := strings.Fields(string(out))
			usage.Identity = "different process"
			if len(fields) > 1 && strings.Contains(string(out), goplsBinary) && strings.Contains(string(out), mcpAddress(r.Port)) {
				usage.Identity = "matched"
				if rss, err := strconv.ParseInt(fields[0], 10, 64); err == nil && rss >= 0 {
					usage.RSSKiB = &rss
				}
			}
		}
		servers = append(servers, usage)
	}
	return json.NewEncoder(w).Encode(struct {
		RecordedServers int
		RegistryIntact  bool
		Servers         []serverUsage
	}{len(servers), intact, servers})
}
