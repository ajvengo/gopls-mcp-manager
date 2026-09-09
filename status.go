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
		{"GOPLS_MANAGER_MAX_LANES", &limits.Lanes},
		{"GOPLS_MANAGER_MAX_CACHE_ENTRIES", &limits.CacheEntries},
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
	// One process-table snapshot avoids a serial fork and timeout per record.
	commands := make(map[int]string, len(records))
	if len(records) != 0 {
		wanted := make(map[int]bool, len(records))
		for _, r := range records {
			wanted[r.PID] = true
		}
		query, cancel := context.WithTimeout(ctx, time.Second)
		// Darwin rejects an entire -p list if one manually edited PID is above
		// its platform limit. Snapshot once, then retain only requested rows.
		out, _ := exec.CommandContext(query, "ps", "-ww", "-axo", "pid=,rss=,command=").Output()
		cancel()
		// Missing or malformed rows remain unknown.
		for line := range strings.SplitSeq(string(out), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 3 {
				continue
			}
			pid, err := strconv.Atoi(fields[0])
			if err == nil && wanted[pid] {
				commands[pid] = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), fields[0]))
			}
		}
	}
	for _, r := range records {
		usage := serverUsage{record: r, Identity: "unknown"}
		if command, ok := commands[r.PID]; ok {
			fields := strings.Fields(command)
			usage.Identity = "different process"
			if len(fields) > 1 && strings.Contains(command, goplsBinary) && strings.Contains(command, mcpAddress(r.Port)) {
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
