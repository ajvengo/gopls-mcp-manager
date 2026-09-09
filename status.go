package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type serverUsage struct {
	record
	RSSKiB   *int64
	Identity string
	// Null is deliberate: process existence is not proof of client activity.
	ActiveClients *int
	LogBytes      *int64
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
		if info, err := os.Lstat(m.logPath(r.Worktree)); err == nil && info.Mode().IsRegular() {
			size := info.Size()
			usage.LogBytes = &size
		}
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
	logs, err := m.logs(ctx, false)
	if err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(struct {
		RecordedServers int
		RegistryIntact  bool
		Servers         []serverUsage
		Logs            logUsage
	}{len(servers), intact, servers, logs})
}
