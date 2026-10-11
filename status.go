package main

import (
	"context"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type serverUsage struct {
	record
	RSSKiB   *int64
	Identity string
	// Null is deliberate: process existence is not proof of client activity.
	ActiveClients *int
	LogBytes      *int64
	OpenFiles     *int // lsof rows, shown only for a matched identity
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
	// Kept split: the rss column and the command line are both read below, and
	// rejoining them only to split again is work already done.
	commands := make(map[int][]string, len(records))
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
				// Fields are pid, rss, then the command line ps was asked for.
				commands[pid] = fields[1:]
			}
		}
	}
	var files map[int]int
	if len(records) != 0 && m.openFiles != nil {
		files, _ = m.openFiles(ctx, records) // unknown on failure, like RSS
	}
	openFiles := 0
	for _, r := range records {
		usage := serverUsage{record: r, Identity: "unknown"}
		if info, err := os.Lstat(m.logPath(r.Worktree)); err == nil && info.Mode().IsRegular() {
			size := info.Size()
			usage.LogBytes = &size
		}
		if fields, ok := commands[r.PID]; ok {
			usage.Identity = "different process"
			if len(fields) > 1 && matchesGopls(strings.Join(fields[1:], " "), r.Port) {
				usage.Identity = "matched"
				if rss, err := strconv.ParseInt(fields[0], 10, 64); err == nil && rss >= 0 {
					usage.RSSKiB = &rss
				}
				if n, ok := files[r.PID]; ok {
					usage.OpenFiles = &n
					openFiles += n
				}
			}
		}
		servers = append(servers, usage)
	}
	logs, err := m.logs(ctx, false)
	if err != nil {
		return err
	}
	return json.MarshalEncode(jsontext.NewEncoder(w), struct {
		RecordedServers int
		RegistryIntact  bool
		OpenFiles       int
		OpenFileBudget  int
		Servers         []serverUsage
		Logs            logUsage
	}{len(servers), intact, openFiles, m.limits.OpenFiles, servers, logs})
}

type logUsage struct {
	Policy                            string
	TrimThresholdBytes                int64
	Files, Oversized, Cleared         int
	Bytes, LargestBytes, ClearedBytes int64
}

// logs scans in bounded batches, including logs whose registry record is gone.
// Observations are snapshots: children may append while we inspect a file.
// Explicit trimming discards an oversized file's entire contents in place.
// Never unlink or rename: detached children retain the O_APPEND descriptor.
func (m *manager) logs(ctx context.Context, trim bool) (logUsage, error) {
	limit := m.limits.LogBytes
	usage := logUsage{Policy: "explicit trim-logs; no automatic retention", TrimThresholdBytes: int64(limit)}
	dir := filepath.Dir(m.logPath(""))
	f, err := os.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return usage, nil
	}
	if err != nil {
		return usage, err
	}
	defer func() { _ = f.Close() }()
	for {
		entries, err := f.ReadDir(128)
		if err != nil && !errors.Is(err, io.EOF) {
			return usage, err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return usage, err
			}
			name := entry.Name()
			if len(name) != 20 || !strings.HasSuffix(name, ".log") {
				continue
			}
			if _, err := hex.DecodeString(name[:16]); err != nil {
				continue
			}
			info, err := entry.Info()
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return usage, err
			}
			if !info.Mode().IsRegular() {
				continue
			}
			usage.Files++
			usage.Bytes += info.Size()
			usage.LargestBytes = max(usage.LargestBytes, info.Size())
			if info.Size() <= int64(limit) {
				continue
			}
			usage.Oversized++
			if trim {
				cleared, err := clearOversizedLog(filepath.Join(dir, name), int64(limit))
				if err != nil {
					return usage, fmt.Errorf("trim %s: %w", name, err)
				}
				if cleared > 0 {
					usage.Cleared++
					usage.ClearedBytes += cleared
				}
			}
		}
		if errors.Is(err, io.EOF) {
			return usage, nil
		}
	}
}

func clearOversizedLog(path string, limit int64) (int64, error) {
	// Refuse symlinks even if the name changed after ReadDir. Recheck size on
	// the actual descriptor; concurrent trimmers need not clear a small file.
	f, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Size() <= limit {
		return 0, nil
	}
	if err := f.Truncate(0); err != nil {
		return 0, err
	}
	return info.Size(), nil
}
