package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"
)

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
	if limit <= 0 {
		limit = config.Default().LogBytes
	}
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
