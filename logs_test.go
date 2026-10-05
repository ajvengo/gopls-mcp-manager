package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogTrimPreservesInheritedAppendDescriptor(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.limits.LogBytes = 8
	path := m.logPath("/retired-worktree")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	writer, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })
	_, err = writer.WriteString("old oversized diagnostics")
	require.NoError(t, err)
	before, err := writer.Stat()
	require.NoError(t, err)
	usage, err := m.logs(t.Context(), false)
	require.Truef(t, err == nil && usage.Files == 1 && usage.Oversized == 1 && usage.Cleared == 0 && usage.Bytes == before.Size(), "observe = %+v, %v", usage, err)
	usage, err = m.logs(t.Context(), true)
	require.Truef(t, err == nil && usage.Cleared == 1 && usage.ClearedBytes == before.Size(), "trim = %+v, %v", usage, err)
	_, err = writer.WriteString("new")
	require.NoError(t, err)
	content, err := os.ReadFile(path)
	require.Truef(t, err == nil && string(content) == "new", "append after trim = %q, %v", content, err)
	after, err := os.Stat(path)
	require.Truef(t, err == nil && os.SameFile(before, after), "trim replaced inode: %v", err)
	usage, err = m.logs(t.Context(), true)
	require.Truef(t, err == nil && usage.Cleared == 0 && usage.Bytes == 3, "small file trimmed: %+v, %v", usage, err)
}

func TestLogTrimIgnoresUnrelatedFilesAndSymlinks(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.limits.LogBytes = 1
	dir := filepath.Dir(m.logPath(""))
	require.NoError(t, os.MkdirAll(dir, 0o700))
	path := filepath.Join(dir, "unrelated.log")
	require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
	link := m.logPath("/symlink")
	require.NoError(t, os.Symlink(path, link))
	usage, err := m.logs(t.Context(), true)
	require.Truef(t, err == nil && usage.Files == 0, "unexpected managed logs: %+v, %v", usage, err)
	_, err = clearOversizedLog(link, 1)
	require.Error(t, err, "trimmer followed symlink")
	data, err := os.ReadFile(path)
	require.Truef(t, err == nil && string(data) == "keep", "unrelated file changed: %q, %v", data, err)
}

func TestRunTrimLogs(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var out bytes.Buffer
	require.NoError(t, run([]string{"manager", "trim-logs"}, &out))
	var usage logUsage
	require.NoError(t, json.Unmarshal(out.Bytes(), &usage))
	require.Truef(t, usage.Files == 0 && strings.Contains(usage.Policy, "explicit"), "empty trim: %+v", usage)
	err := run([]string{"manager", "trim-logs", "extra"}, &out)
	require.Truef(t, err != nil && strings.HasPrefix(err.Error(), "usage:"), "invalid args: %v", err)
}
