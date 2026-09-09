package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogTrimPreservesInheritedAppendDescriptor(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.limits.LogBytes = 8
	path := m.logPath("/retired-worktree")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	writer, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	if _, err := writer.WriteString("old oversized diagnostics"); err != nil {
		t.Fatal(err)
	}
	before, err := writer.Stat()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := m.logs(t.Context(), false)
	if err != nil || usage.Files != 1 || usage.Oversized != 1 || usage.Cleared != 0 || usage.Bytes != before.Size() {
		t.Fatalf("observe = %+v, %v", usage, err)
	}
	usage, err = m.logs(t.Context(), true)
	if err != nil || usage.Cleared != 1 || usage.ClearedBytes != before.Size() {
		t.Fatalf("trim = %+v, %v", usage, err)
	}
	if _, err := writer.WriteString("new"); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil || string(content) != "new" {
		t.Fatalf("append after trim = %q, %v", content, err)
	}
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("trim replaced inode: %v", err)
	}
	usage, err = m.logs(t.Context(), true)
	if err != nil || usage.Cleared != 0 || usage.Bytes != 3 {
		t.Fatalf("small file trimmed: %+v, %v", usage, err)
	}
}

func TestLogTrimIgnoresUnrelatedFilesAndSymlinks(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.limits.LogBytes = 1
	dir := filepath.Dir(m.logPath(""))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "unrelated.log")
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := m.logPath("/symlink")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	usage, err := m.logs(t.Context(), true)
	if err != nil || usage.Files != 0 {
		t.Fatalf("unexpected managed logs: %+v, %v", usage, err)
	}
	if _, err := clearOversizedLog(link, 1); err == nil {
		t.Fatal("trimmer followed symlink")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "keep" {
		t.Fatalf("unrelated file changed: %q, %v", data, err)
	}
}

func TestRunTrimLogs(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var out bytes.Buffer
	if err := run([]string{"manager", "trim-logs"}, &out); err != nil {
		t.Fatal(err)
	}
	var usage logUsage
	if err := json.Unmarshal(out.Bytes(), &usage); err != nil {
		t.Fatal(err)
	}
	if usage.Files != 0 || !strings.Contains(usage.Policy, "explicit") {
		t.Fatalf("empty trim: %+v", usage)
	}
	if err := run([]string{"manager", "trim-logs", "extra"}, &out); err == nil || !strings.HasPrefix(err.Error(), "usage:") {
		t.Fatalf("invalid args: %v", err)
	}
}
