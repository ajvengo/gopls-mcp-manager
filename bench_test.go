package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// The reader goroutine runs target for every message the client sends, and
// nothing else routes while it does. It is not the throughput ceiling though —
// reading one message off the stdio transport costs an order of magnitude more,
// the SDK's decoder allocating a fresh 64KiB buffer per message. Measured here
// so that nobody spends a diff on routing before the decode above it.
func benchRouter(b *testing.B) (*router, string) {
	b.Helper()
	dir := b.TempDir()
	// TempDir on darwin hands back /var/... which is a symlink to /private/var,
	// so a real EvalSymlinks walk happens here just as it does in a session.
	file := filepath.Join(dir, "main.go")
	mustWriteFile(b, file, "package main\n")
	r := newTestRouter(b, dir)
	// Pre-seeded so the benchmark measures the memo hit, which is what a session
	// spends its time on: the git call behind a miss happens a handful of times.
	r.worktrees[containingDir(file)] = dir
	return r, file
}

// The whole routing decision for the message an agent sends most: unmarshal
// plus the memo hit below. Its allocs/op is what says whether target still
// gathers its path arguments into a slice before resolving them.
func BenchmarkTargetToolCall(b *testing.B) {
	r, file := benchRouter(b)
	req := &jsonrpc.Request{
		ID:     mustID(b, float64(1)),
		Method: "tools/call",
		Params: json.RawMessage(`{"name":"go_file_context","arguments":{"file":` + strconv.Quote(file) + `}}`),
	}
	b.ReportAllocs()
	for b.Loop() {
		if worktree, _ := r.target(req); worktree == "" {
			b.Fatal("no worktree")
		}
	}
}

// A file the path memo has not seen, under a directory it has: the miss the
// verbatim-path memo exists to keep off the reader goroutine. The cost over the
// hit above is containingDir's EvalSymlinks walk — one lstat per component —
// plus the stat that tells a file from a directory. The memo grows an entry per
// iteration, as it does per file in a session.
func BenchmarkWorktreeOfNewPath(b *testing.B) {
	r, file := benchRouter(b)
	dir := filepath.Dir(file)
	i := 0
	b.ReportAllocs()
	for b.Loop() {
		if r.worktreeOf(filepath.Join(dir, fmt.Sprintf("f%d.go", i))) == "" {
			b.Fatal("no worktree")
		}
		i++
	}
}
