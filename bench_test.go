package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The reader goroutine runs target for every message the client sends, and
// nothing else routes while it does. It is not the throughput ceiling though:
// reading that message off the transport costs several times more and allocates
// two orders of magnitude more, the SDK's decoder taking a fresh 64KiB buffer
// per message — see BenchmarkStdioCodec, and BenchmarkCallRoundTrip for what
// the two come to together. Measured here so that nobody spends a diff on
// routing before the decode above it.
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

// fileCallParams is the tool call every benchmark here sends: one naming one
// file, which is what target has to resolve a worktree from.
func fileCallParams(file string) json.RawMessage {
	return json.RawMessage(`{"name":"go_file_context","arguments":{"file":` + strconv.Quote(file) + `}}`)
}

// The whole routing decision for the message an agent sends most: unmarshal
// plus the memo hit below. Its allocs/op is what says whether target still
// gathers its path arguments into a slice before resolving them.
func BenchmarkTargetToolCall(b *testing.B) {
	r, file := benchRouter(b)
	req := &jsonrpc.Request{
		ID:     mustID(b, float64(1)),
		Method: "tools/call",
		Params: fileCallParams(file),
	}
	b.ReportAllocs()
	for b.Loop() {
		if worktree, _ := r.target(req); worktree == "" {
			b.Fatal("no worktree")
		}
	}
}

// A tools/call naming many files at once — the shape a multi-file edit or a
// package-wide query sends. One memo lookup per path, and the answer is needed
// before any of them can be routed, so this is the reader goroutine's worst
// case per message. Compared against the single-file benchmark above, it says
// whether the per-path cost is flat.
func BenchmarkTargetToolCallManyFiles(b *testing.B) {
	r, file := benchRouter(b)
	dir := filepath.Dir(file)
	files := make([]string, 64)
	for i := range files {
		path := filepath.Join(dir, fmt.Sprintf("f%d.go", i))
		files[i] = strconv.Quote(path)
		r.paths[path] = r.home
	}
	req := &jsonrpc.Request{
		ID:     mustID(b, float64(1)),
		Method: "tools/call",
		Params: json.RawMessage(`{"name":"go_diagnostics","arguments":{"files":[` + strings.Join(files, ",") + `]}}`),
	}
	b.ReportAllocs()
	for b.Loop() {
		if worktree, _ := r.target(req); worktree == "" {
			b.Fatal("no worktree")
		}
	}
}

// loopbackConn is a gopls that answers every call at once and for free. What
// gopls takes to answer is its own business and dwarfs everything here by
// orders of magnitude; what the benchmark below is after is the bridge around
// that call, so the answer has to cost nothing to stay visible.
func loopbackConn() *fakeConn {
	c := &fakeConn{reads: make(chan jsonrpc.Message, laneQueue)}
	c.onWrite = func(ctx context.Context, msg jsonrpc.Message) error {
		req, ok := msg.(*jsonrpc.Request)
		if !ok || !req.ID.IsValid() {
			return nil // a notification: nothing to answer, and nobody waiting
		}
		select {
		case c.reads <- &jsonrpc.Response{ID: req.ID, Result: json.RawMessage(`{}`)}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c
}

// One tool call, all the way through: the reader decides a worktree, records the
// call, hands it to that worktree's lane, the lane writes it to its upstream,
// the upstream's reader takes the answer back and the writer hands it to the
// client. Every goroutine, channel, mutex and timer a warm session spends per
// message, and nothing else — the dial and the handshake happen once, on the
// first iteration.
//
// This is the number that says what the bridge costs an agent; the component
// benchmarks around it say where it went. Compare against BenchmarkStdioCodec,
// the same message's trip through the transport at either end.
func BenchmarkCallRoundTrip(b *testing.B) {
	r, file := benchRouter(b)
	handshakeReady(r)
	r.dial = func(context.Context, string) (mcp.Connection, error) { return loopbackConn(), nil }

	client := make(chan jsonrpc.Message, laneQueue)
	sink := &fakeConn{writes: make(chan jsonrpc.Message, laneQueue)}
	go r.readFromClient(&fakeConn{reads: client})
	go r.writeToClient(sink)
	b.Cleanup(func() {
		close(client)
		r.closeLanes()
	})

	params := fileCallParams(file)
	b.ReportAllocs()
	var i float64
	for b.Loop() {
		i++
		// Not mustID: MakeID cannot fail for a float64, and its tb.Helper() takes
		// testing's lock and walks the stack every iteration — some 280ns against
		// the ~4µs measured here, which would be the bridge charged 6% for the
		// benchmark's own scaffolding. An id that came out wrong anyway fails the
		// answer check below.
		id, _ := jsonrpc.MakeID(i)
		client <- &jsonrpc.Request{ID: id, Method: "tools/call", Params: params}
		if resp, ok := (<-sink.writes).(*jsonrpc.Response); !ok || resp.Error != nil {
			b.Fatalf("call %v was not answered: %#v", i, resp)
		}
	}
}

// The transport under the bridge, both ends of it: encode a message, frame it,
// read it back, decode it. Every message pays this twice — once arriving from
// the client, once leaving for it — and the bridge's own work per message
// (BenchmarkCallRoundTrip) sits on top of it.
//
// Measured so that nobody spends a diff shaving allocations off the routing
// while the framing above it costs more than all of them together. Same
// newline-delimited JSON codec StdioTransport uses, in memory.
//
// The two ends run concurrently as they do in production, so this is the wall
// time one message spends in the transport, not the sum of both halves; the
// allocations reported are the decoding end's, the one running here.
func BenchmarkStdioCodec(b *testing.B) {
	// No router: nothing here routes, and the path only has to be the length a
	// real one is, since what is measured is the bytes going through the codec.
	file := filepath.Join(b.TempDir(), "main.go")
	left, right := mcp.NewInMemoryTransports()
	writer, err := left.Connect(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	reader, err := right.Connect(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	req := &jsonrpc.Request{
		ID:     mustID(b, float64(1)),
		Method: "tools/call",
		Params: fileCallParams(file),
	}
	go func() {
		for b.Context().Err() == nil {
			if writer.Write(b.Context(), req) != nil {
				return
			}
		}
	}()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := reader.Read(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}

// The map file under its flock: what every ensure pays, and pays serially with
// every other process on the machine, since the lock is one file for all of
// them. Both rows run the same read and the same sweep; they differ only in
// whether the body hands back what it was given, which is the steady state — a
// warm worktree whose record is already there and still alive.
//
// The gap between them is the write: a temp file, an fsync and a rename inside
// the critical section. Nothing here forks or probes, so what is left is the
// syscalls the lock actually holds.
func BenchmarkWithRecords(b *testing.B) {
	records := testRecords(16)
	benches := []struct {
		name string
		body func([]record) ([]record, error)
	}{
		{name: "unchanged", body: func(rs []record) ([]record, error) { return rs, nil }},
		{name: "rewritten", body: func(rs []record) ([]record, error) {
			// One record changed, not one added. The map is the same file across
			// iterations, so a body that appended would report the average over a
			// file that kept growing rather than the cost of one write. The change
			// has to be a real one, or the skip above eats it and both rows measure
			// the same thing. Edited in place because withMap hands every body a
			// copy of its own.
			rs[len(rs)-1].PID++
			return rs, nil
		}},
	}
	for _, bench := range benches {
		b.Run(bench.name, func(b *testing.B) {
			m := newTestManager(b)
			// Every record answers alive without a syscall: the probes are another
			// benchmark's subject, and a real one here would drown the file work.
			m.alive = func(record) bool { return true }
			if err := writeMap(m.mapPath, records); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := m.withRecords(bench.body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func testRecords(n int) []record {
	records := make([]record, n)
	for i := range records {
		records[i] = record{Worktree: fmt.Sprintf("/repo/w%d", i), Port: firstPort + i, PID: 1000 + i}
	}
	return records
}

// The other half of what the map lock holds, and the half BenchmarkWithRecords
// stubs out: every record on the machine is probed on every sweep, and a sweep
// runs on every cold start and every redial. The probes fan out, so the cost is
// the slowest one — but they run under a flock every process on this machine
// shares, so that slowest one is what every other worktree's cold start queues
// behind.
//
// The rows are the verdicts a probe can reach. "live" is the steady state, one
// local round trip. "refused" is conclusively dead; "odd" answers but not like
// ours, so the verdict is inconclusive — both then fork a ps per record, and
// that fork, not the probe, is what they measure. "silent" is §10's case, a
// server that took the connection and said nothing: it costs probeClient's
// whole timeout, so it runs at half a second an iteration by construction and
// wants the default benchtime rather than a count.
func BenchmarkSweepProbes(b *testing.B) {
	for _, bench := range []struct {
		name string
		// Stood up inside the row rather than in the table, so that running one
		// row does not also start the other three servers — silent's accept
		// goroutine holds every connection it takes, and at a long benchtime the
		// rows not being measured would sit on file descriptors for the whole run.
		port func(testing.TB) int
	}{
		{name: "live", port: livePort},
		{name: "refused", port: deadPort},
		{name: "odd", port: func(tb testing.TB) int { port, _ := answeringPort(tb, http.StatusOK); return port }},
		{name: "silent", port: silentPort},
	} {
		b.Run(bench.name, func(b *testing.B) {
			port := bench.port(b)
			records := testRecords(8)
			for i := range records {
				// Every record aims at the one server, since what is being timed is
				// the verdict rather than which port reached it. The pid is this
				// process, so the kill(2) liveness check passes and the probe runs.
				records[i].Port = port
				records[i].PID = os.Getpid()
			}
			b.ReportAllocs()
			for b.Loop() {
				cleanRecords(records, recordAlive)
			}
		})
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
