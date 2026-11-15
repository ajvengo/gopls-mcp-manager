package main

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"unicode/utf8"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"
	"github.com/ajvengo/gopls-mcp-manager/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Descriptor observations are process-wide, so these tests deliberately remain
// serial. Parallel tests in this package are paused while serial tests run.
func auditResourceFDCount(t *testing.T) int {
	t.Helper()
	switch runtime.GOOS {
	case "darwin", "linux":
	default:
		t.Skip("descriptor inventory requires the supported Darwin or Linux /dev/fd")
	}
	entries, err := os.ReadDir("/dev/fd")
	require.NoError(t, err)
	return len(entries)
}

type gatedBody struct {
	io.Reader
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (b *gatedBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered); <-b.release })
	return b.Reader.Read(p)
}

// benchRouter is the warm router the routing benchmarks below share.
func benchRouter(b *testing.B) (*router, string) {
	b.Helper()
	dir := b.TempDir()
	dir, err := filepath.EvalSymlinks(dir)
	require.NoError(b, err)
	// Use the same physical spelling for existing and nonexistent fixture files.
	// Symlink memo consistency is covered separately by the routing tests.
	file := filepath.Join(dir, "main.go")
	mustWriteFile(b, file, "package main\n")
	r := newTestRouter(b, dir)
	// Pre-seeded so the benchmark measures the memo hit, which is what a session
	// spends its time on: the git call behind a miss happens a handful of times.
	parent, _ := containingDir(file)
	r.worktrees[parent] = dir
	return r, file
}

// toolCall is the message every benchmark here routes: one tools/call under a
// fixed id, differing only in the arguments it names.
func toolCall(tb testing.TB, params jsontext.Value) *jsonrpc.Request {
	return &jsonrpc.Request{ID: mustID(tb, float64(1)), Method: "tools/call", Params: params}
}

// fileCallParams is the tool call every benchmark here sends: one naming one
// file, which is what target has to resolve a worktree from.
func fileCallParams(file string) jsontext.Value {
	return jsontext.Value(`{"name":"go_file_context","arguments":{"file":` + strconv.Quote(file) + `}}`)
}

// What routeResolved adds to every tools/call after it has picked a lane, on the
// reader goroutine beside BenchmarkTargetToolCall. The aliased row is the rewrite
// itself; the physical row is what a session that has seen one symlinked spelling
// pays on the calls that need no rewrite — measured at ~380ns against that
// benchmark's ~540ns, which is why a session that has seen none is gated out of
// this path entirely (memoState.aliased).
func BenchmarkCanonicalToolCall(b *testing.B) {
	for _, bench := range []struct{ name, physical string }{
		{name: "physical"},
		{name: "aliased", physical: "/physical/main.go"},
	} {
		b.Run(bench.name, func(b *testing.B) {
			r, file := benchRouter(b)
			r.paths[file] = pathMemo{worktree: r.home, physical: bench.physical}
			r.memo.aliased.Store(true) // past the gate: what is left is the scan
			params := fileCallParams(file)
			b.ReportAllocs()
			for b.Loop() {
				if _, rewritten := r.canonicalToolCall(params); rewritten != (bench.physical != "") {
					require.FailNow(b, "unexpected rewrite")
				}
			}
		})
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
		case c.reads <- &jsonrpc.Response{ID: req.ID, Result: jsontext.Value(`{}`)}:
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
	r.dial = func(context.Context, string, int) (mcp.Connection, error) { return loopbackConn(), nil }

	client := make(chan jsonrpc.Message, laneQueue)
	sink := &fakeConn{writes: make(chan jsonrpc.Message, laneQueue)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.readFromClient(&fakeConn{reads: client})
	}()
	go r.writeToClient(sink)
	b.Cleanup(func() {
		close(client)
		<-done
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
			require.FailNow(b, fmt.Sprintf("call %v was not answered: %#v", i, resp))
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
	require.NoError(b, err)
	reader, err := right.Connect(b.Context())
	require.NoError(b, err)
	writerDone := make(chan struct{})
	b.Cleanup(func() {
		_ = writer.Close()
		_ = reader.Close()
		<-writerDone
	})
	req := toolCall(b, fileCallParams(file))
	go func() {
		defer close(writerDone)
		for b.Context().Err() == nil {
			if writer.Write(b.Context(), req) != nil {
				return
			}
		}
	}()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := reader.Read(b.Context()); err != nil {
			require.NoError(b, err)
		}
	}
}

func testRecords(n int) []record {
	records := make([]record, n)
	for i := range records {
		records[i] = record{Worktree: fmt.Sprintf("/repo/w%d", i), Port: firstPort + i, PID: 1000 + i}
	}
	return records
}

// A full stdio session: real newline framing, manager/registry dialing, and
// real SSE upstreams. Setup and handshakes are excluded from warm benchmarks.
func stdioFixture(tb testing.TB, owner *supervisor, home string) mcp.Connection {
	m := owner.m
	tb.Helper()
	left, right := net.Pipe()
	client, err := (&mcp.IOTransport{Reader: left, Writer: left}).Connect(tb.Context())
	require.NoError(tb, err)
	server := transport.NewStdio(right, right, m.limits.MessageBytes)
	ctx, cancel := context.WithCancel(tb.Context())
	done := inBackground(func() error { return serve(ctx, owner, home, server) })
	tb.Cleanup(func() {
		cancel()
		_ = client.Close()
		select {
		case err := <-done:
			assert.NoErrorf(tb, err, "stdio shutdown")
		case <-time.After(5 * time.Second):
			assert.Fail(tb, "stdio server did not stop")
		}
	})
	init := &jsonrpc.Request{ID: mustID(tb, "initialize"), Method: "initialize", Params: jsontext.Value(`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}`)}
	response := stdioRoundTrip(tb, tb.Context(), client, init)
	require.Nilf(tb, response.Error, "initialize: %+v", response)
	require.NoError(tb, client.Write(tb.Context(), &jsonrpc.Request{Method: "notifications/initialized"}))
	return client
}

// stdioRoundTrip verifies transport success and response identity; callers own
// the assertions on the result or protocol error.
func stdioRoundTrip(tb testing.TB, ctx context.Context, client mcp.Connection, request *jsonrpc.Request) *jsonrpc.Response {
	tb.Helper()
	require.NoError(tb, client.Write(ctx, request))
	msg, err := client.Read(ctx)
	require.NoError(tb, err)
	return wantResponse(tb, msg, request.ID)
}

func BenchmarkStdioRoundTrip(b *testing.B) {
	m, _, _, _ := sseFixture(b, testHome)
	client := stdioFixture(b, m, testHome)
	request := &jsonrpc.Request{ID: mustID(b, "call"), Method: "tools/call", Params: jsontext.Value(`{"name":"where","arguments":{}}`)}
	b.ReportAllocs()
	for b.Loop() {
		response := stdioRoundTrip(b, b.Context(), client, request)
		require.Nilf(b, response.Error, "tool response: %+v", response)
	}
}

func (c *bufferedRootsConn) Read(context.Context) (jsonrpc.Message, error) {
	if len(c.messages) == 0 {
		return nil, io.EOF
	}
	msg := c.messages[0]
	c.messages = c.messages[1:]
	return msg, nil
}

func TestClosedConnectionReleasesBackpressuredReader(t *testing.T) {
	bubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r := newRouter(ctx, routerTestOwner(t, nil), testHome)
		for range cap(r.out) {
			r.out <- &jsonrpc.Request{Method: "notification"}
		}
		connCtx, closeConn := context.WithCancel(ctx)
		conn := testUpstream(r, r.home, newFakeConn(), connCtx, closeConn)
		id := mustID(t, "completed")
		state, err := r.admit(id, func() {})
		require.NoError(t, err)
		require.NoError(t, r.beginOperation(id, state, conn, testHome, func() {}, nil))
		done := make(chan struct{})
		go func() { defer close(done); r.readFromUpstream(testConnection(r, conn, testHome), testHome) }()
		conn.Connection.(*fakeConn).reads <- &jsonrpc.Response{ID: id, Result: []byte(`{}`)}
		synctest.Wait()
		require.Zero(t, r.requestUsage().UpstreamOperations, "the blocked reader no longer holds an operation credit")
		require.NoError(t, conn.Close())
		synctest.Wait()
		select {
		case <-done:
		default:
			cancel() // release the old reader before failing the regression
			<-done
			require.FailNow(t, "closed connection retained a reader behind session output")
		}
		select {
		case err := <-r.errs:
			require.ErrorContains(t, err, "client output queue is full")
		default:
			require.FailNow(t, "an undeliverable terminal result must fail the session")
		}
	})
}

func TestMetricsShutdownDoesNotWaitForStderr(t *testing.T) {
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	oldStderr := os.Stderr
	os.Stderr = writer
	defer func() { os.Stderr = oldStderr }()
	defer func() { _ = reader.Close(); _ = writer.Close() }()
	filled := make(chan struct{})
	go func() { _, _ = writer.Write([]byte(strings.Repeat("x", 1<<20))); close(filled) }()
	var first [1]byte
	_, err = reader.Read(first[:])
	require.NoError(t, err)
	m := &manager{limits: config.Limits{Metrics: true}}
	done := make(chan error, 1)
	owner := routerTestOwner(t, m)
	go func() { done <- serve(t.Context(), owner, testHome, deadConn()) }()
	returned := false
	select {
	case err := <-done:
		require.NoError(t, err)
		returned = true
	case <-time.After(time.Second):
	}
	drained := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, reader); close(drained) }()
	<-filled
	if !returned {
		require.NoError(t, <-done)
	}
	require.NoError(t, writer.Close())
	<-drained
	require.True(t, returned, "final metrics prevented session shutdown from returning")
}

func TestMetricsFinalSnapshotFollowsCleanup(t *testing.T) {
	bubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r := newRouter(ctx, routerTestOwner(t, &manager{limits: config.Limits{Metrics: true}}), testHome)
		var output strings.Builder
		go r.reportUsage(&output)
		_, err := r.admit(mustID(t, "cleanup"), func() {})
		require.NoError(t, err)
		r.clearRequests()
		cancel()
		r.finishUsage()
		select {
		case <-r.usageDone:
		default:
			require.FailNow(t, "writable final metrics did not flush")
		}
		var usage requestUsage
		require.NoError(t, json.Unmarshal([]byte(output.String()), &usage))
		require.Equal(t, "session_requests", usage.Event)
		require.Equal(t, 1, usage.Completed)
		require.Zero(t, usage.Outstanding)
	})
}

func TestOutputSaturationStopsSessionWithBlockedWriter(t *testing.T) {
	bubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		release := make(chan struct{})
		defer close(release)
		client := &fakeConn{reads: make(chan jsonrpc.Message, 128)}
		client.onWrite = func(context.Context, jsonrpc.Message) error {
			<-release // a stdout write need not observe the session context
			return nil
		}
		for i := range cap(client.reads) {
			client.reads <- &jsonrpc.Request{ID: mustID(t, float64(i)), Method: "tools/list"}
		}
		done := make(chan error, 1)
		go func() { done <- serve(ctx, routerTestOwner(t, nil), testHome, client) }()
		synctest.Wait()
		select {
		case err := <-done:
			require.ErrorContains(t, err, "client output queue is full")
		default:
			cancel()
			require.NoError(t, <-done)
			require.FailNow(t, "saturated output kept the session alive")
		}
	})
}

func TestModuleMetadataReadsAreBounded(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "go.mod")
	mustWriteFile(t, path, "module example.com/a\nignore ./generated\n//"+strings.Repeat("x", maxModuleFileBytes))
	filters, err := (&watchBudget{remaining: defaultWatchLimits()}).moduleIgnoreFilters(root)
	require.ErrorIs(t, err, errWatchLimit, "oversize metadata must fail the complete plan")
	require.Nil(t, filters)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Symlink("/dev/zero", path))
	_, err = (&watchBudget{remaining: defaultWatchLimits()}).readModuleFile(path)
	require.Error(t, err, "device input must be rejected before reading")
	require.NoError(t, os.Remove(path))
	target := filepath.Join(root, "regular.mod")
	mustWriteFile(t, target, "module example.com/a\nignore ./generated\n")
	require.NoError(t, os.Symlink(target, path))
	filters, err = (&watchBudget{remaining: defaultWatchLimits()}).moduleIgnoreFilters(root)
	require.NoError(t, err)
	require.Equal(t, []string{"-generated"}, filters, "regular-file symlinks remain supported")
}

// directoryFilterReference matches segments with dynamic programming, without
// regexp construction or production matching helpers. Exhausting a pattern
// matches its whole subtree; ** either consumes no segment or one full segment.
func directoryFilterReference(rules []string, rel string) bool {
	// Go regexp compares Unicode runes, including RuneError for invalid bytes.
	rel = strings.Trim(string([]rune(rel)), "/")
	var parts []string
	if rel != "" {
		parts = strings.Split(rel, "/")
	}
	next, current := make([]bool, len(parts)+1), make([]bool, len(parts)+1)
	for i := len(rules) - 1; i >= 0; i-- {
		rule := rules[i]
		if len(rule) == 0 || len(rule) > config.MaxDirectoryFilterBytes || !utf8.ValidString(rule) || rule[0] != '+' && rule[0] != '-' {
			continue
		}
		prefix := strings.TrimPrefix(path.Clean("/"+rule[1:]), "/")
		var pattern []string
		if prefix != "" {
			pattern = strings.Split(prefix, "/")
		}
		for j := range next {
			next[j] = true
		}
		for p := len(pattern) - 1; p >= 0; p-- {
			for j := len(parts); j >= 0; j-- {
				if pattern[p] == "**" {
					current[j] = next[j] || j < len(parts) && parts[j] != "" && current[j+1]
				} else {
					current[j] = j < len(parts) && pattern[p] == parts[j] && next[j+1]
				}
			}
			next, current = current, next
		}
		if next[0] {
			return rule[0] == '+'
		}
	}
	return true
}

func TestModuleIgnoreFiltersRejectInvalidUTF8(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "go.mod"), "module example.com/a\nignore \"\\x80\"\n")
	require.NotPanics(t, func() { _ = planWatch(root, nil) })
}

// One record per line, so any byte in a path — a newline above all — has to
// survive the trip. writeMap refuses what it cannot represent rather than
// writing an approximation, so the property is: it errors, or it round-trips
// exactly. Anything else means a record read back names a worktree nobody
// asked about.
func FuzzMapRoundTrip(f *testing.F) {
	f.Add("/repo/plain", 62001, 42, int64(0))
	f.Add("/repo/with spaces\tand\na quote\"", 62002, 43, int64(1700000000))
	f.Add("/repo/\\backslash\\", 62003, 44, int64(-1))
	f.Add("/repo/юникод/路径", 62004, 45, int64(0))
	f.Add("/repo/\x00null", 62005, 46, int64(0))
	f.Add("/repo/\xff\xfe", 62006, 47, int64(0)) // invalid UTF-8: must be refused

	f.Fuzz(func(t *testing.T, worktree string, port, pid int, startedAt int64) {
		// Normalized into the domain readMap accepts, so that a rejected record
		// never masquerades as a lost one.
		if worktree == "" {
			t.Skip()
		}
		if port < firstPort || port > lastPort || pid <= 0 || pid > maxProcessID {
			t.Skip()
		}
		want := []record{{Worktree: worktree, Port: port, PID: pid, StartedAt: startedAt}}

		path := newTestManager(t).mapPath
		if err := writeMap(path, want); err != nil {
			if utf8.ValidString(worktree) {
				encoded, encodeErr := json.Marshal(want[0])
				require.NoError(t, encodeErr)
				require.GreaterOrEqual(t, len(encoded), 64<<10, "writeMap refused a valid record within input limits: %v", err)
			}
			return
		}
		require.Truef(t, utf8.ValidString(worktree), "writeMap accepted %q, which JSON cannot represent byte for byte", worktree)
		wantRecords(t, path, "the map did not round-trip", want...)
	})
}

// A port is what a probe is aimed at and what a record claims, so allocation
// must stay inside the range readMap will accept — otherwise a record is
// written that the next read drops, and the gopls behind it is stranded.
func FuzzAllocatePort(f *testing.F) {
	f.Add("/repo/a", 0)
	f.Add("", 0)
	f.Add("/repo/юникод", 7)
	f.Add(strings.Repeat("/deep", 200), 3)
	f.Add("/repo/\xff", 1)

	f.Fuzz(func(t *testing.T, worktree string, taken int) {
		base := basePort(worktree)
		require.Truef(t, base >= firstPort && base <= lastPort, "basePort(%q) = %d, outside %d-%d", worktree, base, firstPort, lastPort)
		again := basePort(worktree)
		require.Equalf(t, base, again, "basePort(%q) is not deterministic: %d then %d", worktree, base, again)
		next := nextPort(base)
		require.Truef(t, next >= firstPort && next <= lastPort, "nextPort(%d) = %d, outside %d-%d", base, next, firstPort, lastPort)

		// Occupy a prefix of the walk, so allocation has to step past it.
		occupied := min(max(taken, 0), 64)
		records := make([]record, 0, occupied)
		for port := base; len(records) < occupied; port = nextPort(port) {
			records = append(records, record{Worktree: worktree, Port: port, PID: 1})
		}
		got, err := allocatePort(worktree, records, func(int) bool { return false })
		require.NoErrorf(t, err, "allocatePort with %d of %d ports taken: %v", occupied, portCount, err)
		require.Truef(t, got >= firstPort && got <= lastPort, "allocatePort() = %d, outside %d-%d", got, firstPort, lastPort)
		require.Falsef(t, slices.ContainsFunc(records, func(r record) bool { return r.Port == got }), "allocatePort() = %d, a port already claimed", got)
	})
}

// The other half of allocation: a port free in the map that the kernel will not
// hand over anyway, which is what portUnavailable answers in production. The
// walk has to step past both kinds and wrap, and a port it returns is one a
// gopls is about to be spawned on — so returning an unavailable one binds
// nothing and strands the record that names it.
func FuzzAllocatePortAroundUnavailable(f *testing.F) {
	f.Add("/repo/a", uint64(0))
	f.Add("/repo/a", ^uint64(0)) // the whole window refuses
	f.Add("/repo/b", uint64(1))
	f.Add("/repo/b", uint64(0b1010101))
	f.Add("", uint64(1<<63))

	f.Fuzz(func(t *testing.T, worktree string, blocked uint64) {
		// A bit per port from the base onward, so refusals land where the walk
		// actually goes rather than somewhere it would never reach. Beyond the
		// window every port is free, which is what makes an error meaningful:
		// only a walk that failed to wrap can report one.
		base := basePort(worktree)
		offset := func(port int) int { return (port - base + portCount) % portCount }
		unavailable := func(port int) bool {
			at := offset(port)
			return at < 64 && blocked&(1<<at) != 0
		}

		got, err := allocatePort(worktree, nil, unavailable)
		require.NoErrorf(t, err, "allocatePort() failed with %d of %d ports free: %v", portCount-64, portCount, err)
		require.Falsef(t, unavailable(got), "allocatePort() = %d, a port that refuses to bind", got)
		require.Truef(t, got >= firstPort && got <= lastPort, "allocatePort() = %d, outside %d-%d", got, firstPort, lastPort)
		// The walk is in order from the base, so the answer is the first port
		// nothing objects to — anything later means a free port was stepped over
		// and two worktrees that should differ can collide further along.
		for port := base; port != got; port = nextPort(port) {
			require.Truef(t, unavailable(port), "allocatePort() = %d (offset %d), skipping free port %d (offset %d)", got, offset(got), port, offset(port))
		}
	})
}

// Every path argument a tool call names reaches this, and the answer decides
// which gopls — which index, which worktree — runs the call.
func FuzzContainingDir(f *testing.F) {
	f.Add("/repo/a.go")
	f.Add("/repo/")
	f.Add("/repo/./b/../a.go")
	f.Add("")
	f.Add("relative/a.go")
	f.Add("/")
	f.Add("//")
	f.Add(strings.Repeat("/a", 500))
	f.Add("/repo/\x00/a.go")

	f.Fuzz(func(t *testing.T, path string) {
		got, physical := containingDir(path)
		// The result is a memo key, so two spellings of one directory must not
		// become two entries and two git forks.
		clean := filepath.Clean(got)
		require.Equalf(t, got, clean, "containingDir(%q) = %q, which is not cleaned (%q)", path, got, clean)
		require.Falsef(t, filepath.IsAbs(path) && !filepath.IsAbs(got), "containingDir(%q) = %q, relative for an absolute argument", path, got)
		// The second result is handed to gopls verbatim (R10), including the
		// spelling synthesized from a resolved parent for a file that does not
		// exist yet — so it has to be a cleaned path of the same kind as the
		// argument, never a relative one substituted for an absolute.
		if physical == "" {
			return
		}
		cleanPhysical := filepath.Clean(physical)
		require.Equalf(t, physical, cleanPhysical, "containingDir(%q) physical = %q, which is not cleaned (%q)", path, physical, cleanPhysical)
		require.Falsef(t, filepath.IsAbs(path) && !filepath.IsAbs(physical), "containingDir(%q) physical = %q, relative for an absolute argument", path, physical)
	})
}

type fakeConn struct {
	reads  chan jsonrpc.Message
	writes chan jsonrpc.Message

	// onWrite replaces the default write, which is what almost every upstream
	// these tests stub differs in: it fails, it answers out of the write itself,
	// it dies first — and is a plain connection otherwise. The exception is
	// brokenConn, which wraps one of these to fail a read instead. A hook that
	// wants the default for messages it does not care about calls queue.
	onWrite func(ctx context.Context, msg jsonrpc.Message) error
}

func (c *fakeConn) Read(ctx context.Context) (jsonrpc.Message, error) {
	select {
	case msg, ok := <-c.reads:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case <-ctx.Done():
		// A real Connection honours its context, and a test for the handshake
		// deadline would hang against one that did not.
		return nil, ctx.Err()
	}
}

func (c *fakeConn) Write(ctx context.Context, msg jsonrpc.Message) error {
	if c.onWrite != nil {
		return c.onWrite(ctx, msg)
	}
	return c.queue(ctx, msg)
}

// queue is the default write: hand the message over and let a test read it back.
func (c *fakeConn) queue(ctx context.Context, msg jsonrpc.Message) error {
	select {
	case c.writes <- msg:
		return nil
	case <-ctx.Done():
		// See TestAWedgedUpstreamFailsItsCallWithinOneBudget, whose retried
		// handshake reaches here with the shared deadline already expired.
		return ctx.Err()
	}
}

// failingConn fails every write, which is what a lane still holding a
// connection to a gopls that has gone away runs into.
func failingConn() *fakeConn {
	return &fakeConn{onWrite: func(context.Context, jsonrpc.Message) error { return io.EOF }}
}

func (c *fakeConn) SessionID() string { return "" }

// newFakeConn is the shape most of these tests want: reads handed over one at a
// time, and room for a single write so that a send need not be raced to be
// taken.
func newFakeConn() *fakeConn {
	return &fakeConn{reads: make(chan jsonrpc.Message), writes: make(chan jsonrpc.Message, 1)}
}

// deadConn is an upstream that has already gone away: msgs are everything it
// will ever hand over, and its reader sees EOF straight after them.
func deadConn(msgs ...jsonrpc.Message) *fakeConn {
	reads := make(chan jsonrpc.Message, len(msgs))
	for _, msg := range msgs {
		reads <- msg
	}
	close(reads)
	return &fakeConn{reads: reads}
}

// newHandshakingConn answers the private handshake out of its own write, so
// that a test wanting a lane on a connected upstream needs no goroutine
// scripting one. Everything the handshake does not swallow is queued as on a
// plain fakeConn, which is what lets a test still await the call it placed.
//
// The buffered read is what keeps that answer from blocking on a handshake that
// is not reading yet — it writes before it reads. The writes are buffered for
// the same reason, deep enough for the whole tail of one handshaked call: the
// initialized notification, then the call itself.
func newHandshakingConn() *fakeConn {
	c := &fakeConn{
		reads:  make(chan jsonrpc.Message, 1),
		writes: make(chan jsonrpc.Message, 2),
	}
	c.onWrite = func(ctx context.Context, msg jsonrpc.Message) error {
		if req, ok := msg.(*jsonrpc.Request); ok && req.Method == "initialize" {
			c.reads <- &jsonrpc.Response{ID: req.ID, Result: jsontext.Value(`{}`)}
			return nil
		}
		return c.queue(ctx, msg)
	}
	return c
}

// testHome is the fallback worktree for the tests that do not care what it is.
const testHome = "/tmp/home"

// testWorktree is the destination every test that needs one routes to: a path
// no test resolves either, so naming it is what picks the lane, not what is on
// disk. Distinct from newTestRouter's home, which is the fallback.
const testWorktree = "/tmp/wt"

// clientInitializeParams stands in for the real client's initialize params. It
// carries the roots capability on purpose: that capability is what makes gopls
// ask roots/list at all, so a replay that dropped the params would leave every
// secondary upstream never asking, and S1-S3 unreachable in production.
const clientInitializeParams = `{"capabilities":{"roots":{"listChanged":true}}}`

// handshakeReady gives the router the client initialize every private handshake
// replays. Without one, upstream() refuses to open any connection but the first.
func handshakeReady(r *router) {
	r.initialize.Store(&jsonrpc.Request{Method: "initialize", Params: jsontext.Value(clientInitializeParams)})
}

// sendCall puts a tools/call on l under a fresh id and hands the id back.
func sendCall(t *testing.T, l *lane, name string) jsonrpc.ID {
	t.Helper()
	id := mustID(t, name)
	l.r.track(id, nil, l.worktree)
	l.send(t.Context(), &jsonrpc.Request{ID: id, Method: "tools/call"}, nil)
	return id
}

// startClient runs the reader over a channel the test sends its client messages
// on, and shuts the reader and every lane it opened down with the test. The
// order matters and is the reason this is shared: closing the reads is what ends
// readFromClient, and only then may closeLanes touch r.lanes, which that
// goroutine owns.
func startClient(t *testing.T, r *router, depth int) chan<- jsonrpc.Message {
	t.Helper()
	reads := make(chan jsonrpc.Message, depth)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.readFromClient(&fakeConn{reads: reads})
	}()
	t.Cleanup(func() {
		close(reads)
		<-done
		r.closeLanes()
	})
	return reads
}

// mustWriteFile puts content on disk, for the tests and benchmarks that need a
// real file for git or EvalSymlinks to walk.
func mustWriteFile(tb testing.TB, path, content string) {
	tb.Helper()
	require.NoError(tb, os.WriteFile(path, []byte(content), 0o600))
}

// bubble runs fn under synctest's clock, which only moves once every goroutine
// in it has blocked. Two things these tests want follow from that: a wait for
// something that never comes costs no real time at all, and "nothing reached
// the client" can be asserted once the bridge has run itself out, rather than
// merely at the instant of looking.
//
// A test stays outside when it does something this clock cannot account for — a
// child process, a real listener or HTTP server, or a real timeout it means to
// measure, which between them cover every test in manager_test.go that is not
// here. So does a test that waits on nothing, which would gain only a closure;
// and t.Run is forbidden inside a bubble.
func bubble(t *testing.T, fn func(t *testing.T)) {
	t.Helper()
	t.Parallel()
	synctest.Test(t, fn)
}

// testWait bounds every wait in this file, all of which are inside a bubble. A
// bridge that never makes the progress a test is waiting for is a bug, and an
// unbounded channel operation leaves synctest to report it — as a bare deadlock
// panic naming no expectation. Getting there first is the whole job.
//
// It costs nothing on a passing run: this timer can only come due once every
// goroutine is already blocked, which is the hang itself.
const testWait = 10 * time.Second

// recvWithin takes the next value from ch, reporting a timeout rather than
// hanging. It answers with an error instead of failing t, so that it is usable
// from a background goroutine: see inBackground.
func recvWithin[T any](ch <-chan T, wanted string) (T, error) {
	select {
	case v := <-ch:
		return v, nil
	case <-time.After(testWait):
		var zero T
		return zero, fmt.Errorf("timed out after %s waiting for %s", testWait, wanted)
	}
}

// recvRequest insists the next message is a request naming method.
func recvRequest(ch <-chan jsonrpc.Message, method string) (*jsonrpc.Request, error) {
	msg, err := recvWithin(ch, method)
	if err != nil {
		return nil, err
	}
	req, ok := msg.(*jsonrpc.Request)
	if !ok || req.Method != method {
		return nil, fmt.Errorf("upstream got %#v, want a %s request", msg, method)
	}
	return req, nil
}

// mustRecv fails the test rather than returning the timeout. Call it only from
// the test's own goroutine — see inBackground for why.
func mustRecv[T any](t *testing.T, ch <-chan T, wanted string) T {
	t.Helper()
	v, err := recvWithin(ch, wanted)
	require.NoError(t, err)
	return v
}

// inBackground runs fn on its own goroutine and hands back its verdict, which
// the test is expected to wait for and assert on itself.
//
// fn must not touch *testing.T. Fatalf ends the goroutine that calls it, which
// from here is fn's own: the test would run on, unaware, and reach a verdict of
// its own — reporting a bridge that answered nothing as a pass. (Outliving the
// test, the other hazard, a bubble rules out on its own: synctest waits for
// every goroutine in it.) The buffer is load-bearing rather than tidy — after a
// test that gave up on fn at testWait, an unbuffered send would park fn forever
// and deadlock the bubble.
func inBackground(fn func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	return done
}

// upstreamPair returns the two ends of one connection: upstream is what the
// router holds, gopls is what a real server would be answering on.
func upstreamPair(t *testing.T) (upstream, gopls mcp.Connection) {
	t.Helper()
	upstreamT, goplsT := mcp.NewInMemoryTransports()
	upstream, err := upstreamT.Connect(t.Context())
	require.NoErrorf(t, err, "connect upstream: %v", err)
	gopls, err = goplsT.Connect(t.Context())
	require.NoErrorf(t, err, "connect gopls: %v", err)
	// Closed, not merely abandoned: each of these runs a decode pump of its own
	// that no context cancels, and a bubble does not end while one is still
	// parked on its half of the pipe.
	t.Cleanup(func() {
		_ = upstream.Close()
		_ = gopls.Close()
	})
	return upstream, gopls
}

// wantClientQuiet fails if anything reached the client, naming what should not
// have. Most of these tests exist to prove the bridge kept something to itself.
//
// The wait is what makes it an assertion rather than a coincidence: without it
// this says only that nothing had arrived yet at the instant it looked. Call
// from inside a bubble — the wait panics anywhere else.
func wantClientQuiet(t *testing.T, r *router, whatWouldBeWrong string) {
	t.Helper()
	synctest.Wait()
	select {
	case msg := <-r.out:
		require.FailNow(t, fmt.Sprintf("%s: %v", whatWouldBeWrong, msg))
	default:
	}
}

// wantResponse insists msg is the answer to id, which is the shape of every
// reply these tests read back off a connection.
func wantResponse(t testing.TB, msg jsonrpc.Message, id jsonrpc.ID) *jsonrpc.Response {
	t.Helper()
	resp, ok := msg.(*jsonrpc.Response)
	require.Truef(t, ok && resp.ID == id, "upstream got %#v, want a response to %v", msg, id)
	return resp
}

// wantWireError insists resp carries a JSON-RPC error under code, and hands it
// back so callers can go on to assert on the message itself.
func wantWireError(t *testing.T, resp *jsonrpc.Response, code int64) *jsonrpc.Error {
	t.Helper()
	var wire *jsonrpc.Error
	ok := errors.As(resp.Error, &wire)
	require.Truef(t, ok && wire.Code == code, "error = %v, want one with code %d", resp.Error, code)
	return wire
}

// wantOneRoot reads a roots/list answer, which names exactly one root — the
// tree the upstream that asked belongs to, and never any other. Callers assert
// on the root itself; that there is only the one is the shared part.
//
//nolint:staticcheck // Verify the roots contract required by legacy gopls SSE sessions.
func wantOneRoot(t *testing.T, resp *jsonrpc.Response) *mcp.Root {
	t.Helper()
	var got mcp.ListRootsResult
	err := json.Unmarshal(resp.Result, &got)
	require.NoErrorf(t, err, "unmarshal roots answer %s: %v", resp.Result, err)
	require.Lenf(t, got.Roots, 1, "got %d roots, want exactly the upstream's own: %#v", len(got.Roots), got.Roots)
	return got.Roots[0]
}

// mustID takes any of the nil/float64/string an id arrives off the wire as, so
// that a numeric id in a test is built the same way as a name — and by the same
// call the benchmarks use.
func mustID(tb testing.TB, raw any) jsonrpc.ID {
	tb.Helper()
	id, err := jsonrpc.MakeID(raw)
	require.NoErrorf(tb, err, "MakeID(%v): %v", raw, err)
	return id
}

// Refusing on the reader is what keeps the client's initialize from reaching a
// connection that has already had one: let through, it would wake the home lane
// into spending the private handshake on that connection, and gopls's
// already-initialized error would come back as the client's initialize result,
// ending the session before it began.
//
// Asserted on the dial rather than that outcome, because the outcome needs the
// lane to lose a race it usually wins: no dial at all holds whoever wins.
//
// A notification is dropped rather than refused, having no id to answer, and
// the lane still must not wake. Not a cancellation: that is refused a dial
// anyway (R7), so the row would pass with the refusal deleted.
func TestAMessageBeforeInitializeNeverWakesALane(t *testing.T) {
	t.Parallel()
	id := mustID(t, "call-before-initialize")
	tests := []struct {
		name string
		msg  *jsonrpc.Request
		want func(t *testing.T, r *router)
	}{
		{"request is refused", &jsonrpc.Request{ID: id, Method: "tools/call"}, func(t *testing.T, r *router) {
			resp := wantClientError(t, r, id, "a tools/call before initialize was left unanswered")
			// The code, not just the failure: H1a names it, and nothing else here
			// would notice it drifting to a generic internal error.
			_ = wantWireError(t, resp, jsonrpc.CodeInvalidRequest)
		}},
		{"notification is dropped", &jsonrpc.Request{Method: "notifications/initialized"}, func(t *testing.T, r *router) {
			wantClientQuiet(t, r, "a notification sent before initialize was answered")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				r := newTestRouter(t, testHome)
				forbidDial(t, r, "a lane opened a connection for a message that arrived before initialize")
				clientReads := startClient(t, r, 1)

				clientReads <- test.msg

				test.want(t, r)
				synctest.Wait()
			})
		})
	}
}

func TestRootlessInitializeIsForwardedWithWorkingRoots(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		upstreamReads := make(chan jsonrpc.Message, 1)
		upstream := &fakeConn{reads: upstreamReads, writes: make(chan jsonrpc.Message, 2)}
		// Dialled rather than handed over pre-connected, because that is the only
		// way home gets a connection while the client's initialize is what opens
		// it: a lane already holding one refuses that message under H2a. laneFor
		// runs the lane and cache starts its reader, so neither is started here.
		fixedDial(r, upstream)
		// Registered before startClient so it runs after it: cleanups are LIFO,
		// and the upstream's reader must not be cut loose before the lanes it
		// belongs to have been closed.
		t.Cleanup(func() { close(upstreamReads) })
		clientReads := startClient(t, r, 1)

		clientReads <- &jsonrpc.Request{
			ID:     mustID(t, "initialize-rootless"),
			Method: "initialize",
			Params: jsontext.Value(`{"capabilities":{"sampling":{}}}`),
		}
		forwarded := mustRecv(t, upstream.writes, "the forwarded initialize").(*jsonrpc.Request)
		got, want := string(forwarded.Params), `{"capabilities":{"roots":{},"sampling":{}}}`
		require.Equalf(t, want, got, "forwarded initialize params = %s, want %s", got, want)

		rootsID := mustID(t, "roots-after-initialize")
		upstreamReads <- &jsonrpc.Request{ID: rootsID, Method: "roots/list"}
		resp := wantResponse(t, mustRecv(t, upstream.writes, "the local roots/list answer"), rootsID)
		root := wantOneRoot(t, resp)
		require.Equalf(t, "file:///tmp/home", root.URI, "root uri = %q, want the home tree", root.URI)
	})
}

// A cold worktree used to be dialled on the goroutine that reads the client, so
// every other worktree's calls queued behind its flock wait, gopls spawn and
// handshake — up to the whole handshake budget. Each worktree has a lane of its
// own now, and a call to one that is already connected must land while a cold
// one is still stuck in its dial.
func TestAColdWorktreeDoesNotHoldUpAnother(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		home := r.home
		warm := newFakeConn()

		handshakeReady(r)
		parkedDial(t, r)
		// Routing by path would have to resolve one through git, which a bubble
		// cannot account for. A sticky worktree sends a path-less call to the same
		// place, which is all this needs.
		r.sticky = "/tmp/cold"
		go connectedLane(r, home, warm).run()
		// LIFO, so this runs after the lanes are closed — see the note above.
		t.Cleanup(func() { close(warm.reads) })
		clientReads := startClient(t, r, 2)

		clientReads <- &jsonrpc.Request{
			ID:     mustID(t, "call-into-the-cold"),
			Method: "tools/call",
			Params: jsontext.Value(`{"arguments":{"query":"Foo"}}`),
		}
		clientReads <- &jsonrpc.Request{ID: mustID(t, "list"), Method: "tools/list"}

		_, err := recvRequest(warm.writes, "tools/list")
		require.NoErrorf(t, err, "%v — want the tools/list that followed the cold call, while the cold worktree is still dialling", err)
	})
}

// No bubble: readFromUpstream runs to completion on this goroutine, so there is
// nothing for a clock to wait on.
func TestHomeUpstreamEOFDoesNotCloseClient(t *testing.T) {
	t.Parallel()

	r := newTestRouter(t, testHome)
	r.readFromUpstream(testConnection(r, deadConn(), r.home), r.home)

	select {
	case err := <-r.errs:
		require.FailNow(t, fmt.Sprintf("home upstream EOF closed the client: %v", err))
	default:
	}
}

// The reader refuses the disorder that produces this — H1a — so reaching it
// takes a lane driven directly. That is the point: were the guard only on the
// reader, a lane whose connection was opened by anything else would hand gopls
// the duplicate initialize and lose the session.
func TestTheClientInitializeIsRefusedByAnAlreadyHandshakenUpstream(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		// Cached means handshaken, which is the state a successful dial leaves.
		held := newFakeConn()
		l := connectedLane(r, r.home, held)

		id := mustID(t, "client-initialize")
		r.track(id, nil, l.worktree)
		l.send(t.Context(), &jsonrpc.Request{ID: id, Method: "initialize", Params: jsontext.Value(`{}`)}, nil)

		wantClientError(t, r, id, "a second initialize was left unanswered")
		select {
		case msg := <-held.writes:
			require.FailNow(t, fmt.Sprintf("upstream got a second initialize %#v, want nothing written to it", msg))
		default:
		}
	})
}

// yieldingConn is an upstream that is already gone, and whose write is certain
// to lose the race it is in: it lets every other goroutine in the bubble block
// first, so a reader started before the write — the regression
// TestSendRetriesInitialInitializeWithoutPrivateHandshake pins — always claims
// the id. Left to a real race, that reader wins about once in ten thousand runs.
func yieldingConn() *fakeConn {
	c := deadConn()
	c.onWrite = func(context.Context, jsonrpc.Message) error {
		synctest.Wait()
		return io.EOF
	}
	return c
}

// A wedged upstream must not be waited on forever, whichever half of opening it
// hangs. Both run on the lane's goroutine, so either would stall every later
// call to that worktree with neither side timing out — and §8 keeps exactly
// such a server alive, so ensure hands its port back.
//
// The rows are the two halves: an upstream that accepts the connection and then
// answers nothing is what H5 bounds; one whose SSE connect never returns a first
// event never reaches H5 at all. The budget is one for both attempts either way,
// which is what the elapsed time and the dial count together say.
func TestAWedgedUpstreamFailsItsCallWithinOneBudget(t *testing.T) {
	tests := []struct {
		name string
		dial func(context.Context) (mcp.Connection, error)
	}{
		{"an upstream that never answers", func(context.Context) (mcp.Connection, error) {
			return newFakeConn(), nil
		}},
		{"a connect that never completes", func(ctx context.Context) (mcp.Connection, error) {
			<-ctx.Done() // what Connect does while it waits for the endpoint event
			return nil, ctx.Err()
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				r := newTestRouter(t, testHome)
				handshakeReady(r)
				dials := 0
				r.dial = func(ctx context.Context, _ string, port int) (mcp.Connection, error) {
					dials++
					return test.dial(ctx)
				}

				start := time.Now()
				id := sendCall(t, newLane(r, testWorktree), "call-into-the-void")

				wantClientError(t, r, id, "a wedged upstream blocked the lane instead of failing the call")
				// The shipped budget, not a shortened one — under this clock it costs
				// the same nothing — and spent exactly once. The elapsed time is what
				// says the two attempts shared one deadline rather than getting a full
				// one each.
				elapsed := time.Since(start)
				assert.Equalf(t, sendBudget, elapsed, "gave up after %s, want the whole %s budget spent once", elapsed, sendBudget)
				// The budget is spent, so the retry must not dial again: that costs an
				// unbounded flock wait to reach a handshake that expires on its first
				// write.
				assert.Equalf(t, 1, dials, "dial count = %d, want no retry once the deadline has passed", dials)
			})
		})
	}
}

// The dial context must outlive the budget — a connection reads its SSE stream
// under it for the whole of its life, so a plain deadline context would cut a
// healthy upstream loose when the budget expired, the failure H5 describes
// caused by the fix for it — and must not outlive the connection, or a wedged
// upstream redialling on every call leaks one live child context per call for
// as long as the client stays. Both halves are asserted on the one connection,
// in the order they happen.
//
// The first is measured while the connection is still in use, which is the only
// place it means anything: on one the lane had already closed, it would hold
// with the context leaked too.
func TestADialContextLivesExactlyAsLongAsItsConnection(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		var dialled context.Context
		r.dial = func(ctx context.Context, _ string, port int) (mcp.Connection, error) {
			dialled = ctx
			return newHandshakingConn(), nil
		}
		l := newLane(r, testWorktree)
		sendCall(t, l, "call-that-outlives-its-budget")

		// Long past the deadline the dial was given, and the connection is still
		// the lane's: a context carrying that deadline would be done by now.
		time.Sleep(2 * sendBudget)
		err := dialled.Err()
		assert.NoErrorf(t, err, "the dialled context is %v after the budget expired, want a context still good for the stream", err)

		// The close every site that gives a connection up performs, and that
		// lane.run performs on its way out.
		require.NoError(t, l.conn.Close())
		assert.Error(t, dialled.Err(), "the dialled context is still live after its connection was closed, want it released with the connection")
	})
}

// A gopls that dies mid-call must fail the calls it still owes. Nothing else
// will ever produce their ids, so a client without a timeout would hang.
//
// failInFlight takes its ids off the awaited list in bulk rather than through
// refuse, and dropping that delete costs nothing the first assertion can see.
// The second is what catches it: an entry per dead call for the rest of the
// session, and the next cancellation routed to a lane that stopped waiting.
func TestUpstreamDeathFailsItsInFlightCalls(t *testing.T) {
	bubble(t, func(t *testing.T) {
		conn := newFakeConn()
		r := newTestRouter(t, testHome)
		// Already connected: no dial or handshake to satisfy.
		l := connectedLane(r, testWorktree, conn)

		id := sendCall(t, l, "call-in-flight")
		mustRecv(t, conn.writes, "the call to reach the upstream, so an answer is owed")

		close(conn.reads) // the gopls dies before answering
		r.readFromUpstream(testConnection(r, conn, testWorktree), testWorktree)

		wantClientError(t, r, id, "upstream died and the in-flight call was left unanswered")

		// Home, because nobody owes this id any more. Named against the lane it
		// was sent to: still owed, the cancellation would go there instead.
		got, _ := r.target(&jsonrpc.Request{
			Method: "notifications/cancelled",
			Params: jsontext.Value(`{"requestId":"call-in-flight"}`),
		})
		require.Equalf(t, testHome, got, "cancelling a call its dead upstream already failed went to %q, want home %q", got, testHome)
	})
}

// The claim rule is symmetric: a reader that loses the race drops the answer it
// read. send() claims an id when its write fails, and the upstream may have
// taken the message anyway — a POST that times out after the server accepted it
// is enough — so its answer arrives for a call the client has already closed.
func TestUpstreamAnswerToAnAlreadyFailedCallIsDropped(t *testing.T) {
	bubble(t, func(t *testing.T) {
		id := mustID(t, "already-failed")
		r := newTestRouter(t, testHome)
		// Nothing owes this id: send() claimed it and answered the client already.
		r.readFromUpstream(testConnection(r, deadConn(&jsonrpc.Response{ID: id, Result: jsontext.Value(`{}`)}), testWorktree), testWorktree)

		wantClientQuiet(t, r, "a call the bridge had already answered was answered a second time")
	})
}

// A reconnect leaves two connections under one worktree, and the replaced one's
// reader is still running.
func TestStaleUpstreamDeathSparesTheReconnectedCall(t *testing.T) {
	bubble(t, func(t *testing.T) {
		live := newFakeConn()
		defer close(live.reads)
		r := newTestRouter(t, testHome)
		l := connectedLane(r, testWorktree, live)

		sendCall(t, l, "call-after-reconnect")
		mustRecv(t, live.writes, "the live connection to take the call it now owes")

		r.readFromUpstream(testConnection(r, deadConn(), testWorktree), testWorktree)

		wantClientQuiet(t, r, "the replaced connection failed a call owned by the live one")
	})
}

// Forwarding would return the client's reply addressed to nobody in particular,
// and leave the upstream stalled on an answer that never comes, with no timeout
// of its own.
func TestUnroutableUpstreamRequestIsRefusedNotForwarded(t *testing.T) {
	bubble(t, func(t *testing.T) {
		id := mustID(t, "sample-1")
		reads := make(chan jsonrpc.Message, 1)
		reads <- &jsonrpc.Request{ID: id, Method: "sampling/createMessage"}
		conn := &fakeConn{reads: reads, writes: make(chan jsonrpc.Message, 1)}

		r := newTestRouter(t, testHome)
		go r.readFromUpstream(testConnection(r, conn, testWorktree), testWorktree)

		resp := wantResponse(t, mustRecv(t, conn.writes, "the refusal sent back to the upstream"), id)
		_ = wantWireError(t, resp, jsonrpc.CodeMethodNotFound)
		close(reads)
		wantClientQuiet(t, r, "the client was asked something it cannot be given an answer for")
	})
}

// A call answered that quickly must leave nothing owed. Recorded after the
// write, the entry outlives the answer, and the next upstream death turns it
// into a second, contradictory reply to an id the client has already closed.
func TestCallAnsweredBeforeItsWriteReturnsLeavesNothingOwed(t *testing.T) {
	bubble(t, func(t *testing.T) {
		id := mustID(t, "answered-during-write")
		r := newTestRouter(t, testHome)
		// The upstream answers before its write returns, the way a gopls on the
		// same machine can, and does not return until the router has finished
		// accounting for that answer. Touching t here is allowed only because
		// this hook runs on the goroutine that called send — the one goroutine
		// where failing the test is safe; see inBackground.
		conn := &fakeConn{reads: make(chan jsonrpc.Message, 1)}
		conn.onWrite = func(context.Context, jsonrpc.Message) error {
			conn.reads <- &jsonrpc.Response{ID: id, Result: jsontext.Value(`{}`)}
			// The reader has now forgotten the call, before send() records it.
			mustRecv(t, r.out, "the answer the reader forwarded during this write")
			return nil
		}
		l := connectedLane(r, testWorktree, conn)

		// Not inBackground: this goroutine has no verdict to report, only an end,
		// and routing it through an error channel would leave a branch that cannot
		// fire and a linter asking why it is unchecked.
		done := make(chan struct{})
		go func() {
			defer close(done)
			r.readFromUpstream(testConnection(r, conn, testWorktree), testWorktree)
		}()

		// Not sendCall: this conn's onWrite is built around this exact id, and
		// minting a second one from the same string would only hide that.
		r.track(id, nil, l.worktree)
		l.send(t.Context(), &jsonrpc.Request{ID: id, Method: "tools/call"}, nil)
		close(conn.reads) // the upstream dies once the call is long since answered
		mustRecv(t, done, "the upstream reader to finish")

		wantClientQuiet(t, r, "an answered call was answered a second time")
	})
}

// gopls asks for its roots as soon as it has seen the capability in initialize,
// which lands inside the private handshake — before the reader that normally
// fields the question is running. One escaping to the client there comes back
// naming the tree the session opened in, and this upstream watches the wrong
// files for the rest of the session.
func TestRootsAskedDuringHandshakeIsAnsweredLocally(t *testing.T) {
	bubble(t, func(t *testing.T) {
		ctx := t.Context()
		upstream, gopls := upstreamPair(t)

		r := newTestRouter(t, testHome)
		handshakeReady(r)
		fixedDial(r, upstream)

		rootsID := mustID(t, "roots-mid-handshake")
		done := inBackground(func() error {
			msg, err := gopls.Read(ctx)
			if err != nil {
				return fmt.Errorf("read initialize: %w", err)
			}
			initialize, ok := msg.(*jsonrpc.Request)
			if !ok || initialize.Method != "initialize" {
				return fmt.Errorf("handshake opened with %#v, want initialize", msg)
			}
			// Asked before the handshake is answered, exactly as gopls does.
			if err := gopls.Write(ctx, &jsonrpc.Request{ID: rootsID, Method: "roots/list"}); err != nil {
				return fmt.Errorf("write roots/list: %w", err)
			}
			if msg, err = gopls.Read(ctx); err != nil {
				return fmt.Errorf("read roots answer: %w", err)
			}
			if resp, ok := msg.(*jsonrpc.Response); !ok || resp.ID != rootsID {
				return fmt.Errorf("mid-handshake roots/list got %#v, want an answer to %v", msg, rootsID)
			}
			if err := gopls.Write(ctx, &jsonrpc.Response{ID: initialize.ID, Result: jsontext.Value(`{}`)}); err != nil {
				return fmt.Errorf("answer initialize: %w", err)
			}
			if _, err := gopls.Read(ctx); err != nil { // notifications/initialized
				return fmt.Errorf("read initialized notification: %w", err)
			}
			return nil
		})

		_, err := newLane(r, testWorktree).upstream(t.Context(), false)
		require.NoErrorf(t, err, "handshake: %v", err)
		require.NoError(t, mustRecv(t, done, "the fake gopls to finish the handshake"))

		wantClientQuiet(t, r, "roots/list from inside the handshake reached the client")
	})
}

// worktreePath reduces a file to its directory itself, because every argument
// it is reached with is shaped like a file and a caller that forgot would get
// "Not a directory" from git — an error the router reads as "no evidence" and
// answers by misrouting to home. It must reduce exactly once, though: a path
// under a directory that does not exist yet must not climb to the grandparent
// and resolve against a tree the caller never named.
//
// One repository pair for every row, because none of them writes.
func TestWorktreePathResolvesEachPathToItsOwnWorktree(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)

	file := filepath.Join(root, "main.go")
	target := filepath.Join(linked, "target.go")
	for path, content := range map[string]string{file: "package main\n", target: "package linked\n"} {
		mustWriteFile(t, path, content)
	}
	rootNested, linkedNested := filepath.Join(root, "nested"), filepath.Join(linked, "nested")
	for _, dir := range []string{rootNested, linkedNested} {
		require.NoError(t, os.Mkdir(dir, 0o700))
	}
	// A link pointing into the other worktree: the reduction to a directory is
	// lexical, so without resolving first the link's own parent is what git
	// would be asked about, and here the two disagree.
	link := filepath.Join(root, "link.go")
	require.NoError(t, os.Symlink(target, link))

	tests := []struct {
		name string
		path string
		want string // "" wants an error instead
	}{
		{"a file in the root", file, root},
		{"a directory in the root", rootNested, root},
		{"the same directory in the linked worktree", linkedNested, linked},
		{"a symlink to a file in the linked worktree", link, linked},
		// R2: a file that does not exist yet still resolves through its parent,
		// and is not made unresolvable by the symlink pass — EvalSymlinks
		// refuses a path whose every component does not exist.
		{"a file not created yet", filepath.Join(root, "not-created-yet.go"), root},
		{"a file under a missing directory", filepath.Join(root, "absent", "x.go"), ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := worktreePath(t.Context(), test.path)
			if test.want == "" {
				require.Errorf(t, err, "worktreePath(%q) = %q, want no answer", test.path, got)
				return
			}
			require.NoErrorf(t, err, "worktreePath(%q): %v", test.path, err)
			require.Equalf(t, test.want, got, "worktreePath(%q) = %q, want %q", test.path, got, test.want)
		})
	}
}

func TestToolCallRoutesToTheWorktreeOwningItsPath(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	file := filepath.Join(linked, "main.go")
	mustWriteFile(t, file, "package main\n")
	rootFile := filepath.Join(root, "main.go")
	mustWriteFile(t, rootFile, "package main\n")

	tests := []struct {
		name      string
		arguments string
		want      []string
	}{
		// The routing that matters: a file in the linked worktree must not be
		// answered by the gopls of the checkout the bridge was started in.
		{name: "file argument", arguments: `{"file":` + strconv.Quote(file) + `}`, want: []string{linked}},
		{name: "files argument", arguments: `{"files":[` + strconv.Quote(file) + `]}`, want: []string{linked}},
		{name: "dir argument", arguments: `{"dir":` + strconv.Quote(linked) + `}`, want: []string{linked}},
		// One worktree named twice is still one destination: the call is
		// answerable, and reporting a repeat would refuse it for no reason.
		{name: "one worktree named twice", arguments: `{"files":[` + strconv.Quote(file) + `,` + strconv.Quote(filepath.Join(linked, "other.go")) + `]}`, want: []string{linked}},
		// Both are reported so that route can refuse the call. Answering from
		// the first would say nothing at all about the file in the second, and
		// the client cannot see the omission.
		{name: "paths in two worktrees", arguments: `{"files":[` + strconv.Quote(file) + `,` + strconv.Quote(rootFile) + `]}`, want: []string{linked, root}},
		// No path, or an unresolvable one: the caller keeps its current server.
		{name: "no path argument", arguments: `{"query":"Foo"}`},
		{name: "unresolvable path", arguments: `{"file":"/nonexistent/x.go"}`},
		// A relative path would resolve against the bridge's own cwd and then
		// stick in the memo, misrouting every later call naming it.
		{name: "relative path", arguments: `{"file":"internal/foo.go"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := newTestRouter(t, testHome)
			got := r.toolCallWorktrees(r.ctx, jsontext.Value(`{"arguments":`+test.arguments+`}`))
			require.Truef(t, slices.Equal(got, test.want), "toolCallWorktrees(%s) = %q, want %q", test.arguments, got, test.want)
		})
	}
}

// A tools/call spanning two worktrees is refused rather than answered from one
// of them: no single gopls knows both trees, and a result covering only the
// files it happened to route by is an omission the client cannot see.
func TestToolCallSpanningTwoWorktreesIsRefused(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	for _, dir := range []string{root, linked} {
		mustWriteFile(t, filepath.Join(dir, "main.go"), "package main\n")
	}

	r := newTestRouter(t, testHome)
	handshakeReady(r)
	// Seeded so the refusal has something to damage: R2a leaves sticky alone,
	// the call having picked no worktree, and a refusal that reset it would send
	// the next path-less call home instead of where the client was working.
	const working = "/repo/where-the-client-was"
	r.sticky = working
	id := mustID(t, "spanning-call")
	arguments := `{"files":[` + strconv.Quote(filepath.Join(linked, "main.go")) + `,` + strconv.Quote(filepath.Join(root, "main.go")) + `]}`
	r.route(&jsonrpc.Request{ID: id, Method: "tools/call", Params: jsontext.Value(`{"arguments":` + arguments + `}`)})

	// Refused on the reader's own goroutine, so the response is already queued
	// and no lane was ever asked for.
	select {
	case msg := <-r.out:
		wire := wantWireError(t, wantResponse(t, msg, id), jsonrpc.CodeInvalidParams)
		require.Truef(t, strings.Contains(wire.Message, root) && strings.Contains(wire.Message, linked), "error %q names neither worktree, want both", wire.Message)
	default:
		require.FailNow(t, "spanning call was routed instead of refused")
	}
	require.Lenf(t, r.lanes, 0, "refused call opened %d lane(s), want none", len(r.lanes))
	require.Equalf(t, working, r.sticky, "sticky = %q after a refused call, want %q left untouched", r.sticky, working)
}

// What worktreeOf answers, and what it costs to answer it: each row queries a
// list of paths in order and then states how many directory entries the memo
// should hold, which is exactly the number of git forks the session paid for.
//
// One repository pair for every row, because none of them writes: newLinkedWorktree
// forks git three times, and a pair per row would pay that over again to
// re-create the same two trees. Each row still gets a router of its own — the
// memo counts are the assertion, so they must not carry across.
func TestWorktreeOfResolvesAndMemoizes(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)

	tests := []struct {
		name string
		// paths are queried in order; repeats are deliberate, and want holds the
		// answer expected for each in the same order.
		paths    []string
		want     []string
		wantMemo int // directory-memo entries afterwards
	}{
		{
			// Every path in a directory has the same worktree, and resolving one
			// costs a ~13ms fork. A memo keyed by the path would pay that again
			// for every file the session names in a directory already resolved.
			// Neither file exists, which is also the answer to "what does it key
			// on for a file gopls is about to create": the directory holding it.
			// The third is that same directory as a dir argument, spelled the way
			// a client may well send one — a key of its own would fork git again
			// for an answer already in the memo.
			name: "one directory, three spellings, one fork",
			paths: []string{
				filepath.Join(linked, "a.go"),
				filepath.Join(linked, "b.go"),
				linked + string(filepath.Separator),
			},
			want:     []string{linked, linked, linked},
			wantMemo: 1,
		},
		{
			// The verbatim-path memo in front of the directory one answers without
			// walking the argument at all, so it is the one that could hand a file
			// the worktree of a file it was merely asked about near. Two trees of
			// one repository is where that would show: they share a git directory
			// and differ only in the path. Twice each and interleaved, so that the
			// second pass — served from the memo — has to answer what the first did.
			name: "two worktrees stay apart across a memo hit",
			paths: []string{
				filepath.Join(root, "a.go"), filepath.Join(linked, "a.go"),
				filepath.Join(root, "a.go"), filepath.Join(linked, "a.go"),
			},
			want: []string{root, linked, root, linked},
			// One per worktree, and not one per query: the repeats were memo hits.
			wantMemo: 2,
		},
		{
			// A path is reduced to a directory exactly once, so a file in a
			// subdirectory that does not exist yet resolves to nothing rather than
			// climbing to the grandparent — which would answer with the enclosing
			// repository, evidence the call never offered and R2 says is not
			// evidence at all. Nothing is memoized, because nothing resolved.
			name:     "a missing directory is no evidence",
			paths:    []string{filepath.Join(root, "absent", "x.go")},
			want:     []string{""},
			wantMemo: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := newTestRouter(t, testHome)
			for i, path := range test.paths {
				got := r.worktreeOf(r.ctx, path)
				require.Equalf(t, test.want[i], got, "worktreeOf(%q) = %q, want %q", path, got, test.want[i])
			}
			require.Lenf(t, r.worktrees, test.wantMemo, "memo holds %d directory entries, want %d: %v", len(r.worktrees), test.wantMemo, r.worktrees)
		})
	}
}

// Both halves go through target(), because the sticky worktree is written
// there and nowhere else: seeded by hand, this test would pass with that write
// deleted, and in production every path-less follow-up — go_workspace,
// go_search — would go home instead of following the call before it.
func TestTargetFallsBackToTheLastPathBearingCall(t *testing.T) {
	t.Parallel()
	_, linked := newLinkedWorktree(t)
	r := newTestRouter(t, testHome)

	bearing := jsontext.Value(`{"arguments":{"file":"` + filepath.Join(linked, "x.go") + `"}}`)
	got, _ := r.target(&jsonrpc.Request{Method: "tools/call", Params: bearing})
	require.Equalf(t, linked, got, "path-bearing tools/call went to %q, want %q", got, linked)

	params := jsontext.Value(`{"arguments":{"query":"Foo"}}`)
	got, _ = r.target(&jsonrpc.Request{Method: "tools/call", Params: params})
	require.Equalf(t, linked, got, "path-less tools/call went to %q, want the worktree the call before it named", got)

	got, _ = r.target(&jsonrpc.Request{Method: "tools/list"})
	require.Equalf(t, r.home, got, "non-call request went to %q, want home", got)
}

// A cancellation can arrive while its call is still queued at a lane that has
// not dialled yet — the cold-start window the lanes exist to keep off the
// reader is both the slowest and the likeliest moment for a client to give up.
// The destination is therefore bound where both messages pass, in the client's
// own order, and not where the lane finally writes the call: bound there, this
// cancellation would find nothing owed and go home, naming an id home never
// issued while the right gopls kept working.
func TestCancellationFindsACallItsLaneHasNotSentYet(t *testing.T) {
	bubble(t, func(t *testing.T) {
		const cold = "/tmp/cold"
		r := newTestRouter(t, testHome)
		r.sticky = cold  // routing by path would have to resolve one through git
		parkedDial(t, r) // where a cold worktree spends its time
		t.Cleanup(r.closeLanes)

		// Routed and picked up, but nothing is written yet, so the lane has no
		// connection to name and has not owed this id.
		r.route(&jsonrpc.Request{
			ID:     mustID(t, "slow-call"),
			Method: "tools/call",
			Params: jsontext.Value(`{"arguments":{"query":"Foo"}}`),
		})
		synctest.Wait()

		got, _ := r.target(&jsonrpc.Request{
			Method: "notifications/cancelled",
			Params: jsontext.Value(`{"requestId":"slow-call"}`),
		})
		require.Equalf(t, cold, got, "cancellation of a queued call went to %q, want %q", got, cold)
	})
}

// A lane with no upstream drops a cancellation rather than dialling one:
// bringing a gopls up costs an flock wait, a spawn and a whole handshake, to
// tell a brand new process about a call it never received. Nothing is stranded
// — the call died with the connection, and that connection's reader failed it.
func TestCancellationForADisconnectedUpstreamIsDroppedNotDialled(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		forbidDial(t, r, "a cancellation dialled the worktree that owed its call")

		cancel := &jsonrpc.Request{
			Method: "notifications/cancelled",
			Params: jsontext.Value(`{"requestId":"call-3"}`),
		}
		// A lane that never dialled, and one whose connection dies under this
		// very write — the retry starts from the same "no upstream to tell", so
		// the refusal has to be asked on every attempt rather than on the way in.
		newLane(r, "/tmp/gone").send(t.Context(), cancel, nil)
		connectedLane(r, "/tmp/dying", failingConn()).send(t.Context(), cancel, nil)

		wantClientQuiet(t, r, "a cancellation produced a message for the client")
	})
}

// A cancellation follows the id, not the default route: sent home it names an
// id home never issued, so the gopls actually running the call never hears it.
func TestCancellationFollowsTheRequestToItsUpstream(t *testing.T) {
	t.Parallel()
	r := newTestRouter(t, testHome)

	r.track(mustID(t, "call-1"), nil, "/repo/linked")
	// A numeric id reaches MakeID as the float64 json.Unmarshal produces, from
	// the notification here and from DecodeMessage when the call was keyed: the
	// two must land on the same ID or every real client's cancellation misroutes.
	r.track(mustID(t, float64(7)), nil, "/repo/numbered")
	// A worktree with no lane at all: routing follows the id regardless, since
	// whether that worktree has an upstream left to tell is the lane's
	// question, not this one — see
	// TestCancellationForADisconnectedUpstreamIsDroppedNotDialled. Which is
	// also why no case here registers a lane or a connection.
	r.track(mustID(t, "call-3"), nil, "/repo/gone")

	tests := []struct {
		name      string
		requestID string
		want      string
	}{
		{"owed id", `"call-1"`, "/repo/linked"},
		{"owed numeric id", `7`, "/repo/numbered"},
		{"disconnected upstream", `"call-3"`, "/repo/gone"},
		// Nobody owes these: an id already answered, and one no client would send.
		{"unowed id", `"call-2"`, r.home},
		{"unusable id", `{"bad":true}`, r.home},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, _ := r.target(&jsonrpc.Request{
				Method: "notifications/cancelled",
				Params: jsontext.Value(`{"requestId":` + test.requestID + `}`),
			})
			require.Equalf(t, test.want, got, "cancellation of %s went to %q, want %q", test.requestID, got, test.want)
		})
	}
}

// brokenConn is a client transport whose read fails with something other than
// the end of the stream — the one case serve reports rather than swallowing.
// Everything else about it is a plain fakeConn.
type brokenConn struct {
	*fakeConn
	err error
}

func (c *brokenConn) Read(context.Context) (jsonrpc.Message, error) { return nil, c.err }

// A client closing its end is how every session ends, so reporting that as an
// error would make a clean exit look like a crash to whatever ran this tool —
// while a transport that broke for some other reason is the one thing the exit
// status has to carry.
func TestServeSwallowsTheEndOfTheClientAndReportsAnythingElse(t *testing.T) {
	t.Parallel()
	broken := errors.New("stdin fell over")
	for _, tc := range []struct {
		name string
		conn mcp.Connection
		want error
	}{
		{name: "client closed", conn: deadConn(), want: nil},
		{name: "transport broke", conn: &brokenConn{fakeConn: deadConn(), err: broken}, want: broken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Serve returning at all is half of what this asserts: it waits for its
			// reader and closes every lane, so a shutdown that ordered those wrongly
			// would hang here rather than answer. The context is the test's own and
			// is never cancelled — serve derives what it cancels, so a session that
			// leaned on the caller for that would hang here too.
			err := serve(t.Context(), routerTestOwner(t, nil), testHome, tc.conn)
			require.ErrorIsf(t, err, tc.want, "serve() = %v, want %v", err, tc.want)
		})
	}
}

// writeToClient is the only path from an answer to the client, and the only
// thing it decides is what to do when that path fails. Reporting once and
// stopping is what lets serve exit; spinning on a transport that is gone would
// burn a core for as long as the client stayed dead.
func TestWriteToClientReportsAFailedWriteOnce(t *testing.T) {
	t.Parallel()
	r := newTestRouter(t, testHome)
	writes := 0
	conn := &fakeConn{onWrite: func(context.Context, jsonrpc.Message) error {
		writes++
		return io.ErrClosedPipe
	}}
	// Two queued, so that a writer which kept going after the failure would take
	// the second one and be caught doing it.
	r.out <- &jsonrpc.Response{ID: mustID(t, float64(1)), Result: jsontext.Value(`{}`)}
	r.out <- &jsonrpc.Response{ID: mustID(t, float64(2)), Result: jsontext.Value(`{}`)}

	r.writeToClient(conn) // returns, or this test times out

	err := <-r.errs
	require.ErrorIsf(t, err, io.ErrClosedPipe, "writeToClient() reported %v, want the write's own error", err)
	require.Equalf(t, 1, writes, "writeToClient() attempted %d writes after the first one failed, want it to stop at 1", writes)
}

// The session ending is the other way out, and it is the ordinary one: serve
// cancels the context before closing the transport, so a writer still holding a
// message must drop it rather than report a failure nobody is left to read.
func TestWriteToClientStopsWithTheSessionWithoutReportingIt(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	r := newRouter(ctx, routerTestOwner(t, nil), testHome)
	cancel()

	r.writeToClient(&fakeConn{writes: make(chan jsonrpc.Message, 1)})

	select {
	case err := <-r.errs:
		require.FailNow(t, fmt.Sprintf("writeToClient() reported %v for a session that ended normally", err))
	default:
	}
}

type observedClose struct {
	mcp.Connection
	closed atomic.Bool
}

func (c *observedClose) Close() error {
	c.closed.Store(true)
	return c.Connection.Close()
}

func TestServerReplyFailureCompletesOutstanding(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"roots/list", "unsupported"} {
		for _, stall := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stall=%v", method, stall), func(t *testing.T) {
				bubble(t, func(t *testing.T) {
					r := newTestRouter(t, testHome)
					conn := newFakeConn()
					conn.onWrite = func(ctx context.Context, _ jsonrpc.Message) error {
						if stall {
							<-ctx.Done()
							return ctx.Err()
						}
						return io.ErrClosedPipe
					}
					id := mustID(t, "outstanding")
					observed := &observedClose{Connection: conn}
					r.track(id, observed, testHome)
					go r.readFromUpstream(testConnection(r, observed, testHome), testHome)
					conn.reads <- &jsonrpc.Request{ID: mustID(t, "server"), Method: method}
					if stall {
						time.Sleep(sendBudget)
					}
					_ = wantWireError(t, wantClientError(t, r, id, "server reply stranded other calls"), jsonrpc.CodeInternalError)
					require.True(t, observed.closed.Load(), "failed connection was not closed")
					require.Zero(t, r.requestUsage().Outstanding, "failed connection retained callers")
					wantClientQuiet(t, r, "failure completed twice")
				})
			})
		}
	}
}

func BenchmarkAdmissionLoaded(b *testing.B) {
	for _, outstanding := range []int{0, 128, 1023} {
		b.Run(fmt.Sprint(outstanding), func(b *testing.B) {
			r := newTestRouter(b, testHome)
			for i := range outstanding {
				r.track(mustID(b, fmt.Sprint(i)), nil, testWorktree)
			}
			id := mustID(b, "new")
			b.ReportAllocs()
			for b.Loop() {
				_, err := r.admit(id, nil)
				require.NoError(b, err)
				r.finish(id, nil, nil)
			}
		})
	}
}

func TestOutstandingLimitsSurviveFastDelivery(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits = config.Limits{PerLane: 2, Session: 3}
		pausedLane(t, r, testHome)
		pausedLane(t, r, testWorktree)
		for i := range 2 {
			queuedCall(t, r, fmt.Sprint(i))
			call := <-r.lanes[testHome].reqs // delivered quickly but never answered
			r.track(call.req.ID, newFakeConn(), testHome)
			call.cancel()
		}
		id := queuedCall(t, r, "lane-full")
		_ = wantWireError(t, wantClientError(t, r, id, "unanswered calls bypassed lane limit"), -32000)
		r.sticky = testWorktree
		queuedCall(t, r, "other")
		id = queuedCall(t, r, "session-full")
		_ = wantWireError(t, wantClientError(t, r, id, "session limit was ignored"), -32000)
		r.cancelCall(&jsonrpc.Request{Params: jsontext.Value(`{"requestId":"0"}`)})
		_ = wantWireError(t, wantClientError(t, r, mustID(t, "0"), "cancel did not release slot"), -32800)
		queuedCall(t, r, "released-slot")
		wantClientQuiet(t, r, "released slot was not reusable")
	})
}

func TestPathArgumentsUseStrictJSON(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, params, file string
	}{
		{"exact names", `{"arguments":{"file":"/repo/a.go"}}`, "/repo/a.go"},
		{"uppercase container", `{"Arguments":{"file":"/repo/a.go"}}`, ""},
		{"uppercase field", `{"arguments":{"File":"/repo/a.go"}}`, ""},
		{"duplicate paths", `{"arguments":{"file":"/repo/a.go","file":"/repo/b.go"}}`, ""},
		{"invalid UTF-8", "{\"arguments\":{\"file\":\"/repo/\xff.go\"}}", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			args := parsePathArguments(jsontext.Value(tc.params))
			require.Equal(t, tc.file, args.File)
		})
	}
}

// newLinkedWorktree returns a fresh repository and a linked worktree of it.
func newLinkedWorktree(t testing.TB) (root, linked string) {
	t.Helper()
	root = filepath.Join(t.TempDir(), "main")
	linked = filepath.Join(t.TempDir(), "linked")
	runGit(t, "init", root)
	runGit(t, "-C", root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "initial")
	runGit(t, "-C", root, "worktree", "add", "--detach", linked)
	return mustEvalSymlinks(t, root), mustEvalSymlinks(t, linked)
}

// symlinkAt returns a fresh symlink pointing at target.
func symlinkAt(t *testing.T, target string) string {
	t.Helper()
	link := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(target, link))
	return link
}

func mustEvalSymlinks(t testing.TB, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	require.NoError(t, err)
	return resolved
}

func runGit(t testing.TB, args ...string) {
	t.Helper()
	output, err := exec.Command("git", args...).CombinedOutput()
	require.NoErrorf(t, err, "git %v: %v\n%s", args, err, output)
}
