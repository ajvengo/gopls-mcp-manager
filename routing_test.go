package main

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditResourceStateReadersRejectSymlinksAndNonregularFiles(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"symlink", "fifo", "directory"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "state")
			switch kind {
			case "symlink":
				target := filepath.Join(dir, "target")
				require.NoError(t, os.WriteFile(target, []byte("{}"), 0o600))
				require.NoError(t, os.Symlink(target, path))
			case "fifo":
				require.NoError(t, syscall.Mkfifo(path, 0o600))
			case "directory":
				require.NoError(t, os.Mkdir(path, 0o700))
			}
			_, _, err := readMap(path)
			require.Error(t, err)
			file, err := openStateFile(path, maxRegistryBytes)
			if file != nil {
				require.NoError(t, file.Close())
			}
			require.Error(t, err)
		})
	}
}

func TestMemoExpiryRevalidatesRetargetedSymlink(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	alias := symlinkAt(t, root)
	r := newTestRouter(t, root)
	path := filepath.Join(alias, "missing.go")
	got := r.worktreeOf(r.ctx, path)
	require.Equalf(t, root, got, "initial target = %s, want %s", got, root)
	require.NoError(t, os.Remove(alias))
	require.NoError(t, os.Symlink(linked, alias))
	r.memo.expires = time.Now().Add(-time.Second)
	got = r.worktreeOf(r.ctx, path)
	require.Equalf(t, linked, got, "expired target = %s, want %s", got, linked)
}

func TestMemoEpochExpiresBothLevels(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits.CacheTTL = time.Second
		r.paths["/old/file.go"] = pathMemo{worktree: testWorktree}
		r.worktrees["/old"] = testWorktree
		_, hit := r.cachedWorktrees(fileCallParams("/old/file.go"))
		require.True(t, hit, "seeded memo missed")
		time.Sleep(time.Second)
		_, hit = r.cachedWorktrees(fileCallParams("/old/file.go"))
		usage := r.requestUsage()
		require.Truef(t, !hit && usage.Paths == 0 && usage.Directories == 0 && usage.MemoExpirations == 1, "expired memo retained state: hit=%v usage=%+v", hit, usage)
		require.Truef(t, usage.MemoHits == 1 && usage.MemoMisses == 1, "memo observations = %+v", usage)
	})
}

func TestMemoRefreshAtCapacityDoesNotEvict(t *testing.T) {
	t.Parallel()
	r := newTestRouter(t, testHome)
	r.limits.CacheEntries = 1
	memoize(r, r.paths, "/a", pathMemo{worktree: testHome})
	memoize(r, r.paths, "/a", pathMemo{worktree: testWorktree})
	require.Truef(t, r.memo.rollovers == 0 && r.paths["/a"].worktree == testWorktree, "updating an existing key rolled the memo over")
	memoize(r, r.paths, "/b", pathMemo{worktree: testHome})
	require.Truef(t, r.memo.rollovers == 1 && len(r.paths) == 1, "new key did not enforce capacity")
}

// The whole routing decision for the message an agent sends most: unmarshal
// plus the memo hit below. Its allocs/op is what says whether target still
// gathers its path arguments into a slice before resolving them.
//
// The reader goroutine runs this for every message and nothing else routes
// while it does — but it is not the throughput ceiling: reading the message off
// the transport costs several times more and allocates two orders of magnitude
// more, the SDK's decoder taking a fresh 64KiB buffer per message. See
// BenchmarkStdioCodec, and BenchmarkCallRoundTrip for the two together, before
// spending a diff on the routing rather than the decode above it.
func BenchmarkTargetToolCall(b *testing.B) {
	r, file := benchRouter(b)
	req := toolCall(b, fileCallParams(file))
	b.ReportAllocs()
	// Checks inside b.Loop stay behind an if: every testify call runs
	// b.Helper, a stack walk the benchmark would otherwise measure.
	for b.Loop() {
		if worktree, _ := r.target(req); worktree == "" {
			require.FailNow(b, "no worktree")
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
		r.paths[path] = pathMemo{worktree: r.home}
	}
	req := toolCall(b, jsontext.Value(`{"name":"go_diagnostics","arguments":{"files":[`+strings.Join(files, ",")+`]}}`))
	b.ReportAllocs()
	for b.Loop() {
		if worktree, _ := r.target(req); worktree == "" {
			require.FailNow(b, "no worktree")
		}
	}
}

// Probe-only microbenchmark. Full maintenance probes every record outside the
// flock; acquisition probes only the requested worktree. See
// BenchmarkAcquisitionVersusSweep for those production paths.
//
// The rows are the verdicts a probe can reach. "live" is the steady state, one
// local round trip. "refused" is conclusively dead; "odd" answers but not like
// ours, so the verdict is inconclusive — both then fork a ps per record, and
// that fork, not the probe, is what they measure. "silent" is §10's case, a
// server that took the connection and said nothing: it costs probeClient's
// whole timeout, so it runs at half a second an iteration by construction and

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
		if r.worktreeOf(r.ctx, filepath.Join(dir, fmt.Sprintf("f%d.go", i))) == "" {
			require.FailNow(b, "no worktree")
		}
		i++
	}
}

func BenchmarkResolveColdPath(b *testing.B) {
	r, file := benchRouter(b)
	params := fileCallParams(file)
	b.ReportAllocs()
	for b.Loop() {
		r.memoMu.Lock()
		delete(r.paths, file)
		r.memoMu.Unlock()
		worktrees, err := r.resolveWorktrees(b.Context(), params)
		if err != nil || len(worktrees) != 1 || worktrees[0] != r.home {
			b.Fatalf("cold resolution = %v, %v", worktrees, err)
		}
	}
}

func TestEndToEndStdioRouting(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	m, _, _, _ := sseFixture(t, root, linked)
	client := stdioFixture(t, m, root)
	for _, tc := range []struct {
		name  string
		args  map[string]any
		want  string
		fails bool
	}{
		{name: "home", args: map[string]any{}, want: root},
		{name: "linked", args: map[string]any{"file": filepath.Join(linked, "main.go")}, want: linked},
		{name: "sticky", args: map[string]any{}, want: linked},
		{name: "mixed worktrees", args: map[string]any{"files": []string{filepath.Join(root, "main.go"), filepath.Join(linked, "main.go")}}, fails: true},
		{name: "sticky after refusal", args: map[string]any{}, want: linked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params, err := json.Marshal(map[string]any{"name": "where", "arguments": tc.args})
			require.NoError(t, err)
			id := mustID(t, tc.name)
			response := stdioRoundTrip(t, t.Context(), client, &jsonrpc.Request{ID: id, Method: "tools/call", Params: params})
			ok := (response.Error != nil) == tc.fails && (tc.fails || strings.Contains(string(response.Result), tc.want))
			require.Truef(t, ok, "response = %+v, want %s, failure %v", response, tc.want, tc.fails)
		})
	}
}

// StartedAt is the one field readMap does not range-check — unlike the other
// three it is no argument to kill(2) — while FuzzMapRoundTrip already shows the
// map carrying arbitrary int64. What it is an argument to is this: a record
// inside its grace is spared by every sweep without being probed at all, so
// grace given wrongly makes that record immortal, holding its port against a
// worktree whose next ensure starts a second gopls beside it.
//
// Both int64 extremes are seeded because the arithmetic is a conversion to Go's
// epoch, which wraps, and a subtraction into a nanosecond Duration, which

// A worktree path is arbitrary bytes off the command line or out of git, and
// this is the one place it becomes a path this process opens for writing. It
// must land inside the log directory whatever it says: a worktree that walked
// out of it would have this tool creating and appending to a file somewhere
// else on every cold start, named by whoever chose the directory.
func FuzzLogPath(f *testing.F) {
	f.Add("/repo/plain")
	f.Add("../../../etc/passwd")
	f.Add("/repo/a/../../..")
	f.Add("")
	f.Add("/")
	f.Add(strings.Repeat("../", 64))
	f.Add("/repo/\x00null")
	f.Add("/repo/юникод/路径")
	f.Add("/repo/\xff\xfe")

	// Built once: logPath only reads the map path, and a temp directory per
	// iteration is two syscalls against a function that makes none.
	m := newTestManager(f)
	wantDir := filepath.Join(filepath.Dir(m.mapPath), "gopls-mcp-logs")

	f.Fuzz(func(t *testing.T, worktree string) {
		got := m.logPath(worktree)

		dir := filepath.Dir(got)
		require.Equalf(t, wantDir, dir, "logPath(%q) = %q, which is under %q rather than %q", worktree, got, dir, wantDir)
		// Cleaning is what a traversal would survive: a name that reduces to
		// something else is one that reached the directory above by another road.
		require.Equalf(t, got, filepath.Clean(got), "logPath(%q) = %q, which cleans to %q", worktree, got, filepath.Clean(got))
		again := m.logPath(worktree)
		require.Equalf(t, got, again, "logPath(%q) = %q, then %q: the same worktree logs to two files", worktree, got, again)
	})
}

// The routing decision itself: which worktrees a tools/call names. Resolution
// shells out to git, so the property asserted is the one that holds whatever it
// answers — and the one the memos in front of it could actually break: asking
// twice must answer twice the same, or a call would route somewhere its own
// repeat did not.
func FuzzToolCallWorktrees(f *testing.F) {
	f.Add([]byte(`{"arguments":{"file":"/repo/a.go"}}`))
	f.Add([]byte(`{"arguments":{"dir":"/repo"}}`))
	f.Add([]byte(`{"arguments":{"files":["/repo/a.go","/repo/b.go"]}}`))
	f.Add([]byte(`{"arguments":{"file":"relative.go"}}`))
	f.Add([]byte(`{"arguments":{"files":[]}}`))
	f.Add([]byte(`{"arguments":{"files":null}}`))
	f.Add([]byte(`{"arguments":{"file":123}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`garbage`))

	f.Fuzz(func(t *testing.T, params []byte) {
		r := newTestRouter(t, testHome)
		first := r.toolCallWorktrees(r.ctx, params)
		again := r.toolCallWorktrees(r.ctx, params)
		require.Truef(t, slices.Equal(first, again), "toolCallWorktrees(%s) = %q, then %q from the memo", params, first, again)
	})
}

// notifications/cancelled carries an id minted by the client, matched against
// the ids in flight (R7). Two ids are tracked under different worktrees, so the
// answer has to be the one belonging to the id named — a target that returned
// the wrong lane, or any lane at all for an id nobody owes, would show here.
func FuzzCancelTarget(f *testing.F) {
	f.Add([]byte(`{"requestId":1,"requestId":2}`))
	f.Add([]byte(`{"requestId":1,"ignored":`))
	f.Add([]byte(`{"requestId":1}`))
	f.Add([]byte(`{"requestId":2}`))
	f.Add([]byte(`{"requestId":3}`))
	f.Add([]byte(`{"requestId":"abc"}`))
	f.Add([]byte(`{"requestId":null}`))
	f.Add([]byte(`{"requestId":{"nested":true}}`))
	f.Add([]byte(`{"requestId":1e400}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`garbage`))

	const otherWorktree = "/tmp/other"
	f.Fuzz(func(t *testing.T, params []byte) {
		r := newTestRouter(t, testHome)
		r.track(mustID(t, float64(1)), nil, testWorktree)
		r.track(mustID(t, float64(2)), nil, otherWorktree)

		var cancelled struct {
			RequestID any `json:"requestId"`
		}
		if err := json.Unmarshal(params, &cancelled); err != nil {
			// v2 can populate RequestID before rejecting a duplicate name or
			// malformed trailing field. Partial decoding grants no cancellation.
			require.Empty(t, r.cancelTarget(params))
			return
		}
		// Only a JSON number names one of the two tracked ids; every other shape
		// — a string, an object, nothing at all — is owed by nobody.
		var want string
		switch id, _ := cancelled.RequestID.(float64); id {
		case 1:
			want = testWorktree
		case 2:
			want = otherWorktree
		}
		got := r.cancelTarget(params)
		require.Equalf(t, want, got, "cancelTarget(%s) = %q, want %q", params, got, want)
	})
}

// A request is routed on its method and params together, and target's answer is
// the lane that will run it. It must always name one.
func FuzzTargetNamesALane(f *testing.F) {
	f.Add("tools/call", []byte(`{"arguments":{"files":["relative.go"],"File":"/opaque.go"},"opaque":{"file":"/other.go"}}`))
	f.Add("notifications/cancelled", []byte(`{"requestId":"same","request\u0049d":1}`))
	f.Add("tools/call", []byte(`{"arguments":{"files":["relative.go",null]}}`))
	f.Add("tools/call", []byte(`{"arguments":{"file":"/repo/a.go","file":"/repo/b.go"}}`))
	f.Add("tools/call", []byte(`{"arguments":{"files":{"nested":["/repo/a.go"]}}}`))
	f.Add("tools/call", []byte(`{"arguments":{"file":"/repo/a.go"}}`))
	f.Add("tools/call", []byte(`{"arguments":{"query":"Foo"}}`))
	f.Add("tools/list", []byte(`{}`))
	f.Add("notifications/cancelled", []byte(`{"requestId":1}`))
	f.Add("initialize", []byte(`{}`))
	f.Add("", []byte(``))

	f.Fuzz(func(t *testing.T, method string, params []byte) {
		r := newTestRouter(t, testHome)
		got, err := r.target(&jsonrpc.Request{Method: method, Params: params})
		if err != nil {
			// The only refusal is a call spanning worktrees, which names them.
			require.Emptyf(t, got, "target() refused with %q, want no worktree alongside the error", got)
			return
		}
		require.NotEmptyf(t, got, "target(%q, %s) = %q with no error: no lane would own it", method, params, got)
	})
}

func TestMemoCapacityRevalidatesEvictedPaths(t *testing.T) {
	t.Parallel()
	r := newTestRouter(t, testHome)
	r.limits.CacheEntries = 2
	for i := range 100 {
		key := fmt.Sprintf("/%d", i)
		memoize(r, r.worktrees, key, testHome)
		memoize(r, r.paths, key, pathMemo{worktree: testHome})
	}
	require.Falsef(t, len(r.worktrees) > 2 || len(r.paths) > 2, "memo exceeded capacity")
	_, ok := r.worktrees["/0"]
	require.False(t, ok, "old memo survived capacity rollover")
	_, ok = r.paths["/0"]
	require.False(t, ok, "old memo survived capacity rollover")
	r.lookupCold = func(context.Context, jsontext.Value) []string {
		return []string{testWorktree}
	}
	got, err := r.target(&jsonrpc.Request{Method: "tools/call", Params: fileCallParams("/0")})
	require.Falsef(t, err != nil || got != testWorktree, "evicted path not resolved afresh: %s, %v", got, err)
}

func TestAccountingTerminalPathsAndSnapshots(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		pausedLane(t, r, testHome)
		conn := newFakeConn()
		for _, name := range []string{"reply", "cancel", "disconnect", "clear"} {
			id := queuedCall(t, r, name)
			r.track(id, conn, testHome)
		}
		r.finish(mustID(t, "reply"), testConnection(r, conn, r.home), nil)
		r.cancelCall(&jsonrpc.Request{Params: jsontext.Value(`{"requestId":"cancel"}`)})
		_ = wantClientError(t, r, mustID(t, "cancel"), "cancel not completed")
		r.failInFlight(testConnection(r, conn, testHome), testHome, io.EOF)
		_ = mustRecv(t, r.out, "disconnect result")
		_ = mustRecv(t, r.out, "disconnect result")
		queuedCall(t, r, "clear-again")
		r.clearRequests()
		require.Falsef(t, len(r.perWorktree) != 0 || r.requestUsage().Admitted != r.requestUsage().Completed, "terminal paths leaked accounting")
		r.rejected("delivery_queue")
		before := r.requestUsage()
		r.rejected("delivery_queue")
		require.Equal(t, 1, before.Rejections["delivery_queue"], "snapshot shares mutable counters")
	})
}

func TestRoutingBudgetDoesNotMultiplyOrGrowWorkers(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.sticky = testWorktree
		entered, blocked := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(blocked) })
		defer unblock()
		done := make(chan error, 1)
		go func() {
			_, err := ownerFilesystemCall(t.Context(), r.owner, func() struct{} {
				close(entered)
				<-blocked
				return struct{}{}
			})
			done <- err
		}()
		mustRecv(t, entered, "owner filesystem worker")
		for range 2 {
			start := time.Now()
			_, err := r.target(&jsonrpc.Request{Method: "tools/call", Params: jsontext.Value(`{"arguments":{"file":"/uncached/file.go"}}`)})
			require.ErrorIsf(t, err, context.DeadlineExceeded, "routing returned %v", err)
			require.Truef(t, time.Since(start) == routingBudget, "routing multiplied its budget")
		}
		unblock()
		require.NoError(t, mustRecv(t, done, "owner filesystem worker release"))
		synctest.Wait()
		require.Equalf(t, testWorktree, r.sticky, "expired routing changed sticky state")
	})
}

func TestLaneChurnRemainsAtRetentionCeiling(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits.Lanes = 4
		handshakeReady(r)
		var dials int
		r.dial = func(context.Context, string, int) (mcp.Connection, error) { dials++; return loopbackConn(), nil }
		defer r.closeLanes()
		for i := range 1000 {
			worktree := fmt.Sprintf("/worktree-%d", i)
			file := worktree + "/file.go"
			r.paths[file] = pathMemo{worktree: worktree}
			r.route(&jsonrpc.Request{ID: mustID(t, float64(i)), Method: "tools/call", Params: fileCallParams(file)})
			msg := mustRecv(t, r.out, "churn request did not finish")
			releaseMessage(msg)
			response, ok := msg.(*jsonrpc.Response)
			require.Truef(t, ok && (response.Error != nil) == (i >= 4), "request %d: %+v", i, msg)
		}
		usage := r.requestUsage()
		require.Truef(t, dials == 4 && usage.Lanes == 4 && usage.Outstanding == 0 && usage.UpstreamOperations == 0, "churn escaped bounds: dials=%d usage=%+v", dials, usage)
	})
}

func TestFullErrorQueueDoesNotBlockIngressExit(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		for range cap(r.errs) {
			r.errs <- io.ErrClosedPipe
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			r.readIngress(deadConn(), make(chan delivery))
		}()
		synctest.Wait()
		select {
		case <-done:
		default:
			require.Error(t, <-r.errs) // release the old implementation before failing
			require.FailNow(t, "ingress exit blocked reporting a redundant session error")
		}
	})
}

func TestFullOutputQueueReleasesTerminalProducers(t *testing.T) {
	for _, terminal := range []string{"upstream failure", "delivery failure", "cancellation"} {
		t.Run(terminal, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				r := newRouter(ctx, routerTestOwner(t, nil), testHome)
				conn := newFakeConn()
				id := mustID(t, "terminal")
				r.track(id, conn, testHome)
				for range cap(r.out) {
					r.out <- &jsonrpc.Request{Method: "notification"}
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					switch terminal {
					case "upstream failure":
						r.failInFlight(testConnection(r, conn, testHome), testHome, io.EOF)
					case "delivery failure":
						r.failDelivery(id, nil, -32000, "delivery failed")
					case "cancellation":
						r.cancelCall(&jsonrpc.Request{Method: "notifications/cancelled", Params: []byte(`{"requestId":"terminal"}`)})
					}
				}()
				synctest.Wait()
				select {
				case <-done:
				default:
					cancel()
					<-done
					require.FailNow(t, "terminal producer retained by saturated output")
				}
				require.Zero(t, r.requestUsage().Outstanding)
				require.Equal(t, 1, r.requestUsage().Completed)
				require.ErrorContains(t, <-r.errs, "client output queue is full")
			})
		})
	}
}

func TestSendReconnectsAndInitializesRestartedHome(t *testing.T) {
	bubble(t, func(t *testing.T) {
		fresh := &fakeConn{
			// Buffered, so that handing over the handshake's answer below cannot
			// block on the router picking it up.
			reads:  make(chan jsonrpc.Message, 1),
			writes: make(chan jsonrpc.Message),
		}
		r := newTestRouter(t, testHome)
		closed, cancel := context.WithCancel(t.Context())
		cancel()
		l := connectedLane(r, r.home, testUpstream(r, r.home, failingConn(), closed, cancel))
		handshakeReady(r)
		fixedDial(r, fresh)

		done := inBackground(func() error {
			initialize, err := recvRequest(fresh.writes, "initialize")
			if err != nil {
				return err
			}
			// The id is the private one and the params are the client's own:
			// asserting only that some id is valid would leave both the fixed
			// id H2 relies on and the replayed capabilities ungated.
			if initialize.ID != initID {
				return fmt.Errorf("reconnect opened with id %v, want the private %v", initialize.ID, initID)
			}
			if string(initialize.Params) != clientInitializeParams {
				return fmt.Errorf("replayed initialize params = %s, want the client's %s", initialize.Params, clientInitializeParams)
			}
			fresh.reads <- &jsonrpc.Response{ID: initialize.ID, Result: jsontext.Value(`{}`)}
			if _, err := recvRequest(fresh.writes, "notifications/initialized"); err != nil {
				return err
			}
			_, err = recvRequest(fresh.writes, "tools/call")
			return err
		})
		// The upstream outlives the assertions below: the call it just accepted is
		// still in flight, and closing it here would — correctly — fail that call.
		t.Cleanup(func() { close(fresh.reads) })

		sendCall(t, l, "call-7")
		require.NoError(t, mustRecv(t, done, "the reconnect assertions to finish"))

		wantClientQuiet(t, r, "successful reconnect returned an error to the client")
	})
}

func TestSendDoesNotReplayAnUncertainInitialize(t *testing.T) {
	bubble(t, func(t *testing.T) {
		fresh := newFakeConn()
		dials := 0
		r := newTestRouter(t, testHome)
		home := r.home
		r.dial = func(context.Context, string, int) (mcp.Connection, error) {
			dials++
			if dials == 1 {
				return yieldingConn(), nil
			}
			return fresh, nil
		}
		id := mustID(t, "client-initialize")
		initialize := &jsonrpc.Request{ID: id, Method: "initialize", Params: jsontext.Value(`{}`)}
		r.initialize.Store(initialize)

		r.track(id, nil, home)
		newLane(r, home).send(t.Context(), initialize, nil)
		response := mustRecv(t, r.out, "the uncertain initialize error").(*jsonrpc.Response)
		require.ErrorContains(t, response.Error, "may have executed")
		require.Equal(t, 1, dials, "uncertain initialize was replayed on another connection")
		require.Empty(t, fresh.writes)
		require.Equal(t, 1, r.requestUsage().AbandonedOperations)
		close(fresh.reads)
	})
}

// The reader has already answered this id, so the retry must not run: the
// client keeps the first reply it gets, so a redial that succeeded would look
// fine here while the client held the error.
func TestSendLeavesACallItsDyingUpstreamAlreadyFailed(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		// The upstream loses the race a real one loses when it dies mid-write:
		// its reader sees the death and fails the in-flight call before the
		// write reports it.
		conn := &fakeConn{reads: make(chan jsonrpc.Message)}
		conn.onWrite = func(context.Context, jsonrpc.Message) error {
			close(conn.reads)
			r.readFromUpstream(testConnection(r, conn, testWorktree), testWorktree)
			return io.EOF
		}
		l := connectedLane(r, testWorktree, conn)
		forbidDial(t, r, "send retried a call its upstream had already failed")

		id := sendCall(t, l, "raced-call")

		wantClientError(t, r, id, "the dying upstream's reader left the call unanswered")
		wantClientQuiet(t, r, "one call was answered twice")
	})
}

func TestRetentionLimitDoesNotAllocateRejectedLane(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits.Lanes = 1
		pausedLane(t, r, testHome)
		r.sticky = testWorktree
		id := queuedCall(t, r, "too-many-lanes")
		_ = wantWireError(t, wantClientError(t, r, id, "retention limit was ignored"), -32000)
		require.Falsef(t, len(r.lanes) != 1 || len(r.awaitingUpstream) != 0 || len(r.perWorktree) != 0, "rejected lane retained resources")
		r.sticky = testHome
		queuedCall(t, r, "existing-lane")
		wantClientQuiet(t, r, "retention cap refused an existing lane")
	})
}

func TestServerReplyKeepsShorterHandshakeDeadline(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		conn := newFakeConn()
		conn.reads = make(chan jsonrpc.Message, 1)
		conn.reads <- &jsonrpc.Request{ID: mustID(t, "roots"), Method: "roots/list"}
		conn.onWrite = func(ctx context.Context, msg jsonrpc.Message) error {
			if _, ok := msg.(*jsonrpc.Response); ok {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		err := newLane(r, testHome).handshake(ctx, testConnection(r, conn, testHome))
		require.Error(t, err, "stalled roots reply succeeded")
		require.Truef(t, time.Since(start) == time.Second, "server reply extended handshake deadline")
	})
}

func TestOutstandingLimitDoesNotAllocateNewLane(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits.Session = 1
		pausedLane(t, r, testHome)
		queuedCall(t, r, "first")
		r.sticky = testWorktree
		id := queuedCall(t, r, "second")
		_ = wantWireError(t, wantClientError(t, r, id, "session cap was ignored"), -32000)
		require.Lenf(t, r.lanes, 1, "session rejection created a new lane")
	})
}

// A lane not running yet makes queue admission independent of scheduling.
func pausedLane(t *testing.T, r *router, worktree string) *lane {
	t.Helper()
	l := newLane(r, worktree)
	r.lanes[worktree] = l
	t.Cleanup(l.cancel)
	return l
}

func queuedCall(t *testing.T, r *router, id string) jsonrpc.ID {
	t.Helper()
	requestID := mustID(t, id)
	r.route(&jsonrpc.Request{ID: requestID, Method: "tools/call"})
	return requestID
}

func TestFullLaneRejectsWithoutBlockingOtherWorktrees(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		pausedLane(t, r, testWorktree)
		pausedLane(t, r, testHome)
		r.sticky = testWorktree
		for i := range laneQueue {
			queuedCall(t, r, fmt.Sprintf("queued-%d", i))
		}
		id := queuedCall(t, r, "overloaded")
		_ = wantWireError(t, wantClientError(t, r, id, "full lane blocked routing"), -32000)
		r.sticky = testHome
		queuedCall(t, r, "other-worktree")
		call := mustRecv(t, r.lanes[testHome].reqs, "another worktree's admitted call")
		call.cancel()
		wantClientQuiet(t, r, "another worktree was rejected with the full lane")
	})
}

func TestQueuedDeliveryExpiresWithoutDialling(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		l := pausedLane(t, r, testHome)
		forbidDial(t, r, "expired queued work dialled an upstream")
		id := queuedCall(t, r, "expired")
		time.Sleep(sendBudget)
		close(l.reqs)
		go l.run()
		mustRecv(t, l.done, "expired lane to drain")
		_ = wantWireError(t, wantClientError(t, r, id, "expired call left its caller waiting"), jsonrpc.CodeInternalError)
	})
}

func TestQueuedCancellationPreventsDelivery(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		l := pausedLane(t, r, testHome)
		forbidDial(t, r, "cancelled queued work dialled an upstream")
		id := queuedCall(t, r, "cancelled")
		r.route(&jsonrpc.Request{Method: "notifications/cancelled", Params: jsontext.Value(`{"requestId":"cancelled"}`)})
		close(l.reqs)
		go l.run()
		mustRecv(t, l.done, "cancelled lane to drain")
		_ = wantWireError(t, wantClientError(t, r, id, "cancelled call left its caller waiting"), -32800)
	})
}

func TestCancellationBypassesFullDeliveryQueue(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		l := pausedLane(t, r, testHome)
		for i := range laneQueue {
			queuedCall(t, r, fmt.Sprintf("queued-%d", i))
		}
		upstream := newFakeConn()
		r.track(mustID(t, "sent"), upstream, testHome)
		go l.runControls()
		r.route(&jsonrpc.Request{Method: "notifications/cancelled", Params: jsontext.Value(`{"requestId":"sent"}`)})
		_, err := recvRequest(upstream.writes, "notifications/cancelled")
		require.NoError(t, err)
		_ = wantWireError(t, wantClientError(t, r, mustID(t, "sent"), "cancellation did not complete locally"), -32800)
		wantClientQuiet(t, r, "cancellation replied twice")
	})
}

func TestFullControlQueueDoesNotBlockRouting(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		l := pausedLane(t, r, testHome)
		r.track(mustID(t, "sent"), newFakeConn(), testHome)
		for i := range laneQueue + 1 {
			id := fmt.Sprintf("sent-%d", i)
			r.track(mustID(t, id), newFakeConn(), testHome)
			r.route(&jsonrpc.Request{Method: "notifications/cancelled", Params: jsontext.Value(fmt.Sprintf(`{"requestId":%q}`, id))})
			_ = wantWireError(t, wantClientError(t, r, mustID(t, id), "cancellation did not complete locally"), -32800)
		}
		queuedCall(t, r, "still-routes")
		call := mustRecv(t, l.reqs, "delivery after a full cancellation queue")
		call.cancel()
		wantClientQuiet(t, r, "dropped advisory notification produced a response")
	})
}

func TestDeliveryCancellationStopsADial(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		r.dial = func(ctx context.Context, _ string, port int) (mcp.Connection, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		id := queuedCall(t, r, "dialling")
		synctest.Wait()
		r.route(&jsonrpc.Request{Method: "notifications/cancelled", Params: jsontext.Value(`{"requestId":"dialling"}`)})
		_ = wantWireError(t, wantClientError(t, r, id, "cancelled dial did not stop"), -32800)
		r.closeLanes()
	})
}

func TestCancellationCannotOvertakeItsCall(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		l := pausedLane(t, r, testHome)
		writing := make(chan struct{})
		release := make(chan struct{})
		cancelled := make(chan jsontext.Value, 1)
		conn := newFakeConn()
		conn.onWrite = func(ctx context.Context, msg jsonrpc.Message) error {
			req := msg.(*jsonrpc.Request)
			if req.Method == "notifications/cancelled" {
				cancelled <- req.Params
				return nil
			}
			close(writing)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		l.conn = testConnection(r, conn, l.worktree)
		queuedCall(t, r, "writing")
		go l.run()
		mustRecv(t, writing, "the call's write to begin")
		params := jsontext.Value(`{"requestId":"writing","reason":"client stopped","extra":{"keep":true}}`)
		r.route(&jsonrpc.Request{Method: "notifications/cancelled", Params: params})
		synctest.Wait()
		select {
		case <-cancelled:
			require.FailNow(t, "cancellation overtook its call's write")
		default:
		}
		close(release)
		require.Equal(t, params, mustRecv(t, cancelled, "cancellation after delivery"))
		close(l.reqs)
		mustRecv(t, l.done, "lane shutdown")
	})
}

func TestSymlinkedDirectorySharesMemoForExistingAndMissingFiles(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	alias := symlinkAt(t, linked)
	mustWriteFile(t, filepath.Join(linked, "existing.go"), "package example\n")
	r := newTestRouter(t, root)
	for _, path := range []string{filepath.Join(alias, "existing.go"), filepath.Join(alias, "missing.go"), linked} {
		got := r.worktreeOf(r.ctx, path)
		require.Equalf(t, linked, got, "worktreeOf(%q) = %q, want %q", path, got, linked)
	}
	require.Lenf(t, r.worktrees, 1, "same physical directory paid for %d git lookups", len(r.worktrees))
}

// The spelling that leaves this manager has to be the physical one: gopls
// answers a symlinked one with a short result and no error (R10).
func TestToolCallForwardsPhysicalPathSpelling(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	alias := symlinkAt(t, linked)
	// newLinkedWorktree resolves linked, so a regular file under it is already
	// spelled physically.
	physical := filepath.Join(linked, "existing.go")
	mustWriteFile(t, physical, "package example\n")
	aliased := filepath.Join(alias, "existing.go")

	tests := []struct {
		name      string
		arguments string
		want      string // "" means the message must be forwarded untouched
	}{
		{
			name:      "file argument through a symlink",
			arguments: `{"file":` + strconv.Quote(aliased) + `}`,
			want:      `{"file":` + strconv.Quote(physical) + `}`,
		},
		{
			name:      "files argument rewrites only the aliased entry",
			arguments: `{"files":[` + strconv.Quote(aliased) + `,` + strconv.Quote(physical) + `]}`,
			want:      `{"files":[` + strconv.Quote(physical) + `,` + strconv.Quote(physical) + `]}`,
		},
		{
			// Resolvable through its parent: the file an agent just asked to
			// create is named before it exists.
			name:      "missing file through a symlink",
			arguments: `{"file":` + strconv.Quote(filepath.Join(alias, "missing.go")) + `}`,
			want:      `{"file":` + strconv.Quote(filepath.Join(linked, "missing.go")) + `}`,
		},
		{
			name:      "dir argument through a symlink",
			arguments: `{"dir":` + strconv.Quote(alias) + `}`,
			want:      `{"dir":` + strconv.Quote(linked) + `}`,
		},
		// Arguments this manager does not model, and every field beside
		// arguments, must survive a rewrite verbatim.
		{
			name:      "unmodelled arguments survive",
			arguments: `{"file":` + strconv.Quote(aliased) + `,"symbol":"Map.Range"}`,
			want:      `{"file":` + strconv.Quote(physical) + `,"symbol":"Map.Range"}`,
		},
		// Nothing to substitute: a physical spelling, a path we cannot resolve
		// and so cannot verify, and a relative one that is not ours to resolve.
		{name: "already physical", arguments: `{"file":` + strconv.Quote(physical) + `}`},
		{name: "unresolvable path", arguments: `{"file":"/nonexistent/x.go"}`},
		{name: "relative path", arguments: `{"file":"internal/foo.go"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := newTestRouter(t, root)
			params := jsontext.Value(`{"name":"go_symbol_references","arguments":` + test.arguments + `}`)
			// Routing is what populates the memo this reads; canonicalToolCall
			// never waits for a filesystem syscall of its own.
			r.toolCallWorktrees(r.ctx, params)

			got, rewritten := r.canonicalToolCall(params)
			if test.want == "" {
				require.Falsef(t, rewritten, "canonicalToolCall(%s) rewrote to %s, want it forwarded untouched", test.arguments, got)
				require.Equalf(t, string(params), string(got), "canonicalToolCall returned %s, want %s", got, params)
				return
			}
			require.Truef(t, rewritten, "canonicalToolCall(%s) reported no rewrite, want the physical spelling", test.arguments)
			var want, have map[string]any
			err := json.Unmarshal([]byte(`{"name":"go_symbol_references","arguments":`+test.want+`}`), &want)
			require.NoError(t, err)
			err = json.Unmarshal(got, &have)
			require.NoError(t, err)
			require.Equalf(t, want, have, "canonicalToolCall(%s) = %s, want %s", test.arguments, got, test.want)
		})
	}
}

// The rewrite has to reach the wire, not just the helper: this is the whole
// point of R10, and the delivery path is where a guard on the method or a lost
// copy would drop it.
func TestDeliveredToolCallCarriesThePhysicalSpelling(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	alias := symlinkAt(t, linked)
	physical := filepath.Join(linked, "existing.go")
	mustWriteFile(t, physical, "package example\n")

	r := newTestRouter(t, root)
	l := pausedLane(t, r, linked)
	params := jsontext.Value(`{"name":"go_symbol_references","arguments":{"file":` +
		strconv.Quote(filepath.Join(alias, "existing.go")) + `,"symbol":"Map.Range"}}`)
	r.route(&jsonrpc.Request{ID: mustID(t, "aliased"), Method: "tools/call", Params: params})

	call := mustRecv(t, l.reqs, "the aliased call to be delivered")
	call.cancel()
	var delivered struct {
		Arguments struct {
			File   string `json:"file"`
			Symbol string `json:"symbol"`
		} `json:"arguments"`
		Name string `json:"name"`
	}
	err := json.Unmarshal(call.req.Params, &delivered)
	require.NoError(t, err)
	assert.Equalf(t, physical, delivered.Arguments.File, "delivered file = %q, want the physical spelling %q", delivered.Arguments.File, physical)
	assert.Truef(t, delivered.Name == "go_symbol_references" && delivered.Arguments.Symbol == "Map.Range", "delivered call = %s, want the name and unmodelled arguments preserved", call.req.Params)
}
