package main

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestShutdownReleasesPendingMirror(t *testing.T) {
	t.Parallel()
	m := &manager{pending: newPendingCalls()}
	r := newRouter(t.Context(), m, testHome)
	conn := newFakeConn()
	id := mustID(t, "lost")
	state, err := r.admit(id, func() {})
	require.NoError(t, err)
	require.NoError(t, r.beginOperation(id, state, conn, testHome, func() {}, nil))
	r.failInFlight(conn, testHome, io.EOF)
	require.Len(t, m.pending.ops, 1, "uncertain work remains until router shutdown")
	r.clearRequests()
	require.Empty(t, m.pending.ops, "shutdown must release the manager's connection references")
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

func TestRootsPlanningHonoursBudget(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		entered, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		r.plan = func(string, []string) watchPlan {
			close(entered)
			<-release
			return watchPlan{}
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		done := make(chan error, 1)
		id := mustID(t, "roots")
		go func() { done <- r.answerRoots(ctx, newFakeConn(), testHome, id) }()
		<-entered
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		default:
			require.FailNow(t, "roots planning outlived its caller's budget")
		}
	})
}

func TestExpiredRootsPlanCannotRememberSnapshot(t *testing.T) {
	bubble(t, func(t *testing.T) {
		m := &manager{watch: &watchPlans{plans: make(map[string]watchSnapshot)}}
		r := newRouter(t.Context(), m, testHome)
		ctx, cancel := context.WithCancel(t.Context())
		r.plan = func(string, []string) watchPlan {
			cancel()
			return watchPlan{partial: map[string]partialEntries{testHome: {}}}
		}
		conn := &identifiedConn{Connection: newFakeConn(), id: serverID{pid: 1, started: 1}}
		err := r.answerRoots(ctx, conn, testHome, mustID(t, "roots"))
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, m.watch.plans, "expired planning published an obsolete server snapshot")
	})
}

func TestRootsWorkersStayBoundedAcrossTimeouts(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		release := make(chan struct{})
		defer close(release)
		var calls atomic.Int64
		r.plan = func(string, []string) watchPlan {
			calls.Add(1)
			<-release
			return watchPlan{}
		}
		id := mustID(t, "roots")
		for range 20 {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			err := r.answerRoots(ctx, newFakeConn(), testHome, id)
			cancel()
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
		require.Equal(t, int64(1), calls.Load(), "timeouts must not create replacement filesystem workers")
	})
}

func TestLateRootsPlanCannotReplaceNewerGeneration(t *testing.T) {
	bubble(t, func(t *testing.T) {
		m := &manager{watch: &watchPlans{plans: make(map[string]watchSnapshot)}}
		r := newRouter(t.Context(), m, testHome)
		entered, release := make(chan struct{}), make(chan struct{})
		r.plan = func(string, []string) watchPlan {
			close(entered)
			<-release
			return watchPlan{roots: []string{testHome}}
		}
		old, current := serverID{pid: 1, started: 1}, serverID{pid: 2, started: 2}
		conn := &identifiedConn{Connection: newFakeConn(), id: old}
		done := make(chan error, 1)
		id := mustID(t, "roots")
		go func() { done <- r.answerRoots(t.Context(), conn, testHome, id) }()
		<-entered
		time.Sleep(time.Nanosecond)
		m.rememberWatch(testHome, current, watchPlan{started: time.Now(), roots: []string{testHome}})
		close(release)
		require.NoError(t, <-done)
		require.Equal(t, current, m.watch.plans[testHome].server)
	})
}

// Simulate messages already decoded when Close cancels the connection, followed
// by EOF. Reader cleanup will park forwarding a failure to the full output.
type bufferedRootsConn struct {
	mcp.Connection
	messages []jsonrpc.Message
}

func (c *bufferedRootsConn) Read(context.Context) (jsonrpc.Message, error) {
	if len(c.messages) == 0 {
		return nil, io.EOF
	}
	msg := c.messages[0]
	c.messages = c.messages[1:]
	return msg, nil
}

func TestClosedConnectionCannotStartObsoleteRootsPlan(t *testing.T) {
	bubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r := newRouter(ctx, nil, testHome)
		var planned atomic.Int64
		r.plan = func(string, []string) watchPlan { planned.Add(1); return watchPlan{} }
		for range cap(r.out) {
			r.out <- &jsonrpc.Request{Method: "notification"}
		}
		connCtx, closeConn := context.WithCancel(ctx)
		conn := &boundedConn{Connection: &bufferedRootsConn{
			Connection: newFakeConn(),
			messages:   []jsonrpc.Message{&jsonrpc.Request{ID: mustID(t, "roots"), Method: "roots/list"}},
		}, release: closeConn, ctx: connCtx}
		r.track(mustID(t, "unanswered"), conn, testHome)
		require.NoError(t, conn.Close())
		done := make(chan struct{})
		go func() { defer close(done); r.readFromUpstream(conn, testHome) }()
		synctest.Wait()
		require.Zero(t, planned.Load(), "queued asks must observe Close before reader cleanup finishes")
		cancel()
		<-done
	})
}

func TestClosedConnectionReleasesBackpressuredReader(t *testing.T) {
	bubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r := newRouter(ctx, nil, testHome)
		for range cap(r.out) {
			r.out <- &jsonrpc.Request{Method: "notification"}
		}
		connCtx, closeConn := context.WithCancel(ctx)
		conn := &boundedConn{Connection: newFakeConn(), release: closeConn, ctx: connCtx}
		id := mustID(t, "completed")
		state, err := r.admit(id, func() {})
		require.NoError(t, err)
		require.NoError(t, r.beginOperation(id, state, conn, testHome, func() {}, nil))
		done := make(chan struct{})
		go func() { defer close(done); r.readFromUpstream(conn, testHome) }()
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

func TestFullOutputQueueReleasesTerminalProducers(t *testing.T) {
	for _, terminal := range []string{"upstream failure", "delivery failure", "cancellation"} {
		t.Run(terminal, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				r := newRouter(ctx, nil, testHome)
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
						r.failInFlight(conn, testHome, io.EOF)
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

// Serial: replace only the Go stderr variable, restoring it after the output
// worker has returned. Real pipe writes stay outside synctest bubbles.
func TestHTTPErrorCancelsBeforeDiagnosticWrite(t *testing.T) {
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	oldStderr := os.Stderr
	os.Stderr = writer
	defer func() { os.Stderr = oldStderr }()
	defer func() { _ = reader.Close(); _ = writer.Close() }()
	b := newHTTPBackend(t.Context(), nil, testHome)
	b.start()
	// This exceeds pipe capacity, making the diagnostic block until we close
	// the pipe. Backend cancellation must precede that write.
	b.r.errs <- errors.New(strings.Repeat("diagnostic", 128<<10))
	var first [1]byte
	_, err = reader.Read(first[:]) // diagnostic has captured os.Stderr
	require.NoError(t, err)
	cancelled := false
	select {
	case <-b.r.ctx.Done():
		cancelled = true
	case <-time.After(time.Second):
	}
	// Drain to completion before restoring the process-global variable.
	drained := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, reader); close(drained) }()
	b.close()
	require.NoError(t, writer.Close())
	<-drained
	require.True(t, cancelled, "blocked diagnostics delayed cancellation after terminal output loss")
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
	go func() { done <- serve(t.Context(), m, testHome, deadConn()) }()
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
		r := newRouter(ctx, &manager{limits: config.Limits{Metrics: true}}, testHome)
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
		go func() { done <- serve(ctx, nil, testHome, client) }()
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
	require.Empty(t, moduleIgnoreFilters(root), "oversize metadata should contribute no filters")
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Symlink("/dev/zero", path))
	_, err := readModuleFile(path)
	require.Error(t, err, "device input must be rejected before reading")
	require.NoError(t, os.Remove(path))
	target := filepath.Join(root, "regular.mod")
	mustWriteFile(t, target, "module example.com/a\nignore ./generated\n")
	require.NoError(t, os.Symlink(target, path))
	require.Equal(t, []string{"-generated"}, moduleIgnoreFilters(root), "regular-file symlinks remain supported")
}

func FuzzDirectoryFilterPrecedence(f *testing.F) {
	f.Add("-,+cmd,-cmd/generated,+cmd/generated/public", "cmd/generated/public/nested")
	f.Add("-**/vendor,+internal/**/vendor", "internal/a/vendor")
	f.Add("-**, +cmd", "cmd/internal")
	f.Add("-docs,+docs/go", "docs/go")
	f.Add("-**/node_modules", "a/node_modules")
	f.Fuzz(func(t *testing.T, raw, rel string) {
		if len(raw) > 4096 || len(rel) > 1024 {
			t.Skip()
		}
		rules, err := config.ParseDirectoryFilters(raw)
		if err != nil {
			return
		}
		_ = newDirFilter(rules).worthListing(rel)
		require.True(t, pathIncludeFunc(append(rules, "+"))(rel), "last global include must win")
		require.False(t, pathIncludeFunc(append(rules, "-"))(rel), "last global exclude must win")
	})
}

func TestModuleIgnoreFiltersRejectInvalidUTF8(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "go.mod"), "module example.com/a\nignore \"\\x80\"\n")
	require.NotPanics(t, func() { _ = planWatch(root, nil) })
}

func TestDirectoryFilterCompilationRejectsUnusableRules(t *testing.T) {
	t.Parallel()
	for _, rule := range []string{"", "+\x80", "+" + strings.Repeat("**/", 2000)} {
		require.NotPanics(t, func() {
			filter := newDirFilter([]string{"-blocked", rule})
			require.False(t, filter.include("blocked"))
		})
	}
	_, err := config.ParseDirectoryFilters("+" + strings.Repeat("a", config.MaxDirectoryFilterBytes))
	require.Error(t, err)
}

func TestProbeDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var reached atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
	}))
	defer target.Close()
	listener, port := listenInAllocationRange(t, t.Name())
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, target.URL, http.StatusTemporaryRedirect)
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()
	alive, conclusive := endpointProbe(t.Context(), port)
	require.Zero(t, reached.Load(), "readiness probe left its recorded endpoint")
	require.False(t, alive)
	require.False(t, conclusive, "a redirect is not proof that the shared process is gone")
}

func TestCleanupLockFilesBoundedAcrossGenerations(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	for i := range 100 {
		require.NoError(t, m.signalTerminating(t.Context(), record{PID: i + 1, Port: firstPort, Terminating: true}))
	}
	locks, err := filepath.Glob(m.mapPath + ".cleanup-*.lock")
	require.NoError(t, err)
	require.Len(t, locks, 1, "historical process generations accumulated lock inodes")
}

func TestFailedUsageCommitPreservesConcurrentCounts(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions")
	}
	m := newTestManager(t)
	m.uses = &useCounts{counts: make(map[string]int)}
	r := record{Worktree: "/used", PID: 1, Port: firstPort}
	mustWriteMap(t, m.mapPath, []record{r})
	require.NoError(t, withFileLock(t.Context(), m.mapPath, func() error { return nil }))
	m.used(r.Worktree)
	dir := filepath.Dir(m.mapPath)
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(dir, 0o700)) })
	// The observation runs after the failed registry commit and before the
	// caller returns. A rollback must merge, not replace, newer deliveries.
	m.observe = func(lockTiming) { m.used(r.Worktree) }
	_, err := m.evictionVictims(context.Background())
	require.Error(t, err, "regression requires a failed persistent write")
	require.Equal(t, 2, m.uses.counts[r.Worktree], "failed commit lost delivered usage")
}

func TestProcessIdentityRejectsSubstringMatches(t *testing.T) {
	t.Parallel()
	for _, command := range []string{
		"gopls mcp -listen 127.0.0.1:611001",
		"not-gopls mcp -listen 127.0.0.1:61100",
		"editor --log=/tmp/gopls --server=127.0.0.1:61100",
		"gopls other-command 127.0.0.1:61100",
	} {
		require.False(t, matchesGopls(command, firstPort), "unrelated process matched: %s", command)
	}
	for _, command := range []string{"gopls mcp -listen 127.0.0.1:61100", "/usr/local/bin/gopls mcp -listen 127.0.0.1:61100"} {
		require.True(t, matchesGopls(command, firstPort))
	}
}

func TestProcessFixtureCleanupReapsItsChild(t *testing.T) {
	t.Parallel()
	var pid int
	t.Run("fixture", func(t *testing.T) {
		pid = startFakeGopls(t, firstPort).Process.Pid
	})
	require.Eventually(t, func() bool {
		return syscall.Kill(pid, 0) == syscall.ESRCH
	}, time.Second, time.Millisecond, "fixture left a child process or zombie behind")
}
