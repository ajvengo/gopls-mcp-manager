package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusMeasuresWithoutChangingRegistry(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	cmd := startFakeGopls(t, firstPort)
	r := record{Worktree: "/repo/status", PID: cmd.Process.Pid, Port: firstPort, Terminating: true}
	mustWriteMap(t, m.mapPath, []record{r})
	appendToMap(t, m.mapPath, "broken record\n")
	before, err := os.ReadFile(m.mapPath)
	require.NoError(t, err)
	var out bytes.Buffer
	err = m.status(t.Context(), &out)
	require.NoError(t, err)
	var report struct {
		RecordedServers int
		RegistryIntact  bool
		Servers         []serverUsage
	}
	err = json.Unmarshal(out.Bytes(), &report)
	require.NoError(t, err)
	require.Falsef(t, report.RecordedServers != 1 || report.RegistryIntact || len(report.Servers) != 1, "bad status: %s", &out)
	usage := report.Servers[0]
	require.Falsef(t, usage.Identity != "matched" || usage.RSSKiB == nil || *usage.RSSKiB < 0 || usage.ActiveClients != nil, "bad process accounting: %s", &out)
	after, err := os.ReadFile(m.mapPath)
	require.NoError(t, err)
	require.Truef(t, bytes.Equal(before, after), "status mutated registry")
	wantRunning(t, cmd, "status signalled the server")
}

func BenchmarkRegistryProbeLockTime(b *testing.B) {
	m := newTestManager(b)
	err := writeMap(m.mapPath, testRecords(8))
	require.NoError(b, err)
	m.alive = func(context.Context, record) probeVerdict { time.Sleep(10 * time.Millisecond); return probeLive }
	var held atomic.Int64
	m.observe = func(t lockTiming) { held.Add(int64(t.Held)) }
	b.ReportAllocs()
	for b.Loop() {
		_, err := m.withRecords(b.Context(), func(rs []record) ([]record, error) { return rs, nil })
		require.NoError(b, err)
	}
	b.ReportMetric(float64(held.Load())/float64(b.N), "lock-held-ns/op")
}

func TestSweepRetainsAProcessIgnoringTermination(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	port := deadPort(t)
	// Use a mapped port; the helper itself deliberately has no listener.
	if port < firstPort || port > lastPort {
		port = firstPort
	}
	cmd := exec.Command("sh", "-c", "trap '' TERM; echo ready; while :; do sleep 1; done", goplsBinary, "mcp", "-listen", mcpAddress(port))
	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	err = cmd.Start()
	require.NoError(t, err)
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	_, err = bufio.NewReader(out).ReadString('\n')
	require.NoError(t, err)
	r := record{Worktree: "/repo/ignores-term", PID: cmd.Process.Pid, Port: port}
	mustWriteMap(t, m.mapPath, []record{r})
	// Probe verdict is injected to keep the port independent of other tests.
	m.alive = func(context.Context, record) probeVerdict { return probeTerminate }
	err = m.list(t.Context(), io.Discard)
	require.NoError(t, err)
	r.Terminating = true
	wantRecords(t, m.mapPath, "signal lost the process record", r)
	wantRunning(t, cmd, "helper did not ignore SIGTERM")
	m.start = func(context.Context, string, int) (*childProcess, error) {
		assert.Fail(t, "spawned beside a terminating process")
		return nil, errors.New("unexpected spawn")
	}
	_, err = m.ensure(t.Context(), r.Worktree)
	require.Error(t, err, "ensure returned a terminating server")
	err = cmd.Process.Kill()
	require.NoError(t, err)
	_ = cmd.Wait()
	m.alive = recordAlive
	err = m.list(t.Context(), io.Discard)
	require.NoError(t, err)
	wantRecords(t, m.mapPath, "confirmed exit was not forgotten")
}

func TestSweepSignalsOnlyAfterPersistingTermination(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	cmd := startFakeGopls(t, firstPort)
	r := record{Worktree: "/repo/normal-exit", PID: cmd.Process.Pid, Port: firstPort}
	mustWriteMap(t, m.mapPath, []record{r})
	m.alive = func(context.Context, record) probeVerdict { return probeTerminate }
	var checked bool
	m.observe = func(lockTiming) {
		if checked {
			return
		}
		checked = true
		wantRunning(t, cmd, "process signalled before registry commit")
		r.Terminating = true
		wantRecords(t, m.mapPath, "termination was not persisted before signalling", r)
	}
	err := m.list(t.Context(), io.Discard)
	require.NoError(t, err)
	wantSignalled(t, cmd, "sweep did not send SIGTERM")
	wantRecords(t, m.mapPath, "signal alone removed record", r)
}

func TestSweepReconcilesOnlyTheProbedIdentity(t *testing.T) {
	t.Parallel()
	for _, verdict := range []probeVerdict{probeGone, probeTerminate} {
		t.Run(fmt.Sprint(verdict), func(t *testing.T) {
			t.Parallel()
			m := newTestManager(t)
			old := record{Worktree: "/repo/replaced", PID: 10, Port: firstPort}
			replacement := old
			replacement.PID++
			mustWriteMap(t, m.mapPath, []record{old})
			entered, release := make(chan struct{}), make(chan struct{})
			m.alive = func(context.Context, record) probeVerdict { close(entered); <-release; return verdict }
			done := inBackground(func() error { return m.list(t.Context(), io.Discard) })
			mustRecv(t, entered, "snapshot probe")
			// This must acquire the global lock while the probe remains blocked.
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			_, err := m.withMap(ctx, func([]record) ([]record, error) { return []record{replacement}, nil })
			close(release)
			require.NoError(t, err)
			err = mustRecv(t, done, "reconciled sweep")
			require.NoError(t, err)
			wantRecords(t, m.mapPath, "stale probe changed replacement", replacement)
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
		r.cancelCall(&jsonrpc.Request{Params: json.RawMessage(`{"requestId":"0"}`)})
		_ = wantWireError(t, wantClientError(t, r, mustID(t, "0"), "cancel did not release slot"), -32800)
		queuedCall(t, r, "released-slot")
		wantClientQuiet(t, r, "released slot was not reusable")
	})
}

func TestExecutionDeadlineCompletesOnce(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits.Execution = time.Second
		conn := newFakeConn()
		l := connectedLane(r, testHome, conn)
		id := sendCall(t, l, "expires")
		time.Sleep(time.Second)
		_ = wantWireError(t, wantClientError(t, r, id, "accepted call never expired"), -32000)
		go r.readFromUpstream(conn, testHome)
		conn.reads <- &jsonrpc.Response{ID: id, Result: json.RawMessage(`{}`)}
		synctest.Wait()
		wantClientQuiet(t, r, "late answer completed an expired call twice")
		require.Lenf(t, r.awaitingUpstream, 0, "expired request retained")
	})
}

func TestExecutionTimeoutDoesNotParkCallbacksBehindOutput(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits.Execution = time.Second
		for range cap(r.out) {
			r.out <- &jsonrpc.Response{}
		}
		id := mustID(t, "timeout")
		r.track(id, newFakeConn(), testHome)
		r.startExecution(id, r.awaitingUpstream[id].state)
		time.Sleep(time.Second)
		err := mustRecv(t, r.errs, "session failure instead of blocked timer")
		require.Error(t, err, "missing output failure")
		synctest.Wait()
		require.Lenf(t, r.awaitingUpstream, 0, "expired call still retained")
	})
}

func TestRoutingBudgetDoesNotMultiplyOrGrowWorkers(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.sticky = testWorktree
		blocked := make(chan struct{})
		var calls atomic.Int64
		r.resolver = &pathResolver{jobs: make(chan resolution), lookup: func(context.Context, json.RawMessage) []string {
			calls.Add(1)
			<-blocked // models a filesystem syscall ignoring cancellation
			return []string{"/repo/late"}
		}}
		go r.resolver.run(t.Context())
		for range 2 {
			start := time.Now()
			_, err := r.target(&jsonrpc.Request{Method: "tools/call", Params: json.RawMessage(`{"arguments":{"file":"/uncached/file.go"}}`)})
			require.ErrorIsf(t, err, context.DeadlineExceeded, "routing returned %v", err)
			require.Truef(t, time.Since(start) == routingBudget, "routing multiplied its budget")
		}
		require.Equalf(t, int64(1), calls.Load(), "stalled resolution grew more workers")
		close(blocked)
		synctest.Wait()
		require.Equalf(t, testWorktree, r.sticky, "expired routing changed sticky state")
	})
}
