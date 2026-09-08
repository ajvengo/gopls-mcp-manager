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

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

func TestStatusMeasuresWithoutChangingRegistry(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	cmd := startFakeGopls(t, firstPort)
	r := record{Worktree: "/repo/status", PID: cmd.Process.Pid, Port: firstPort, Terminating: true}
	mustWriteMap(t, m.mapPath, []record{r})
	appendToMap(t, m.mapPath, "broken record\n")
	before, err := os.ReadFile(m.mapPath)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := m.status(t.Context(), &out); err != nil {
		t.Fatal(err)
	}
	var report struct {
		RecordedServers int
		RegistryIntact  bool
		Servers         []serverUsage
	}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.RecordedServers != 1 || report.RegistryIntact || len(report.Servers) != 1 {
		t.Fatalf("bad status: %s", &out)
	}
	usage := report.Servers[0]
	if usage.Identity != "matched" || usage.RSSKiB == nil || *usage.RSSKiB < 0 || usage.ActiveClients != nil {
		t.Fatalf("bad process accounting: %s", &out)
	}
	after, err := os.ReadFile(m.mapPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("status mutated registry")
	}
	wantRunning(t, cmd, "status signalled the server")
}

func TestCallLimitConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{
		{"GOPLS_MANAGER_EXECUTION_TIMEOUT", "0s", true},
		{"GOPLS_MANAGER_EXECUTION_TIMEOUT", "2m", true},
		{"GOPLS_MANAGER_EXECUTION_TIMEOUT", "-1s", false},
		{"GOPLS_MANAGER_MAX_OUTSTANDING", "0", false},
		{"GOPLS_MANAGER_MAX_OUTSTANDING_PER_LANE", "12", true},
	} {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			_, err := limitsFromEnv()
			if (err == nil) != tc.valid {
				t.Fatalf("configuration error = %v", err)
			}
		})
	}
}

func BenchmarkRegistryProbeLockTime(b *testing.B) {
	m := newTestManager(b)
	if err := writeMap(m.mapPath, testRecords(8)); err != nil {
		b.Fatal(err)
	}
	m.alive = func(context.Context, record) probeVerdict { time.Sleep(10 * time.Millisecond); return probeLive }
	var held atomic.Int64
	m.observe = func(t lockTiming) { held.Add(int64(t.Held)) }
	b.ReportAllocs()
	for b.Loop() {
		if _, err := m.withRecords(b.Context(), func(rs []record) ([]record, error) { return rs, nil }); err != nil {
			b.Fatal(err)
		}
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
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	if _, err := bufio.NewReader(out).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	r := record{Worktree: "/repo/ignores-term", PID: cmd.Process.Pid, Port: port}
	mustWriteMap(t, m.mapPath, []record{r})
	// Probe verdict is injected to keep the port independent of other tests.
	m.alive = func(context.Context, record) probeVerdict { return probeTerminate }
	if err := m.list(t.Context(), io.Discard); err != nil {
		t.Fatal(err)
	}
	r.Terminating = true
	wantRecords(t, m.mapPath, "signal lost the process record", r)
	wantRunning(t, cmd, "helper did not ignore SIGTERM")
	m.start = func(context.Context, string, int) (*childProcess, error) {
		t.Error("spawned beside a terminating process")
		return nil, errors.New("unexpected spawn")
	}
	if _, err := m.ensure(t.Context(), r.Worktree); err == nil {
		t.Fatal("ensure returned a terminating server")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	m.alive = recordAlive
	if err := m.list(t.Context(), io.Discard); err != nil {
		t.Fatal(err)
	}
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
	if err := m.list(t.Context(), io.Discard); err != nil {
		t.Fatal(err)
	}
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
			if err != nil {
				t.Fatal(err)
			}
			if err := mustRecv(t, done, "reconciled sweep"); err != nil {
				t.Fatal(err)
			}
			wantRecords(t, m.mapPath, "stale probe changed replacement", replacement)
		})
	}
}

func TestOutstandingLimitsSurviveFastDelivery(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits = callLimits{PerLane: 2, Session: 3}
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
		if len(r.awaitingUpstream) != 0 {
			t.Fatal("expired request retained")
		}
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
		r.startExecution(id)
		time.Sleep(time.Second)
		if err := mustRecv(t, r.errs, "session failure instead of blocked timer"); err == nil {
			t.Fatal("missing output failure")
		}
		synctest.Wait()
		if len(r.awaitingUpstream) != 0 {
			t.Fatal("expired call still retained")
		}
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
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("routing returned %v", err)
			}
			if time.Since(start) != routingBudget {
				t.Fatal("routing multiplied its budget")
			}
		}
		if calls.Load() != 1 {
			t.Fatal("stalled resolution grew more workers")
		}
		close(blocked)
		synctest.Wait()
		if r.sticky != testWorktree {
			t.Fatal("expired routing changed sticky state")
		}
	})
}
