package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

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
					go r.readFromUpstream(observed, testHome)
					conn.reads <- &jsonrpc.Request{ID: mustID(t, "server"), Method: method}
					if stall {
						time.Sleep(sendBudget)
					}
					_ = wantWireError(t, wantClientError(t, r, id, "server reply stranded other calls"), jsonrpc.CodeInternalError)
					if !observed.closed.Load() {
						t.Fatal("failed connection was not closed")
					}
					if r.requestUsage().Outstanding != 0 {
						t.Fatal("failed connection retained callers")
					}
					wantClientQuiet(t, r, "failure completed twice")
				})
			})
		}
	}
}

func TestAcquisitionDoesNotProbeUnrelatedRecords(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	records := testRecords(3)
	records[1].Terminating = true
	mustWriteMap(t, m.mapPath, records)
	var probes atomic.Int64
	m.alive = func(_ context.Context, r record) probeVerdict {
		probes.Add(1)
		if r != records[0] {
			t.Error("acquisition probed an unrelated worktree")
		}
		return probeLive
	}
	port, err := m.ensure(t.Context(), records[0].Worktree)
	if err != nil || port != records[0].Port || probes.Load() != 1 {
		t.Fatalf("ensure = %d, %v; probes %d", port, err, probes.Load())
	}
	wantRecords(t, m.mapPath, "acquisition changed unrelated records", records...)
	probes.Store(0)
	m.alive = func(context.Context, record) probeVerdict { probes.Add(1); return probeLive }
	if err := m.list(t.Context(), io.Discard); err != nil {
		t.Fatal(err)
	}
	if probes.Load() != int64(len(records)) {
		t.Fatal("explicit sweep omitted records")
	}
}

func TestIngressCancellationBypassesBlockedResolution(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		r.sticky = testWorktree
		entered, release := make(chan struct{}), make(chan struct{})
		r.resolver = &pathResolver{jobs: make(chan resolution), lookup: func(context.Context, json.RawMessage) []string {
			close(entered)
			<-release
			return []string{"/late/worktree"}
		}}
		go r.resolver.run(t.Context())
		r.dial = func(context.Context, string) (mcp.Connection, error) { return loopbackConn(), nil }
		client := startClient(t, r, 4)
		id := mustID(t, "resolving")
		client <- &jsonrpc.Request{ID: id, Method: "tools/call", Params: json.RawMessage(`{"arguments":{"file":"/cold/file.go"}}`)}
		mustRecv(t, entered, "filesystem lookup")
		start := time.Now()
		client <- &jsonrpc.Request{ID: mustID(t, "pathless"), Method: "tools/call"}
		client <- &jsonrpc.Request{Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":"resolving"}`)}
		_ = wantWireError(t, wantClientError(t, r, id, "cancellation waited for filesystem"), -32800)
		response := mustRecv(t, r.out, "pathless call after cancellation")
		if resp, ok := response.(*jsonrpc.Response); !ok || resp.ID != mustID(t, "pathless") || resp.Error != nil {
			t.Fatalf("pathless call failed: %#v", response)
		}
		if time.Since(start) != 0 {
			t.Fatal("cancellation or pathless call waited for routing timeout")
		}
		close(release)
		synctest.Wait()
		if r.sticky != testWorktree {
			t.Fatal("cancelled resolution committed late sticky state")
		}
		wantClientQuiet(t, r, "late lookup completed twice")
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
		if len(r.lanes) != 1 || len(r.awaitingUpstream) != 0 || len(r.perWorktree) != 0 {
			t.Fatal("rejected lane retained resources")
		}
		r.sticky = testHome
		queuedCall(t, r, "existing-lane")
		wantClientQuiet(t, r, "retention cap refused an existing lane")
	})
}

func TestFullIngressQueueStillAcceptsCancellation(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		l := pausedLane(t, r, testHome)
		entered, release := make(chan struct{}), make(chan struct{})
		r.resolver = &pathResolver{jobs: make(chan resolution), lookup: func(context.Context, json.RawMessage) []string {
			close(entered)
			<-release
			return []string{testHome}
		}}
		go r.resolver.run(t.Context())
		client := startClient(t, r, laneQueue+4)
		client <- &jsonrpc.Request{ID: mustID(t, "resolving"), Method: "tools/call", Params: fileCallParams("/uncached/file.go")}
		mustRecv(t, entered, "blocked resolution")
		for i := range laneQueue {
			client <- &jsonrpc.Request{ID: mustID(t, fmt.Sprint(i)), Method: "tools/call"}
		}
		client <- &jsonrpc.Request{ID: mustID(t, "overflow"), Method: "tools/call"}
		client <- &jsonrpc.Request{Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":"0"}`)}
		start := time.Now()
		_ = wantWireError(t, wantClientError(t, r, mustID(t, "overflow"), "full ingress blocked reader"), -32000)
		_ = wantWireError(t, wantClientError(t, r, mustID(t, "0"), "full ingress blocked cancellation"), -32800)
		if time.Since(start) != 0 {
			t.Fatal("cancellation waited for resolution timeout")
		}
		client <- &jsonrpc.Request{Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":"resolving"}`)}
		_ = wantClientError(t, r, mustID(t, "resolving"), "resolving call not cancelled")
		close(release)
		synctest.Wait()
		if len(l.reqs) != laneQueue-1 {
			t.Fatalf("delivered %d queued calls", len(l.reqs))
		}
		if r.requestUsage().Rejections["routing_queue"] != 1 {
			t.Fatal("routing overload not counted")
		}
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
		if err := newLane(r, testHome).handshake(ctx, conn); err == nil {
			t.Fatal("stalled roots reply succeeded")
		}
		if time.Since(start) != time.Second {
			t.Fatal("server reply extended handshake deadline")
		}
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
		if len(r.lanes) != 1 {
			t.Fatal("session rejection created a new lane")
		}
	})
}

func TestMemoCapacityRevalidatesEvictedPaths(t *testing.T) {
	t.Parallel()
	r := newTestRouter(t, testHome)
	r.limits.CacheEntries = 2
	for _, cache := range []map[string]string{r.paths, r.worktrees} {
		for i := range 100 {
			r.memoize(cache, fmt.Sprintf("/%d", i), testHome)
		}
		if len(cache) > 2 {
			t.Fatal("memo exceeded capacity")
		}
		if _, ok := cache["/0"]; ok {
			t.Fatal("old memo survived capacity rollover")
		}
	}
	r.resolver = &pathResolver{jobs: make(chan resolution), lookup: func(context.Context, json.RawMessage) []string {
		return []string{testWorktree}
	}}
	go r.resolver.run(t.Context())
	got, err := r.target(&jsonrpc.Request{Method: "tools/call", Params: fileCallParams("/0")})
	if err != nil || got != testWorktree {
		t.Fatalf("evicted path not resolved afresh: %s, %v", got, err)
	}
}

func TestStatusSnapshotKeepsIdentityPerRecord(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	process := startFakeGopls(t, firstPort)
	records := []record{
		{Worktree: "/matched", Port: firstPort, PID: process.Process.Pid},
		{Worktree: "/wrong-port", Port: firstPort + 1, PID: process.Process.Pid},
		{Worktree: "/missing", Port: firstPort + 2, PID: 2147483647},
	}
	mustWriteMap(t, m.mapPath, records)
	var output bytes.Buffer
	if err := m.status(t.Context(), &output); err != nil {
		t.Fatal(err)
	}
	var report struct{ Servers []serverUsage }
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Servers) != 3 {
		t.Fatalf("status: %s", &output)
	}
	for i, identity := range []string{"matched", "different process", "unknown"} {
		if report.Servers[i].Identity != identity || (report.Servers[i].RSSKiB != nil) != (i == 0) {
			t.Fatalf("row %d: %+v", i, report.Servers[i])
		}
	}
	wantRecords(t, m.mapPath, "status changed records", records...)
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
		r.finish(mustID(t, "reply"), conn, nil)
		r.cancelCall(&jsonrpc.Request{Params: json.RawMessage(`{"requestId":"cancel"}`)})
		_ = wantClientError(t, r, mustID(t, "cancel"), "cancel not completed")
		r.failInFlight(conn, testHome, io.EOF)
		_ = mustRecv(t, r.out, "disconnect result")
		_ = mustRecv(t, r.out, "disconnect result")
		queuedCall(t, r, "clear-again")
		r.clearRequests()
		if len(r.perWorktree) != 0 || r.requestUsage().Admitted != r.requestUsage().Completed {
			t.Fatal("terminal paths leaked accounting")
		}
		r.rejected("delivery_queue")
		before := r.requestUsage()
		r.rejected("delivery_queue")
		if before.Rejections["delivery_queue"] != 1 {
			t.Fatal("snapshot shares mutable counters")
		}
	})
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
				if err := r.admit(id, testHome, nil); err != nil {
					b.Fatal(err)
				}
				r.finish(id, nil, nil)
			}
		})
	}
}

func BenchmarkAcquisitionVersusSweep(b *testing.B) {
	for _, whole := range []bool{false, true} {
		b.Run(fmt.Sprintf("whole=%v", whole), func(b *testing.B) {
			m := newTestManager(b)
			records := testRecords(8)
			if err := writeMap(m.mapPath, records); err != nil {
				b.Fatal(err)
			}
			m.alive = func(ctx context.Context, r record) probeVerdict {
				if r != records[0] {
					_ = waitContext(ctx, 10*time.Millisecond)
				}
				return probeLive
			}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				if whole {
					err = m.list(b.Context(), io.Discard)
				} else {
					_, err = m.ensure(b.Context(), records[0].Worktree)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Keep the expected set explicit: this benchmark measures production
// acquisition/reconciliation, unlike the old cleanRecords-only probe fixture.
func TestNewRetentionSettings(t *testing.T) {
	for _, name := range []string{"GOPLS_MANAGER_MAX_LANES", "GOPLS_MANAGER_MAX_CACHE_ENTRIES"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "2")
			limits, err := limitsFromEnv()
			if err != nil || !slices.Contains([]int{limits.Lanes, limits.CacheEntries}, 2) {
				t.Fatalf("limits: %+v, %v", limits, err)
			}
			t.Setenv(name, "0")
			if _, err := limitsFromEnv(); err == nil {
				t.Fatal("zero retention limit accepted")
			}
		})
	}
}
