package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
					require.True(t, observed.closed.Load(), "failed connection was not closed")
					require.Zero(t, r.requestUsage().Outstanding, "failed connection retained callers")
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
		assert.Equalf(t, records[0], r, "acquisition probed an unrelated worktree")
		return probeLive
	}
	port, err := m.ensure(t.Context(), records[0].Worktree)
	require.Falsef(t, err != nil || port != records[0].Port || probes.Load() != 1, "ensure = %d, %v; probes %d", port, err, probes.Load())
	wantRecords(t, m.mapPath, "acquisition changed unrelated records", records...)
	probes.Store(0)
	m.alive = func(context.Context, record) probeVerdict { probes.Add(1); return probeLive }
	err = m.list(t.Context(), io.Discard)
	require.NoError(t, err)
	require.Equal(t, int64(len(records)), probes.Load(), "explicit sweep omitted records")
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
		resp, ok := response.(*jsonrpc.Response)
		require.Falsef(t, !ok || resp.ID != mustID(t, "pathless") || resp.Error != nil, "pathless call failed: %#v", response)
		require.Zerof(t, time.Since(start), "cancellation or pathless call waited for routing timeout")
		close(release)
		synctest.Wait()
		require.Equal(t, testWorktree, r.sticky, "cancelled resolution committed late sticky state")
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
		require.Falsef(t, len(r.lanes) != 1 || len(r.awaitingUpstream) != 0 || len(r.perWorktree) != 0, "rejected lane retained resources")
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
		require.Zerof(t, time.Since(start), "cancellation waited for resolution timeout")
		client <- &jsonrpc.Request{Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":"resolving"}`)}
		_ = wantClientError(t, r, mustID(t, "resolving"), "resolving call not cancelled")
		close(release)
		synctest.Wait()
		require.Lenf(t, l.reqs, laneQueue-1, "delivered %d queued calls", len(l.reqs))
		require.Equal(t, 1, r.requestUsage().Rejections["routing_queue"], "routing overload not counted")
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
		err := newLane(r, testHome).handshake(ctx, conn)
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
	r.resolver = &pathResolver{jobs: make(chan resolution), lookup: func(context.Context, json.RawMessage) []string {
		return []string{testWorktree}
	}}
	go r.resolver.run(t.Context())
	got, err := r.target(&jsonrpc.Request{Method: "tools/call", Params: fileCallParams("/0")})
	require.Falsef(t, err != nil || got != testWorktree, "evicted path not resolved afresh: %s, %v", got, err)
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
	m.openFiles = func(context.Context, []record) (map[int]int, error) {
		return map[int]int{process.Process.Pid: 7}, nil
	}
	var output bytes.Buffer
	err := m.status(t.Context(), &output)
	require.NoError(t, err)
	var report struct {
		OpenFiles int
		Servers   []serverUsage
	}
	err = json.Unmarshal(output.Bytes(), &report)
	require.NoError(t, err)
	require.Lenf(t, report.Servers, 3, "status: %s", &output)
	for i, identity := range []string{"matched", "different process", "unknown"} {
		row := report.Servers[i]
		require.Falsef(t, row.Identity != identity || (row.RSSKiB != nil) != (i == 0) || (row.OpenFiles != nil) != (i == 0),
			"row %d: %+v", i, row)
	}
	require.Equalf(t, 7, report.OpenFiles, "open files = %d, want only the matched row's 7", report.OpenFiles)
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
		require.Falsef(t, len(r.perWorktree) != 0 || r.requestUsage().Admitted != r.requestUsage().Completed, "terminal paths leaked accounting")
		r.rejected("delivery_queue")
		before := r.requestUsage()
		r.rejected("delivery_queue")
		require.Equal(t, 1, before.Rejections["delivery_queue"], "snapshot shares mutable counters")
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
				_, err := r.admit(id, nil)
				require.NoError(b, err)
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
			err := writeMap(m.mapPath, records)
			require.NoError(b, err)
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
				require.NoError(b, err)
			}
		})
	}
}
