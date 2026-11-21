package main

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditResourceCancelledLockWaitReleasesDescriptors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "registry")
		err := withFileLock(t.Context(), path, func() error {
			var lock syscall.Stat_t
			require.NoError(t, syscall.Stat(path+".lock", &lock))
			for range 64 {
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				done := make(chan error, 1)
				go func() {
					done <- withFileLock(ctx, path, func() error { return errors.New("contended lock unexpectedly acquired") })
				}()
				synctest.Wait()
				require.Equal(t, 2, lockDescriptorCount(t, lock), "holder and waiter must each have the lock file open")
				cancel()
				require.ErrorIs(t, mustRecv(t, done, "cancelled lock wait to return"), context.Canceled)
			}
			require.Equal(t, 1, lockDescriptorCount(t, lock), "cancelled lock waits retained descriptors")
			return nil
		})
		require.NoError(t, err)
	})
}

func lockDescriptorCount(t *testing.T, lock syscall.Stat_t) int {
	t.Helper()
	entries, err := os.ReadDir("/dev/fd")
	require.NoError(t, err)
	count := 0
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		var opened syscall.Stat_t
		if syscall.Fstat(fd, &opened) == nil && opened.Dev == lock.Dev && opened.Ino == lock.Ino {
			count++
		}
	}
	return count
}

func TestCancellationNumericIDDoesNotAliasAnotherRequest(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		id        float64
		wire      string
		cancelled bool
	}{
		{"fractional positive", 1, `1.1`, false},
		{"fractional negative", -1, `-1.1`, false},
		{"fractional zero", 0, `0.1`, false},
		{"upper overflow", -0x1p63, `9223372036854775808`, false},
		{"lower overflow", -0x1p63, `-18446744073709551616`, false},
		{"integer", 1, `1`, true},
		{"integer decimal", 1, `1.0`, true},
		{"integer exponent", 1, `1e0`, true},
		{"lowest integer", -0x1p63, `-9223372036854775808`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := newTestRouter(t, testHome)
			id := mustID(t, test.id)
			r.track(id, nil, testWorktree)
			req := &jsonrpc.Request{Method: "notifications/cancelled", Params: jsontext.Value(`{"requestId":` + test.wire + `}`)}
			if test.cancelled {
				require.Equal(t, testWorktree, r.cancelTarget(req.Params))
			} else {
				require.Empty(t, r.cancelTarget(req.Params))
			}
			r.cancelCall(req)
			usage := r.requestUsage()
			if test.cancelled {
				require.Zero(t, usage.Outstanding)
				require.Equal(t, 1, usage.Cancelled)
			} else {
				require.Equal(t, 1, usage.Outstanding)
				require.Zero(t, usage.Cancelled)
			}
		})
	}
}

// A retry places the call on a fresh connection, and its cancellation route has
// to come back with it. claim() ends the failed attempt by dropping the id's
// record whole, so a track() that restored only the ownership would leave the
// client's cancellation routed home, which is the failure
// TestCancellationFollowsTheRequestToItsUpstream describes.
func TestCancellationAfterUncertainWriteCannotReachANewConnection(t *testing.T) {
	bubble(t, func(t *testing.T) {
		fresh := newHandshakingConn()
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		// Never routed, only sent: the route under test is the one track() puts
		// back, not the one route() recorded on the way in.
		l := connectedLane(r, testWorktree, failingConn())
		dials := 0
		r.dial = func(context.Context, string, int) (mcp.Connection, error) { dials++; return fresh, nil }

		id := sendCall(t, l, "call-retried")
		response := mustRecv(t, r.out, "uncertain write failure").(*jsonrpc.Response)
		require.ErrorContains(t, response.Error, "may have executed")

		// Encoded from the type the router decodes into, rather than spelled by
		// hand: a test that splices the id into JSON itself also assumes it is a
		// string, and would keep passing if the field were renamed under it.
		params, err := json.Marshal(mcp.CancelledParams{RequestID: id.Raw()})
		require.NoError(t, err)
		r.cancelCall(&jsonrpc.Request{Method: "notifications/cancelled", Params: params})
		require.Zero(t, dials, "cancellation/redelivery opened a fresh connection after an uncertain write")
		require.Empty(t, fresh.writes)
		require.Equal(t, 1, r.requestUsage().AbandonedOperations)
	})
}

func TestIngressCancellationBypassesBlockedResolution(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		r.sticky = testWorktree
		entered := make(chan struct{})
		r.lookupCold = func(ctx context.Context, _ jsontext.Value) []string {
			close(entered)
			<-ctx.Done()
			return []string{"/late/worktree"}
		}
		r.dial = func(context.Context, string, int) (mcp.Connection, error) { return loopbackConn(), nil }
		client := startClient(t, r, 4)
		id := mustID(t, "resolving")
		client <- &jsonrpc.Request{ID: id, Method: "tools/call", Params: jsontext.Value(`{"arguments":{"file":"/cold/file.go"}}`)}
		mustRecv(t, entered, "filesystem lookup")
		start := time.Now()
		client <- &jsonrpc.Request{ID: mustID(t, "pathless"), Method: "tools/call"}
		client <- &jsonrpc.Request{Method: "notifications/cancelled", Params: jsontext.Value(`{"requestId":"resolving"}`)}
		_ = wantWireError(t, wantClientError(t, r, id, "cancellation waited for filesystem"), -32800)
		response := mustRecv(t, r.out, "pathless call after cancellation")
		resp, ok := response.(*jsonrpc.Response)
		require.Falsef(t, !ok || resp.ID != mustID(t, "pathless") || resp.Error != nil, "pathless call failed: %#v", response)
		require.Zerof(t, time.Since(start), "cancellation or pathless call waited for routing timeout")
		synctest.Wait()
		require.Equal(t, testWorktree, r.sticky, "cancelled resolution committed late sticky state")
		wantClientQuiet(t, r, "late lookup completed twice")
	})
}

func TestFullIngressQueueStillAcceptsCancellation(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		l := pausedLane(t, r, testHome)
		entered := make(chan struct{})
		r.lookupCold = func(ctx context.Context, _ jsontext.Value) []string {
			close(entered)
			<-ctx.Done()
			return []string{testHome}
		}
		client := startClient(t, r, laneQueue+4)
		client <- &jsonrpc.Request{ID: mustID(t, "resolving"), Method: "tools/call", Params: fileCallParams("/uncached/file.go")}
		mustRecv(t, entered, "blocked resolution")
		for i := range laneQueue {
			client <- &jsonrpc.Request{ID: mustID(t, fmt.Sprint(i)), Method: "tools/call"}
		}
		client <- &jsonrpc.Request{ID: mustID(t, "overflow"), Method: "tools/call"}
		client <- &jsonrpc.Request{Method: "notifications/cancelled", Params: jsontext.Value(`{"requestId":"0"}`)}
		start := time.Now()
		_ = wantWireError(t, wantClientError(t, r, mustID(t, "overflow"), "full ingress blocked reader"), -32000)
		_ = wantWireError(t, wantClientError(t, r, mustID(t, "0"), "full ingress blocked cancellation"), -32800)
		require.Zerof(t, time.Since(start), "cancellation waited for resolution timeout")
		client <- &jsonrpc.Request{Method: "notifications/cancelled", Params: jsontext.Value(`{"requestId":"resolving"}`)}
		_ = wantClientError(t, r, mustID(t, "resolving"), "resolving call not cancelled")
		synctest.Wait()
		require.Lenf(t, l.reqs, laneQueue-1, "delivered %d queued calls", len(l.reqs))
		require.Equal(t, 1, r.requestUsage().Rejections["routing_queue"], "routing overload not counted")
	})
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

func TestAbandonedOperationsBoundFurtherDelivery(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits.PerLane = 2
		conn := newFakeConn()
		var delivered int
		conn.onWrite = func(context.Context, jsonrpc.Message) error { delivered++; return nil }
		l := connectedLane(r, testHome, conn)
		for i := range 1000 {
			id := sendCall(t, l, fmt.Sprint(i))
			if i < 2 {
				params, _ := json.Marshal(map[string]any{"requestId": id.Raw()})
				r.cancelCall(&jsonrpc.Request{Params: params})
			}
			_ = wantClientError(t, r, id, "call did not finish locally")
		}
		usage := r.requestUsage()
		require.Truef(t, delivered == 2 && usage.Outstanding == 0 && usage.UpstreamOperations == 2 && usage.AbandonedOperations == 2, "unresolved work escaped bound: writes=%d usage=%+v", delivered, usage)
		go r.readFromUpstream(testConnection(r, conn, testHome), testHome)
		conn.reads <- &jsonrpc.Response{ID: mustID(t, "0"), Result: jsontext.Value(`{}`)}
		synctest.Wait()
		wantClientQuiet(t, r, "late response completed cancellation twice")
		_ = sendCall(t, l, "after-result")
		require.Truef(t, delivered == 3 && r.requestUsage().UpstreamOperations == 2, "terminal result did not release exactly one credit")
	})
}

func TestDisconnectedOperationsDoNotReleaseCredits(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits.PerLane = 1
		conn := newFakeConn()
		conn.onWrite = func(context.Context, jsonrpc.Message) error { return nil }
		id := sendCall(t, connectedLane(r, testHome, conn), "lost")
		r.failInFlight(testConnection(r, conn, testHome), testHome, io.EOF)
		_ = wantClientError(t, r, id, "disconnect did not fail client")
		replacement := newFakeConn()
		replacement.onWrite = func(context.Context, jsonrpc.Message) error {
			assert.Fail(t, "replacement bypassed operation cap")
			return nil
		}
		id = sendCall(t, connectedLane(r, testHome, replacement), "replacement")
		_ = wantClientError(t, r, id, "operation cap did not reject replacement")
		usage := r.requestUsage()
		require.Truef(t, usage.UpstreamOperations == 1 && usage.AbandonedOperations == 1, "disconnect lost accounting: %+v", usage)
	})
}

func TestExecutionTimeoutQueuesAdvisoryCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits.Execution = time.Second
		conn := newFakeConn()
		l := connectedLane(r, testHome, conn)
		id := sendCall(t, l, "timeout")
		time.Sleep(time.Second)
		_ = wantClientError(t, r, id, "execution did not time out")
		control := mustRecv(t, l.controls, "timeout did not queue cancellation")
		require.Truef(t, control.conn == testConnection(r, conn, testHome) && control.req.Method == "notifications/cancelled", "timeout cancellation has wrong owner")
		usage := r.requestUsage()
		require.Truef(t, usage.UpstreamOperations == 1 && usage.AbandonedOperations == 1, "timeout released unconfirmed work: %+v", usage)
	})
}

func TestAbandonedIDCannotBeReusedOnItsConnection(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		conn := newFakeConn()
		conn.onWrite = func(context.Context, jsonrpc.Message) error { return nil }
		l := connectedLane(r, testHome, conn)
		id := sendCall(t, l, "same")
		r.cancelCall(&jsonrpc.Request{Params: jsontext.Value(`{"requestId":"same"}`)})
		_ = wantClientError(t, r, id, "cancel did not complete")
		_ = sendCall(t, l, "same")
		_ = wantClientError(t, r, id, "unresolved id was reused")
		_, claimed := r.finish(id, testConnection(r, conn, r.home), nil)
		require.False(t, claimed, "old response claimed a new client")
		_ = sendCall(t, l, "same")
		usage := r.requestUsage()
		require.Truef(t, usage.UpstreamOperations == 1 && usage.AbandonedOperations == 0, "resolved id could not be reused: %+v", usage)
	})
}

func TestDeliveryCompletionCannotTouchReusedID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		writeErr error
		response bool
	}{
		{"cancel then successful write", nil, false},
		{"cancel then uncertain write", io.ErrUnexpectedEOF, false},
		{"cancel then rejected size", transport.ErrMessageTooLarge, false},
		{"response before write returns", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				r := newTestRouter(t, testHome)
				r.limits.Execution = time.Second
				id := mustID(t, "reused")
				conn := newFakeConn()
				var replacement *callState
				conn.onWrite = func(context.Context, jsonrpc.Message) error {
					if tc.response {
						_, ok := r.finish(id, testConnection(r, conn, r.home), nil)
						require.True(t, ok, "early response had no owner")
					} else {
						r.cancelCall(&jsonrpc.Request{Params: jsontext.Value(`{"requestId":"reused"}`)})
						_ = wantClientError(t, r, id, "cancelled call did not complete")
					}
					_, err := r.admit(id, nil)
					require.NoError(t, err)
					replacement = r.awaitingUpstream[id].state
					return tc.writeErr
				}
				_, err := r.admit(id, nil)
				require.NoError(t, err)
				connectedLane(r, testHome, conn).send(t.Context(), &jsonrpc.Request{ID: id, Method: "tools/call"}, r.awaitingUpstream[id].state)
				owner, ok := r.awaitingUpstream[id]
				require.Truef(t, ok && owner.state == replacement && owner.conn == nil && owner.state.timer == nil, "old write touched replacement: %+v", owner)
				wantClientQuiet(t, r, "old write completed replacement")
			})
		})
	}
}

func TestCancelledQueueEntryCannotClaimReusedID(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		id := mustID(t, "reused")
		req := &jsonrpc.Request{ID: id, Method: "tools/call"}
		old, _ := r.prepare(req)
		r.cancelCall(&jsonrpc.Request{Params: jsontext.Value(`{"requestId":"reused"}`)})
		_ = wantClientError(t, r, id, "queued cancellation did not complete")
		replacement, _ := r.prepare(req)
		defer replacement.cancel()
		conn := newFakeConn()
		conn.onWrite = func(context.Context, jsonrpc.Message) error {
			assert.Fail(t, "cancelled queue entry delivered")
			return nil
		}
		connectedLane(r, testHome, conn).send(old.ctx, old.req, old.state)
		owner, ok := r.awaitingUpstream[id]
		require.Truef(t, ok && owner.state == replacement.state && replacement.ctx.Err() == nil && owner.conn == nil, "stale queue entry touched new owner: %+v", owner)
		wantClientQuiet(t, r, "stale queue entry failed new request")
	})
}

func TestDelayedCancellationCannotCancelReusedID(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		conn := newFakeConn()
		var cancellations int
		conn.onWrite = func(_ context.Context, msg jsonrpc.Message) error {
			if req, ok := msg.(*jsonrpc.Request); ok && req.Method == "notifications/cancelled" {
				cancellations++
			}
			return nil
		}
		l := connectedLane(r, testHome, conn)
		id := sendCall(t, l, "reused")
		r.cancelCall(&jsonrpc.Request{Method: "notifications/cancelled", Params: jsontext.Value(`{"requestId":"reused"}`)})
		_ = wantClientError(t, r, id, "cancelled request did not complete")
		_, claimed := r.finish(id, testConnection(r, conn, r.home), nil)
		require.False(t, claimed, "late response completed the cancelled request twice")
		_ = sendCall(t, l, "reused")
		go l.runControls()
		synctest.Wait()
		require.Zero(t, cancellations, "old advisory cancellation reached the reused upstream id")
		require.Equal(t, 1, r.requestUsage().Outstanding)
	})
}

func TestCancellationWriteFencesIDReuse(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		conn := newFakeConn()
		entered, release := make(chan struct{}), make(chan struct{})
		var writes int
		conn.onWrite = func(_ context.Context, msg jsonrpc.Message) error {
			if req := msg.(*jsonrpc.Request); req.Method == "notifications/cancelled" {
				close(entered)
				<-release
			} else {
				writes++
			}
			return nil
		}
		l := connectedLane(r, testHome, conn)
		id := sendCall(t, l, "same")
		r.cancelCall(&jsonrpc.Request{Method: "notifications/cancelled", Params: jsontext.Value(`{"requestId":"same"}`)})
		_ = wantClientError(t, r, id, "cancelled call did not finish")
		go l.runControls()
		<-entered
		r.finish(id, testConnection(r, conn, r.home), nil)
		r.track(id, nil, testHome)
		done := make(chan struct{})
		go func() { defer close(done); l.send(t.Context(), &jsonrpc.Request{ID: id, Method: "tools/call"}, nil) }()
		synctest.Wait()
		assert.Equal(t, 1, writes, "new id use overtook the old cancellation write")
		close(release)
		<-done
		require.Equal(t, 2, writes)
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
		go r.readFromUpstream(testConnection(r, conn, testHome), testHome)
		conn.reads <- &jsonrpc.Response{ID: id, Result: jsontext.Value(`{}`)}
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
