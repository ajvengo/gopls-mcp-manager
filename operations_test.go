package main

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/transport"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		go r.readFromUpstream(conn, testHome)
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
		r.failInFlight(conn, testHome, io.EOF)
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
		require.Truef(t, control.conn == conn && control.req.Method == "notifications/cancelled", "timeout cancellation has wrong owner")
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
		_, claimed := r.finish(id, conn, nil)
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
						_, ok := r.finish(id, conn, nil)
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
		_, claimed := r.finish(id, conn, nil)
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
		r.finish(id, conn, nil)
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
