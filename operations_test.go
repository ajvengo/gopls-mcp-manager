package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/transport"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
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
		if delivered != 2 || usage.Outstanding != 0 || usage.UpstreamOperations != 2 || usage.AbandonedOperations != 2 {
			t.Fatalf("unresolved work escaped bound: writes=%d usage=%+v", delivered, usage)
		}
		go r.readFromUpstream(conn, testHome)
		conn.reads <- &jsonrpc.Response{ID: mustID(t, "0"), Result: json.RawMessage(`{}`)}
		synctest.Wait()
		wantClientQuiet(t, r, "late response completed cancellation twice")
		_ = sendCall(t, l, "after-result")
		if delivered != 3 || r.requestUsage().UpstreamOperations != 2 {
			t.Fatal("terminal result did not release exactly one credit")
		}
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
			t.Error("replacement bypassed operation cap")
			return nil
		}
		id = sendCall(t, connectedLane(r, testHome, replacement), "replacement")
		_ = wantClientError(t, r, id, "operation cap did not reject replacement")
		if usage := r.requestUsage(); usage.UpstreamOperations != 1 || usage.AbandonedOperations != 1 {
			t.Fatalf("disconnect lost accounting: %+v", usage)
		}
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
		if control.conn != conn || control.req.Method != "notifications/cancelled" {
			t.Fatal("timeout cancellation has wrong owner")
		}
		if usage := r.requestUsage(); usage.UpstreamOperations != 1 || usage.AbandonedOperations != 1 {
			t.Fatalf("timeout released unconfirmed work: %+v", usage)
		}
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
		r.cancelCall(&jsonrpc.Request{Params: json.RawMessage(`{"requestId":"same"}`)})
		_ = wantClientError(t, r, id, "cancel did not complete")
		_ = sendCall(t, l, "same")
		_ = wantClientError(t, r, id, "unresolved id was reused")
		if _, claimed := r.finish(id, conn, nil); claimed {
			t.Fatal("old response claimed a new client")
		}
		_ = sendCall(t, l, "same")
		if usage := r.requestUsage(); usage.UpstreamOperations != 1 || usage.AbandonedOperations != 0 {
			t.Fatalf("resolved id could not be reused: %+v", usage)
		}
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
						if _, ok := r.finish(id, conn, nil); !ok {
							t.Fatal("early response had no owner")
						}
					} else {
						r.cancelCall(&jsonrpc.Request{Params: json.RawMessage(`{"requestId":"reused"}`)})
						_ = wantClientError(t, r, id, "cancelled call did not complete")
					}
					if _, err := r.admit(id, nil); err != nil {
						t.Fatal(err)
					}
					replacement = r.awaitingUpstream[id].state
					return tc.writeErr
				}
				if _, err := r.admit(id, nil); err != nil {
					t.Fatal(err)
				}
				connectedLane(r, testHome, conn).send(t.Context(), &jsonrpc.Request{ID: id, Method: "tools/call"}, r.awaitingUpstream[id].state)
				owner, ok := r.awaitingUpstream[id]
				if !ok || owner.state != replacement || owner.conn != nil || owner.state.timer != nil {
					t.Fatalf("old write touched replacement: %+v", owner)
				}
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
		r.cancelCall(&jsonrpc.Request{Params: json.RawMessage(`{"requestId":"reused"}`)})
		_ = wantClientError(t, r, id, "queued cancellation did not complete")
		replacement, _ := r.prepare(req)
		defer replacement.cancel()
		conn := newFakeConn()
		conn.onWrite = func(context.Context, jsonrpc.Message) error { t.Error("cancelled queue entry delivered"); return nil }
		connectedLane(r, testHome, conn).send(old.ctx, old.req, old.state)
		owner, ok := r.awaitingUpstream[id]
		if !ok || owner.state != replacement.state || replacement.ctx.Err() != nil || owner.conn != nil {
			t.Fatalf("stale queue entry touched new owner: %+v", owner)
		}
		wantClientQuiet(t, r, "stale queue entry failed new request")
	})
}
