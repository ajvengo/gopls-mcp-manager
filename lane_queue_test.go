package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

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
		r.route(&jsonrpc.Request{Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":"cancelled"}`)})
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
		r.route(&jsonrpc.Request{Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":"sent"}`)})
		if _, err := recvRequest(upstream.writes, "notifications/cancelled"); err != nil {
			t.Fatal(err)
		}
		wantClientQuiet(t, r, "advisory cancellation produced a response")
	})
}

func TestFullControlQueueDoesNotBlockRouting(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		l := pausedLane(t, r, testHome)
		r.track(mustID(t, "sent"), newFakeConn(), testHome)
		for range laneQueue + 1 {
			r.route(&jsonrpc.Request{Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":"sent"}`)})
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
		r.dial = func(ctx context.Context, _ string) (mcp.Connection, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		id := queuedCall(t, r, "dialling")
		synctest.Wait()
		r.route(&jsonrpc.Request{Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":"dialling"}`)})
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
		cancelled := make(chan struct{}, 1)
		conn := newFakeConn()
		conn.onWrite = func(ctx context.Context, msg jsonrpc.Message) error {
			req := msg.(*jsonrpc.Request)
			if req.Method == "notifications/cancelled" {
				cancelled <- struct{}{}
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
		l.conn = conn
		queuedCall(t, r, "writing")
		go l.run()
		mustRecv(t, writing, "the call's write to begin")
		r.route(&jsonrpc.Request{Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":"writing"}`)})
		synctest.Wait()
		select {
		case <-cancelled:
			t.Fatal("cancellation overtook its call's write")
		default:
		}
		close(release)
		mustRecv(t, cancelled, "cancellation after delivery")
		close(l.reqs)
		mustRecv(t, l.done, "lane shutdown")
	})
}

func TestSymlinkedDirectorySharesMemoForExistingAndMissingFiles(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(linked, alias); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(linked, "existing.go"), "package example\n")
	r := newTestRouter(t, root)
	for _, path := range []string{filepath.Join(alias, "existing.go"), filepath.Join(alias, "missing.go"), linked} {
		if got := r.worktreeOf(path); got != linked {
			t.Fatalf("worktreeOf(%q) = %q, want %q", path, got, linked)
		}
	}
	if len(r.worktrees) != 1 {
		t.Fatalf("same physical directory paid for %d git lookups", len(r.worktrees))
	}
}
