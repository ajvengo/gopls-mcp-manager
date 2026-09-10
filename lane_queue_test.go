package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
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
			r.route(&jsonrpc.Request{Method: "notifications/cancelled", Params: json.RawMessage(fmt.Sprintf(`{"requestId":%q}`, id))})
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

// gopls answers a symlinked spelling of a file inside its view from whatever
// single package it can reach, and reports the short result as if it were
// complete — measured on v0.23.0: three references for a /tmp spelling where
// the physical spelling of the same file in the same session gives seven. So
// the spelling that leaves this manager has to be the physical one (R10).
func TestToolCallForwardsPhysicalPathSpelling(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(linked, alias); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(linked, "existing.go"), "package example\n")
	aliased := filepath.Join(alias, "existing.go")
	physical := filepath.Join(linked, "existing.go")
	if resolved, err := filepath.EvalSymlinks(physical); err == nil {
		physical = resolved // the test's own root may sit under a symlink too
	}

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
			params := json.RawMessage(`{"name":"go_symbol_references","arguments":` + test.arguments + `}`)
			// Routing is what populates the memo this reads; canonicalToolCall
			// never waits for a filesystem syscall of its own.
			r.toolCallWorktrees(params)

			got, rewritten := r.canonicalToolCall(params)
			if test.want == "" {
				if rewritten {
					t.Fatalf("canonicalToolCall(%s) rewrote to %s, want it forwarded untouched", test.arguments, got)
				}
				if string(got) != string(params) {
					t.Fatalf("canonicalToolCall returned %s, want %s", got, params)
				}
				return
			}
			if !rewritten {
				t.Fatalf("canonicalToolCall(%s) reported no rewrite, want the physical spelling", test.arguments)
			}
			var want, have map[string]any
			if err := json.Unmarshal([]byte(`{"name":"go_symbol_references","arguments":`+test.want+`}`), &want); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(got, &have); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(have, want) {
				t.Fatalf("canonicalToolCall(%s) = %s, want %s", test.arguments, got, test.want)
			}
		})
	}
}

// The rewrite has to reach the wire, not just the helper: this is the whole
// point of R10, and the delivery path is where a guard on the method or a lost
// copy would drop it.
func TestDeliveredToolCallCarriesThePhysicalSpelling(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(linked, alias); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(linked, "existing.go"), "package example\n")
	physical, err := filepath.EvalSymlinks(filepath.Join(linked, "existing.go"))
	if err != nil {
		t.Fatal(err)
	}

	r := newTestRouter(t, root)
	l := pausedLane(t, r, linked)
	params := json.RawMessage(`{"name":"go_symbol_references","arguments":{"file":` +
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
	if err := json.Unmarshal(call.req.Params, &delivered); err != nil {
		t.Fatal(err)
	}
	if delivered.Arguments.File != physical {
		t.Errorf("delivered file = %q, want the physical spelling %q", delivered.Arguments.File, physical)
	}
	if delivered.Name != "go_symbol_references" || delivered.Arguments.Symbol != "Map.Range" {
		t.Errorf("delivered call = %s, want the name and unmodelled arguments preserved", call.req.Params)
	}
}
