package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type callLimits struct {
	PerLane, Session int
	Execution        time.Duration
}

func defaultCallLimits() callLimits { return callLimits{PerLane: 128, Session: 1024} }

// The state pointer is a generation token. An expired timer cannot complete a
// later request that happens to use the same wire id.
type callState struct{ timer *time.Timer }

type requestUsage struct {
	Event                                            string
	Admitted, Completed, Rejected, Peak, Outstanding int
}

func (r *router) requestUsage() requestUsage {
	r.mu.Lock()
	defer r.mu.Unlock()
	usage := r.usage
	usage.Event = "session_requests"
	usage.Outstanding = len(r.awaitingUpstream)
	return usage
}

func (r *router) admit(id jsonrpc.ID, worktree string, cancel func()) error {
	if !id.IsValid() {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.awaitingUpstream[id]; exists {
		r.usage.Rejected++
		return fmt.Errorf("request id %v is already outstanding", id)
	}
	if len(r.awaitingUpstream) >= r.limits.Session {
		r.usage.Rejected++
		return fmt.Errorf("session outstanding request limit reached")
	}
	count := 0
	for _, owner := range r.awaitingUpstream {
		if owner.worktree == worktree {
			count++
		}
	}
	if count >= r.limits.PerLane {
		r.usage.Rejected++
		return fmt.Errorf("gopls for %s reached its outstanding request limit", worktree)
	}
	r.awaitingUpstream[id] = owed{worktree: worktree, cancel: cancel, state: &callState{}}
	r.usage.Admitted++
	r.usage.Peak = max(r.usage.Peak, len(r.awaitingUpstream))
	return nil
}

// All terminal paths remove state here while holding mu. Queued delivery is
// cancelled before unlocking so a lane cannot revive it. Network output always
// happens after unlocking, so a slow client cannot hold the tracker.
func (r *router) removeLocked(id jsonrpc.ID, owner owed) {
	if owner.conn == nil && owner.cancel != nil {
		owner.cancel()
	}
	if owner.state != nil && owner.state.timer != nil {
		owner.state.timer.Stop()
	}
	delete(r.awaitingUpstream, id)
	r.usage.Completed++
}

func (r *router) place(id jsonrpc.ID, conn mcp.Connection, cancel context.CancelFunc, placed <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if owner, ok := r.awaitingUpstream[id]; ok {
		owner.conn, owner.cancel, owner.placed = conn, cancel, placed
		r.awaitingUpstream[id] = owner
	}
}

func (r *router) finish(id jsonrpc.ID, conn mcp.Connection, state *callState) (owed, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.awaitingUpstream[id]
	if !ok || (conn != nil && owner.conn != conn) || (state != nil && owner.state != state) {
		return owed{}, false
	}
	r.removeLocked(id, owner)
	return owner, true
}

func (r *router) fail(id jsonrpc.ID, code int64, format string, args ...any) {
	if _, ok := r.finish(id, nil, nil); ok {
		r.forward(errorResponse(id, code, format, args...))
	}
}

func (r *router) retry(id jsonrpc.ID, conn mcp.Connection) bool {
	if !id.IsValid() {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.awaitingUpstream[id]
	if !ok || owner.conn != conn {
		return false
	}
	owner.conn = nil
	r.awaitingUpstream[id] = owner
	return true
}

func (r *router) startExecution(id jsonrpc.ID) {
	if r.limits.Execution <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.awaitingUpstream[id]
	if !ok {
		return
	}
	state := owner.state
	state.timer = time.AfterFunc(r.limits.Execution, func() {
		if _, ok := r.finish(id, nil, state); ok {
			// Timer callbacks must not accumulate behind a client that stopped
			// reading. If the bounded output cannot accept a terminal answer,
			// fail the session instead of leaving an unbounded blocked callback.
			select {
			case r.out <- errorResponse(id, -32000, "execution deadline exceeded; upstream work may still be running"):
			case <-r.ctx.Done():
			default:
				select {
				case r.errs <- fmt.Errorf("client output queue is full during execution timeout"):
				default:
				}
			}
		}
	})
}

func (r *router) clearRequests() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, owner := range r.awaitingUpstream {
		r.removeLocked(id, owner)
		if owner.cancel != nil {
			owner.cancel()
		}
	}
}

// owed is a request in flight: the worktree it was routed to, and the upstream
// that has taken it, once one has.
//
// The worktree is known strictly earlier than the connection — route picks a
// destination before any lane has one to name — so conn is nil for a call still
// queued. A cancellation arriving in that window still has to find where its
// call went (R7), which is why one record covers both stages rather than the
// route appearing only once an upstream owes an answer.
//
// conn is compared by connection, never by worktree: send() replaces a dead
// connection with a live one under the same worktree, and matching by name
// would let the dead one's reader fail calls the reconnect had already placed
// successfully.
type owed struct {
	conn     mcp.Connection
	worktree string
	cancel   context.CancelFunc // stops queued/dialling delivery, never the shared server
	placed   <-chan struct{}    // closes when the write returns, before cancellation is forwarded
	state    *callState
}

func (r *router) cancelCall(req *jsonrpc.Request) {
	var params mcp.CancelledParams
	if json.Unmarshal(req.Params, &params) != nil {
		return
	}
	id, err := jsonrpc.MakeID(params.RequestID)
	if err != nil {
		return
	}
	owner, ok := r.finish(id, nil, nil)
	if !ok {
		return
	}
	if owner.conn == nil && owner.cancel != nil {
		owner.cancel()
	}
	r.forward(errorResponse(id, -32800, "request cancelled"))
	if owner.conn == nil {
		return
	}
	// Notifications are advisory and have no response id. If this separate
	// bounded queue is full, drop the notification rather than block routing.
	if l := r.lanes[owner.worktree]; l != nil {
		select {
		case l.controls <- cancellation{req: req, conn: owner.conn, placed: owner.placed, deadline: time.Now().Add(sendBudget)}:
		default:
		}
	}
}

// refuse rejects a request before admission. It never removes tracker state:
// an invalid duplicate id must not delete the original outstanding request.
// Admitted failures use fail, which competes for terminal ownership.
func (r *router) refuse(req *jsonrpc.Request, code int64, format string, args ...any) {
	if !req.ID.IsValid() {
		return
	}
	r.forward(errorResponse(req.ID, code, format, args...))
}

// failInFlight answers every request still owed by conn with an error. The
// calls in flight when a gopls dies get no retry — nothing else will ever
// produce their ids — so left alone they strand a client that has no timeout.
//
// One worktree names them all: a connection belongs to one lane, and a lane to
// one worktree, so every id matched here was tracked under the caller's own.
func (r *router) failInFlight(conn mcp.Connection, worktree string, cause error) {
	// A slice, not a map: nothing looks these up, they are collected under the
	// lock and then walked once.
	var stranded []jsonrpc.ID
	r.mu.Lock()
	for id, owner := range r.awaitingUpstream {
		if owner.conn == conn {
			stranded = append(stranded, id)
			// The claim and the answer, same as refuse makes them, but once for
			// the whole set: taking the id here is what says this caller answers
			// it, and leaving it would route a later cancellation to a lane that
			// has stopped waiting. Nothing can put it back before the forward
			// below — a retry only re-tracks an id whose claim it won, and the
			// claim is this delete.
			r.removeLocked(id, owner)
		}
	}
	r.mu.Unlock()

	for _, id := range stranded {
		r.forward(errorResponse(id, jsonrpc.CodeInternalError, "gopls for %s went away mid-call: %v", worktree, cause))
	}
}
