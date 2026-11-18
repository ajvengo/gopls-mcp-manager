package main

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"maps"
	"math"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The state pointer is a generation token. An expired timer cannot complete a
// later request that happens to use the same wire id.
type callState struct {
	timer    *time.Timer
	identity *operationIdentity
}

type requestUsage struct {
	Event                                                                 string
	Admitted, Completed, Rejected, Peak, Outstanding                      int
	Rejections                                                            map[string]int
	Stages                                                                map[string]stageTiming
	Lanes, Paths, Directories, Cancelled, ExecutionExpired, RoutingFailed int
	MemoHits, MemoMisses, MemoExpirations, MemoRollovers                  int64
	UpstreamOperations, AbandonedOperations                               int
	SSEBufferBytes, PeakSSEBufferBytes                                    int
}

type stageTiming struct{ Count, TotalNS, MaxNS int64 }

func (r *router) now() time.Time {
	if !r.limits.Metrics {
		return time.Time{}
	}
	return time.Now()
}

// A zero start skips the metrics lock when collection is disabled.
func (r *router) timed(stage string, start time.Time) {
	if !r.limits.Metrics || start.IsZero() {
		return
	}
	elapsed := time.Since(start).Nanoseconds()
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.usage.Stages[stage]
	t.Count++
	t.TotalNS += elapsed
	t.MaxNS = max(t.MaxNS, elapsed)
	r.usage.Stages[stage] = t
}

func (r *router) requestUsage() requestUsage {
	r.mu.Lock()
	defer r.mu.Unlock()

	usage := r.usage
	usage.Event = "session_requests"
	usage.Outstanding = len(r.awaitingUpstream)
	usage.SSEBufferBytes, usage.PeakSSEBufferBytes = r.sseBudget.Snapshot()
	usage.UpstreamOperations, usage.AbandonedOperations = r.owner.sessionUsage(r.remote)

	usage.Rejections = maps.Clone(r.usage.Rejections)
	usage.Stages = maps.Clone(r.usage.Stages)
	r.lanesMu.Lock()
	usage.Lanes = len(r.lanes)
	r.lanesMu.Unlock()
	r.memoMu.Lock()
	r.expireMemosLocked()
	usage.Paths, usage.Directories = len(r.paths), len(r.worktrees)
	usage.MemoHits, usage.MemoMisses = r.memo.hits, r.memo.misses
	usage.MemoExpirations, usage.MemoRollovers = r.memo.expirations, r.memo.rollovers
	r.memoMu.Unlock()
	return usage
}

// admit tracks requests before routing. bindWorktree charges the lane later.
func (r *router) admit(id jsonrpc.ID, cancel func()) (*callState, error) {
	if !validOwnerID(id) {
		return nil, fmt.Errorf("opaque request id exceeds %d bytes", maxOwnerIDBytes)
	}
	if !id.IsValid() {
		state := &callState{identity: &operationIdentity{}}
		return state, r.owner.admit(state)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.awaitingUpstream[id]; exists {
		r.rejectedLocked("duplicate_id")
		return nil, fmt.Errorf("request id %v is already outstanding", id)
	}
	if len(r.awaitingUpstream) >= r.limits.Session {
		r.rejectedLocked("session_outstanding")
		return nil, fmt.Errorf("session outstanding request limit reached")
	}
	state := &callState{identity: &operationIdentity{}}
	if err := r.owner.admit(state); err != nil {
		r.rejectedLocked("owner_outstanding")
		return nil, err
	}
	r.awaitingUpstream[id] = owed{cancel: cancel, state: state}
	r.usage.Admitted++
	r.usage.Peak = max(r.usage.Peak, len(r.awaitingUpstream))
	return state, nil
}

// All terminal paths remove state here while holding mu. Queued delivery is
// cancelled before unlocking so a lane cannot revive it. Network output always
// happens after unlocking, so a slow client cannot hold the tracker.
func (r *router) removeLocked(id jsonrpc.ID, owner owed) {
	r.owner.releaseQueued(owner.state)
	r.owner.abandon(operationKey{owner.conn, id})
	if owner.conn == nil && owner.cancel != nil {
		owner.cancel()
	}
	if owner.state != nil && owner.state.timer != nil {
		owner.state.timer.Stop()
	}
	delete(r.awaitingUpstream, id)
	if owner.worktree != "" {
		release(r.perWorktree, owner.worktree)
	}
	r.usage.Completed++
}

// Drop empty entries so worktree churn cannot retain an unbounded index.
func release(counts map[string]int, key string) {
	counts[key]--
	if counts[key] <= 0 {
		delete(counts, key)
	}
}

// ownerLocked checks the admission token to fence reused IDs. Client
// cancellation passes nil to select the current admission. Caller holds mu.
func (r *router) ownerLocked(id jsonrpc.ID, state *callState) (owed, bool) {
	owner, ok := r.awaitingUpstream[id]
	if !ok || (state != nil && owner.state != state) {
		return owed{}, false
	}
	return owner, true
}

func (r *router) finish(id jsonrpc.ID, conn mcp.Connection, state *callState) (owed, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if conn != nil {
		r.owner.answered(operationKey{conn, id})
	}
	owner, ok := r.ownerLocked(id, state)
	if !ok || (conn != nil && owner.conn != conn) {
		return owed{}, false
	}
	r.removeLocked(id, owner)
	return owner, true
}

func (r *router) startExecution(id jsonrpc.ID, state *callState) {
	if r.limits.Execution <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.ownerLocked(id, state); !ok {
		return
	}
	state.timer = time.AfterFunc(r.limits.Execution, func() {
		if owner, ok := r.finish(id, nil, state); ok {
			r.queueCancellation(owner, id, cancellationRequest(id, "execution deadline exceeded"))
			r.mu.Lock()
			r.usage.ExecutionExpired++
			r.mu.Unlock()
			// Timer callbacks must not accumulate behind a client that stopped
			// reading. If the bounded output cannot accept a terminal answer,
			// fail the session instead of leaving an unbounded blocked callback.
			r.forward(errorResponse(id, -32000, "execution deadline exceeded; upstream work may still be running"))
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
	r.owner.abandonSession(r.remote)
}

// owed tracks one local admission from ingress through delivery. Routing fills
// in the worktree before delivery assigns conn. The token fences ID reuse, and
// exact connection identity fences replies from a replaced upstream.
type owed struct {
	conn     mcp.Connection
	worktree string
	cancel   context.CancelFunc // stops queued/dialling delivery, never the shared server
	placed   <-chan struct{}    // closes when the write returns, before cancellation is forwarded
	state    *callState
}

func (r *router) rejected(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rejectedLocked(reason)
}

func (r *router) rejectedLocked(reason string) {
	r.usage.Rejected++
	r.usage.Rejections[reason]++
}

// Cancel here so untracked notifications release their delivery context too.
func (r *router) failIngress(call delivery, code int64, message string) {
	defer call.cancel()
	defer releaseMessage(call.req)
	if !r.failDelivery(call.req.ID, call.state, code, message) {
		return
	}
	r.mu.Lock()
	r.usage.RoutingFailed++
	r.mu.Unlock()
}

// Transfer accounting and cancellation atomically after ordered resolution.
func (r *router) bindWorktree(call delivery, worktree string, cancel context.CancelFunc) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.awaitingUpstream[call.req.ID]
	if call.ctx.Err() != nil || (call.req.ID.IsValid() && (!ok || owner.state != call.state)) {
		return context.Canceled
	}
	r.lanesMu.Lock()
	_, exists := r.lanes[worktree]
	full := r.limits.Lanes > 0 && len(r.lanes) >= r.limits.Lanes
	r.lanesMu.Unlock()
	if !exists && full {
		r.rejectedLocked("lanes")
		return fmt.Errorf("session retained lane limit reached")
	}
	if !call.req.ID.IsValid() {
		return r.owner.bind(call.state, worktree)
	}
	if r.perWorktree[worktree] >= r.limits.PerLane {
		r.rejectedLocked("lane_outstanding")
		return fmt.Errorf("gopls for %s reached its outstanding request limit", worktree)
	}
	owner.worktree, owner.cancel = worktree, cancel
	if err := r.owner.bind(call.state, worktree); err != nil {
		return err
	}
	r.awaitingUpstream[call.req.ID] = owner
	r.perWorktree[worktree]++
	return nil
}

// Reject numbers that MakeID would truncate into a different request ID.
func cancelledID(params jsontext.Value) (jsonrpc.ID, bool) {
	var cancelled mcp.CancelledParams
	if json.Unmarshal(params, &cancelled) != nil {
		return jsonrpc.ID{}, false
	}
	if number, ok := cancelled.RequestID.(float64); ok && (math.Trunc(number) != number || number < -0x1p63 || number >= 0x1p63) {
		return jsonrpc.ID{}, false
	}
	id, err := jsonrpc.MakeID(cancelled.RequestID)
	return id, err == nil
}

func (r *router) cancelCall(req *jsonrpc.Request) {
	id, named := cancelledID(req.Params)
	if !named {
		return
	}
	if owner, ok := r.cancelRequest(id); ok {
		r.queueCancellation(owner, id, req)
	}
}

func (r *router) cancelRequest(id jsonrpc.ID) (owed, bool) {
	owner, ok := r.finish(id, nil, nil)
	if !ok {
		return owed{}, false
	}
	r.mu.Lock()
	r.usage.Cancelled++
	r.mu.Unlock()
	r.forward(errorResponse(id, -32800, "request cancelled"))
	return owner, true
}

func cancellationRequest(id jsonrpc.ID, reason string) *jsonrpc.Request {
	params, _ := json.Marshal(mcp.CancelledParams{RequestID: id.Raw(), Reason: reason})
	return &jsonrpc.Request{Method: "notifications/cancelled", Params: params}
}

func (r *router) queueCancellation(owner owed, id jsonrpc.ID, req *jsonrpc.Request) {
	if owner.conn == nil {
		return
	}
	// Notifications are advisory and have no response id. If this separate
	// bounded queue is full, drop the notification rather than block routing.
	r.lanesMu.Lock()
	l := r.lanes[owner.worktree]
	r.lanesMu.Unlock()
	if l != nil {
		l.controlMu.Lock()
		defer l.controlMu.Unlock()
		if l.ctx.Err() != nil {
			return
		}
		if err := retainMessage(r.owner, req); err != nil {
			return // Advisory overload never delays local cancellation.
		}
		select {
		case l.controls <- cancellation{req: req, id: id, conn: owner.conn, placed: owner.placed, state: owner.state, deadline: time.Now().Add(sendBudget)}:
		default:
			releaseMessage(req)
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

// Claim failed calls on the exact connection before writing their errors.
func (r *router) failInFlight(conn mcp.Connection, worktree string, cause error) {
	var stranded []jsonrpc.ID
	r.mu.Lock()
	for id, owner := range r.awaitingUpstream {
		if owner.conn == conn {
			stranded = append(stranded, id)
			r.removeLocked(id, owner)
		}
	}
	r.mu.Unlock()

	for _, id := range stranded {
		r.forward(errorResponse(id, jsonrpc.CodeInternalError, "gopls for %s went away mid-call; request may have executed: %v", worktree, cause))
	}
}

// An operation survives local client completion. Connection identity prevents
// an old result from releasing a retry's credit. Counts include uncertain writes
// and lost connections: transport failure is not proof that gopls stopped work.
type operationKey struct {
	conn mcp.Connection
	id   jsonrpc.ID
}

// Reserve and place atomically, before Write: cancellation cannot make a
// delivered operation invisible, and a fast result cannot precede reservation.
func (r *router) beginOperation(id jsonrpc.ID, state *callState, conn mcp.Connection, worktree string, cancel context.CancelFunc, placed <-chan struct{}) error {
	if !id.IsValid() {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	waiter, ok := r.ownerLocked(id, state)
	if !ok {
		return context.Canceled
	}
	if err := r.owner.begin(state, operationKey{conn, id}, worktree, r.remote, r.limits.Session, r.limits.PerLane); err != nil {
		r.rejectedLocked("upstream_operations")
		return err
	}
	waiter.conn, waiter.cancel, waiter.placed = conn, cancel, placed
	r.awaitingUpstream[id] = waiter
	return nil
}

// Direct send callers may omit expected; queued deliveries always carry their
// original generation. A cancelled queue entry cannot claim a reused ID.
func (r *router) placeDelivery(id jsonrpc.ID, expected *callState, cancel context.CancelFunc) (*callState, bool) {
	if !id.IsValid() {
		return nil, true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	waiter, ok := r.ownerLocked(id, expected)
	if !ok {
		return nil, false
	}
	waiter.conn, waiter.cancel, waiter.placed = nil, cancel, nil
	r.awaitingUpstream[id] = waiter
	return waiter.state, true
}

// Claim terminal ownership under mu and send the reply after unlocking.
func (r *router) failDelivery(id jsonrpc.ID, state *callState, code int64, message string) bool {
	if _, ok := r.finish(id, nil, state); !ok {
		return false
	}
	r.forward(errorResponse(id, code, "%s", message))
	return true
}
