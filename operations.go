package main

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// An operation survives local client completion. Connection identity prevents
// an old result from releasing a retry's credit. Counts include uncertain writes
// and lost connections: transport failure is not proof that gopls stopped work.
type operationKey struct {
	conn mcp.Connection
	id   jsonrpc.ID
}

type operation struct {
	worktree  string
	abandoned bool
}

// Reserve and place atomically, before Write: cancellation cannot make a
// delivered operation invisible, and a fast result cannot precede reservation.
func (r *router) beginOperation(id jsonrpc.ID, state *callState, conn mcp.Connection, worktree string, cancel context.CancelFunc, placed <-chan struct{}) error {
	if !id.IsValid() {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.ownerLocked(id, state)
	if !ok {
		return context.Canceled
	}
	key := operationKey{conn, id}
	if _, exists := r.operations[key]; exists {
		return fmt.Errorf("request id %v still has an unresolved upstream operation", id)
	}
	if len(r.operations) >= r.limits.Session || r.operationsPerWorktree[worktree] >= r.limits.PerLane {
		r.rejectedLocked("upstream_operations")
		return fmt.Errorf("upstream operation limit reached; cancelled or disconnected work may still be running")
	}
	r.operations[key] = operation{worktree: worktree}
	r.operationsPerWorktree[worktree]++
	owner.conn, owner.cancel, owner.placed = conn, cancel, placed
	r.awaitingUpstream[id] = owner
	return nil
}

// A terminal response or proven rejection before POST releases a credit.
// Caller holds mu; this is atomic with matching the response to its client.
func (r *router) endOperationLocked(key operationKey) {
	if op, ok := r.operations[key]; ok {
		delete(r.operations, key)
		release(r.operationsPerWorktree, op.worktree)
	}
}

func (r *router) abandonOperationLocked(key operationKey) {
	if op, ok := r.operations[key]; ok {
		op.abandoned = true
		r.operations[key] = op
	}
}

// Direct send callers may omit expected; queued deliveries always carry their
// original generation. A cancelled queue entry cannot claim a reused ID.
func (r *router) placeDelivery(id jsonrpc.ID, expected *callState, cancel context.CancelFunc) (*callState, bool) {
	if !id.IsValid() {
		return nil, true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.ownerLocked(id, expected)
	if !ok {
		return nil, false
	}
	owner.conn, owner.cancel, owner.placed = nil, cancel, nil
	r.awaitingUpstream[id] = owner
	return owner.state, true
}

// failDelivery answers a call this caller has just taken terminal ownership of,
// and reports whether it was still the caller's to answer. The one spelling of
// that pair: claiming the id under mu is what says nobody else will answer it,
// and the reply goes out after unlocking so a slow client cannot hold the
// tracker.
func (r *router) failDelivery(id jsonrpc.ID, state *callState, code int64, message string) bool {
	if _, ok := r.finish(id, nil, state); !ok {
		return false
	}
	r.forward(errorResponse(id, code, "%s", message))
	return true
}
