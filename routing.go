package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// laneFor returns worktree's lane, opening one on first use. Called only from
// readFromClient, which owns r.lanes.
func (r *router) laneFor(worktree string) *lane {
	r.lanesMu.Lock()
	defer r.lanesMu.Unlock()
	l, ok := r.lanes[worktree]
	if !ok {
		l = newLane(r, worktree)
		r.lanes[worktree] = l
		go l.run()
	}
	return l
}

// route admits req without waiting for lane capacity. Its budget starts here,
// so waiting in the queue does not buy another delivery budget.
func (r *router) route(req *jsonrpc.Request) {
	if req.Method == "notifications/cancelled" && !req.ID.IsValid() {
		r.cancelCall(req)
		return
	}
	if call, ok := r.prepare(req); ok {
		r.routePrepared(call)
	}
}

// prepare tracks calls before they wait for routing, so cancellation can finish
// both queued and resolving work without racing admission.
func (r *router) prepare(req *jsonrpc.Request) (delivery, bool) {
	ctx, cancel := context.WithTimeout(r.ctx, routingBudget+sendBudget)
	if err := r.admit(req.ID, "", cancel); err != nil {
		cancel()
		r.refuse(req, -32000, "%s", err)
		return delivery{}, false
	}
	r.mu.Lock()
	owner := r.awaitingUpstream[req.ID]
	owner.ingress = ctx
	if req.ID.IsValid() {
		r.awaitingUpstream[req.ID] = owner
	}
	r.mu.Unlock()
	return delivery{req: req, ctx: ctx, cancel: cancel, state: owner.state, queued: time.Now()}, true
}

func (r *router) routePrepared(call delivery) {
	r.timed("routing_queue", call.queued)
	start := time.Now()
	defer r.timed("routing", start)
	req := call.req
	if call.ctx.Err() != nil {
		r.failIngress(call, jsonrpc.CodeInternalError, "routing queue deadline exceeded")
		call.cancel()
		return
	}
	worktree, err := r.targetContext(call.ctx, req)
	r.routeResolved(call, worktree, err)
}

// Only the routing owner calls this, including after asynchronous HTTP lookup.
func (r *router) routeResolved(call delivery, worktree string, err error) {
	req := call.req
	if call.ctx.Err() != nil {
		err = call.ctx.Err()
	}
	if err != nil {
		code := int64(jsonrpc.CodeInvalidParams)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			code = jsonrpc.CodeInternalError
		}
		r.failIngress(call, code, err.Error())
		call.cancel()
		return
	}
	ctx, stop := context.WithTimeout(call.ctx, sendBudget)
	cancel := func() { stop(); call.cancel() }
	if err := r.bindWorktree(call, worktree, cancel); err != nil {
		cancel()
		r.failIngress(call, -32000, err.Error())
		return
	}
	l := r.laneFor(worktree)
	select {
	case l.reqs <- delivery{req: req, ctx: ctx, cancel: cancel, state: call.state, queued: time.Now()}:
	case <-r.ctx.Done():
		cancel()
	default:
		cancel()
		r.rejected("delivery_queue")
		r.failIngress(call, -32000, fmt.Sprintf("gopls for %s is overloaded: delivery queue is full", worktree))
	}
}

// target picks the worktree that should answer req, or reports why no single
// one can — see toolCallWorktrees.
func (r *router) target(req *jsonrpc.Request) (string, error) {
	return r.targetContext(r.ctx, req)
}

func (r *router) targetContext(ctx context.Context, req *jsonrpc.Request) (string, error) {
	switch req.Method {
	case "tools/call":
		worktrees, err := r.resolveWorktrees(ctx, req.Params)
		if err != nil {
			return "", err
		}
		return r.targetWorktrees(worktrees)
	case "notifications/cancelled":
		if worktree := r.cancelTarget(req.Params); worktree != "" {
			return worktree, nil
		}
	}
	return r.home, nil
}

func (r *router) targetWorktrees(worktrees []string) (string, error) {
	if len(worktrees) > 1 {
		return "", fmt.Errorf("call names paths in %d worktrees (%s); one gopls answers for one tree, so split the call",
			len(worktrees), strings.Join(worktrees, ", "))
	}
	if len(worktrees) == 1 {
		if !r.stateless {
			r.sticky = worktrees[0]
		}
		return worktrees[0], nil
	}
	if !r.stateless && r.sticky != "" {
		return r.sticky, nil
	}
	return r.home, nil
}

// cancelTarget reports the worktree whose upstream owes the request this
// notification cancels — see R7. An id nobody owes reports nothing, and the
// notification goes home like every other one.
func (r *router) cancelTarget(params json.RawMessage) string {
	var cancelled mcp.CancelledParams
	if err := json.Unmarshal(params, &cancelled); err != nil {
		return ""
	}
	// Ids arrive off the wire as the same nil/float64/string that json.Unmarshal
	// produces here, and both sides go through MakeID, so one rebuilt from the
	// notification compares equal to the one the route was recorded under.
	id, err := jsonrpc.MakeID(cancelled.RequestID)
	if err != nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Whether that worktree still has an upstream to tell is deliberately not
	// asked: the lane owns its connection and is the only party whose answer
	// cannot already be stale by the time it is acted on, so it decides — see
	// send.
	return r.awaitingUpstream[id].worktree
}
