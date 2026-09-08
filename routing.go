package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// laneFor returns worktree's lane, opening one on first use. Called only from
// readFromClient, which owns r.lanes.
func (r *router) laneFor(worktree string) *lane {
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
	worktree, err := r.target(req)
	if err != nil {
		code := int64(jsonrpc.CodeInvalidParams)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			code = jsonrpc.CodeInternalError
		}
		r.refuse(req, code, "%s", err)
		return
	}
	// Recorded here, not where the lane writes it: a cancellation arriving while
	// the call is still queued must find it owed, or cancelTarget would send it
	// home naming an id home never issued. Both messages pass through here in
	// the client's own order, so here the answer always exists.
	l := r.laneFor(worktree)
	ctx, cancel := context.WithTimeout(l.ctx, sendBudget)
	if err := r.admit(req.ID, worktree, cancel); err != nil {
		cancel()
		r.refuse(req, -32000, "%s", err)
		return
	}
	select {
	case l.reqs <- delivery{req: req, ctx: ctx, cancel: cancel}:
	case <-r.ctx.Done():
		cancel()
	default:
		cancel()
		r.fail(req.ID, -32000, "gopls for %s is overloaded: delivery queue is full", worktree)
	}
}

// target picks the worktree that should answer req, or reports why no single
// one can — see toolCallWorktrees.
func (r *router) target(req *jsonrpc.Request) (string, error) {
	switch req.Method {
	case "tools/call":
		worktrees, err := r.resolveWorktrees(req.Params)
		if err != nil {
			return "", err
		}
		if len(worktrees) > 1 {
			// Sticky is deliberately left alone: this call picked no worktree,
			// so the one before it is still the best guess for the one after.
			return "", fmt.Errorf("call names paths in %d worktrees (%s); one gopls answers for one tree, so split the call",
				len(worktrees), strings.Join(worktrees, ", "))
		}
		if len(worktrees) == 1 {
			r.sticky = worktrees[0]
			return worktrees[0], nil
		}
		if r.sticky != "" {
			return r.sticky, nil
		}
	case "notifications/cancelled":
		if worktree := r.cancelTarget(req.Params); worktree != "" {
			return worktree, nil
		}
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
