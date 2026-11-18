package main

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
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

// Cancellation bypasses delivery so it can stop a call still waiting to route.
func (r *router) admitted(req *jsonrpc.Request) (delivery, bool) {
	if req.Method == "notifications/cancelled" && !req.ID.IsValid() {
		r.cancelCall(req)
		return delivery{}, false
	}
	return r.prepare(req)
}

// prepare tracks calls before they wait for routing, so cancellation can finish
// both queued and resolving work without racing admission.
func (r *router) prepare(req *jsonrpc.Request) (delivery, bool) {
	if err := retainMessage(r.owner, req); err != nil {
		r.reportError(err)
		return delivery{}, false
	}
	ctx, cancel := context.WithTimeout(r.ctx, routingBudget+sendBudget)
	state, err := r.admit(req.ID, cancel)
	if err != nil {
		releaseMessage(req)
		cancel()
		r.refuse(req, -32000, "%s", err)
		return delivery{}, false
	}
	if !req.ID.IsValid() && state != nil {
		stop := cancel
		cancel = func() { stop(); r.owner.releaseQueued(state) }
	}
	return delivery{req: req, ctx: ctx, cancel: cancel, state: state, queued: r.now()}, true
}

func (r *router) routePrepared(call delivery) {
	r.timed("routing_queue", call.queued)
	defer r.timed("routing", r.now())
	worktree, err := r.targetContext(call.ctx, call.req)
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
		return
	}
	ctx, stop := context.WithTimeout(call.ctx, sendBudget)
	// Capture only the cancel function. The tracker outlives the request bytes.
	parentCancel := call.cancel
	cancel := func() { stop(); parentCancel() }
	if err := r.bindWorktree(call, worktree, cancel); err != nil {
		cancel()
		r.failIngress(call, -32000, err.Error())
		return
	}
	l := r.laneFor(worktree)
	if req.Method == "tools/call" {
		// Preserve the admitted request when rewriting paths for delivery (R10).
		if params, rewritten := r.canonicalToolCall(req.Params); rewritten {
			forwarded := *req
			forwarded.Params = params
			if err := retainMessage(r.owner, &forwarded); err != nil {
				r.failIngress(call, -32000, err.Error())
				return
			}
			defer releaseMessage(call.req)
			req = &forwarded
		}
	}
	select {
	case l.reqs <- delivery{req: req, ctx: ctx, cancel: cancel, state: call.state, queued: r.now()}:
	case <-r.ctx.Done():
		cancel()
		releaseMessage(req)
	default:
		releaseMessage(req)
		// Stop the delivery timer too.
		cancel()
		r.rejected("delivery_queue")
		r.failIngress(call, -32000, fmt.Sprintf("gopls for %s is overloaded: delivery queue is full", worktree))
	}
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
func (r *router) cancelTarget(params jsontext.Value) string {
	id, ok := cancelledID(params)
	if !ok {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.awaitingUpstream[id].worktree
}

const routingBudget = 10 * time.Second

func (r *router) resolveWorktrees(parent context.Context, params jsontext.Value) ([]string, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if found, complete := r.cachedWorktrees(params); complete {
		return found, parent.Err()
	}
	ctx, cancel := context.WithTimeout(parent, routingBudget)
	defer cancel()
	lookup := r.lookupCold
	if lookup == nil {
		lookup = r.lookupWorktrees
	}
	found := lookup(ctx, params)
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("routing budget exhausted: %w", err)
	}
	return found, nil
}

func (r *router) lookupWorktrees(ctx context.Context, params jsontext.Value) []string {
	found, _ := ownerFilesystemCall(ctx, r.owner, func() []string { return r.toolCallWorktrees(ctx, params) })
	return found
}

// Parsing and memo hits never wait for a filesystem syscall held by the worker.
func (r *router) cachedWorktrees(params jsontext.Value) ([]string, bool) {
	args := parsePathArguments(params)
	r.memoMu.Lock()
	defer r.memoMu.Unlock()
	r.expireMemosLocked()
	// Avoid capturing found and allocating on memo hits.
	var found []string
	var ok bool
	for _, path := range [...]string{args.File, args.Dir} {
		if found, ok = r.appendMemoizedWorktree(found, path); !ok {
			return nil, false
		}
	}
	for _, path := range args.Files {
		if found, ok = r.appendMemoizedWorktree(found, path); !ok {
			return nil, false
		}
	}
	return found, true
}

// appendMemoizedWorktree is appendWorktreeOf without the filesystem: it adds
// only what the memo already knows, and reports whether path needed nothing
// more. Caller holds memoMu.
func (r *router) appendMemoizedWorktree(found []string, path string) ([]string, bool) {
	if !filepath.IsAbs(path) {
		return found, true
	}
	memo, ok := r.paths[path]
	if !ok {
		r.memo.misses++
		return found, false
	}
	r.memo.hits++
	if !slices.Contains(found, memo.worktree) {
		found = append(found, memo.worktree)
	}
	return found, true
}
