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

// admitted is the delivery req travels as, or no delivery at all: a client's
// cancellation never becomes one — it answers a call already admitted, and
// queueing it behind that call's own routing is how it would arrive too late to
// stop anything. Stated here rather than at each of the two entry points, which
// is what kept the rule in step with itself.
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
	ctx, cancel := context.WithTimeout(r.ctx, routingBudget+sendBudget)
	state, err := r.admit(req.ID, cancel)
	if err != nil {
		cancel()
		r.refuse(req, -32000, "%s", err)
		return delivery{}, false
	}
	return delivery{req: req, ctx: ctx, cancel: cancel, state: state, queued: r.now()}, true
}

func (r *router) routePrepared(call delivery) {
	r.timed("routing_queue", call.queued)
	defer r.timed("routing", r.now())
	// A call whose budget went while it waited is not checked for here: every
	// path below reports it. resolveWorktrees refuses an expired parent before
	// it queues any filesystem work, and routeResolved fails whatever reaches it
	// on its own ctx check — so an expired queue entry is answered once, in the
	// words of the deadline that actually passed.
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
	// Closed over by name, not through call: a closure captures whole variables,
	// and this one outlives the delivery — it is held in the tracker for the
	// length of the upstream call, which would keep the client's original
	// request and its params bytes alive with it.
	parentCancel := call.cancel
	cancel := func() { stop(); parentCancel() }
	if err := r.bindWorktree(call, worktree, cancel); err != nil {
		cancel()
		r.failIngress(call, -32000, err.Error())
		return
	}
	l := r.laneFor(worktree)
	if req.Method == "tools/call" {
		// Forward the physical spelling of every path argument (R10). A copy is
		// delivered because the original request is still the one this ingress
		// admitted and may report on.
		if params, rewritten := r.canonicalToolCall(req.Params); rewritten {
			forwarded := *req
			forwarded.Params = params
			req = &forwarded
		}
	}
	select {
	case l.reqs <- delivery{req: req, ctx: ctx, cancel: cancel, state: call.state, queued: r.now()}:
	case <-r.ctx.Done():
		cancel()
	default:
		// The compound cancel, not failIngress's: this one also stops the send
		// budget's timer by name rather than waiting for the parent to reach it.
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
	// Whether that worktree still has an upstream to tell is deliberately not
	// asked: the lane owns its connection and is the only party whose answer
	// cannot already be stale by the time it is acted on, so it decides — see
	// send.
	return r.awaitingUpstream[id].worktree
}

const routingBudget = 10 * time.Second

type resolution struct {
	ctx    context.Context
	params jsontext.Value
	reply  chan []string
}

// One worker, no buffered jobs: a filesystem call that the OS cannot cancel
// occupies at most this worker. A timed-out job cannot commit sticky state.
// Work is committed on the reader in wire order, preserving pathless routing
// and cancellation-after-admission semantics.
type pathResolver struct {
	jobs   chan resolution
	lookup func(context.Context, jsontext.Value) []string
}

func (p *pathResolver) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-p.jobs:
			if job.ctx.Err() != nil {
				continue
			}
			job.reply <- p.lookup(job.ctx, job.params)
		}
	}
}

func (r *router) resolveWorktrees(parent context.Context, params jsontext.Value) ([]string, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if found, complete := r.cachedWorktrees(params); complete {
		return found, parent.Err()
	}
	if r.resolver == nil {
		// Unsynchronised, and safe for the same reason the worker is single:
		// resolveWorktrees has one caller per router — the stdio reader, or the
		// HTTP backend's one cold-resolution goroutine — so the first miss of a
		// session is the only thing that ever reaches this branch. Built here
		// rather than in newRouter so a session that never misses the memo, which
		// is most of them, starts no goroutine at all.
		//
		// Only this worker performs filesystem lookups. It touches the memo maps
		// and their lock, never request tracking or lane state — and it runs
		// under the job's own context, not the session's, so a routing budget
		// that expires cuts the lookup it paid for and no other.
		r.resolver = &pathResolver{jobs: make(chan resolution), lookup: r.toolCallWorktrees}
		go r.resolver.run(r.ctx)
	}
	ctx, cancel := context.WithTimeout(parent, routingBudget)
	defer cancel()
	job := resolution{ctx: ctx, params: params, reply: make(chan []string, 1)}
	select {
	case r.resolver.jobs <- job:
	case <-ctx.Done():
		return nil, fmt.Errorf("routing budget exhausted: %w", ctx.Err())
	}
	select {
	case found := <-job.reply:
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("routing budget exhausted: %w", err)
		}
		return found, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("routing budget exhausted: %w", ctx.Err())
	}
}

// Parsing and memo hits never wait for a filesystem syscall held by the worker.
func (r *router) cachedWorktrees(params jsontext.Value) ([]string, bool) {
	args := parsePathArguments(params)
	r.memoMu.Lock()
	defer r.memoMu.Unlock()
	r.expireMemosLocked()
	// Written out rather than as a closure over found, for the reason
	// toolCallWorktrees gives: this runs on the goroutine that routes every
	// worktree's calls, and a closure capturing found heap-allocates both on
	// each tools/call, memo hit included.
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
