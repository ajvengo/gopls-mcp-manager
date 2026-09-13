package main

import "github.com/modelcontextprotocol/go-sdk/jsonrpc"

// route and target are the two ingress steps run back to back, which is what a
// test wants to say and what no production path says: the stdio reader admits
// on one goroutine and routes on another, so that a cancellation is not queued
// behind the call it cancels. They live here so the routing file describes only
// what the binary runs.

// route admits req without waiting for lane capacity. Its budget starts here,
// so waiting in the queue does not buy another delivery budget.
func (r *router) route(req *jsonrpc.Request) {
	if call, ok := r.admitted(req); ok {
		r.routePrepared(call)
	}
}

// target picks the worktree that should answer req, or reports why no single
// one can — see toolCallWorktrees.
func (r *router) target(req *jsonrpc.Request) (string, error) {
	return r.targetContext(r.ctx, req)
}
