package main

import (
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// track seeds or updates an outstanding request for tests of delivered traffic.
func (r *router) track(id jsonrpc.ID, conn mcp.Connection, worktree string) {
	if !id.IsValid() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	owner := r.awaitingUpstream[id]
	if owner.state == nil {
		owner.state = &callState{}
	}
	owner.conn, owner.worktree = conn, worktree
	r.awaitingUpstream[id] = owner
}
