package main

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Routing state is owned by the readFromClient goroutine; each upstream belongs
// to its own lane; the in-flight map is shared under mu, because the
// per-upstream reader goroutines touch it too.
type router struct {
	ctx       context.Context
	m         *manager
	home      string
	lanes     map[string]*lane  // worktree -> its upstream, see lane
	lanesMu   sync.Mutex        // protects lane lookup by the cancellation reader
	worktrees map[string]string // containing directory -> worktree, see worktreeOf
	paths     map[string]string // path argument, verbatim -> worktree, see worktreeOf
	memoMu    *sync.Mutex       // shared with the single filesystem worker
	// ctx bounds the dial, and only the dial: the connection it hands back is
	// read under this same context for the rest of its life, so the caller
	// cancels it on expiry rather than passing a deadline down. See dialBounded.
	dial func(context.Context, string) (mcp.Connection, error)

	mu sync.Mutex
	// Client requests still in flight, so that a dying gopls fails its callers
	// instead of leaving them hanging. Holding an id is also what confers the
	// right to answer it: see finish in requests.go.
	awaitingUpstream map[jsonrpc.ID]owed
	perWorktree      map[string]int
	limits           callLimits
	usage            requestUsage
	resolver         *pathResolver

	out  chan jsonrpc.Message
	errs chan error

	// Replayed to every upstream opened later. Written by readFromClient and
	// read by every lane's handshake, so it carries its own synchronisation —
	// and is read at handshake time rather than captured when the lane is
	// built. A lane opened by a tool call that arrived before initialize does
	// not wait for one: handshake fails that call outright, and it is a later
	// attempt, if any, that finds the pointer filled.
	initialize atomic.Pointer[jsonrpc.Request]
	sticky     string // worktree of the last path-bearing call
	stateless  bool   // HTTP calls must not inherit another client's worktree
}

func newRouter(ctx context.Context, m *manager, home string) *router {
	r := &router{
		ctx:              ctx,
		m:                m,
		home:             home,
		lanes:            make(map[string]*lane),
		worktrees:        make(map[string]string),
		paths:            make(map[string]string),
		memoMu:           new(sync.Mutex),
		awaitingUpstream: make(map[jsonrpc.ID]owed),
		perWorktree:      make(map[string]int),
		usage:            requestUsage{Rejections: make(map[string]int), Stages: make(map[string]stageTiming)},
		limits:           defaultCallLimits(),
		out:              make(chan jsonrpc.Message, 64),
		errs:             make(chan error, 4),
	}
	r.dial = r.dialGopls
	return r
}
