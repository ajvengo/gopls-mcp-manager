package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/transport"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// bridge multiplexes one stdio MCP client across per-worktree gopls servers.
//
// A gopls instance only knows the worktree it was started in: asked about a file
// outside it, it answers "no package metadata" rather than routing anywhere. One
// upstream therefore cannot serve a client whose files live in a linked worktree
// — which is exactly what an agent working under worktree isolation asks for.
//
// So every tools/call carrying a file path is routed to the gopls of THAT path's
// worktree, started on demand. The client sees one server; each worktree keeps
// its own index, which is the isolation the worktree was created for.
//
// Calls with no path argument (go_workspace, go_search, go_package_api) carry no
// routable evidence. They follow the most recent path-bearing call, falling back
// to the worktree the bridge was started in.
func bridge(ctx context.Context, m *manager, home string) error {
	stdio := transport.NewStdio(os.Stdin, os.Stdout, m.limits.MessageBytes)
	return serve(ctx, m, home, stdio)
}

// serve is the session over an already-connected client transport: everything
// bridge does once it has one. Split out so that the shutdown order below — the
// one thing here with a wrong answer — is reachable over a pair of in-memory
// transports, since the one bridge opens is this process's real stdin.
//
// The session's context is derived here rather than handed in with a cancel to
// match, so that nothing outside can end the session early or forget to end it
// at all. Connecting a transport does not tie it to a context — StdioTransport's
// Connect ignores the one it is given — so what stops the traffic is this
// context, which every Read and Write below runs under, and Close, which ends
// the connection itself. Both happen here, in that order.
func serve(ctx context.Context, m *manager, home string, stdio mcp.Connection) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r := newRouter(ctx, m, home)
	if r.limits.Metrics {
		defer func() { _ = json.NewEncoder(os.Stderr).Encode(r.requestUsage()) }()
		go r.reportUsage()
	}
	// Only the reader is waited for, because r.lanes is its state and
	// closeLanes must not race it. The writer is left where it stands: the
	// stdio transport's Close closes stdin and no-ops on stdout, so a Write
	// already inside a full stdout pipe never unblocks, and waiting for it would
	// hang the exit on a client that stopped reading us.
	readerDone := make(chan struct{})
	go r.writeToClient(stdio)
	go func() {
		defer close(readerDone)
		r.readFromClient(stdio)
	}()

	err := <-r.errs
	cancel()
	_ = stdio.Close()
	<-readerDone
	r.closeLanes()
	r.awaitLanes()
	r.clearRequests()
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, mcp.ErrConnectionClosed) {
		return nil
	}
	return err
}

// One reporter bounds blocked metric output to one goroutine per session.
// Snapshots contain fixed stage/reason names, never worktree labels.
func (r *router) reportUsage() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			_ = json.NewEncoder(os.Stderr).Encode(r.requestUsage())
		}
	}
}

// closeLanes ends every lane and lets it close its own connection.
//
// Only ever after the goroutine that owns r.lanes has stopped — it is the only
// sender on l.reqs, so closing these channels is safe exactly because it is
// provably gone. Every caller waits for theirs first.
//
// Waiting for the lanes to finish is deliberately the caller's, not folded in
// here: it only terminates once the session context is cancelled, which the two
// shutdown paths do first and a test closing a router's queues does not.
func (r *router) closeLanes() {
	for _, l := range r.lanes {
		close(l.reqs)
	}
}

// awaitLanes waits out whatever each lane is still finishing — the cleanup of a
// failed child, most of the time — so the process cannot exit and abandon it.
// Manager waits are cancellable and that cleanup has its own short termination
// budget, so this is bounded. Only meaningful after closeLanes.
func (r *router) awaitLanes() {
	for _, l := range r.lanes {
		<-l.done
	}
}

func (r *router) writeToClient(stdio mcp.Connection) {
	for {
		select {
		case <-r.ctx.Done():
			return
		case msg := <-r.out:
			if err := stdio.Write(r.ctx, msg); err != nil {
				r.errs <- err
				return
			}
		}
	}
}

func (r *router) readFromClient(stdio mcp.Connection) {
	// The ingress reader handles cancellation immediately. This goroutine alone
	// commits routing and sticky state, preserving the order of ordinary calls.
	incoming := make(chan delivery, laneQueue)
	go func() {
		defer close(incoming)
		r.readIngress(stdio, incoming)
	}()
	for call := range incoming {
		r.routePrepared(call)
	}
}

func (r *router) readIngress(stdio mcp.Connection, incoming chan<- delivery) {
	for {
		msg, err := stdio.Read(r.ctx)
		if err != nil {
			r.errs <- err
			return
		}
		req, ok := msg.(*jsonrpc.Request)
		if !ok {
			// Not a request, and this bridge asks the client nothing, so a stray
			// response has nothing to route to.
			continue
		}
		if req.Method == "initialize" {
			req.Params = withRootsCapability(req.Params)
			r.initialize.Store(req)
		} else if r.initialize.Load() == nil {
			// A request the client sent before its own initialize, which the
			// protocol disallows. Refused here rather than by the lane that
			// would run it, because only this loop sees the client's own order:
			// a lane could dial before the initialize behind it is stored. The
			// invariant does not rest on winning that race — a connection then
			// owed a second initialize refuses it itself (see upstream).
			r.refuse(req, jsonrpc.CodeInvalidRequest, "%s arrived before the client sent initialize", req.Method)
			continue
		}
		call, ok := r.admitted(req)
		if !ok {
			continue
		}
		select {
		case incoming <- call:
		case <-r.ctx.Done():
			call.cancel()
			return
		default:
			r.rejected("routing_queue")
			r.failIngress(call, -32000, "routing queue is full")
		}
	}
}

// forward hands a message to the client, giving up once the session is over so
// that a stopped writer cannot strand its sender.
func (r *router) forward(msg jsonrpc.Message) {
	select {
	case r.out <- msg:
	case <-r.ctx.Done():
	}
}
