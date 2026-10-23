package main

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"
	"github.com/ajvengo/gopls-mcp-manager/internal/protocol"
	"github.com/ajvengo/gopls-mcp-manager/internal/transport"
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
	lanes     map[string]*lane    // worktree -> its upstream, see lane
	lanesMu   sync.Mutex          // protects lane lookup by the cancellation reader
	worktrees map[string]string   // containing directory -> worktree, see worktreeOf
	paths     map[string]pathMemo // path argument, verbatim -> its resolution, see worktreeOf
	memoMu    sync.Mutex          // guards both memo maps and the epoch below
	memo      memoState           // expiry epoch and observations, under memoMu
	// ctx bounds the dial, and only the dial: the connection it hands back is
	// read under this same context for the rest of its life, so the caller
	// cancels it on expiry rather than passing a deadline down. See dialBounded.
	dial func(context.Context, string) (mcp.Connection, error)
	// plan computes a worktree's roots for answerRoots. A seam, like dial: the
	// real one walks the tree, which a test cannot hold open any other way.
	plan func(worktree string, global []string) watchPlan

	mu sync.Mutex
	// Client requests still in flight, so that a dying gopls fails its callers
	// instead of leaving them hanging. Holding an id is also what confers the
	// right to answer it: see finish in requests.go.
	awaitingUpstream      map[jsonrpc.ID]owed
	perWorktree           map[string]int
	operations            map[operationKey]operation
	operationsPerWorktree map[string]int
	limits                config.Limits
	usage                 requestUsage
	resolver              *pathResolver
	sseBudget             *transport.Budget

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
		ctx:                   ctx,
		m:                     m,
		home:                  home,
		lanes:                 make(map[string]*lane),
		worktrees:             make(map[string]string),
		paths:                 make(map[string]pathMemo),
		awaitingUpstream:      make(map[jsonrpc.ID]owed),
		perWorktree:           make(map[string]int),
		operations:            make(map[operationKey]operation),
		operationsPerWorktree: make(map[string]int),
		usage:                 requestUsage{Rejections: make(map[string]int), Stages: make(map[string]stageTiming)},
		out:                   make(chan jsonrpc.Message, 64),
		errs:                  make(chan error, 4),
	}
	// The manager's policy is the session's, and WithDefaults is what lets every
	// reader below take a limit at face value — including on the zero-value
	// manager a test hands in, and on no manager at all.
	if m != nil {
		r.limits = m.limits
	}
	r.limits = r.limits.WithDefaults()
	r.dial = r.dialGopls
	r.plan = planWatch
	r.sseBudget = transport.NewBudget(r.limits.SSEBytes)
	return r
}

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
		defer func() { _ = json.MarshalEncode(jsontext.NewEncoder(os.Stderr), r.requestUsage()) }()
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
			_ = json.MarshalEncode(jsontext.NewEncoder(os.Stderr), r.requestUsage())
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
			req.Params = protocol.WithRootsCapability(req.Params)
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

// errorResponse builds the one error shape this bridge sends, in both
// directions: refuse and failInFlight forward it to the client,
// answeredUpstream writes it back to a gopls. Shared so they cannot drift into
// answering the same way in different words.
func errorResponse(id jsonrpc.ID, code int64, format string, args ...any) *jsonrpc.Response {
	return &jsonrpc.Response{ID: id, Error: &jsonrpc.Error{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}}
}

func (r *router) readFromUpstream(conn mcp.Connection, worktree string) {
	// roots/list is answered off this loop: planning walks the worktree, and
	// nothing else from this upstream is forwarded while the loop is in it. One
	// worker per reader keeps the walks serial and the goroutines bounded; the
	// queue is small because gopls asks rarely, and a full one only slows the
	// reader to the old behaviour.
	asks := make(chan jsonrpc.ID, 4)
	defer close(asks)
	go r.answerRootsAsks(conn, worktree, asks)
	for {
		msg, err := conn.Read(r.ctx)
		if err != nil {
			// An upstream dying is not the stdio client's problem. Its cached
			// connection fails the next write, which redials and retries once.
			r.failInFlight(conn, worktree, err)
			return
		}
		if req, ok := msg.(*jsonrpc.Request); ok && req.ID.IsValid() && req.Method == "roots/list" {
			select {
			case asks <- req.ID:
			case <-r.ctx.Done():
				return
			}
			continue
		}
		if answered, err := r.answeredUpstream(r.ctx, conn, worktree, msg); err != nil {
			_ = conn.Close()
			r.failInFlight(conn, worktree, err)
			return
		} else if answered {
			continue
		}
		if resp, ok := msg.(*jsonrpc.Response); ok {
			if _, claimed := r.finish(resp.ID, conn, nil); !claimed {
				// send() got to this id first — its write failed after the upstream
				// had already taken the message — and answered it. Forwarding now
				// would be a second reply to a call the client has closed.
				continue
			}
		}
		r.forward(msg)
	}
}

// answerRootsAsks replies to the roots/list ids the reader hands over, each
// under its own delivery ceiling (S4a). A failed reply closes the connection,
// which fails the reader's Read, and fails the connection's calls here as well:
// failInFlight claims each id under r.mu, so whichever of the two runs first
// answers it and the other finds nothing. Later asks on a failed connection are
// drained unanswered, so the reader never blocks on a dead worker. It ends when
// the reader closes asks.
func (r *router) answerRootsAsks(conn mcp.Connection, worktree string, asks <-chan jsonrpc.ID) {
	failed := false
	for id := range asks {
		if failed {
			continue
		}
		ctx, cancel := context.WithTimeout(r.ctx, sendBudget)
		err := r.answerRoots(ctx, conn, worktree, id)
		cancel()
		if err != nil {
			failed = true
			_ = conn.Close()
			r.failInFlight(conn, worktree, err)
		}
	}
}

// answeredUpstream handles a server-initiated request here rather than passing
// it to the client, and reports whether it did.
//
// Every read loop that can see one has to call this — see answerRoots for what
// goes wrong when a roots/list reaches the client. Anything else is refused: an
// upstream holding an unanswerable question waits forever, so a definite error
// is the kinder reply.
//
// ctx is the caller's budget for talking to conn: the handshake answers roots
// inside its deadline (S2); steady-state replies get the delivery ceiling. Only
// the handshake reaches roots/list here: the steady-state reader hands it to
// answerRootsAsks, so its walk of the worktree blocks no response.
func (r *router) answeredUpstream(ctx context.Context, conn mcp.Connection, worktree string, msg jsonrpc.Message) (bool, error) {
	req, ok := msg.(*jsonrpc.Request)
	if !ok || !req.ID.IsValid() {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, sendBudget)
	defer cancel()
	if req.Method == "roots/list" {
		return true, r.answerRoots(ctx, conn, worktree, req.ID)
	}
	return true, conn.Write(ctx, errorResponse(req.ID, jsonrpc.CodeMethodNotFound,
		"gopls-mcp-manager does not relay %q to the client", req.Method))
}

// answerRoots tells an upstream the roots of the worktree it serves — the
// worktree itself, or the directories covering it minus what its filters
// exclude (see watchroots.go) — instead of forwarding the question to the client.
//
// This is what makes a gopls notice a file created after it started: its watcher
// only watches the roots the client reports, and it only asks for them once it
// has seen the roots capability in initialize — which is why withRootsCapability
// puts one there. Left to the client, every upstream would hear the same answer,
// the single tree the session was opened in, and every other worktree would watch
// nothing and answer "no package metadata" for a file that is right there on
// disk — indistinguishable from a genuinely broken tree.
func (r *router) answerRoots(ctx context.Context, conn mcp.Connection, worktree string, id jsonrpc.ID) error {
	plan := r.plan(worktree, r.limits.DirectoryFilters)
	r.m.rememberWatch(worktree, serverOf(conn), plan)
	// Strings in a fixed shape: marshaling them cannot fail, and there would be
	// nobody to tell anyway — the id belongs to the upstream, so an error reply
	// sent to the client would answer a question it never asked.
	//nolint:staticcheck // Legacy gopls SSE sessions still need roots for file watching.
	roots := make([]*mcp.Root, len(plan.roots))
	for i, dir := range plan.roots {
		//nolint:staticcheck // Legacy gopls SSE sessions still need roots for file watching.
		roots[i] = &mcp.Root{URI: (&url.URL{Scheme: "file", Path: dir}).String(), Name: filepath.Base(dir)}
	}
	//nolint:staticcheck // Legacy gopls SSE sessions still need roots for file watching.
	result, _ := json.Marshal(mcp.ListRootsResult{Roots: roots})
	return conn.Write(ctx, &jsonrpc.Response{ID: id, Result: result})
}
