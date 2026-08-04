package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// How long one client message gets to reach an upstream and handshake it, its
// retry included; see send. It contains ensure's readiness wait, so it is
// derived from it: raising that wait alone would fail every cold worktree's
// first call. The remainder is the handshake's, and generous because §8 keeps
// a server that listens and answers nothing alive.
const sendBudget = readyTimeout + 20*time.Second

// initID is the id every replayed initialize is sent under. One value for all
// of them: an id only has to be unique on the connection it is used on, and a
// connection sees exactly one handshake — the lane that owns it dials, hands it
// this, and never handshakes it again. A literal string cannot fail MakeID.
var initID, _ = jsonrpc.MakeID("gopls-mcp-manager-init")

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
	ctx, cancel := context.WithCancel(ctx)

	stdio, err := (&mcp.StdioTransport{}).Connect(ctx)
	if err != nil {
		cancel()
		return err
	}
	return serve(ctx, cancel, m, home, stdio)
}

// serve is the session over an already-connected client transport: everything
// bridge does once it has one. Split out so that the shutdown order below — the
// one thing here with a wrong answer — is reachable over a pair of in-memory
// transports, since the one bridge opens is this process's real stdin.
//
// The cancel is passed in rather than derived, because ctx is what stdio was
// connected under and cancelling a narrower one would leave the transport
// running past the session.
func serve(ctx context.Context, cancel context.CancelFunc, m *manager, home string, stdio mcp.Connection) error {
	r := newRouter(ctx, m, home)

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
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, mcp.ErrConnectionClosed) {
		return nil
	}
	return err
}

// Routing state is owned by the readFromClient goroutine; each upstream belongs
// to its own lane; the in-flight map is shared under mu, because the
// per-upstream reader goroutines touch it too.
type router struct {
	ctx       context.Context
	m         *manager
	home      string
	lanes     map[string]*lane  // worktree -> its upstream, see lane
	worktrees map[string]string // containing directory -> worktree, see worktreeOf
	paths     map[string]string // path argument, verbatim -> worktree, see worktreeOf
	// ctx bounds the dial, and only the dial: the connection it hands back is
	// read under this same context for the rest of its life, so the caller
	// cancels it on expiry rather than passing a deadline down. See dialBounded.
	dial func(context.Context, string) (mcp.Connection, error)

	mu sync.Mutex
	// Client requests still in flight, so that a dying gopls fails its callers
	// instead of leaving them hanging. Holding an id is also what confers the
	// right to answer it: see claim.
	awaitingUpstream map[jsonrpc.ID]owed

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
}

// owed is a request in flight: the worktree it was routed to, and the upstream
// that has taken it, once one has.
//
// The worktree is known strictly earlier than the connection — route picks a
// destination before any lane has one to name — so conn is nil for a call still
// queued. A cancellation arriving in that window still has to find where its
// call went (R7), which is why one record covers both stages rather than the
// route appearing only once an upstream owes an answer.
//
// conn is compared by connection, never by worktree: send() replaces a dead
// connection with a live one under the same worktree, and matching by name
// would let the dead one's reader fail calls the reconnect had already placed
// successfully.
type owed struct {
	conn     mcp.Connection
	worktree string
}

func newRouter(ctx context.Context, m *manager, home string) *router {
	r := &router{
		ctx:              ctx,
		m:                m,
		home:             home,
		lanes:            make(map[string]*lane),
		worktrees:        make(map[string]string),
		paths:            make(map[string]string),
		awaitingUpstream: make(map[jsonrpc.ID]owed),
		out:              make(chan jsonrpc.Message, 64),
		errs:             make(chan error, 4),
	}
	r.dial = r.dialGopls
	return r
}

// laneQueue is how far one worktree may run ahead before the reader waits for
// it. Deep enough that a client pipelining calls at a cold worktree still gets
// them all taken while it starts; a full queue is the one case where a slow
// worktree can still hold the reader up, and it takes 64 outstanding calls to
// the same tree to reach.
const laneQueue = 64

// lane is one worktree's upstream and the goroutine that owns it.
//
// A lane per worktree keeps the cost of a cold start — an flock wait, a gopls
// spawn, a whole handshake — on the worktree that pays it: the reader picks a
// target and hands the request over, and each upstream is dialled, handshaken,
// retried and replaced by the single goroutine that owns it. Sharing the
// reader's goroutine would block every other worktree behind that start.
//
// Order within a worktree is the channel's. Between worktrees there never was
// any: the client's ids are what pair answers with calls.
type lane struct {
	r        *router
	worktree string
	reqs     chan *jsonrpc.Request

	// ctx is this lane's own child of the session context, and the parent every
	// per-request budget in send is derived from. One child per lane rather than
	// two per request: WithTimeout registers itself on its parent under that
	// parent's own mutex, so deriving each budget straight from the session
	// context put every lane on one process-wide lock twice a message — on the
	// register and again on the cancel — which is contention that grows with
	// exactly the concurrency the lanes were introduced to buy.
	ctx    context.Context
	cancel context.CancelFunc

	// conn belongs to the goroutine running this lane, which is what replaces the
	// lock a shared connection map would have needed. It is nil before the first
	// dial and again whenever a write drops one.
	conn mcp.Connection
}

func newLane(r *router, worktree string) *lane {
	ctx, cancel := context.WithCancel(r.ctx)
	return &lane{
		r:        r,
		worktree: worktree,
		reqs:     make(chan *jsonrpc.Request, laneQueue),
		ctx:      ctx,
		cancel:   cancel,
	}
}

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

func (l *lane) run() {
	// Releases this lane's registration on the session context. The connections
	// it dialled keep contexts of their own, rooted at the session rather than
	// here, so this ends the lane's budgets and nothing else — see dialBounded.
	defer l.cancel()
draining:
	for req := range l.reqs {
		// Whatever is left in the queue when the session ends is not worth an
		// upstream: dialling would run ensure, spawning a whole gopls for a client
		// that has already gone away, and the answer would have nowhere to go.
		//
		// Asked through Done rather than Err: Err takes the session context's own
		// mutex, which every lane would then share once a message — the very
		// contention lane.ctx exists to keep off the session context.
		select {
		case <-l.r.ctx.Done():
			break draining
		default:
		}
		l.send(req)
	}
	if l.conn != nil {
		_ = l.conn.Close()
	}
}

// route hands req to the lane that should answer it, giving up once the session
// is over so that a stopped lane cannot strand the reader.
func (r *router) route(req *jsonrpc.Request) {
	worktree, err := r.target(req)
	if err != nil {
		r.refuse(req, jsonrpc.CodeInvalidParams, "%s", err)
		return
	}
	// Recorded here, not where the lane writes it: a cancellation arriving while
	// the call is still queued must find it owed, or cancelTarget would send it
	// home naming an id home never issued. Both messages pass through here in
	// the client's own order, so here the answer always exists.
	r.track(req.ID, nil, worktree)
	l := r.laneFor(worktree)
	select {
	case l.reqs <- req:
	case <-r.ctx.Done():
	}
}

// closeLanes ends every lane and lets it close its own connection.
//
// Only ever after readFromClient has stopped: it owns r.lanes and is the only
// sender on l.reqs, so closing these channels is safe because that goroutine is
// provably gone. bridge waits for readerDone first, and is the only caller.
//
// Not waited for: a lane inside m.ensure is parked on an flock and a readiness
// poll that take no context, so waiting would hang the exit on another
// process's cold start.
func (r *router) closeLanes() {
	for _, l := range r.lanes {
		close(l.reqs)
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
		r.route(req)
	}
}

// refuse answers req with an error, for a message no upstream took: the reader
// could choose no lane for it, none may run it yet, or the lane that owned it
// could not place it. It is the only way the bridge reports a problem for a
// call the client is still waiting on.
//
// Notifications are dropped instead of refused: a notification has no id to
// answer, and inventing a response for one would be a message the client has
// nothing to match against.
//
// Answering ends the call, so its route goes too — a cancellation arriving now
// belongs at home, not at a lane that has stopped waiting for it. This is the
// plain delete, not a claim: every caller has already established that the
// reply is theirs to send, and one with nothing to drop loses nothing by saying
// so, since refuse can be answering a message that was never tracked.
func (r *router) refuse(req *jsonrpc.Request, code int64, format string, args ...any) {
	if !req.ID.IsValid() {
		return
	}
	r.mu.Lock()
	delete(r.awaitingUpstream, req.ID)
	r.mu.Unlock()
	r.forward(errorResponse(req.ID, code, format, args...))
}

// withRootsCapability makes every upstream ask this bridge for its roots. Some
// MCP clients omit the optional capability; without it gopls installs no file
// watcher and newly-created Go files remain invisible until its daemon restarts.
// Parameters we cannot rewrite — invalid ones, or ones that will not marshal
// back — are forwarded untouched and left for gopls to reject. The client's own
// spelling of each key is the one rewritten; see jsonKey for why.
func withRootsCapability(params json.RawMessage) json.RawMessage {
	var initialize map[string]json.RawMessage
	if err := json.Unmarshal(params, &initialize); err != nil || initialize == nil {
		return params
	}

	capabilitiesKey := jsonKey(initialize, "capabilities")
	var capabilities map[string]json.RawMessage
	if raw := initialize[capabilitiesKey]; !absentJSON(raw) {
		if err := json.Unmarshal(raw, &capabilities); err != nil || capabilities == nil {
			return params
		}
	}
	if capabilities == nil {
		capabilities = make(map[string]json.RawMessage)
	}
	if rootsKey := jsonKey(capabilities, "roots"); absentJSON(capabilities[rootsKey]) {
		capabilities[rootsKey] = json.RawMessage(`{}`)
	}

	rawCapabilities, err := json.Marshal(capabilities)
	if err != nil {
		return params
	}
	initialize[capabilitiesKey] = rawCapabilities

	rewritten, err := json.Marshal(initialize)
	if err != nil {
		return params
	}
	return rewritten
}

// jsonKey is the key in object that a Go decoder would read as name: the exact
// spelling when it is there, and otherwise a case-insensitive match, since
// encoding/json falls back to one. Writing our own spelling beside a client's
// would leave two keys mapping to one field, and which of them gopls took would
// come down to the order they marshalled in — so the capability we add for it
// could silently not be the one it read.
func jsonKey(object map[string]json.RawMessage, name string) string {
	if _, exact := object[name]; exact {
		return name
	}
	for key := range object {
		if strings.EqualFold(key, name) {
			return key
		}
	}
	return name
}

// absentJSON reports whether a client left this value out, spelled either way:
// the key missing entirely, or present and null.
func absentJSON(raw json.RawMessage) bool {
	return len(raw) == 0 || strings.TrimSpace(string(raw)) == "null"
}

// target picks the worktree that should answer req, or reports why no single
// one can — see toolCallWorktrees.
func (r *router) target(req *jsonrpc.Request) (string, error) {
	switch req.Method {
	case "tools/call":
		worktrees := r.toolCallWorktrees(req.Params)
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
	// An id nobody owes yields "", which is already how this reports "no answer".
	// Whether that worktree still has an upstream to tell is deliberately not
	// asked: the lane owns its connection and is the only party whose answer
	// cannot already be stale by the time it is acted on, so it decides — see
	// send.
	return r.awaitingUpstream[id].worktree
}

func (l *lane) send(req *jsonrpc.Request) {
	// Only the first home connection receives the client's own initialize; see
	// upstream. Derived here rather than passed in, so that the id and the
	// message it is sent with cannot disagree.
	id, initial := req.ID, req.Method == "initialize"
	// One budget for both attempts, because the retry redials: a per-attempt
	// deadline would let a wedged upstream stall the client twice over.
	//
	// The write needs bounding because the SSE transport delivers a message as
	// an HTTP POST on a client with no timeout. Only the delivery is bounded;
	// how long gopls then takes to answer stays its own business. A server that
	// takes the POST and never completes it is what this exists for — §8 keeps
	// exactly such a server alive — and under r.ctx alone it would park this
	// lane for the rest of the session.
	ctx, cancel := context.WithTimeout(l.ctx, sendBudget)
	defer cancel()
	var err error
	for range 2 {
		// A message nobody awaits an answer to never opens a connection:
		// dialling would run ensure — flock, spawn, handshake — to hand a brand
		// new process a notification about work it never did. Dropping it
		// strands nothing; R7's cancellation names a call the reader has already
		// failed (F2). Asked on the id rather than the method, so no later
		// notification becomes a second special case, and asked every attempt,
		// because the retry arrives having just dropped its connection.
		if l.conn == nil && !id.IsValid() {
			return
		}
		var conn mcp.Connection
		conn, err = l.upstream(ctx, initial)
		if err != nil {
			continue
		}
		// Recorded before the write, not after: a local gopls can answer
		// before Write returns, and a registration landing after its response
		// was already deleted would outlive the call and produce a second,
		// contradictory reply to the same id later on.
		l.r.track(id, conn, l.worktree)
		if err = conn.Write(ctx, req); err == nil {
			// Cached only once the connection has taken a write: a dial
			// handing back an already-dead upstream then fails above, on this
			// goroutine, and the retry is reached. A reader started before the
			// write would race it for the id instead, leaving scheduling order
			// to decide whether the client saw a transparent retry or an error.
			l.cache(conn)
			return
		}
		// A failed write and conn's own reader seeing the upstream die are the
		// same event racing, and whoever claims the id owns the reply.
		claimed := l.r.claim(id)
		// Forgotten unconditionally: a write can only fail on the connection the
		// lane just used, so l.conn is either conn itself or already nil.
		l.conn = nil
		_ = conn.Close()
		if !claimed {
			// Its reader answered this call already. Retrying would reply again
			// to an id the client has closed — a reply it ignores, so the retry
			// would look fine while the client kept the error.
			return
		}
	}
	// A notification has nowhere to report a failure to, which refuse is what
	// already knows.
	l.r.refuse(req, jsonrpc.CodeInternalError, "gopls for %s: %v", l.worktree, err)
}

// track records a request as being in flight to worktree, and with conn once an
// upstream has taken it — a nil conn is a call routed but not yet written. Ids
// the client did not ask an answer for are not tracked.
//
// A retry passes through here twice: the write that failed lost the record to
// claim, and the call it is placing is live again.
func (r *router) track(id jsonrpc.ID, conn mcp.Connection, worktree string) {
	if !id.IsValid() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.awaitingUpstream[id] = owed{conn: conn, worktree: worktree}
}

// claim ends id's flight, reporting whether this caller is the party that got
// to answer it. Exactly one may, and taking the id off the awaited list is what
// confers that right — see also failInFlight, which claims in bulk. A
// notification is nobody's to answer, so claiming its absent id succeeds; an id
// already off the list has been answered by somebody else, and claiming it
// fails.
//
// A call no upstream has taken yet is nobody's to answer either, so it loses:
// only the party that took the record away from a connection owing a reply has
// won a race against that connection's reader.
func (r *router) claim(id jsonrpc.ID) bool {
	if !id.IsValid() {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	won := r.awaitingUpstream[id].conn != nil
	delete(r.awaitingUpstream, id)
	return won
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

// forward hands a message to the client, giving up once the session is over so
// that a stopped writer cannot strand its sender.
func (r *router) forward(msg jsonrpc.Message) {
	select {
	case r.out <- msg:
	case <-r.ctx.Done():
	}
}

// cache adopts conn as this lane's upstream and starts the goroutine that reads
// it. Both happen here and nowhere else, so that a lane's connection is never
// one nobody reads — that would swallow the upstream's answers, and its death,
// for the rest of the session. Idempotent: the common path re-offers a
// connection the lane already holds.
func (l *lane) cache(conn mcp.Connection) {
	if l.conn == conn {
		return
	}
	l.conn = conn
	go l.r.readFromUpstream(conn, l.worktree)
}

// upstream returns this lane's connection, dialling and handshaking one if it
// has none. ctx is the budget for the whole call, shared with the caller's
// other attempt: see send. A connection it dialled is not adopted here — see
// cache.
//
// A connection is initialized exactly once, and this is where that holds: it is
// answered from the state of the connection, not from the order the client's
// messages happened to arrive in. Either branch below hands back an upstream
// that has had its one initialize — the private replay for every message but
// the client's own, and the caller's own write for that one.
func (l *lane) upstream(ctx context.Context, initial bool) (mcp.Connection, error) {
	if l.conn != nil {
		if initial {
			// Cached means handshaken, so the client's initialize would be this
			// connection's second — which gopls answers with the error that ends
			// the session. One diagnosable error beats a dead upstream, and this
			// does not depend on the reader having seen the disorder.
			return nil, errors.New("the client's initialize reached an upstream that already has one")
		}
		return l.conn, nil
	}
	// The first attempt can burn the whole budget in the handshake. Dialling
	// again would spend an unbounded flock wait, and possibly a spawn, on a
	// handshake guaranteed to expire on its first write.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := l.dialBounded(ctx)
	if err != nil {
		return nil, err
	}
	// Only the first home connection receives the client's initialize directly.
	// Every later connection, including a restarted home daemon, is initialized
	// behind the client's back because the client handshakes only once.
	if !initial {
		if err := l.handshake(ctx, conn); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

// dialBounded dials this lane's upstream, giving up when budget is done.
//
// The connection cannot be dialled under budget itself: the SSE transport reads
// its stream under the dial's context for the connection's whole life, so a
// deadline there would cut a healthy upstream loose on expiry (H5). Hence a
// cancellable context of its own, cancelled only by a watchdog that is disarmed
// the moment the dial returns.
//
// This bounds the connect, not the whole of dialGopls: ensure's flock and
// readiness poll take no context and stay unbounded (§10).
func (l *lane) dialBounded(budget context.Context) (mcp.Connection, error) {
	ctx, cancel := context.WithCancel(l.r.ctx)
	watchdog := context.AfterFunc(budget, cancel)
	conn, err := l.r.dial(ctx, l.worktree)
	if !watchdog() {
		// The watchdog won: ctx is cancelled, so a connection handed back here
		// is already unreadable — its stream lives under that same context.
		if err == nil {
			_ = conn.Close()
		}
		// budget is provably done, so this names why: a deadline of send's own
		// making, or the session going away underneath it.
		return nil, budget.Err()
	}
	if err != nil {
		cancel()
		return nil, err
	}
	// Not cancelled here: this context is the connection's for as long as it
	// lives. It is handed to the connection instead, which releases it when it
	// is closed.
	return &boundedConn{Connection: conn, release: cancel}, nil
}

// boundedConn ties dialBounded's context to the life of its connection, so
// closing the connection releases it. It cannot be released when the dial
// returns — the SSE stream reads under it throughout — but every site that
// gives a connection up closes it. Left to the session context instead, the
// children accumulate: a wedged upstream redials on every call.
type boundedConn struct {
	mcp.Connection
	release context.CancelFunc
}

func (c *boundedConn) Close() error {
	defer c.release()
	return c.Connection.Close()
}

func (r *router) dialGopls(ctx context.Context, worktree string) (mcp.Connection, error) {
	port, err := r.m.ensure(worktree)
	if err != nil {
		return nil, err
	}
	conn, err := (&mcp.SSEClientTransport{Endpoint: mcpURL(port)}).Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect to shared gopls MCP: %w", err)
	}
	return conn, nil
}

// handshake replays the client's initialize under a private id and swallows the
// reply, so the client sees exactly one initialize result — the home server's.
// ctx bounds it: this runs on the lane's goroutine, so an upstream that answers
// nothing would otherwise block every later call to that worktree too. It bounds
// nothing but the handshake — the connection keeps the context it was dialled
// with, which outlives this one on purpose; see dialBounded.
func (l *lane) handshake(ctx context.Context, conn mcp.Connection) error {
	initialize := l.r.initialize.Load()
	if initialize == nil {
		// The other half of upstream's rule: a connection gets exactly one
		// initialize, and there is none to replay yet. The reader already
		// refuses such a request, so this holds the requirement regardless of
		// who let the call through.
		return errors.New("tool call arrived before the client sent initialize")
	}
	if err := conn.Write(ctx, &jsonrpc.Request{ID: initID, Method: initialize.Method, Params: initialize.Params}); err != nil {
		return err
	}
	for {
		msg, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if resp, ok := msg.(*jsonrpc.Response); ok && resp.ID == initID {
			if resp.Error != nil {
				return resp.Error
			}
			break
		}
		// gopls asks for its roots the moment it has seen the capability in
		// initialize — which is inside this window, before the reader that
		// normally fields the question is even running.
		if l.r.answeredUpstream(ctx, conn, l.worktree, msg) {
			continue
		}
		l.r.forward(msg) // anything else it volunteered is the client's
	}
	return conn.Write(ctx, &jsonrpc.Request{Method: "notifications/initialized"})
}

func (r *router) readFromUpstream(conn mcp.Connection, worktree string) {
	for {
		msg, err := conn.Read(r.ctx)
		if err != nil {
			// An upstream dying is not the stdio client's problem. Its cached
			// connection fails the next write, which redials and retries once.
			r.failInFlight(conn, worktree, err)
			return
		}
		if r.answeredUpstream(r.ctx, conn, worktree, msg) {
			continue
		}
		if resp, ok := msg.(*jsonrpc.Response); ok && !r.claim(resp.ID) {
			// send() got to this id first — its write failed after the upstream
			// had already taken the message — and answered it. Forwarding now
			// would be a second reply to a call the client has closed.
			continue
		}
		r.forward(msg)
	}
}

// failInFlight answers every request still owed by conn with an error. The
// calls in flight when a gopls dies get no retry — nothing else will ever
// produce their ids — so left alone they strand a client that has no timeout.
//
// One worktree names them all: a connection belongs to one lane, and a lane to
// one worktree, so every id matched here was tracked under the caller's own.
func (r *router) failInFlight(conn mcp.Connection, worktree string, cause error) {
	// A slice, not a map: nothing looks these up, they are collected under the
	// lock and then walked once.
	var stranded []jsonrpc.ID
	r.mu.Lock()
	for id, owner := range r.awaitingUpstream {
		if owner.conn == conn {
			stranded = append(stranded, id)
			// The claim and the answer, same as refuse makes them, but once for
			// the whole set: taking the id here is what says this caller answers
			// it, and leaving it would route a later cancellation to a lane that
			// has stopped waiting. Nothing can put it back before the forward
			// below — a retry only re-tracks an id whose claim it won, and the
			// claim is this delete.
			delete(r.awaitingUpstream, id)
		}
	}
	r.mu.Unlock()

	for _, id := range stranded {
		r.forward(errorResponse(id, jsonrpc.CodeInternalError, "gopls for %s went away mid-call: %v", worktree, cause))
	}
}

// answeredUpstream handles a server-initiated request here rather than passing
// it to the client, and reports whether it did.
//
// Every read loop that can see one has to call this: a roots/list slipping
// through to the client comes back naming the tree the session opened in, the
// whole failure this bridge exists to prevent. Anything else is refused —
// an upstream holding an unanswerable question waits forever, so a definite
// error is the kinder reply.
//
// ctx is the caller's budget for talking to conn: the handshake answers roots
// inside its deadline (S2), a running reader has only the session.
func (r *router) answeredUpstream(ctx context.Context, conn mcp.Connection, worktree string, msg jsonrpc.Message) bool {
	req, ok := msg.(*jsonrpc.Request)
	if !ok || !req.ID.IsValid() {
		return false
	}
	if req.Method == "roots/list" {
		r.answerRoots(ctx, conn, worktree, req.ID)
		return true
	}
	_ = conn.Write(ctx, errorResponse(req.ID, jsonrpc.CodeMethodNotFound,
		"gopls-mcp-manager does not relay %q to the client", req.Method))
	return true
}

// answerRoots tells an upstream that its one workspace root is the worktree it
// serves, instead of forwarding the question to the client.
//
// This is what makes a gopls notice a file created after it started: its watcher
// only watches the roots the client reports, and it only asks for them once it
// has seen the roots capability in initialize — which is why withRootsCapability
// puts one there. Left to the client, every upstream would hear the same answer,
// the single tree the session was opened in, and every other worktree would watch
// nothing and answer "no package metadata" for a file that is right there on
// disk — indistinguishable from a genuinely broken tree.
func (r *router) answerRoots(ctx context.Context, conn mcp.Connection, worktree string, id jsonrpc.ID) {
	// Two strings in a fixed shape: marshaling them cannot fail, and there would
	// be nobody to tell anyway — the id belongs to the upstream, so an error
	// reply sent to the client would answer a question it never asked.
	result, _ := json.Marshal(mcp.ListRootsResult{Roots: []*mcp.Root{{
		URI:  (&url.URL{Scheme: "file", Path: worktree}).String(),
		Name: filepath.Base(worktree),
	}}})
	// A write failure needs no recovery, and must not touch the lane holding
	// this connection: that state belongs to the lane's own goroutine, and this
	// runs on whichever one is reading the upstream. Left alone, the upstream
	// keeps the file set it started with — today's behaviour — and the next call
	// for it redials anyway.
	_ = conn.Write(ctx, &jsonrpc.Response{ID: id, Result: result})
}

// toolCallWorktrees reports the worktrees owning the path arguments of a
// tools/call, in the order the arguments name them and without repeats. A call
// naming no path we can resolve reports none.
//
// Every path is resolved rather than just the first, because more than one
// answer is not a tie to be broken: tools like go_diagnostics take a files
// array, no single gopls knows a tree it was not started for, and routing such
// a call by its first path answers about that worktree while saying nothing at
// all about the files in the other — an omission the client cannot see, since
// what comes back is a well-formed result for the files it did cover. Both are
// reported so that route can refuse it instead. The extra work is a memo lookup
// per path on the hit that already covers the routing path (see worktreeOf).
func (r *router) toolCallWorktrees(params json.RawMessage) []string {
	var call struct {
		Arguments struct {
			File  string   `json:"file"`
			Dir   string   `json:"dir"`
			Files []string `json:"files"`
		} `json:"arguments"`
	}
	if err := json.Unmarshal(params, &call); err != nil {
		return nil
	}
	// The scalar arguments are ranged over as an array rather than concatenated
	// with Files into one slice: this runs on the goroutine that routes every
	// worktree's calls, and the combined slice was a heap allocation on every
	// tools/call — including the overwhelmingly common one naming a single file.
	var found []string
	for _, path := range [...]string{call.Arguments.File, call.Arguments.Dir} {
		found = r.appendWorktreeOf(found, path)
	}
	for _, path := range call.Arguments.Files {
		found = r.appendWorktreeOf(found, path)
	}
	return found
}

// appendWorktreeOf adds the worktree owning path to found, unless the path is
// no evidence or names a worktree already there.
func (r *router) appendWorktreeOf(found []string, path string) []string {
	// An absent argument is no evidence, and a relative one is worse: it would
	// resolve against this process's own cwd — the home worktree — and beat the
	// sticky routing that was right, for every worktree but home. gopls's
	// schemas ask for absolute paths.
	if !filepath.IsAbs(path) {
		return found
	}
	worktree := r.worktreeOf(path)
	if worktree == "" || slices.Contains(found, worktree) {
		return found
	}
	return append(found, worktree)
}

// worktreeOf resolves one path argument, memoizing the answer under the
// directory holding it rather than the path itself: resolution shells out to
// git at ~13ms a call, every path in one directory has the same answer, and a
// session names many files under the same handful of directories. Only
// successes are cached, so a path that becomes resolvable later still gets its
// own lookup; a worktree removed mid-session keeps answering until the routed
// gopls reports the path itself.
//
// The verbatim path is memoized in front of that, because reaching the
// directory memo is not free: containingDir lstats every component of the
// argument, and a profile put two thirds of the routing path there — time the
// reader goroutine spends routing nobody else's call. Kept as a second map so
// that the directory memo's entry count stays exactly the number of git forks
// the session has paid for.
func (r *router) worktreeOf(path string) string {
	if worktree, ok := r.paths[path]; ok {
		return worktree
	}
	dir := containingDir(path)
	worktree, ok := r.worktrees[dir]
	if !ok {
		var err error
		if worktree, err = worktreeOfDir(r.ctx, dir); err != nil {
			return ""
		}
		r.worktrees[dir] = worktree
	}
	// ponytail: unbounded, like the directory memo it fronts — an entry is two
	// strings and a session names the files it is working on, not the tree. Cap
	// it if some caller ever walks a repository through here.
	r.paths[path] = worktree
	return worktree
}

// containingDir is input itself when it names a directory, and its parent
// otherwise — including when it cannot be stat'd at all, since a path we were
// handed and cannot see is a file far more often than a directory.
//
// Cleaned either way, because this is a memo key: a dir argument spelled with a
// trailing separator names the same directory as one without, and must not fork
// git twice. filepath.Dir cleans on its own.
//
// Symlinks are resolved first, because the reduction is lexical: a link to a
// file in another worktree is not a directory, so its parent would be the
// link's own and the named tree would never be asked. Resolving also makes the
// key physical, which lets two spellings of one tree share an entry (R4, R5).
//
// A failure leaves input alone: EvalSymlinks needs every component to exist,
// and a path that does not exist yet must still resolve through its parent
// (R2) — a file created moments ago is the ordinary case.
func containingDir(input string) string {
	if resolved, err := filepath.EvalSymlinks(input); err == nil {
		input = resolved
	}
	if info, err := os.Stat(input); err == nil && info.IsDir() {
		return filepath.Clean(input)
	}
	return filepath.Dir(input)
}

// worktreePath resolves a file or directory to the root of the worktree holding
// it. A path that does not exist, or is in no repository, resolves to an error:
// callers routing a tool call treat that as "no evidence, keep the current
// server", which then reports its own error for the path.
//
// The reduction to a directory happens here and in worktreeOf, which needs it
// for its memo key anyway — never twice on one argument: a second pass over a
// directory that does not exist climbs to its parent, resolving a path against
// a tree the caller never named. worktreeOfDir is what makes that unsayable.
func worktreePath(ctx context.Context, input string) (string, error) {
	return worktreeOfDir(ctx, containingDir(input))
}

// worktreeOfDir resolves a directory — never a file — to the root of the
// worktree holding it.
//
// The context is the caller's own — the session's on the routing path — so that
// a git parked on a dead mount is cancelled when the session ends rather than
// outliving it: this runs on the goroutine that reads the client, and a hang
// here is a teardown that waits out the timeout below with nothing left to
// serve. The timeout stays on top of it, because the caller's context may have
// no deadline at all.
func worktreeOfDir(ctx context.Context, dir string) (string, error) {
	// Bounded, because this runs on the goroutine that reads the client: a git
	// that never returns — a stalled network filesystem is enough — would wedge
	// the session with neither side timing out, which is the failure H5 bounds
	// for the handshake. Generous, since rev-parse reads no tree: only a hang
	// should ever reach it.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// git -C resolves its own cwd physically, so the toplevel it prints already
	// has the symlinks taken out of it — and a relative dir resolves against
	// this process's cwd, which is what the "." default means.
	out, err := exec.CommandContext(ctx, "git", "-C", dir, "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse --show-toplevel: %w", err)
	}
	worktree, err := filepath.EvalSymlinks(strings.TrimSpace(string(out)))
	if err != nil {
		return "", err
	}
	// Refused at the one place a worktree is minted, so no port is allocated and
	// no gopls spawned for a path the map cannot hold: a Unix path is arbitrary
	// bytes, but the map file is JSON, and json.Marshal replaces invalid UTF-8
	// with U+FFFD rather than failing (§10). writeMap refuses it too — this is
	// what keeps it from ever getting that far.
	if !utf8.ValidString(worktree) {
		return "", fmt.Errorf("worktree path is not valid UTF-8: %q", worktree)
	}
	return worktree, nil
}
