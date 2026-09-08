package main

import (
	"context"
	"errors"
	"fmt"
	"time"

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

// laneQueue bounds each worktree's pending deliveries and advisory controls.
// Excess requests are refused; excess notifications are dropped. Neither queue
// can hold up routing to another worktree.
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
	reqs     chan delivery
	controls chan cancellation
	done     chan struct{}

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
		reqs:     make(chan delivery, laneQueue),
		controls: make(chan cancellation, laneQueue),
		done:     make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (l *lane) run() {
	defer close(l.done)
	// Releases this lane's registration on the session context. The connections
	// it dialled keep contexts of their own, rooted at the session rather than
	// here, so this ends the lane's budgets and nothing else — see dialBounded.
	defer l.cancel()
	go l.runControls()
draining:
	for call := range l.reqs {
		// Whatever is left in the queue when the session ends is not worth an
		// upstream: dialling would run ensure, spawning a whole gopls for a client
		// that has already gone away, and the answer would have nowhere to go.
		//
		// Asked through Done rather than Err: Err takes the session context's own
		// mutex, which every lane would then share once a message — the very
		// contention lane.ctx exists to keep off the session context.
		select {
		case <-l.r.ctx.Done():
			call.cancel()
			break draining
		default:
		}
		l.send(call.ctx, call.req)
		call.cancel()
	}
	for call := range l.reqs {
		call.cancel()
	}
	if l.conn != nil {
		_ = l.conn.Close()
	}
}

type delivery struct {
	req    *jsonrpc.Request
	ctx    context.Context
	cancel context.CancelFunc
}

// A control carries the exact connection that owes its id. It cannot dial or
// accidentally cancel a call on a replacement connection.
type cancellation struct {
	req      *jsonrpc.Request
	conn     mcp.Connection
	deadline time.Time
	placed   <-chan struct{}
}

func (l *lane) runControls() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case control := <-l.controls:
			ctx, cancel := context.WithDeadline(l.ctx, control.deadline)
			if control.placed != nil {
				select {
				case <-control.placed:
				case <-ctx.Done():
				}
			}
			if ctx.Err() == nil {
				_ = control.conn.Write(ctx, control.req)
			}
			cancel()
		}
	}
}

func (l *lane) send(parent context.Context, req *jsonrpc.Request) {
	// Derived here rather than passed in, so that the id and the message it is
	// sent with cannot disagree; see upstream for the one-initialize rule.
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
	ctx, cancel := context.WithTimeout(parent, sendBudget)
	defer cancel()
	// Retain the delivery cancellation even across a retry's private handshake.
	l.r.place(id, nil, cancel, nil)
	var err error
	for range 2 {
		if err = ctx.Err(); err != nil {
			break
		}
		// A deadline can have passed before its timer callback is scheduled.
		// Check the clock too, so queued work cannot sneak into a dial then.
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			err = context.DeadlineExceeded
			break
		}
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
		placed := make(chan struct{})
		l.r.place(id, conn, cancel, placed)
		err = conn.Write(ctx, req)
		close(placed)
		if err == nil {
			// Cached only once the connection has taken a write: a dial
			// handing back an already-dead upstream then fails above, on this
			// goroutine, and the retry is reached. A reader started before the
			// write would race it for the id instead, leaving scheduling order
			// to decide whether the client saw a transparent retry or an error.
			l.cache(conn)
			l.r.startExecution(id)
			return
		}
		// A failed write and conn's own reader seeing the upstream die are the
		// same event racing, and whoever claims the id owns the reply.
		claimed := l.r.retry(id, conn)
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
	code := int64(jsonrpc.CodeInternalError)
	if errors.Is(err, context.Canceled) {
		code = -32800
	}
	l.r.fail(id, code, "gopls for %s: %v", l.worktree, err)
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
// messages happened to arrive in.
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
	// again would attempt manager work for a handshake guaranteed to expire
	// on its first write.
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
// ensure also observes this cancellation during lock acquisition, probes and
// readiness. Failed-child cleanup can outlive the delivery budget briefly.
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
	// Handed to the connection, which releases it on Close — see boundedConn.
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
	port, err := r.m.ensure(ctx, worktree)
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
