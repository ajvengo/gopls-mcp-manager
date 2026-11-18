package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/transport"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Allow for readiness and handshake within one delivery budget.
const sendBudget = readyTimeout + 20*time.Second

// Each connection uses the private initialize ID once.
var initID, _ = jsonrpc.MakeID("gopls-mcp-manager-init")

// laneQueue bounds each worktree's pending deliveries and advisory controls.
// Excess requests are refused; excess notifications are dropped. Neither queue
// can hold up routing to another worktree.
const laneQueue = 64

// A lane serializes delivery for one worktree, isolating cold starts.
type lane struct {
	r         *router
	worktree  string
	reqs      chan delivery
	controls  chan cancellation
	controlMu sync.Mutex
	done      chan struct{}
	// Serialize operation placement with advisory check/write, so an old
	// cancellation cannot race a new use of the same connection/id. Waiting
	// for this gate spends the caller's existing delivery budget.
	writes chan struct{}

	// ctx owns this lane's control worker and shutdown. Delivery contexts start
	// at ingress so queueing and resolution share the request lifetime before
	// its destination lane is known.
	ctx    context.Context
	cancel context.CancelFunc

	// Only the delivery loop owns conn.
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
		writes:   make(chan struct{}, 1),
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (l *lane) run() {
	defer close(l.done)
	// Dialled connections have their own session-scoped contexts.
	defer l.cancel()
	go l.runControls()
	for call := range l.reqs {
		// Drain on shutdown without starting gopls for a disconnected client.
		select {
		case <-l.r.ctx.Done():
		default:
			l.r.timed("delivery_queue", call.queued)
			l.send(call.ctx, call.req, call.state)
		}
		call.cancel()
		releaseMessage(call.req)
	}
	if l.conn != nil {
		_ = l.conn.Close()
	}
}

type delivery struct {
	queued time.Time
	state  *callState
	req    *jsonrpc.Request
	ctx    context.Context
	cancel context.CancelFunc

	barrier chan struct{}
}

// A control carries the exact connection that owes its id. It cannot dial or
// accidentally cancel a call on a replacement connection.
type cancellation struct {
	req      *jsonrpc.Request
	id       jsonrpc.ID
	conn     mcp.Connection
	deadline time.Time
	placed   <-chan struct{}
	state    *callState
}

func (l *lane) runControls() {
	defer func() {
		l.controlMu.Lock()
		defer l.controlMu.Unlock()
		for {
			select {
			case control := <-l.controls:
				releaseMessage(control.req)
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-l.ctx.Done():
			return
		case control := <-l.controls:
			l.sendCancellation(control)
			releaseMessage(control.req)
		}
	}
}

func (l *lane) lockWrites(ctx context.Context) error {
	select {
	case l.writes <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-l.writes
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *lane) sendCancellation(control cancellation) {
	ctx, cancel := context.WithDeadline(l.ctx, control.deadline)
	defer cancel()
	if control.placed != nil {
		select {
		case <-control.placed:
		case <-ctx.Done():
			return
		}
	}
	if err := l.lockWrites(ctx); err != nil {
		return
	}
	defer func() { <-l.writes }()
	l.r.mu.Lock()
	stale := l.r.owner.staleCancellation(operationKey{control.conn, control.id}, control.state)
	l.r.mu.Unlock()
	if stale {
		return // this id now belongs to a later upstream operation
	}
	// With no operation left, sending is harmless: the gate prevents a new
	// operation from placing this id until this write has completed.
	_ = control.conn.Write(ctx, control.req)
}

func (l *lane) send(parent context.Context, req *jsonrpc.Request, expected *callState) {
	id, initial := req.ID, req.Method == "initialize"
	// Share one delivery deadline across dials, handshakes and SSE POSTs.
	ctx, cancel := context.WithTimeout(parent, sendBudget)
	defer cancel()
	// Retain the delivery cancellation even across a retry's private handshake.
	state, owned := l.r.placeDelivery(id, expected, cancel)
	if !owned {
		return
	}
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
		// A notification cannot justify spawning and initializing gopls.
		if l.conn == nil && !id.IsValid() {
			return
		}
		var conn mcp.Connection
		conn, err = l.upstream(ctx, initial)
		if err != nil {
			continue
		}
		// Reserve before Write because gopls can answer before Write returns.
		placed := make(chan struct{})
		if err := l.lockWrites(ctx); err != nil {
			l.giveUp(conn, id, state, err)
			return
		}
		if err := l.r.beginOperation(id, state, conn, l.worktree, cancel, placed); err != nil {
			<-l.writes
			l.giveUp(conn, id, state, err)
			return
		}
		err = conn.Write(ctx, req)
		close(placed)
		<-l.writes
		if errors.Is(err, transport.ErrMessageTooLarge) || errors.Is(err, errOwnerPreDelivery) {
			// The bounded transport rejected before POST; unlike a write failure,
			// this proves no upstream operation was started by this attempt.
			l.r.mu.Lock()
			l.r.owner.answered(operationKey{conn, id})
			l.r.mu.Unlock()
			l.giveUp(conn, id, state, err)
			return
		}
		if err == nil {
			// Start the reader after the first write so it cannot race delivery
			// failure for terminal ownership.
			l.cache(conn)
			l.r.startExecution(id, state)

			return
		}
		// Once POST/Write started, a transport error does not prove rejection.
		// Retain its operation credit and report uncertainty rather than replay
		// a mutation. A known retired cache is discarded before Write in upstream.
		l.conn = nil
		_ = conn.Close()
		l.r.failDelivery(id, state, jsonrpc.CodeInternalError,
			fmt.Sprintf("gopls for %s: upstream write failed; request may have executed: %v", l.worktree, err))
		return
	}
	code := int64(jsonrpc.CodeInternalError)
	if errors.Is(err, context.Canceled) {
		code = -32800
	}
	l.r.failDelivery(id, state, code, fmt.Sprintf("gopls for %s: %v", l.worktree, err))
}

// Close an unadopted connection because it has no reader to clean it up.
func (l *lane) giveUp(conn mcp.Connection, id jsonrpc.ID, state *callState, cause error) {
	if conn != l.conn {
		_ = conn.Close()
	}
	l.r.failDelivery(id, state, -32000, cause.Error())
}

// Adopt the connection with its reader so replies always have a consumer.
func (l *lane) cache(conn mcp.Connection) {
	if l.conn == conn {
		return
	}
	l.conn = conn
	l.r.startUpstreamReader(conn, l.worktree)
}

// Initialize each new connection once. cache adopts it after delivery.
func (l *lane) upstream(ctx context.Context, initial bool) (mcp.Connection, error) {
	if current, ok := l.conn.(interface{ available() bool }); ok && !current.available() {
		// This transport retired before any new Write; redial the first fresh
		// call without replaying an ambiguously delivered operation.
		_ = l.conn.Close()
		l.conn = nil
	}
	if l.conn != nil {
		if initial {
			// A second initialize would end the upstream session.
			return nil, errors.New("the client's initialize reached an upstream that already has one")
		}
		return l.conn, nil
	}
	// Recheck the shared budget before any new dial.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := l.dialBounded(ctx)
	if err != nil {
		return nil, err
	}
	// The first home connection uses the client's initialize. All later
	// handshakes are private.
	if !initial {
		if err := l.handshake(ctx, conn); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

// SSE retains the dial context for the stream's lifetime. Use a watchdog to
// enforce the delivery deadline only until dial returns.
func (l *lane) dialBounded(budget context.Context) (mcp.Connection, error) {
	ctx, cancel := context.WithCancel(l.r.ctx)
	watchdog := context.AfterFunc(budget, cancel)
	server, err := l.r.owner.ensureRecord(ctx, l.worktree)
	var conn mcp.Connection
	if err == nil {
		conn, err = l.r.dial(ctx, l.worktree, server.Port)
	}
	if !watchdog() {
		cancel()
		if conn != nil {
			_ = conn.Close()
		}
		return nil, budget.Err()
	}
	if err == nil {
		var owned mcp.Connection
		owned, err = l.r.owner.connection(conn, server, ctx, cancel)
		if err == nil {
			return owned, nil
		}
		_ = conn.Close()
	}
	cancel()
	return nil, err
}

func (r *router) dialGopls(ctx context.Context, worktree string, port int) (mcp.Connection, error) {
	conn, err := transport.ConnectSSE(ctx, mcpURL(port), r.limits.MessageBytes, r.sseBudget)
	if err != nil {
		return nil, fmt.Errorf("connect to shared gopls MCP: %w", err)
	}
	return conn, nil
}

// serverOf associates watch snapshots with the generation that received them.
func serverOf(conn mcp.Connection) serverID {
	if c, ok := conn.(interface{ server() serverID }); ok {
		return c.server()
	}
	return serverID{}
}

// Consume the private initialize reply here. The deadline applies only to
// the handshake, leaving the SSE stream alive for later requests.
func (l *lane) handshake(ctx context.Context, conn mcp.Connection) error {
	defer l.r.timed("handshake", time.Now())
	initialize := l.r.initialize.Load()
	if initialize == nil {
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
		// gopls can ask for roots before the steady-state reader starts.
		if answered, err := l.r.answeredUpstream(ctx, conn, l.worktree, msg); err != nil {
			return err
		} else if answered {
			continue
		}
		l.r.forward(msg) // anything else it volunteered is the client's
	}
	return conn.Write(ctx, &jsonrpc.Request{Method: "notifications/initialized"})
}
