package main

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/url"
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

// One routing loop commits route decisions. Each lane owns its upstream.
// Upstream readers share request state under mu.
type router struct {
	ctx context.Context

	owner     *supervisor
	home      string
	lanes     map[string]*lane    // worktree -> its upstream, see lane
	lanesMu   sync.Mutex          // protects lane lookup by the cancellation reader
	worktrees map[string]string   // containing directory -> worktree, see worktreeOf
	paths     map[string]pathMemo // path argument, verbatim -> its resolution, see worktreeOf
	memoMu    sync.Mutex          // guards both memo maps and the epoch below
	memo      memoState           // expiry epoch and observations, under memoMu
	// The connection keeps this context after dial. See dialBounded.
	dial       func(context.Context, string, int) (mcp.Connection, error)
	plan       func(worktree string, global []string) watchPlan
	lookupCold func(context.Context, jsontext.Value) []string

	mu sync.Mutex
	// Claim the matching admission before replying. See finish.
	awaitingUpstream map[jsonrpc.ID]owed
	perWorktree      map[string]int
	remote           *ownerSession

	limits     config.Limits
	usage      requestUsage
	finalUsage chan requestUsage // one final snapshot, consumed by the existing metrics worker
	usageDone  chan struct{}
	sseBudget  *transport.Budget

	out                   chan jsonrpc.Message
	errs                  chan error
	retentionMu           sync.Mutex
	retentionClosed       bool
	initializeReservation *ownerMessageReservation

	// Each private handshake loads this template and fails if it is unset.
	initialize atomic.Pointer[jsonrpc.Request]
	sticky     string // worktree of the last path-bearing call
	stateless  bool   // HTTP calls must not inherit another client's worktree

	upstreamReaders atomic.Int64
}

func newRouter(ctx context.Context, owner *supervisor, home string) *router {
	r := &router{
		ctx: ctx, owner: owner, home: home,
		lanes: make(map[string]*lane), worktrees: make(map[string]string), paths: make(map[string]pathMemo),
		awaitingUpstream: make(map[jsonrpc.ID]owed), perWorktree: make(map[string]int),
		remote: &ownerSession{perWorktree: make(map[string]int)},
		limits: owner.m.limits, sseBudget: owner.sseBudget,
		usage:      requestUsage{Rejections: make(map[string]int), Stages: make(map[string]stageTiming)},
		finalUsage: make(chan requestUsage, 1), usageDone: make(chan struct{}),
		out: make(chan jsonrpc.Message, 64), errs: make(chan error, 4),
	}
	r.dial, r.plan = r.dialGopls, planWatch
	return r
}

func serve(ctx context.Context, owner *supervisor, home string, stdio mcp.Connection) error {
	return serveRouter(newRouter(ctx, owner, home), stdio)
}

func serveRouter(r *router, stdio mcp.Connection) error {
	m := r.owner.m
	ctx, cancel := context.WithCancel(r.ctx)
	r.ctx = ctx
	defer cancel()
	if r.limits.Metrics {
		defer r.finishUsage()
		go r.reportUsage(m.diagnosticWriter())
	}
	// Join the lane producer before closing its queues. A blocked stdout
	// write can survive Close, so shutdown cannot join the writer.
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
	r.stopRouting()
	r.releaseRetainedMessages()
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, mcp.ErrConnectionClosed) {
		return nil
	}
	return err
}

// One reporter bounds blocked metric output to one goroutine per session.
// Snapshots contain fixed stage/reason names, never worktree labels.
func (r *router) reportUsage(output io.Writer) {
	defer close(r.usageDone)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			// The session owner publishes after clearing request accounting.
			// Keep final output on this same worker: blocked stderr must neither
			// block shutdown nor create an additional diagnostic goroutine.
			usage := <-r.finalUsage
			_ = json.MarshalEncode(jsontext.NewEncoder(output), usage)
			return
		case <-ticker.C:
			if err := json.MarshalEncode(jsontext.NewEncoder(output), r.requestUsage()); err != nil {
				r.reportError(fmt.Errorf("metrics output: %w", err))
				return
			}
		}
	}
}

func (r *router) finishUsage() {
	if r.limits.Metrics {
		select {
		case r.finalUsage <- r.requestUsage():
		default:
		}
		// Give writable diagnostics a chance to flush before CLI exit, without
		// making an unread stderr pipe a prerequisite for session shutdown.
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.usageDone:
		case <-timer.C:
		}
	}
}

// Stop the queue producer before closing the lanes.
func (r *router) closeLanes() {
	for _, l := range r.lanes {
		close(l.reqs)
	}
}

// The routing producer must have stopped before closing the lanes.
func (r *router) stopRouting() {
	r.closeLanes()
	for _, l := range r.lanes {
		<-l.done
	}
	r.clearRequests()
}

func (r *router) writeToClient(stdio mcp.Connection) {
	for {
		select {
		case <-r.ctx.Done():
			return
		case msg := <-r.out:
			err := stdio.Write(r.ctx, msg)
			releaseMessage(msg)
			if err != nil {
				r.reportError(err)
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
		if call.barrier != nil {
			close(call.barrier)
			continue
		}
		r.routePrepared(call)
	}
}

func (r *router) readIngress(stdio mcp.Connection, incoming chan<- delivery) {
	for {
		msg, err := stdio.Read(r.ctx)
		if err != nil {
			r.reportError(err)
			return
		}
		if peer, ok := stdio.(*restartPeer); ok {
			if barrier := peer.barrier(msg); barrier != nil {
				select {
				case incoming <- delivery{barrier: barrier}:
				case <-r.ctx.Done():
					return
				}
				continue
			}
		}
		req, ok := msg.(*jsonrpc.Request)
		if !ok {
			// Not a request, and this bridge asks the client nothing, so a stray
			// response has nothing to route to.
			continue
		}
		if req.Method == "initialize" {
			req.Params = protocol.WithRootsCapability(req.Params)
			owner := r.owner
			if r.initialize.Load() != nil {
				r.refuse(req, jsonrpc.CodeInvalidRequest, "client already initialized")
				continue
			}
			if len(req.Params)+len(req.Method) > maxOwnerInitializeBytes || !validOwnerID(req.ID) {
				r.reportError(fmt.Errorf("initialize or opaque id exceeds owner limit"))
				return
			}
			var err error
			r.initializeReservation, err = owner.reserveMessageBytes(maxOwnerInitializeBytes)
			if err != nil {
				r.reportError(err)
				return
			}

			r.initialize.Store(req)
		} else if r.initialize.Load() == nil {
			// Enforce initialize in wire order before dispatching to lanes.
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
			releaseMessage(call.req)
			return
		default:
			r.rejected("routing_queue")
			r.failIngress(call, -32000, "routing queue is full")
		}
	}
}

// forward hands a message to the client. Saturation fails the session: a reader
// may already have released its response's operation credit, so parking here
// would let reconnects accumulate readers and decoded responses without a bound.
// Never silently discard a terminal response while leaving the session usable.
func (r *router) forward(msg jsonrpc.Message) {
	if err := retainMessage(r.owner, msg); err != nil {
		r.reportError(err)
		return
	}
	r.retentionMu.Lock()
	defer r.retentionMu.Unlock()
	if r.retentionClosed {
		releaseMessage(msg)
		return
	}
	select {
	case r.out <- msg:
	case <-r.ctx.Done():
		releaseMessage(msg)
	default:
		releaseMessage(msg)
		r.reportError(fmt.Errorf("client output queue is full"))
	}
}

// One pending failure is enough to stop serve. A second reporter must never
// block the ingress reader that serve waits for during shutdown.
func (r *router) reportError(err error) {
	select {
	case r.errs <- err:
	default:
	}
}

func errorResponse(id jsonrpc.ID, code int64, format string, args ...any) *jsonrpc.Response {
	return &jsonrpc.Response{ID: id, Error: &jsonrpc.Error{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}}
}

func (r *router) readFromUpstream(conn mcp.Connection, worktree string) {
	parent := r.ctx
	if c, ok := conn.(interface{ lifetime() context.Context }); ok && c.lifetime() != nil {
		parent = c.lifetime()
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// Plan roots off the read loop so worktree walks can run alongside replies.
	asks := make(chan jsonrpc.ID, 4)
	defer close(asks)
	go r.answerRootsAsks(ctx, conn, worktree, asks)
	for {
		msg, err := conn.Read(ctx)
		if err != nil {
			// Fail this connection's calls while keeping the client attached.
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
				// Another terminal path may already have claimed this admission.
				continue
			}
		}
		r.forward(msg)
	}
}

// After a failed roots reply, drain asks until the reader closes the queue.
func (r *router) answerRootsAsks(parent context.Context, conn mcp.Connection, worktree string, asks <-chan jsonrpc.ID) {
	failed := false
	for id := range asks {
		if failed {
			continue
		}
		ctx, cancel := context.WithTimeout(parent, sendBudget)
		err := r.answerRoots(ctx, conn, worktree, id)
		cancel()
		if err != nil {
			failed = true
			_ = conn.Close()
			r.failInFlight(conn, worktree, err)
		}
	}
}

// Handle server requests on their originating connection. Only handshake
// roots replies run inline. Steady-state roots use answerRootsAsks.
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

// Report filtered roots for this upstream so gopls watches its own worktree.
func (r *router) answerRoots(ctx context.Context, conn mcp.Connection, worktree string, id jsonrpc.ID) error {
	owner := r.owner
	var g *ownedGeneration
	owner.mu.Lock()
	g = owner.generations[worktree]
	if !owner.rootsGenerationLocked(worktree, g, conn) {
		owner.mu.Unlock()
		return fmt.Errorf("roots plan belongs to a retired generation")
	}
	if g.rootReplies == nil {
		g.rootReplies = make(chan struct{}, 1)
	}
	replies := g.rootReplies
	owner.mu.Unlock()
	// The first watcher view must not depend on HTTP acknowledgement order.
	select {
	case replies <- struct{}{}:
		defer func() { <-replies }()
	case <-ctx.Done():
		return ctx.Err()
	case <-owner.ctx.Done():
		return owner.ctx.Err()
	}
	owner.mu.Lock()
	current := owner.rootsGenerationLocked(worktree, g, conn)
	owner.mu.Unlock()
	if !current {
		return fmt.Errorf("roots plan belongs to a retired generation")
	}

	plan, err := r.planRoots(ctx, worktree)
	if err != nil {
		return err
	}
	if plan.err != nil {
		return plan.err
	}
	quota, estimateErr := ownerFilesystemCall(ctx, owner, func() watchFDQuota {
		return owner.estimateQuota(ctx, plan, owner.m.limits.OpenFiles)
	})
	owner.mu.Lock()
	if !owner.rootsGenerationLocked(worktree, g, conn) {
		owner.mu.Unlock()
		return fmt.Errorf("roots plan belongs to a retired generation")
	}
	err = errors.Join(estimateErr, quota.err)
	if err == nil && quota.quota > g.fdQuota {
		err = fmt.Errorf("roots plan needs %d descriptors but generation reserved %d; a new generation is required", quota.quota, g.fdQuota)
	}
	if err != nil {
		g.err = err
		owner.mu.Unlock()
		return err
	}
	owner.mu.Unlock()

	//nolint:staticcheck // Legacy gopls SSE sessions still need roots for file watching.
	roots := make([]*mcp.Root, len(plan.roots))
	for i, dir := range plan.roots {
		//nolint:staticcheck // Legacy gopls SSE sessions still need roots for file watching.
		roots[i] = &mcp.Root{URI: (&url.URL{Scheme: "file", Path: dir}).String(), Name: filepath.Base(dir)}
	}
	//nolint:staticcheck // Legacy gopls SSE sessions still need roots for file watching.
	result, err := json.Marshal(mcp.ListRootsResult{Roots: roots})
	if err != nil {
		return err
	}
	writeErr := conn.Write(ctx, &jsonrpc.Response{ID: id, Result: result})
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.rootsGenerationLocked(worktree, g, conn) {
		if writeErr != nil {
			return writeErr
		}
		return fmt.Errorf("roots plan belongs to a retired generation")
	}

	if errors.Is(writeErr, transport.ErrMessageTooLarge) || errors.Is(writeErr, errOwnerPreDelivery) {
		return writeErr
	}
	plan.uncertain = writeErr != nil
	if plan.uncertain {
		g.err = fmt.Errorf("roots delivery is uncertain: %w", writeErr)
	}
	r.owner.m.rememberWatch(worktree, serverOf(conn), plan)
	return writeErr
}

func (s *supervisor) rootsGenerationLocked(worktree string, g *ownedGeneration, conn mcp.Connection) bool {
	upstream, ok := conn.(*ownedUpstream)
	return ok && upstream.s == s && upstream.g == g && g != nil && s.generations[worktree] == g && g.state == generationReady
}

// One owner worker bounds uninterruptible planning across all sessions.
func (r *router) planRoots(ctx context.Context, worktree string) (watchPlan, error) {
	if err := ctx.Err(); err != nil {
		return watchPlan{}, err
	}
	started := time.Now()
	plan, err := ownerFilesystemCall(ctx, r.owner, func() watchPlan { return r.plan(worktree, r.limits.DirectoryFilters) })
	plan.started = started
	return plan, err
}
