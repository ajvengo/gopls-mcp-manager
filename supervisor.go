package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"
	"github.com/ajvengo/gopls-mcp-manager/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const guardianFDAllowance = guardianFDReservation

const childFDHeadroom = 256

// The estimate bounds our admitted transports/pipes plus control headroom.
// It is a reservation in the global managed FD budget, not measured usage.
func supervisorFDAllowance(limits config.Limits) (int, error) {
	for _, count := range []int{limits.OwnerSessions, limits.Lanes, limits.SharedServers, limits.HTTPConnections} {
		if count <= 0 || count > 1<<20 {
			return 0, fmt.Errorf("supervisor resource counts must be between 1 and 1048576")
		}
	}
	needed := int64(256) + int64(limits.HTTPConnections) + 2*int64(limits.OwnerSessions)*int64(limits.Lanes) + 4*int64(limits.SharedServers)
	if needed > int64(limits.OpenFiles) {
		return 0, fmt.Errorf("GOPLS_MANAGER_MAX_OPEN_FILES=%d cannot hold supervisor transport/control reservation %d", limits.OpenFiles, needed)
	}
	return int(needed), nil
}

func enforceSupervisorFDLimit(limits config.Limits) error {
	needed, err := supervisorFDAllowance(limits)
	if err != nil {
		return err
	}
	var current syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &current); err != nil {
		return err
	}
	if current.Max < uint64(needed) {
		return fmt.Errorf("supervisor needs %d file descriptors but hard RLIMIT_NOFILE is %d", needed, current.Max)
	}
	current.Cur = uint64(needed)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &current); err != nil {
		return fmt.Errorf("set supervisor file descriptor reservation: %w", err)
	}
	return nil
}

func (s *supervisor) reserveGenerationFDs(ctx context.Context, g *ownedGeneration) error {
	quota, err := ownerFilesystemCall(ctx, s, func() watchFDQuota {
		plan := planWatch(g.worktree, s.m.limits.DirectoryFilters)
		return s.estimateQuota(ctx, plan, s.m.limits.OpenFiles)
	})
	if err != nil {
		return err
	}
	if quota.err != nil {
		return quota.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fdReserved+quota.quota+guardianFDAllowance > s.m.limits.OpenFiles {
		return fmt.Errorf("managed file descriptor reservation exhausted; no process spawned")
	}
	g.fdQuota = quota.quota
	s.fdReserved += quota.quota + guardianFDAllowance
	return nil
}

type watchFDQuota struct {
	quota int
	err   error
}

// Admission and later roots replies use the same full watched-tree estimator.
// Replanning may change roots after a module/filter edit, but the child's kernel
// quota and the charged reservation remain immutable for its entire generation.
func estimateWatchFDQuota(ctx context.Context, plan watchPlan, managedLimit int) watchFDQuota {
	if plan.err != nil {
		return watchFDQuota{err: plan.err}
	}
	count := childFDHeadroom
	budget := &watchBudget{remaining: defaultWatchLimits()}
	for _, root := range plan.roots {
		count++
		if count+guardianFDAllowance > managedLimit || count > 1<<20 {
			return watchFDQuota{err: fmt.Errorf("watched tree cannot fit the configured managed file descriptor budget")}
		}
		if err := countWatchDescriptors(ctx, root, 0, budget, &count, managedLimit); err != nil {
			return watchFDQuota{err: err}
		}
	}
	return watchFDQuota{quota: count}
}

func countWatchDescriptors(ctx context.Context, dir string, depth int, budget *watchBudget, count *int, limit int) error {
	if depth > maxWatchDepth {
		return fmt.Errorf("FD preflight directory depth exceeds %d", maxWatchDepth)
	}
	f, err := os.OpenFile(dir, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	for {
		entries, err := f.ReadDir(watchDirectoryBatch)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := takeWatchBudget(&budget.remaining.entries, 1, "FD preflight entry count"); err != nil {
				return err
			}
			if err := budget.nameBytes(dir, entry.Name()); err != nil {
				return err
			}
			if entry.IsDir() && skippedByWatcher(entry.Name()) {
				continue
			}
			*count++
			if *count+guardianFDAllowance > limit || *count > 1<<20 {
				return fmt.Errorf("watched tree cannot fit the configured managed file descriptor budget")
			}
			if entry.IsDir() {
				if err := countWatchDescriptors(ctx, filepath.Join(dir, entry.Name()), depth+1, budget, count, limit); err != nil {
					return err
				}
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}

const maxOwnerIDBytes = 1024

const maxOwnerInitializeBytes = 64 << 10

// Reserve maximum wire payload per message; this bounds retained payload,
// not transient codec allocations or total Go heap.
type ownerMessageReservation struct {
	s     *supervisor
	bytes int
	once  sync.Once
}

func validOwnerID(id jsonrpc.ID) bool {
	value, ok := id.Raw().(string)
	return !ok || len(value) <= maxOwnerIDBytes
}

func ownerMessageID(msg jsonrpc.Message) jsonrpc.ID {
	switch value := msg.(type) {
	case *jsonrpc.Request:
		return value.ID
	case *jsonrpc.Response:
		return value.ID
	}
	return jsonrpc.ID{}
}

func validateHTTPWireID(body []byte) error {
	var envelope struct {
		ID jsontext.Value `json:"id"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	if len(envelope.ID) != 0 && envelope.ID[0] == '"' {
		// The encoded string can expand each original byte to six characters.
		if len(envelope.ID) > 6*maxOwnerIDBytes+2 {
			return fmt.Errorf("opaque request id exceeds owner limit")
		}
		var value string
		if err := json.Unmarshal(envelope.ID, &value); err != nil {
			return err
		}
		if len(value) > maxOwnerIDBytes {
			return fmt.Errorf("opaque request id exceeds owner limit")
		}
	}
	return nil
}

func (s *supervisor) reserveMessageBytes(bytes int) (*ownerMessageReservation, error) {

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.ctx.Err() != nil {
		return nil, fmt.Errorf("supervisor is shutting down")
	}
	if bytes <= 0 || s.retainedBytes > s.m.limits.OwnerRetainedBytes-bytes {
		return nil, fmt.Errorf("global retained message budget exhausted")
	}
	s.retainedBytes += bytes
	return &ownerMessageReservation{s: s, bytes: bytes}, nil
}

func (r *ownerMessageReservation) release() {
	if r != nil {
		r.once.Do(func() {
			r.s.mu.Lock()
			r.s.retainedBytes -= r.bytes
			r.s.mu.Unlock()
		})
	}
}

// Extra stays off-wire and carries the reservation through every owner queue.
func retainMessage(s *supervisor, msg jsonrpc.Message) error {

	if !validOwnerID(ownerMessageID(msg)) {
		return fmt.Errorf("opaque request id exceeds %d bytes", maxOwnerIDBytes)
	}
	reservation, err := s.reserveMessageBytes(s.m.limits.MessageBytes)
	if err != nil {
		return err
	}
	switch value := msg.(type) {
	case *jsonrpc.Request:
		value.Extra = reservation
	case *jsonrpc.Response:
		value.Extra = reservation
	}
	return nil
}

func messageReservation(msg jsonrpc.Message) *ownerMessageReservation {
	var extra any
	switch value := msg.(type) {
	case *jsonrpc.Request:
		extra = value.Extra
	case *jsonrpc.Response:
		extra = value.Extra
	}
	reservation, _ := extra.(*ownerMessageReservation)
	return reservation
}

func releaseMessage(msg jsonrpc.Message) { messageReservation(msg).release() }

func (r *router) releaseRetainedMessages() {
	r.retentionMu.Lock()
	r.retentionClosed = true
	for {
		select {
		case msg := <-r.out:
			releaseMessage(msg)
		default:
			r.retentionMu.Unlock()
			r.initializeReservation.release()
			return
		}
	}
}

const supervisorTermGrace = 10 * time.Second

var errOwnerPreDelivery = errors.New("supervisor rejected before upstream delivery")

type generationState string

const (
	generationStarting    generationState = "starting"
	generationReady       generationState = "ready"
	generationDraining    generationState = "draining"
	generationTerminating generationState = "terminating"
	generationExited      generationState = "exited"
)

type ownedGeneration struct {
	id, worktree, reason  string
	state                 generationState
	record                record
	child                 *guardianChild
	startup               *guardianStartError
	ready                 chan struct{}
	exited                chan struct{}
	err                   error
	completed             time.Time
	writes                int
	connections           map[*ownedUpstream]struct{}
	drainDone             chan struct{}
	fdQuota               int
	terminating, cleaning bool
	rootReplies           chan struct{}
}

type ownerReservation struct{ worktree string }

type ownerOperation struct {
	generation *ownedGeneration
	abandoned  time.Time
	session    *ownerSession
	identity   *operationIdentity
}

// mu is the single admission/drain authority. No network/process/storage call
// happens while it is held. Router locks may acquire mu; mu never acquires a
// router lock or closes a connection while held.
type supervisor struct {
	ctx               context.Context
	m                 *manager
	boot              string
	mu                sync.Mutex
	generations       map[string]*ownedGeneration
	queued            map[*callState]ownerReservation
	operations        map[remoteKey]ownerOperation
	controls          map[remoteKey]ownerOperation
	now               func() time.Time
	closing           bool
	filesystem        chan ownerFilesystemJob
	sseBudget         *transport.Budget
	frontends         *supervisorFrontends
	fdReserved        int
	retainedBytes     int
	estimateQuota     func(context.Context, watchPlan, int) watchFDQuota
	validateToolchain func(context.Context, string) error
	start             func(context.Context, string, int, int) (*guardianChild, error)
	ready             func(context.Context, int) error
	updateRecords     func(context.Context, func([]record) ([]record, error)) ([]record, error)
	portBusy          func(int) bool
	restart           restartState
}

type ownerFilesystemJob struct {
	ctx context.Context
	run func()
}

func ownerNonce() string {
	var nonce [16]byte
	_, _ = rand.Read(nonce[:]) // crypto/rand.Read terminates on failure.
	return hex.EncodeToString(nonce[:])
}

func newSupervisor(ctx context.Context, m *manager) *supervisor {
	ctx, abort := context.WithCancelCause(ctx)
	s := &supervisor{ctx: ctx, m: m, boot: ownerNonce(), now: time.Now,
		generations: make(map[string]*ownedGeneration), queued: make(map[*callState]ownerReservation),
		operations: make(map[remoteKey]ownerOperation), controls: make(map[remoteKey]ownerOperation), filesystem: make(chan ownerFilesystemJob)}
	s.restart.abort = abort
	s.sseBudget = transport.NewBudget(m.limits.SSEBytes)
	s.fdReserved, _ = supervisorFDAllowance(m.limits) // Production validates before constructing the owner.
	s.estimateQuota = estimateWatchFDQuota
	s.portBusy = portUnavailable
	s.validateToolchain = m.validateManagedToolchain
	s.start, s.ready = m.startOwnedGuardian, awaitReady
	s.updateRecords = m.withMap

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case job := <-s.filesystem:
				if job.ctx.Err() == nil {
					job.run()
				}
			}
		}
	}()
	return s
}

// An uncancellable filesystem syscall can retain this one global worker, never
// one replacement per expired router/session. Buffered results cannot block it
// after an abandoned caller's deadline.
func ownerFilesystemCall[T any](ctx context.Context, s *supervisor, run func() T) (T, error) {
	var zero T
	reply := make(chan T, 1)
	job := ownerFilesystemJob{ctx: ctx, run: func() { reply <- run() }}
	select {
	case s.filesystem <- job:
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-s.ctx.Done():
		return zero, s.ctx.Err()
	}
	select {
	case result := <-reply:
		return result, ctx.Err()
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-s.ctx.Done():
		return zero, s.ctx.Err()
	}
}

func (s *supervisor) admit(state *callState) error {

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.ctx.Err() != nil {
		return fmt.Errorf("supervisor is shutting down")
	}
	if s.operationUsageLocked() >= s.m.limits.OwnerOperations {
		return fmt.Errorf("global supervisor operation limit reached")
	}
	s.queued[state] = ownerReservation{}
	return nil
}

func (s *supervisor) bind(state *callState, worktree string) error {

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.queued[state]; !ok || s.closing {
		return context.Canceled
	}
	// Post-drain work reserves a replacement, never the retiring generation;
	// ensureRecord waits for confirmed exit before starting that replacement.
	s.queued[state] = ownerReservation{worktree: worktree}
	return nil
}

func (s *supervisor) releaseQueued(state *callState) {
	s.mu.Lock()
	delete(s.queued, state)
	s.mu.Unlock()

}

type connectionIdentity [1]byte
type operationIdentity [1]byte
type remoteKey struct {
	conn *connectionIdentity
	id   jsonrpc.ID
}
type ownerSession struct {
	operations, abandoned int
	perWorktree           map[string]int
}

func remoteKeyOf(key operationKey) remoteKey {
	if c, ok := key.conn.(*ownedUpstream); ok {
		return remoteKey{c.identity, key.id}
	}
	return remoteKey{}
}

func (s *supervisor) begin(state *callState, key operationKey, worktree string, session *ownerSession, sessionLimit, laneLimit int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.generations[worktree]
	conn, ok := key.conn.(*ownedUpstream)
	if !ok || conn.s != s || g == nil || conn.g != g || g.state != generationReady || s.closing {
		return fmt.Errorf("upstream generation is no longer ready")
	}
	remote := remoteKeyOf(key)
	if _, exists := s.operations[remote]; exists {
		return fmt.Errorf("request id %v still has an unresolved upstream operation", key.id)
	}
	if session != nil && (session.operations >= sessionLimit || session.perWorktree[worktree] >= laneLimit) {
		return fmt.Errorf("upstream operation limit reached; cancelled or disconnected work may still be running")
	}
	_, reserved := s.queued[state]
	if !reserved && s.operationUsageLocked() >= s.m.limits.OwnerOperations {
		return fmt.Errorf("global supervisor operation limit reached")
	}
	delete(s.queued, state)
	if state.identity == nil {
		state.identity = &operationIdentity{}
	}
	s.operations[remote] = ownerOperation{generation: g, session: session, identity: state.identity}
	if session != nil {
		session.operations++
		session.perWorktree[worktree]++
	}
	return nil
}

func (s *supervisor) operationUsageLocked() int {
	return len(s.queued) + len(s.operations) + len(s.controls)
}

// Caller and server-control IDs occupy distinct namespaces.
func (s *supervisor) operationMaps() [2]map[remoteKey]ownerOperation {
	return [2]map[remoteKey]ownerOperation{s.operations, s.controls}
}

func generationConnections(g *ownedGeneration) []*ownedUpstream {
	connections := make([]*ownedUpstream, 0, len(g.connections))
	for conn := range g.connections {
		connections = append(connections, conn)
	}
	return connections
}

func closeSupervisedConnections(connections []*ownedUpstream) {
	for _, conn := range connections {
		_ = conn.Close()
	}
}

func (s *supervisor) serverControl(local operationKey, g *ownedGeneration) error {
	key := remoteKeyOf(local)
	s.mu.Lock()
	defer s.mu.Unlock()
	if g.state != generationReady || s.closing {
		return fmt.Errorf("server control belongs to a retiring generation")
	}
	if _, exists := s.controls[key]; exists {
		return fmt.Errorf("duplicate unresolved server control id")
	}
	if s.operationUsageLocked() >= s.m.limits.OwnerOperations {
		return fmt.Errorf("global supervisor operation limit reached for server control")
	}
	s.controls[key] = ownerOperation{generation: g}
	return nil
}

// Remote records retain only identities and counters, never a session or timer.
func (s *supervisor) completeOperationLocked(operations map[remoteKey]ownerOperation, key remoteKey, completed bool) {
	if op, ok := operations[key]; ok {
		delete(operations, key)
		if op.session != nil {
			op.session.operations--
			release(op.session.perWorktree, op.generation.worktree)
			if !op.abandoned.IsZero() {
				op.session.abandoned--
			}
		}
		if completed {
			op.generation.completed = s.now()
		}
	}
}
func (s *supervisor) abandonOperationLocked(operations map[remoteKey]ownerOperation, key remoteKey) {
	if op, ok := operations[key]; ok && op.abandoned.IsZero() {
		op.abandoned = s.now()
		operations[key] = op
		if op.session != nil {
			op.session.abandoned++
		}
	}
}
func (s *supervisor) abandonSession(session *ownerSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, op := range s.operations {
		if op.session == session {
			s.abandonOperationLocked(s.operations, key)
		}
	}
}
func (s *supervisor) sessionUsage(session *ownerSession) (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return session.operations, session.abandoned
}
func (s *supervisor) staleCancellation(key operationKey, state *callState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, exists := s.operations[remoteKeyOf(key)]
	return exists && op.identity != state.identity
}

func (s *supervisor) answered(key operationKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeOperationLocked(s.operations, remoteKeyOf(key), true)
}

func (s *supervisor) rejectedBeforeDelivery(key operationKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeOperationLocked(s.operations, remoteKeyOf(key), false)
}

func (s *supervisor) abandon(key operationKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.abandonOperationLocked(s.operations, remoteKeyOf(key))
}

func (s *supervisor) ensureRecord(ctx context.Context, worktree string) (record, error) {
	s.mu.Lock()
	g := s.generations[worktree]
	if s.closing {
		s.mu.Unlock()
		return record{}, fmt.Errorf("supervisor is shutting down")
	}
	if g == nil {
		if len(s.generations) >= s.m.limits.SharedServers {
			s.mu.Unlock()
			return record{}, fmt.Errorf("global shared gopls server limit reached")
		}
		g = &ownedGeneration{id: ownerNonce(), worktree: worktree, state: generationStarting,
			ready: make(chan struct{}), exited: make(chan struct{}), connections: make(map[*ownedUpstream]struct{})}
		s.generations[worktree] = g
		go s.startGeneration(g)
	}
	if g.state != generationStarting && g.state != generationReady {
		exited := g.exited
		s.mu.Unlock()
		select {
		case <-exited:
			return s.ensureRecord(ctx, worktree)
		case <-ctx.Done():
			return record{}, fmt.Errorf("wait for confirmed gopls generation exit: %w", ctx.Err())
		}
	}
	ready := g.ready
	s.mu.Unlock()
	select {
	case <-ready:
		s.mu.Lock()
		defer s.mu.Unlock()
		if g.state != generationReady {
			if g.err != nil {
				return record{}, fmt.Errorf("gopls generation is %s: %w", g.state, g.err)
			}
			return record{}, fmt.Errorf("gopls generation is %s", g.state)
		}
		return g.record, nil
	case <-ctx.Done():
		return record{}, ctx.Err()
	}
}

func (s *supervisor) startGeneration(g *ownedGeneration) {
	defer s.m.timed("ensure", time.Now())
	ctx, cancel := context.WithTimeout(s.ctx, readyTimeout+2*exitGrace)
	defer cancel()
	err := s.prepareGeneration(ctx, g)
	s.mu.Lock()
	if err == nil {
		if s.closing || s.ctx.Err() != nil || g.state != generationStarting {
			err = context.Canceled
		} else {
			g.state, g.completed = generationReady, s.now()
		}
	}
	s.mu.Unlock()
	if err != nil {
		err = fmt.Errorf("%w; see %s", err, s.m.logPath(g.worktree))
		// A partial start retains capacity until guardian or lease cleanup proof.
		switch {
		case g.child == nil && g.startup == nil:
			s.confirmExited(g)
		case g.child != nil:
			if g.startup == nil {
				err = errors.Join(err, g.child.stopFailedStart())
			}
			select {
			case <-g.child.ConfirmedDone:
				s.confirmExited(g)
			default:
			}
		}
	}
	s.mu.Lock()
	g.err = err
	if err != nil && g.state != generationExited {
		g.state = generationTerminating
	}
	observe := g.child != nil && g.state != generationExited
	close(g.ready)
	s.mu.Unlock()
	if observe {
		go s.observeGeneration(g)
	}
}

func (s *supervisor) prepareGeneration(ctx context.Context, g *ownedGeneration) error {
	if err := s.reserveGenerationFDs(ctx, g); err != nil {
		return err
	}
	_, err := s.updateRecords(ctx, func(records []record) ([]record, error) {
		if len(records) >= s.m.limits.SharedServers {
			return nil, fmt.Errorf("shared gopls server limit reached")
		}
		if slices.ContainsFunc(records, func(r record) bool { return r.Worktree == g.worktree }) {
			return nil, fmt.Errorf("refuse adoption of an existing process reservation")
		}
		s.mu.Lock()
		reserved := slices.Clone(records)
		for _, other := range s.generations {
			if other != g && other.record.Port != 0 {
				reserved = append(reserved, other.record)
			}
		}
		s.mu.Unlock()
		port, err := allocatePort(g.worktree, reserved, s.portBusy)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		g.record = record{Worktree: g.worktree, Port: port, OwnerBoot: s.boot, Generation: g.id}
		s.mu.Unlock()
		return records, nil
	})
	if err != nil {
		return err
	}
	child, err := s.start(ctx, g.worktree, g.record.Port, g.fdQuota)
	s.mu.Lock()
	g.child = child
	errors.As(err, &g.startup)
	if child != nil {
		if child.Process != nil {
			g.record.PID = child.Process.Pid
		}
		g.record.GuardianPID = child.GuardianPID
		now := time.Now()
		g.record.StartedAt, g.record.StartedAtMilli = now.Unix(), now.UnixMilli()
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = s.updateRecords(ctx, func(records []record) ([]record, error) {
		if slices.ContainsFunc(records, func(r record) bool { return r.Worktree == g.worktree || r.Port == g.record.Port }) {
			return nil, fmt.Errorf("generation conflicts with an existing process reservation")
		}
		return append(records, g.record), nil
	})
	if err != nil {
		return err
	}
	defer s.m.timed("readiness", time.Now())
	return s.ready(ctx, g.record.Port)
}

func (s *supervisor) observeGeneration(g *ownedGeneration) {
	select {
	case <-g.exited:
		return
	case <-g.child.ConfirmedDone:
	case <-g.child.UncertainDone:
		s.fenceUncertain(g)
		select {
		case <-g.child.ConfirmedDone:
		case <-g.exited:
			return
		}
	}
	s.confirmExited(g)
}

func (s *supervisor) connection(conn mcp.Connection, r record, ctx context.Context, release context.CancelFunc) (mcp.Connection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.generations[r.Worktree]
	if g == nil || g.id != r.Generation || g.state != generationReady || s.closing {
		return nil, fmt.Errorf("gopls generation retired before connection admission")
	}
	c := &ownedUpstream{Connection: conn, s: s, g: g, ctx: ctx, release: release, identity: &connectionIdentity{}}
	g.connections[c] = struct{}{}
	return c, nil
}

type ownedUpstream struct {
	mcp.Connection
	s        *supervisor
	g        *ownedGeneration
	ctx      context.Context
	release  context.CancelFunc
	identity *connectionIdentity
}

func (c *ownedUpstream) lifetime() context.Context { return c.ctx }

func (c *ownedUpstream) server() serverID { return c.g.record.server() }

func (c *ownedUpstream) available() bool {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	_, registered := c.g.connections[c]
	return registered && c.ctx.Err() == nil && c.g.state == generationReady && !c.s.closing
}

func (c *ownedUpstream) Write(ctx context.Context, msg jsonrpc.Message) error {
	c.s.mu.Lock()
	if c.g.state != generationReady || c.s.closing {
		c.s.mu.Unlock()
		return fmt.Errorf("%w: upstream generation is draining", errOwnerPreDelivery)
	}
	if req, ok := msg.(*jsonrpc.Request); ok && req.ID.IsValid() {
		key := remoteKeyOf(operationKey{c, req.ID})
		if _, reserved := c.s.operations[key]; !reserved {
			// Private handshakes are real operations too; they do not bypass the
			// global cap or idle protection just because the router swallows them.
			if c.s.operationUsageLocked() >= c.s.m.limits.OwnerOperations {
				c.s.mu.Unlock()
				return fmt.Errorf("%w: global operation limit reached during handshake", errOwnerPreDelivery)
			}
			c.s.operations[key] = ownerOperation{generation: c.g}
		}
	}
	c.g.writes++
	c.s.mu.Unlock()
	var err error
	if req, ok := msg.(*jsonrpc.Request); ok && req.ID.IsValid() {
		validation, taskErr := ownerFilesystemCall(ctx, c.s, func() error { return c.s.validateToolchain(ctx, c.g.worktree) })
		err = errors.Join(validation, taskErr)
		if err != nil {
			c.s.rejectedBeforeDelivery(operationKey{c, req.ID})
			err = fmt.Errorf("%w: %v", errOwnerPreDelivery, err)
		}
	}
	if err == nil {
		err = c.Connection.Write(ctx, msg)
	}
	if response, ok := msg.(*jsonrpc.Response); ok && err == nil {
		c.s.mu.Lock()
		key := remoteKeyOf(operationKey{c, response.ID})
		if op, exists := c.s.controls[key]; exists {
			delete(c.s.controls, key)
			op.generation.completed = c.s.now()
		}
		c.s.mu.Unlock()
	}
	if errors.Is(err, transport.ErrMessageTooLarge) {
		if req, ok := msg.(*jsonrpc.Request); ok {
			c.s.rejectedBeforeDelivery(operationKey{c, req.ID})
		}
	}
	c.s.mu.Lock()
	c.g.writes--
	c.s.mu.Unlock()
	return err
}

func (c *ownedUpstream) Read(ctx context.Context) (jsonrpc.Message, error) {
	msg, err := c.Connection.Read(ctx)
	if err == nil {
		if !validOwnerID(ownerMessageID(msg)) {
			_ = c.Close()
			return nil, fmt.Errorf("upstream opaque id exceeds %d bytes", maxOwnerIDBytes)
		}
	}
	if resp, ok := msg.(*jsonrpc.Response); ok {
		c.s.mu.Lock()
		key := remoteKeyOf(operationKey{c, resp.ID})
		if op, exists := c.s.operations[key]; exists && op.session == nil {
			c.s.completeOperationLocked(c.s.operations, key, true)
		}
		c.s.mu.Unlock()
	}
	if request, ok := msg.(*jsonrpc.Request); ok && request.ID.IsValid() && err == nil {
		err = c.s.serverControl(operationKey{c, request.ID}, c.g)
		if err != nil {
			msg = nil
		}
	}
	if err != nil {
		_ = c.Close()
	}
	return msg, err
}

func (c *ownedUpstream) Close() error {
	c.release()
	c.s.mu.Lock()
	delete(c.g.connections, c)
	for _, operations := range c.s.operationMaps() {
		for key, op := range operations {
			if key.conn == c.identity && op.abandoned.IsZero() {
				c.s.abandonOperationLocked(operations, key)
			}
		}
	}
	c.s.mu.Unlock()
	return c.Connection.Close()
}

func (s *supervisor) protectedLocked(g *ownedGeneration, now time.Time) bool {
	if g.writes != 0 {
		return true
	}
	for _, reservation := range s.queued {
		if reservation.worktree == g.worktree {
			return true
		}
	}
	for _, operations := range s.operationMaps() {
		for _, op := range operations {
			if op.generation == g && (op.abandoned.IsZero() || now.Sub(op.abandoned) < s.m.limits.Idle) {
				return true
			}
		}
	}
	return false
}

// drain commits admission exclusion first. Unknown operations can only be
// reclaimed as controlled generation recovery after their separate grace;
// they are never declared completed just because a local client disappeared.
func (s *supervisor) drain(g *ownedGeneration, reason string, idle bool) bool {
	s.mu.Lock()
	now := s.now()
	if g.state != generationReady || s.protectedLocked(g, now) || idle && now.Sub(g.completed) < s.m.limits.Idle {
		s.mu.Unlock()
		return false
	}
	connections := markDrainingLocked(g, reason)
	s.mu.Unlock()
	closeSupervisedConnections(connections)
	go s.terminate(g)
	return true
}

func markDrainingLocked(g *ownedGeneration, reason string) []*ownedUpstream {
	g.state, g.reason = generationDraining, reason
	g.terminating = true
	g.drainDone = make(chan struct{})
	return generationConnections(g)
}

func (s *supervisor) terminate(g *ownedGeneration) {
	var err error
	defer func() {
		s.mu.Lock()
		if err != nil {
			g.err = err
		}
		g.terminating = false
		close(g.drainDone)
		s.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), supervisorTermGrace)
	defer cancel()
	_, err = s.updateRecords(ctx, func(records []record) ([]record, error) {
		found := false
		for i := range records {
			if records[i].OwnerBoot == s.boot && records[i].Generation == g.id {
				records[i].Terminating = true
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("owned generation reservation is missing; refusing retirement until registry reconciliation")
		}
		return records, nil
	})
	if err != nil {
		return
	}
	s.mu.Lock()
	if g.state == generationExited {
		s.mu.Unlock()
		return
	}
	g.state = generationTerminating
	s.mu.Unlock()
	if g.child == nil {
		err = fmt.Errorf("owned process handle is unavailable")
		return
	}
	if err = g.child.Signal(syscall.SIGTERM); err != nil {
		return
	}
	select {
	case <-g.child.ConfirmedDone:
	case <-ctx.Done():
		err = fmt.Errorf("gopls did not confirm exit within %s; record retained", supervisorTermGrace)
	}
}

func (s *supervisor) fenceUncertain(g *ownedGeneration) {
	s.mu.Lock()
	if s.generations[g.worktree] != g || g.state == generationExited {
		s.mu.Unlock()
		return
	}
	g.state, g.reason = generationTerminating, "guardian ownership became uncertain"
	if g.err == nil {
		g.err = fmt.Errorf("guardian lost; process/group exit is unconfirmed and capacity remains reserved")
	}
	connections := generationConnections(g)
	s.mu.Unlock()
	closeSupervisedConnections(connections)
}

func (s *supervisor) confirmExited(g *ownedGeneration) {
	s.mu.Lock()
	if g.cleaning || s.generations[g.worktree] != g {
		s.mu.Unlock()
		return
	}
	g.cleaning = true
	g.state = generationExited
	if s.m.watch != nil {
		s.m.watch.mu.Lock()
		delete(s.m.watch.plans, g.worktree)
		s.m.watch.mu.Unlock()
	}
	for _, operations := range s.operationMaps() {
		for key, op := range operations {
			if op.generation == g {
				s.completeOperationLocked(operations, key, false)
			}
		}
	}

	connections := generationConnections(g)
	s.mu.Unlock()
	closeSupervisedConnections(connections)
	ctx, cancel := context.WithTimeout(context.Background(), exitGrace)
	defer cancel()
	_, err := s.updateRecords(ctx, func(records []record) ([]record, error) {
		return slices.DeleteFunc(records, func(r record) bool { return r.OwnerBoot == s.boot && r.Generation == g.id }), nil
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	g.cleaning = false
	if err != nil {
		g.err = err
		return
	}
	if s.generations[g.worktree] == g {
		if s.m.ownerLogs != nil {
			s.m.ownerLogs.Release(s.m.logPath(g.worktree))
		}
		if g.fdQuota != 0 {
			s.fdReserved -= g.fdQuota + guardianFDAllowance
		}
		delete(s.generations, g.worktree)
		if g.exited != nil {
			close(g.exited)
		}
	}
}

func (s *supervisor) list(ctx context.Context, w io.Writer) error {
	records, err := s.maintenanceSweep(ctx)
	if err != nil {
		return err
	}
	return writeRecords(w, records)
}
func (s *supervisor) idleSweep() {
	s.mu.Lock()
	var candidates []*ownedGeneration
	var retry []*ownedGeneration
	var exited []*ownedGeneration
	now := s.now()
	for _, g := range s.generations {
		if g.state == generationReady && now.Sub(g.completed) >= s.m.limits.Idle {
			candidates = append(candidates, g)
		}
		if g.state == generationDraining && !g.terminating {
			g.terminating = true
			g.drainDone = make(chan struct{})
			retry = append(retry, g)
		}
		if g.state == generationExited && !g.cleaning {
			exited = append(exited, g)
		}
	}
	s.mu.Unlock()
	for _, g := range candidates {
		s.drain(g, "idle or abandoned operation grace expired", true)
	}
	for _, g := range retry {
		go s.terminate(g)
	}
	for _, g := range exited {
		go s.confirmExited(g)
	}
}

func (s *supervisor) maintenanceSweep(ctx context.Context) ([]record, error) {
	s.idleSweep()
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	s.mu.Lock()
	var generations []*ownedGeneration
	for _, g := range s.generations {
		if g.state == generationReady || g.state == generationTerminating && g.startup != nil {
			generations = append(generations, g)
		}
	}
	s.mu.Unlock()
	for _, g := range generations {
		if ctx.Err() != nil {
			break
		}
		if g.startup != nil {
			type releaseResult struct {
				released bool
				err      error
			}
			released, err := ownerFilesystemCall(ctx, s, func() releaseResult {
				ok, err := g.startup.Released()
				return releaseResult{ok, err}
			})
			if err == nil && released.err == nil && released.released {
				s.confirmExited(g)
			}
		}
		type verdict struct {
			stale bool
			err   error
		}
		observed, err := ownerFilesystemCall(ctx, s, func() verdict {
			_, stale, err := s.m.watchChanged(g.record)
			if err != nil {
				return verdict{err: err}
			}
			info, err := os.Stat(g.worktree)
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
				return verdict{stale: true}
			}
			if err != nil {
				return verdict{err: err}
			}
			return verdict{stale: stale || !info.IsDir()}
		})
		if err == nil && observed.err == nil && observed.stale {
			s.drain(g, "stale watches or deleted worktree", false)
		}
	}
	records, _, err := readMap(s.m.mapPath)
	return records, err
}

func (s *supervisor) shutdown() {
	s.mu.Lock()
	s.closing = true
	var connections []*ownedUpstream
	var children []*guardianChild
	for _, g := range s.generations {
		for conn := range g.connections {
			connections = append(connections, conn)
		}
		if g.child != nil {
			children = append(children, g.child)
		}
	}
	s.mu.Unlock()
	closeSupervisedConnections(connections)
	for _, child := range children {
		if child.CloseOwner != nil {
			_ = child.CloseOwner()
		} else {
			_ = child.Signal(syscall.SIGTERM)
		}
	}
}

var _ mcp.Connection = (*ownedUpstream)(nil)
