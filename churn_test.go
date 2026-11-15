package main

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

// Serial so the profiles do not include concurrently running package tests.
// Each cycle exercises a backend/router, private handshake, roots worker,
// successful call, uncertain-write reconnect, cancelled unresolved call and
// shutdown against one manager.
func TestAuditSessionChurn(t *testing.T) {
	m := routerTestOwner(t, nil)
	profileDir := os.Getenv("GOPLS_MANAGER_AUDIT_PROFILES")
	snapshot := func(batch int) {
		t.Helper()
		runtime.GC()
		// Heap profiles may lag reclamation by a collection. Also clear pool
		// victims before comparing quiescent batches.
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		t.Logf("batch=%d heap_bytes=%d heap_objects=%d goroutines=%d", batch, mem.HeapAlloc, mem.HeapObjects, runtime.NumGoroutine())
		if profileDir == "" {
			return
		}
		require.True(t, filepath.IsAbs(profileDir))
		require.NoError(t, os.MkdirAll(profileDir, 0o700))
		for _, name := range []string{"heap", "goroutine"} {
			f, err := os.Create(filepath.Join(profileDir, fmt.Sprintf("batch-%02d.%s", batch, name)))
			require.NoError(t, err)
			require.NoError(t, pprof.Lookup(name).WriteTo(f, 0))
			require.NoError(t, f.Close())
		}
	}
	snapshot(0)
	for batch := 1; batch <= 10; batch++ {
		for range 100 {
			ctx, cancel := context.WithCancel(t.Context())
			b := newHTTPBackend(ctx, m, testHome)
			b.r.plan = func(worktree string, _ []string) watchPlan { return watchPlan{roots: []string{worktree}} }
			dials := 0
			executed := make(chan struct{}, 1)
			b.r.dial = func(context.Context, string, int) (mcp.Connection, error) {
				dials++
				generation := dials
				conn := &fakeConn{reads: make(chan jsonrpc.Message, 4)}
				conn.onWrite = func(ctx context.Context, msg jsonrpc.Message) error {
					req, ok := msg.(*jsonrpc.Request)
					if !ok || !req.ID.IsValid() {
						return nil
					}
					if req.Method == "tools/call" && !strings.Contains(string(req.Params), `"reply"`) {
						executed <- struct{}{}
						return nil
					}
					if generation == 1 && req.Method == "tools/list" {
						return io.ErrUnexpectedEOF
					}
					result := jsontext.Value(`{}`)
					if req.Method == "initialize" {
						conn.reads <- &jsonrpc.Request{ID: initID, Method: "roots/list"}
					}
					select {
					case conn.reads <- &jsonrpc.Response{ID: req.ID, Result: result}:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return conn, nil
			}
			b.start()
			_, err := b.initialize(ctx)
			require.NoError(t, err)
			require.ErrorContains(t, b.call(ctx, "tools/list", map[string]any{}, new(jsontext.Value)), "may have executed")
			// A later distinct request may reconnect; the failed one is never replayed.
			require.NoError(t, b.call(ctx, "tools/list", map[string]any{}, new(jsontext.Value)))
			require.Equal(t, 2, dials, "cycle did not reconnect")
			require.NoError(t, b.call(ctx, "tools/call", map[string]any{"mode": "reply"}, new(jsontext.Value)))
			// Expiring delivered work retains its operation until confirmed process exit.
			callCtx, stopCall := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- b.call(callCtx, "tools/call", map[string]any{}, new(jsontext.Value)) }()
			require.Eventually(t, func() bool { return b.r.requestUsage().UpstreamOperations == 2 }, time.Second, time.Millisecond)
			mustRecv(t, executed, "unanswered call reached upstream")
			stopCall()
			require.ErrorIs(t, <-done, context.Canceled)
			b.close()
			cancel()
			require.Equal(t, 2, b.r.requestUsage().UpstreamOperations, "disconnect cannot release remote credits")
			m.mu.Lock()
			g := m.generations[testHome]
			m.mu.Unlock()
			require.NoError(t, g.child.Signal(syscall.SIGTERM))
			<-g.exited
			require.Zero(t, b.r.requestUsage().UpstreamOperations)
		}
		require.Eventually(t, func() bool {
			var stacks bytes.Buffer
			_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
			for _, symbol := range []string{"(*httpBackend).start.func", "(*router).readFromUpstream", "(*router).answerRootsAsks", "(*lane).runControls", "(*router).planRoots.func"} {
				if strings.Contains(stacks.String(), symbol) {
					return false
				}
			}
			return true
		}, 2*time.Second, time.Millisecond, "session-owned goroutines survived shutdown")
		snapshot(batch)
	}
}

func TestAuditDecodedBackpressure(t *testing.T) {
	runtime.GC()
	var before, full, after runtime.MemStats
	runtime.ReadMemStats(&before)
	func() {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r := newRouter(ctx, routerTestOwner(t, nil), testHome)
		conn := &fakeConn{reads: make(chan jsonrpc.Message, cap(r.out)+1)}
		for range cap(conn.reads) {
			params := jsontext.Value(fmt.Sprintf(`{"text":%q}`, strings.Repeat("x", 256<<10)))
			conn.reads <- &jsonrpc.Request{Method: "notifications/progress", Params: params}
		}
		done := make(chan struct{})
		go func() { defer close(done); r.readFromUpstream(testConnection(r, conn, testHome), testHome) }()
		require.Eventually(t, func() bool { return len(r.out) == cap(r.out) }, time.Second, time.Millisecond)
		runtime.GC()
		runtime.ReadMemStats(&full)
		used, _ := r.sseBudget.Snapshot()
		require.Zero(t, used, "decoded output retention is outside the SSE wire budget")
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			require.FailNow(t, "backpressured reader survived shutdown")
		}
	}()
	// A second collection also clears pool victims and avoids attributing a
	// just-completed collection's profile lag to retained session messages.
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	t.Logf("decoded_queue=64 payload_bytes=%d heap_before=%d heap_full=%d heap_after=%d", 256<<10, before.HeapAlloc, full.HeapAlloc, after.HeapAlloc)
}

func TestAuditCodecBenchmarkCleanup(t *testing.T) {
	// Serial: compare the SDK pumps owned by one benchmark invocation without
	// including concurrent transport tests in the same package.
	pumps := func() int {
		var stacks bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
		return strings.Count(stacks.String(), "mcp.newIOConnLimited.func1")
	}
	before := pumps()
	result := testing.Benchmark(BenchmarkStdioCodec)
	require.Positive(t, result.N)
	require.Eventually(t, func() bool { return pumps() <= before }, time.Second, time.Millisecond,
		"codec benchmark retained SDK decoder pumps after cleanup")
}

const (
	ownerSequenceClients = 3
	ownerSequenceIDs     = 4
	ownerSequenceSlots   = 4
	ownerSequenceLimit   = 4
	ownerSequenceBytes   = 128
)

// The reference is a ledger of externally observed events, rather than a
// second supervisor: queue tickets and bidirectional delivery receipts form
// separate rows. Only a terminal reply, certified pre-delivery rejection, or
// confirmed generation exit may remove a delivered-work receipt.
type ownerSequenceGeneration struct {
	actual    *ownedGeneration
	worktree  int
	completed time.Duration
	retiring  bool
	exited    bool
}

type ownerSequenceConnection struct {
	actual     *ownedUpstream
	transport  *fakeConn
	generation *ownerSequenceGeneration
	closed     bool
}

type ownerSequenceReceipt struct {
	connection *ownerSequenceConnection
	id         int
	control    bool
	abandoned  bool
	abandonAt  time.Duration
}

type ownerSequenceLedger struct {
	t           *testing.T
	s           *supervisor
	epoch       time.Time
	elapsed     time.Duration
	current     [2]*ownerSequenceGeneration
	lastExited  [2]*ownerSequenceGeneration
	clients     [ownerSequenceClients]*ownerSequenceConnection
	previous    [ownerSequenceClients]*ownerSequenceConnection
	connections []*ownerSequenceConnection
	states      [ownerSequenceSlots]*callState
	queued      [ownerSequenceSlots]bool
	targets     [ownerSequenceSlots]int
	receipts    []ownerSequenceReceipt
}

func newOwnerSequenceLedger(t *testing.T) *ownerSequenceLedger {
	t.Helper()
	s := stateSupervisor(t)
	s.m.limits.OwnerOperations = ownerSequenceLimit
	s.validateToolchain = func(context.Context, string) error { return nil }
	l := &ownerSequenceLedger{t: t, s: s, epoch: time.Now()}
	for i := range l.states {
		l.states[i] = &callState{}
		l.targets[i] = -1
	}
	return l
}

func ownerSequenceWorktree(index int) string {
	if index == 0 {
		return testHome
	}
	return testWorktree
}

func ownerSequenceID(t *testing.T, id int) jsonrpc.ID {
	t.Helper()
	return mustID(t, string(rune('a'+id)))
}

func (l *ownerSequenceLedger) usage() int {
	count := len(l.receipts)
	for _, queued := range l.queued {
		if queued {
			count++
		}
	}
	return count
}

func (l *ownerSequenceLedger) receipt(c *ownerSequenceConnection, id int, control bool) int {
	for i, receipt := range l.receipts {
		if receipt.connection == c && receipt.id == id && receipt.control == control {
			return i
		}
	}
	return -1
}

func (l *ownerSequenceLedger) forget(index int, completion bool) {
	if index < 0 {
		return
	}
	if completion {
		l.receipts[index].connection.generation.completed = l.elapsed
	}
	l.receipts = append(l.receipts[:index], l.receipts[index+1:]...)
}

func (l *ownerSequenceLedger) markClosed(c *ownerSequenceConnection) {
	c.closed = true
	for i := range l.receipts {
		r := &l.receipts[i]
		if r.connection == c && !r.abandoned {
			r.abandoned, r.abandonAt = true, l.elapsed
		}
	}
}

func (l *ownerSequenceLedger) protected(g *ownerSequenceGeneration) bool {
	for i, queued := range l.queued {
		if queued && l.targets[i] == g.worktree {
			return true
		}
	}
	for _, receipt := range l.receipts {
		if receipt.connection.generation == g && (!receipt.abandoned || l.elapsed-receipt.abandonAt < time.Hour) {
			return true
		}
	}
	return false
}

func (l *ownerSequenceLedger) attach(client, tree int) {
	if c := l.clients[client]; c != nil && !c.closed {
		return // A client has at most one current attachment.
	}
	g := l.current[tree]
	if g == nil {
		g = &ownerSequenceGeneration{actual: readyTestGeneration(l.t, l.s, ownerSequenceWorktree(tree)), worktree: tree, completed: l.elapsed}
		l.current[tree] = g
	}
	if g.retiring {
		return // Acquisition must wait for explicit confirmed exit.
	}
	base := &fakeConn{reads: make(chan jsonrpc.Message, 1), onWrite: func(context.Context, jsonrpc.Message) error { return nil }}
	conn, err := l.s.connection(base, g.actual.record, context.Background(), func() {})
	require.NoError(l.t, err)
	c := &ownerSequenceConnection{actual: conn.(*ownedUpstream), transport: base, generation: g}
	l.previous[client], l.clients[client] = l.clients[client], c
	l.connections = append(l.connections, c)
}

func (l *ownerSequenceLedger) sweep() {
	for _, g := range l.current {
		if g == nil || g.retiring || l.elapsed-g.completed < time.Hour || l.protected(g) {
			continue
		}
		g.retiring = true
		for _, c := range l.connections {
			if c.generation == g {
				l.markClosed(c)
			}
		}
	}
	l.s.idleSweep()
	synctest.Wait() // Let in-memory drain transactions reach their barrier.
}

func (l *ownerSequenceLedger) confirm(tree int) {
	g := l.current[tree]
	if g == nil {
		return
	}
	l.s.confirmExited(g.actual)
	g.exited = true
	for i := len(l.receipts) - 1; i >= 0; i-- {
		if l.receipts[i].connection.generation == g {
			l.forget(i, false)
		}
	}
	for _, c := range l.connections {
		if c.generation == g {
			l.markClosed(c)
		}
	}
	l.lastExited[tree], l.current[tree] = g, nil
}

// A blocked, ambiguous transport write protects its generation even after
// local abandonment grace expires. The barrier orders the adversarial sweep;
// no scheduler timing or real timeout decides the expected outcome.
func (l *ownerSequenceLedger) blockedWrite(c *ownerSequenceConnection, id int) {
	if c == nil || c.closed || c.generation.retiring || c.generation.exited {
		return
	}
	index := l.receipt(c, id, false)
	request := &jsonrpc.Request{ID: ownerSequenceID(l.t, id), Method: "tools/call"}
	if index < 0 && l.usage() >= ownerSequenceLimit {
		require.ErrorIs(l.t, c.actual.Write(l.t.Context(), request), errOwnerPreDelivery)
		return
	}
	if index < 0 {
		l.receipts = append(l.receipts, ownerSequenceReceipt{connection: c, id: id})
		index = len(l.receipts) - 1
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	c.transport.onWrite = func(context.Context, jsonrpc.Message) error { close(entered); <-release; return io.ErrClosedPipe }
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	go func() { done <- c.actual.Write(l.t.Context(), request) }()
	<-entered
	l.s.abandon(operationKey{c.actual, request.ID})
	if !l.receipts[index].abandoned {
		l.receipts[index].abandoned, l.receipts[index].abandonAt = true, l.elapsed
	}
	time.Sleep(2 * time.Hour)
	l.elapsed += 2 * time.Hour
	l.s.idleSweep()
	synctest.Wait()
	require.Equal(l.t, generationReady, supervisorGenerationState(l.s, c.generation.actual), "sweep overtook blocked upstream write")
	// Other generations may retire while this one remains protected.
	for _, other := range l.current {
		if other == nil || other == c.generation || other.retiring || l.elapsed-other.completed < time.Hour || l.protected(other) {
			continue
		}
		other.retiring = true
		for _, attachment := range l.connections {
			if attachment.generation == other {
				l.markClosed(attachment)
			}
		}
	}
	close(release)
	released = true
	require.ErrorIs(l.t, <-done, io.ErrClosedPipe)
	c.transport.onWrite = func(context.Context, jsonrpc.Message) error { return nil }
	// An ambiguous write error did not prove non-delivery; retain the receipt.
}

func (l *ownerSequenceLedger) step(opcode, operand byte) {
	selection := int(operand & 127)
	client, id, slot, tree := selection%3, selection/3%4, selection/12%4, selection/48%2
	c := l.clients[client]
	if operand >= 128 && l.previous[client] != nil {
		c = l.previous[client]
	}
	key := operationKey{}
	if c != nil {
		key = operationKey{c.actual, ownerSequenceID(l.t, id)}
	}
	switch opcode % 16 {
	case 0:
		l.attach(client, tree)
	case 1:
		if l.queued[slot] {
			return // Router admission uses fresh callState identities.
		}
		err := l.s.admit(l.states[slot])
		if l.usage() >= ownerSequenceLimit {
			require.Error(l.t, err)
		} else {
			require.NoError(l.t, err)
			l.queued[slot], l.targets[slot] = true, -1
		}
	case 2:
		err := l.s.bind(l.states[slot], ownerSequenceWorktree(tree))
		if l.queued[slot] {
			require.NoError(l.t, err)
			l.targets[slot] = tree
		} else {
			require.ErrorIs(l.t, err, context.Canceled)
		}
	case 3:
		if c == nil || c.closed || c.generation.exited || l.queued[slot] && l.targets[slot] != c.generation.worktree {
			return
		}
		allowed := !c.generation.retiring && l.receipt(c, id, false) < 0 && (l.queued[slot] || l.usage() < ownerSequenceLimit)
		err := l.s.begin(l.states[slot], key, ownerSequenceWorktree(c.generation.worktree), nil, 0, 0)
		if allowed {
			require.NoError(l.t, err)
			l.queued[slot], l.targets[slot] = false, -1
			l.receipts = append(l.receipts, ownerSequenceReceipt{connection: c, id: id})
		} else {
			require.Error(l.t, err)
		}
	case 4:
		if c != nil {
			l.s.abandon(key)
			if i := l.receipt(c, id, false); i >= 0 && !l.receipts[i].abandoned {
				l.receipts[i].abandoned, l.receipts[i].abandonAt = true, l.elapsed
			}
		}
	case 5:
		if c != nil {
			c.transport.reads <- &jsonrpc.Response{ID: key.id}
			_, err := c.actual.Read(l.t.Context())
			require.NoError(l.t, err)
			l.forget(l.receipt(c, id, false), true)
		}
	case 6:
		if c == nil || c.closed || c.generation.retiring || c.generation.exited {
			return
		}
		allowed := l.receipt(c, id, true) < 0 && l.usage() < ownerSequenceLimit
		c.transport.reads <- &jsonrpc.Request{ID: key.id, Method: "roots/list"}
		_, err := c.actual.Read(l.t.Context())
		if allowed {
			require.NoError(l.t, err)
			l.receipts = append(l.receipts, ownerSequenceReceipt{connection: c, id: id, control: true})
		} else {
			require.Error(l.t, err)
			l.markClosed(c)
		}
	case 7:
		if c == nil || c.closed {
			return
		}
		err := c.actual.Write(l.t.Context(), &jsonrpc.Response{ID: key.id})
		if c.generation.retiring || c.generation.exited {
			require.ErrorIs(l.t, err, errOwnerPreDelivery)
		} else {
			require.NoError(l.t, err)
			l.forget(l.receipt(c, id, true), true)
		}
	case 8:
		if c != nil {
			require.NoError(l.t, c.actual.Close())
			l.markClosed(c)
		}
	case 9:
		l.s.releaseQueued(l.states[slot])
		l.queued[slot], l.targets[slot] = false, -1
	case 10:
		duration := [...]time.Duration{time.Hour - time.Nanosecond, time.Nanosecond, time.Hour, 2 * time.Hour}[operand%4]
		time.Sleep(duration)
		l.elapsed += duration
	case 11:
		l.sweep()
	case 12:
		l.confirm(tree)
	case 13:
		if previous := l.lastExited[tree]; previous != nil {
			l.s.confirmExited(previous.actual) // Old completion cannot reap its replacement.
		}
	case 14:
		l.blockedWrite(c, id)
	case 15:
		if c != nil {
			l.s.rejectedBeforeDelivery(key)
			l.forget(l.receipt(c, id, false), false)
		}
	}
}

func (l *ownerSequenceLedger) check() {
	l.t.Helper()
	synctest.Wait()
	require.LessOrEqual(l.t, l.usage(), ownerSequenceLimit, "reference exceeded admission limit")
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	queued := 0
	for slot, reserved := range l.queued {
		if !reserved {
			continue
		}
		queued++
		actual, exists := l.s.queued[l.states[slot]]
		require.True(l.t, exists, "queue reservation lost")
		if l.targets[slot] < 0 {
			require.Empty(l.t, actual.worktree)
		} else {
			require.Equal(l.t, ownerSequenceWorktree(l.targets[slot]), actual.worktree)
		}
	}
	require.Len(l.t, l.s.queued, queued)
	require.Equal(l.t, l.usage(), len(l.s.queued)+len(l.s.operations)+len(l.s.controls), "global credit mismatch")
	tools, controls := 0, 0
	for _, receipt := range l.receipts {
		key := operationKey{receipt.connection.actual, ownerSequenceID(l.t, receipt.id)}
		actual, exists := l.s.operations[remoteKeyOf(key)]
		if receipt.control {
			controls++
			actual, exists = l.s.controls[remoteKeyOf(key)]
		} else {
			tools++
		}
		require.True(l.t, exists, "exact conn+ID receipt lost or falsely refunded")
		require.Same(l.t, receipt.connection.generation.actual, actual.generation)
		require.Equal(l.t, receipt.abandoned, !actual.abandoned.IsZero())
		if receipt.abandoned {
			require.Equal(l.t, l.epoch.Add(receipt.abandonAt), actual.abandoned, "abandonment clock was renewed")
		}
	}
	require.Len(l.t, l.s.operations, tools)
	require.Len(l.t, l.s.controls, controls)
	live := 0
	for tree, g := range l.current {
		actual := l.s.generations[ownerSequenceWorktree(tree)]
		if g == nil {
			require.Nil(l.t, actual, "confirmed exit retained current generation")
			continue
		}
		live++
		require.Same(l.t, g.actual, actual)
		require.Equal(l.t, !g.retiring, actual.state == generationReady, "idle protection/drain mismatch")
		require.Equal(l.t, l.epoch.Add(g.completed), actual.completed, "completion clock changed without a terminal receipt")
		attachments := 0
		for _, c := range l.connections {
			if c.generation == g && !c.closed {
				attachments++
				_, present := actual.connections[c.actual]
				require.True(l.t, present)
			}
		}
		require.Len(l.t, actual.connections, attachments)
	}
	require.Len(l.t, l.s.generations, live)
}

func ownerSequenceSeed(actions ...[2]byte) []byte {
	input := make([]byte, 0, 2*len(actions))
	for _, action := range actions {
		input = append(input, action[0], action[1])
	}
	return input
}

func FuzzSupervisorLifecycleAccounting(f *testing.F) {
	// Zero/one-hour boundary; queued protection; cancellation without refund;
	// duplicate/reused ID on distinct connections; late old-generation replies;
	// bidirectional ID namespaces; and a sweep ordered inside an ambiguous write.
	for _, input := range [][]byte{
		{},
		ownerSequenceSeed([2]byte{0, 0}, [2]byte{10, 0}, [2]byte{11, 0}, [2]byte{10, 1}, [2]byte{11, 0}),
		ownerSequenceSeed([2]byte{0, 0}, [2]byte{1, 0}, [2]byte{2, 0}, [2]byte{10, 3}, [2]byte{11, 0}, [2]byte{3, 0}, [2]byte{8, 0}, [2]byte{10, 2}, [2]byte{11, 0}, [2]byte{12, 0}),
		ownerSequenceSeed([2]byte{0, 0}, [2]byte{0, 1}, [2]byte{3, 0}, [2]byte{3, 1}, [2]byte{5, 0}, [2]byte{8, 1}, [2]byte{5, 1}),
		ownerSequenceSeed([2]byte{0, 0}, [2]byte{3, 0}, [2]byte{12, 0}, [2]byte{0, 0}, [2]byte{3, 0}, [2]byte{5, 128}, [2]byte{13, 0}, [2]byte{5, 0}),
		ownerSequenceSeed([2]byte{0, 0}, [2]byte{3, 0}, [2]byte{6, 0}, [2]byte{5, 0}, [2]byte{7, 0}),
		ownerSequenceSeed([2]byte{0, 0}, [2]byte{14, 0}, [2]byte{11, 0}, [2]byte{12, 0}),
		ownerSequenceSeed([2]byte{0, 0}, [2]byte{1, 0}, [2]byte{1, 12}, [2]byte{1, 24}, [2]byte{1, 36}, [2]byte{6, 0}, [2]byte{9, 0}, [2]byte{0, 0}, [2]byte{3, 3}, [2]byte{15, 3}),
	} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > ownerSequenceBytes {
			return
		}
		synctest.Test(t, func(t *testing.T) {
			ledger := newOwnerSequenceLedger(t)
			ledger.check()
			for step := 0; step+1 < len(input); step += 2 {
				t.Logf("step=%d opcode=%d operand=%d input=%x", step/2, input[step]%16, input[step+1], input)
				ledger.step(input[step], input[step+1])
				ledger.check()
			}
		})
	})
}

// Serial because process-wide quiescent profiles cannot attribute other tests'
// parallel allocations/workers to this owner. All coordination uses fake time;
// there are no real sockets, process signals or registry syscalls in the bubble.
func TestSupervisorTenThousandSessionChurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.validateToolchain = func(context.Context, string) error { return nil }
		readyTestGeneration(t, s, testHome)
		var warmHeap uint64
		var warmGoroutines int
		for session := range 10000 {
			b := newHTTPBackend(t.Context(), s, testHome)
			b.r.dial = func(context.Context, string, int) (mcp.Connection, error) {
				conn := &fakeConn{reads: make(chan jsonrpc.Message, 4)}
				conn.onWrite = func(_ context.Context, msg jsonrpc.Message) error {
					if request, ok := msg.(*jsonrpc.Request); ok && request.ID.IsValid() {
						result := jsontext.Value(`{"tools":[]}`)
						if request.Method == "initialize" {
							result = jsontext.Value(`{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"churn","version":"1"}}`)
						}
						conn.reads <- &jsonrpc.Response{ID: request.ID, Result: result}
					}
					return nil
				}
				return conn, nil
			}
			b.start()
			_, err := b.initialize(t.Context())
			require.NoError(t, err)
			var listed mcp.ListToolsResult
			require.NoError(t, b.call(t.Context(), "tools/list", jsontext.Value(`{}`), &listed))
			b.close()
			synctest.Wait()
			state := s.snapshot()
			require.Zero(t, state.Queued, "session%d retained queue credit", session)
			require.Zero(t, state.Operations, "session%d retained upstream identity", session)
			require.Zero(t, state.RetainedMessageBytes, "session%d retained payload", session)
			require.Len(t, state.Generations, 1)
			require.Zero(t, state.Generations[0].Attachments)
			if session == 999 || session == 9999 {
				runtime.GC()
				var memory runtime.MemStats
				runtime.ReadMemStats(&memory)
				workers := runtime.NumGoroutine()
				t.Logf("sessions=%d live_heap=%d goroutines=%d", session+1, memory.HeapAlloc, workers)
				if session == 999 {
					warmHeap, warmGoroutines = memory.HeapAlloc, workers
				} else {
					require.LessOrEqual(t, memory.HeapAlloc, warmHeap+8<<20, "live heap grows across retired owner sessions")
					require.LessOrEqual(t, workers, warmGoroutines+8, "retired session workers accumulate")
				}
			}
		}

	})
	if dir := os.Getenv("GOPLS_MANAGER_AUDIT_PROFILES"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		for _, name := range []string{"heap", "goroutine"} {
			file, err := os.Create(filepath.Join(dir, fmt.Sprintf("owner-10000-%s.pprof", name)))
			require.NoError(t, err)
			if name == "heap" {
				require.NoError(t, pprof.WriteHeapProfile(file))
			} else {
				require.NoError(t, pprof.Lookup(name).WriteTo(file, 0))
			}
			require.NoError(t, file.Close())
		}
	}
}
