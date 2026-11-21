package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json/jsontext"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"
	"github.com/ajvengo/gopls-mcp-manager/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfirmedExitRestoresLongSessionCapacity(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.m.limits.Session, s.m.limits.PerLane = 1, 1
		r := newRouter(t.Context(), s, testHome)
		defer r.clearRequests()
		id := mustID(t, "reused")
		for range 128 {
			g := readyTestGeneration(t, s, testHome)
			conn := readyTestConnection(t, s, g)
			state, err := r.admit(id, func() {})
			require.NoError(t, err)
			require.NoError(t, r.beginOperation(id, state, conn, testHome, func() {}, nil))
			require.Equal(t, 1, r.requestUsage().UpstreamOperations)
			_, ok := r.finish(id, nil, state)
			require.True(t, ok)
			s.confirmExited(g)
		}
		usage := r.requestUsage()
		require.Zero(t, usage.UpstreamOperations)
		require.Zero(t, usage.AbandonedOperations)
		require.Empty(t, r.remote.perWorktree)
		require.Empty(t, s.operations)
	})
}

func TestUnconfirmedExitRetainsRouterOperationCredits(t *testing.T) {
	for _, name := range []string{"connection loss", "uncertain guardian", "termination pending"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := stateSupervisor(t)
				s.m.limits.Session, s.m.limits.PerLane = 1, 1
				r := newRouter(t.Context(), s, testHome)
				defer r.clearRequests()
				g := readyTestGeneration(t, s, testHome)
				conn := readyTestConnection(t, s, g)
				id := mustID(t, "cancelled")
				state, err := r.admit(id, func() {})
				require.NoError(t, err)
				require.NoError(t, r.beginOperation(id, state, conn, testHome, func() {}, nil))
				_, ok := r.finish(id, nil, state)
				require.True(t, ok)
				switch name {
				case "connection loss":
					require.NoError(t, conn.Close())
				case "uncertain guardian":
					s.fenceUncertain(g)
				case "termination pending":
					s.mu.Lock()
					g.state = generationTerminating
					s.mu.Unlock()
				}
				require.Equal(t, 1, r.requestUsage().AbandonedOperations)
				next := readyTestGeneration(t, s, testHome+"/other")
				fresh := readyTestConnection(t, s, next)
				second := mustID(t, "fresh")
				state, err = r.admit(second, func() {})
				require.NoError(t, err)
				require.ErrorContains(t, r.beginOperation(second, state, fresh, testHome+"/other", func() {}, nil), "upstream operation limit")
				s.confirmExited(g)
				require.NoError(t, r.beginOperation(second, state, fresh, testHome+"/other", func() {}, nil))
				require.Equal(t, 1, r.requestUsage().UpstreamOperations)
			})
		})
	}
}

func TestSupervisorUncertainPostDoesNotReplayExecutedTool(t *testing.T) {
	t.Parallel()
	var executions atomic.Int32
	executed := make(chan struct{}, 2)
	sdk := mcp.NewServer(&mcp.Implementation{Name: "counter", Version: "1"}, nil)
	sdk.AddTool(&mcp.Tool{Name: "mutate", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			executions.Add(1)
			executed <- struct{}{}
			return &mcp.CallToolResult{}, nil
		})
	upstream := mcp.NewSSEHandler(func(*http.Request) *mcp.Server { return sdk }, nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			upstream.ServeHTTP(w, req)
			return
		}
		body, err := io.ReadAll(req.Body)
		if !assert.NoError(t, err) {
			return
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
		if !strings.Contains(string(body), `"method":"tools/call"`) {
			upstream.ServeHTTP(w, req)
			return
		}
		// Let the real SDK execute the mutation and send its SSE result, but
		// lose the HTTP acknowledgement before the router starts this new
		// connection's reader. EOF cannot establish that execution did not occur.
		upstream.ServeHTTP(httptest.NewRecorder(), req)
		select {
		case <-executed:
		case <-req.Context().Done():
			return
		}
		conn, _, err := http.NewResponseController(w).Hijack()
		if assert.NoError(t, err) {
			assert.NoError(t, conn.Close())
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	r := newRouter(ctx, routerTestOwner(t, nil), testHome)
	r.initialize.Store(&jsonrpc.Request{Method: "initialize", Params: jsontext.Value(
		`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}`)})
	r.dial = func(ctx context.Context, _ string, port int) (mcp.Connection, error) {
		return transport.ConnectSSE(ctx, server.URL, r.limits.MessageBytes, r.sseBudget)
	}
	call, ok := r.prepare(&jsonrpc.Request{ID: mustID(t, "mutate"), Method: "tools/call", Params: jsontext.Value(`{"name":"mutate","arguments":{}}`)})
	require.True(t, ok)
	defer call.cancel()
	l := newLane(r, testHome)
	defer l.cancel()
	l.send(ctx, call.req, call.state)
	response := <-r.out
	require.Equal(t, int32(1), executions.Load(), "lost POST acknowledgement caused a duplicate mutation")
	require.ErrorContains(t, response.(*jsonrpc.Response).Error, "may have executed")
	require.Equal(t, 1, r.requestUsage().AbandonedOperations)
	r.clearRequests()
}

func testSupervisor(t *testing.T) *supervisor {
	t.Helper()
	m := newTestManager(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return newSupervisor(ctx, &m)
}

// Pure lifecycle fixtures never perform filesystem/process/network operations
// inside a fake-time bubble. Storage fault tests override this transaction seam.
func stateSupervisor(t *testing.T) *supervisor {
	t.Helper()
	m := &manager{limits: config.Default()}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	s := newSupervisor(ctx, m)
	useMemoryRegistry(s)
	return s
}

func readyTestGeneration(t *testing.T, s *supervisor, worktree string) *ownedGeneration {
	t.Helper()
	ready := make(chan struct{})
	close(ready)
	g := &ownedGeneration{id: ownerNonce(), worktree: worktree, state: generationReady, ready: ready,
		completed: s.now(), connections: make(map[*ownedUpstream]struct{})}
	g.record = record{Worktree: worktree, Port: firstPort, PID: 123, OwnerBoot: s.boot, Generation: g.id}
	s.mu.Lock()
	s.generations[worktree] = g
	s.mu.Unlock()
	_, err := s.updateRecords(t.Context(), func(records []record) ([]record, error) { return append(records, g.record), nil })
	require.NoError(t, err)
	return g
}

func readyTestConnection(t *testing.T, s *supervisor, g *ownedGeneration) mcp.Connection {
	t.Helper()
	conn, err := s.connection(newFakeConn(), g.record, context.Background(), func() {})
	require.NoError(t, err)
	return conn
}

func supervisorGenerationState(s *supervisor, g *ownedGeneration) generationState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return g.state
}

func TestSupervisorIdleBoundaryAndQuietConnections(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		g := readyTestGeneration(t, s, testHome)
		conn := readyTestConnection(t, s, g)
		time.Sleep(time.Hour - time.Nanosecond)
		s.idleSweep()
		require.Equal(t, generationReady, supervisorGenerationState(s, g))
		time.Sleep(time.Nanosecond)
		s.idleSweep()
		require.NotEqual(t, generationReady, supervisorGenerationState(s, g), "quiet attachment pinned the generation")
		require.Error(t, conn.Write(t.Context(), &jsonrpc.Request{Method: "heartbeat"}), "retired transport still delivered")
		<-g.drainDone
		synctest.Wait()
	})
}

func TestSupervisorQueuedAndActiveWorkSurviveIdle(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		g := readyTestGeneration(t, s, testHome)
		state := &callState{}
		require.NoError(t, s.admit(state))
		require.NoError(t, s.bind(state, testHome))
		time.Sleep(2 * time.Hour)
		s.idleSweep()
		require.Equal(t, generationReady, supervisorGenerationState(s, g), "queued delivery lost its target protection")
		conn := readyTestConnection(t, s, g)
		key := operationKey{conn, mustID(t, "active")}
		require.NoError(t, s.begin(state, key, testHome, nil, 0, 0))
		time.Sleep(2 * time.Hour)
		s.idleSweep()
		require.Equal(t, generationReady, supervisorGenerationState(s, g), "long active operation was treated as idle")
		s.answered(key)
		s.idleSweep()
		require.Equal(t, generationReady, supervisorGenerationState(s, g), "completion did not reset idle clock")
		time.Sleep(time.Hour)
		s.idleSweep()
		require.NotEqual(t, generationReady, supervisorGenerationState(s, g))
		<-g.drainDone
		synctest.Wait()
	})
}

func TestSupervisorAbandonedOperationsRetireOnlyAfterGrace(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		g := readyTestGeneration(t, s, testHome)
		state := &callState{}
		require.NoError(t, s.admit(state))
		require.NoError(t, s.bind(state, testHome))
		conn := readyTestConnection(t, s, g)
		key := operationKey{conn, mustID(t, "unknown")}
		require.NoError(t, s.begin(state, key, testHome, nil, 0, 0))
		time.Sleep(time.Hour)
		s.abandon(key)
		s.idleSweep()
		require.Equal(t, generationReady, supervisorGenerationState(s, g))
		require.Len(t, s.operations, 1, "disconnect falsely completed remote work")
		time.Sleep(time.Hour)
		s.idleSweep()
		require.NotEqual(t, generationReady, supervisorGenerationState(s, g))
		require.Len(t, s.operations, 1, "signalling/draining prematurely released uncertain work")
		<-g.drainDone
		s.confirmExited(g)
		require.Empty(t, s.operations)
		synctest.Wait()
	})
}

func TestSupervisorGlobalAdmissionDoesNotMultiplyWithRouters(t *testing.T) {
	t.Parallel()
	s := testSupervisor(t)
	s.m.limits.OwnerOperations = 2
	r1, r2 := newRouter(t.Context(), s, testHome), newRouter(t.Context(), s, testHome)
	id1, id2, id3 := mustID(t, "a"), mustID(t, "b"), mustID(t, "c")
	_, err := r1.admit(id1, func() {})
	require.NoError(t, err)
	_, err = r2.admit(id2, func() {})
	require.NoError(t, err)
	_, err = r2.admit(id3, func() {})
	require.ErrorContains(t, err, "global")
	require.Len(t, r2.awaitingUpstream, 1, "rejected global admission allocated local state")
	require.Same(t, r1.sseBudget, r2.sseBudget, "SSE budget multiplied per attachment")
	r1.clearRequests()
	r2.clearRequests()
	require.Empty(t, s.queued)
}

func TestSupervisorSharedFilesystemWorkerSurvivesSessionChurnBoundedly(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		release := make(chan struct{})
		defer close(release)
		var runs atomic.Int32
		for range 32 {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			_, err := ownerFilesystemCall(ctx, s, func() int {
				runs.Add(1)
				<-release
				return 1
			})
			cancel()
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
		require.Equal(t, int32(1), runs.Load(), "expired sessions created replacement filesystem workers")
	})
}

type completionOnCloseConn struct {
	mcp.Connection
	completed func()
}

func (c *completionOnCloseConn) Close() error {
	c.completed()
	return c.Connection.Close()
}

func TestSupervisorIdleCandidateRechecksLastCompletion(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.m.limits.OwnerOperations = 2
		now := time.Now()
		s.now = func() time.Time { return now }
		var generations []*ownedGeneration
		var keys []operationKey
		for _, worktree := range []string{"/first", "/second"} {
			g := &ownedGeneration{id: worktree, worktree: worktree, state: generationReady,
				completed: now.Add(-2 * time.Hour), connections: make(map[*ownedUpstream]struct{}), exited: make(chan struct{})}
			conn := &ownedUpstream{s: s, g: g, ctx: context.Background(), release: func() {}, identity: &connectionIdentity{}}
			g.connections[conn] = struct{}{}
			s.generations[worktree] = g
			key := operationKey{conn, mustID(t, worktree)}
			s.operations[remoteKeyOf(key)] = ownerOperation{generation: g, abandoned: now.Add(-2 * time.Hour)}
			generations, keys = append(generations, g), append(keys, key)
		}
		for i, g := range generations {
			for conn := range g.connections {
				other := keys[1-i]
				conn.Connection = &completionOnCloseConn{Connection: newFakeConn(), completed: func() { s.answered(other) }}
			}
		}
		t.Cleanup(func() {
			s.mu.Lock()
			var drains []<-chan struct{}
			for _, g := range generations {
				if g.drainDone != nil {
					drains = append(drains, g.drainDone)
				}
			}
			s.mu.Unlock()
			for _, done := range drains {
				<-done
			}
		})
		s.idleSweep()
		s.mu.Lock()
		ready := 0
		for _, g := range generations {
			if g.state == generationReady {
				ready++
			}
		}
		s.mu.Unlock()
		require.Equal(t, 1, ready, "definitive completion between selection and verdict must invalidate idle eligibility")
	})
}

func TestSupervisorCancellationDoesNotRefundQueuedPayload(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.m.limits.OwnerRetainedBytes = 2 * s.m.limits.MessageBytes
		r := newRouter(t.Context(), s, testHome)
		request := &jsonrpc.Request{ID: mustID(t, "queued"), Method: "tools/list"}
		call, ok := r.prepare(request)
		require.True(t, ok)
		r.cancelCall(&jsonrpc.Request{Params: jsontext.Value(`{"requestId":"queued"}`)})
		require.Empty(t, s.queued)
		require.Equal(t, 2*s.m.limits.MessageBytes, s.snapshot().RetainedMessageBytes,
			"queued original and terminal cancellation response both remain retained")
		other := newRouter(t.Context(), s, testHome)
		_, ok = other.prepare(&jsonrpc.Request{Method: "notification"})
		require.False(t, ok, "new session bypassed retained payload ceiling")
		require.ErrorContains(t, <-other.errs, "retained")
		call.cancel()
		releaseMessage(call.req) // The queue owner actually discards its entry.
		r.releaseRetainedMessages()
		require.Zero(t, s.snapshot().RetainedMessageBytes)
	})
}

func TestSupervisorBlockedWriterKeepsPayloadChargedAcrossChurn(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.m.limits.OwnerRetainedBytes = s.m.limits.MessageBytes
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r := newRouter(ctx, s, testHome)
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		writer := &fakeConn{onWrite: func(context.Context, jsonrpc.Message) error {
			close(entered)
			<-release
			return io.EOF
		}}
		r.forward(&jsonrpc.Response{ID: mustID(t, "answer"), Result: jsontext.Value(`{}`)})
		go func() { defer close(done); r.writeToClient(writer) }()
		<-entered
		cancel()
		r.releaseRetainedMessages()
		require.Equal(t, s.m.limits.MessageBytes, s.snapshot().RetainedMessageBytes,
			"shutdown refunded a response still held by a blocked writer")
		for range 32 {
			next := newRouter(t.Context(), s, testHome)
			next.forward(&jsonrpc.Response{})
			require.ErrorContains(t, <-next.errs, "retained")
			require.Empty(t, next.out, "terminal reply silently succeeded without capacity")
		}
		close(release)
		<-done
		synctest.Wait()
		require.Zero(t, s.snapshot().RetainedMessageBytes)
	})
}

func TestSupervisorOpaqueIDBoundaryAndControls(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		g := readyTestGeneration(t, s, testHome)
		r := newRouter(t.Context(), s, testHome)
		state, err := r.admit(mustID(t, strings.Repeat("x", maxOwnerIDBytes)), func() {})
		require.NoError(t, err)
		s.releaseQueued(state)
		_, err = r.admit(mustID(t, strings.Repeat("x", maxOwnerIDBytes+1)), func() {})
		require.ErrorContains(t, err, "opaque")
		conn := readyTestConnection(t, s, g)
		id := mustID(t, "control")
		require.NoError(t, s.serverControl(operationKey{conn, id}, g))
		time.Sleep(2 * time.Hour)
		s.idleSweep()
		require.Equal(t, generationReady, supervisorGenerationState(s, g))
		require.Equal(t, 1, s.snapshot().Operations)
		require.NoError(t, conn.Write(t.Context(), &jsonrpc.Response{ID: id}))
		s.idleSweep()
		require.Equal(t, generationReady, supervisorGenerationState(s, g), "control completion did not reset idle")
		require.Zero(t, s.snapshot().Operations)
	})
}

func TestSupervisorUnknownOrFailedControlCannotRefreshIdle(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		g := readyTestGeneration(t, s, testHome)
		conn, err := s.connection(failingConn(), g.record, context.Background(), func() {})
		require.NoError(t, err)
		id := mustID(t, "failed")
		require.NoError(t, s.serverControl(operationKey{conn, id}, g))
		before := g.completed
		time.Sleep(2 * time.Hour)
		require.Error(t, conn.Write(t.Context(), &jsonrpc.Response{ID: id}))
		require.Equal(t, before, g.completed)
		require.Len(t, s.controls, 1)
		unknown := readyTestConnection(t, s, g)
		require.NoError(t, unknown.Write(t.Context(), &jsonrpc.Response{ID: mustID(t, "unknown")}))
		require.Equal(t, before, g.completed)
	})
}

func TestSupervisorCorruptRegistryNeverLosesReservations(t *testing.T) {
	t.Parallel()
	for _, content := range []string{
		"{broken-record}\n", "{\"Worktree\":\"\",\"PID\":123,\"Port\":61100}\n",
		"{\"Worktree\":\"/secret-audit\",\"PID\":0,\"Port\":61100}\n", "{\"Worktree\":\"/repo\",\"PID\":123,\"Port\":1}\n",
		"{\"Worktree\":\"/repo\",\"PID\":123,\"Port\":61100}\n{broken-record}\n",
	} {
		m := newTestManager(t)
		s := newSupervisor(t.Context(), &m)
		require.NoError(t, os.WriteFile(m.mapPath, []byte(content), 0o600))
		var starts atomic.Int32
		s.start = func(context.Context, string, int, int) (*guardianChild, error) {
			starts.Add(1)
			return nil, errors.New("unexpected spawn")
		}
		_, err := s.ensureRecord(t.Context(), t.TempDir())
		require.Error(t, err)
		require.Contains(t, err.Error(), m.mapPath)
		require.NotContains(t, err.Error(), "secret-audit")
		require.Zero(t, starts.Load(), "corruption reached process startup")
		got, err := os.ReadFile(m.mapPath)
		require.NoError(t, err)
		require.Equal(t, content, string(got), "corruption silently dropped a process reservation")
	}
}

func TestSupervisorGuardianLossRetainsCapacityThroughStorageFailure(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.m.limits.SharedServers = 1
		g := readyTestGeneration(t, s, testHome)
		baseline := s.fdReserved
		g.fdQuota = 512
		s.fdReserved += g.fdQuota + guardianFDAllowance
		reserved := s.fdReserved
		s.m.watch = &watchPlans{plans: map[string]watchSnapshot{testHome: {}}}
		connection := readyTestConnection(t, s, g)
		state := &callState{}
		require.NoError(t, s.admit(state))
		require.NoError(t, s.bind(state, testHome))
		key := operationKey{connection, mustID(t, "unknown")}
		require.NoError(t, s.begin(state, key, testHome, nil, 0, 0))
		s.fenceUncertain(g)
		require.Equal(t, generationTerminating, supervisorGenerationState(s, g))
		require.Equal(t, reserved, s.snapshot().FDReserved)
		require.Equal(t, 1, s.snapshot().Operations)
		require.False(t, s.snapshot().Generations[0].ConfirmedExit)
		require.Contains(t, s.snapshot().Generations[0].Error, "unconfirmed")
		require.False(t, connection.(*ownedUpstream).available())
		_, err := s.ensureRecord(t.Context(), "/other-worktree")
		require.ErrorContains(t, err, "server limit")
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, err = s.ensureRecord(ctx, testHome)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Error(t, s.begin(&callState{}, operationKey{connection, mustID(t, "late")}, testHome, nil, 0, 0))
		require.Error(t, s.serverControl(operationKey{connection, mustID(t, "late-control")}, g))
		persist := s.updateRecords
		s.updateRecords = func(context.Context, func([]record) ([]record, error)) ([]record, error) {
			return nil, io.ErrClosedPipe
		}
		s.confirmExited(g) // Explicit fixture proof; persistence still fails.
		require.Equal(t, generationExited, supervisorGenerationState(s, g))
		require.True(t, s.snapshot().Generations[0].ConfirmedExit)
		require.Equal(t, reserved, s.snapshot().FDReserved, "uncommitted cleanup refunded reservation")
		require.Zero(t, s.snapshot().Operations)
		require.Empty(t, s.m.watch.plans)
		s.updateRecords = persist
		s.idleSweep()
		// Wait for all finite cleanup work before asserting quiescent ownership.
		synctest.Wait()
		require.Empty(t, s.snapshot().Generations)
		require.Equal(t, baseline, s.snapshot().FDReserved)
		replacement := readyTestGeneration(t, s, testHome)
		s.fenceUncertain(g)
		s.confirmExited(g)
		require.Same(t, replacement, s.generations[testHome], "stale cleanup altered a replacement generation")
	})
}

func TestSupervisorDeliveryProofDistinguishesRejectionFromUncertainty(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		g := readyTestGeneration(t, s, testHome)
		underlying := newFakeConn()
		conn, err := s.connection(underlying, g.record, context.Background(), func() {})
		require.NoError(t, err)
		var writes int
		writeErr := error(nil)
		underlying.onWrite = func(context.Context, jsonrpc.Message) error { writes++; return writeErr }
		s.validateToolchain = func(context.Context, string) error { return errors.New("toolchain refused") }
		before := g.completed
		for index, expected := range []error{errOwnerPreDelivery, transport.ErrMessageTooLarge, io.ErrClosedPipe} {
			state := &callState{}
			id := mustID(t, float64(index+1))
			require.NoError(t, s.admit(state))
			require.NoError(t, s.bind(state, testHome))
			require.NoError(t, s.begin(state, operationKey{conn, id}, testHome, nil, 0, 0))
			if index > 0 {
				s.validateToolchain = func(context.Context, string) error { return nil }
				writeErr = expected
			}
			err := conn.Write(t.Context(), &jsonrpc.Request{ID: id, Method: "tools/call"})
			require.ErrorIs(t, err, expected)
			require.Equal(t, index, writes, "proven pre-delivery failure wrote a caller request")
			if index < 2 {
				require.Zero(t, s.snapshot().Operations)
			} else {
				require.Equal(t, 1, s.snapshot().Operations)
			}
			require.Equal(t, before, g.completed, "failed delivery refreshed completion clock")
			require.Zero(t, g.writes)
		}
		s.m.limits.OwnerOperations = 1
		err = conn.Write(t.Context(), &jsonrpc.Request{ID: mustID(t, "private-handshake"), Method: "initialize"})
		require.ErrorIs(t, err, errOwnerPreDelivery, "private handshakes bypassed global capacity")
		require.Equal(t, 2, writes)
		require.NoError(t, conn.Close())
		require.Equal(t, 1, s.snapshot().Operations, "closing transport erased uncertain execution")
	})
}

func TestSupervisorStartupFailureCleanup(t *testing.T) {
	for _, failure := range []string{"spawn", "readiness", "registry commit", "registry cleanup"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			s := testSupervisor(t)
			root := t.TempDir()
			baseline := s.snapshot().FDReserved
			proof := make(chan struct{})
			var signals atomic.Int32
			s.start = func(context.Context, string, int, int) (*guardianChild, error) {
				if failure == "spawn" {
					return nil, io.ErrClosedPipe
				}
				return &guardianChild{Process: &os.Process{Pid: 123}, ConfirmedDone: proof, Signal: func(os.Signal) error { signals.Add(1); close(proof); return nil }}, nil
			}
			s.ready = func(context.Context, int, int) error {
				if failure == "readiness" {
					return io.ErrClosedPipe
				}
				return nil
			}
			persist := s.updateRecords
			var transactions atomic.Int32
			s.updateRecords = func(ctx context.Context, update func([]record) ([]record, error)) ([]record, error) {
				n := transactions.Add(1)
				if n == 2 && (failure == "registry commit" || failure == "registry cleanup") || n > 2 && failure == "registry cleanup" {
					return nil, io.ErrClosedPipe
				}
				return persist(ctx, update)
			}
			_, err := s.ensureRecord(t.Context(), root)
			require.ErrorIs(t, err, io.ErrClosedPipe)
			if failure != "spawn" {
				select {
				case <-proof:
				default:
					t.Fatal("failed start returned before cleanup proof")
				}
				require.EqualValues(t, 1, signals.Load())
			}
			if failure == "registry cleanup" {
				require.Len(t, s.snapshot().Generations, 1)
				require.True(t, s.snapshot().Generations[0].ConfirmedExit)
				require.Greater(t, s.snapshot().FDReserved, baseline, "failed durable cleanup refunded the FD reservation")
				s.mu.Lock()
				exited := s.generations[root].exited
				s.mu.Unlock()
				s.updateRecords = persist
				s.idleSweep()
				mustRecv(t, exited, "retry of durable registry cleanup")
			}
			require.Empty(t, s.snapshot().Generations)
			require.Equal(t, baseline, s.snapshot().FDReserved)
			require.Empty(t, mustReadMap(t, s.m.mapPath))
		})
	}
}

func TestSupervisorConcurrentStartsReserveDistinctPorts(t *testing.T) {
	t.Parallel()
	s := testSupervisor(t)
	s.m.limits.SharedServers = 2
	parent := t.TempDir()
	seen := make(map[int]string)
	var roots []string
	for i := range portCount + 1 {
		path := filepath.Join(parent, strconv.Itoa(i))
		port := basePort(path)
		if other, ok := seen[port]; ok {
			roots = []string{other, path}
			break
		}
		seen[port] = path
	}
	require.Len(t, roots, 2)
	for _, root := range roots {
		require.NoError(t, os.Mkdir(root, 0o700))
	}
	started := make(chan int, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var starts atomic.Int32
	s.start = func(_ context.Context, _ string, port, _ int) (*guardianChild, error) {
		starts.Add(1)
		started <- port
		<-release
		done := make(chan struct{})
		var stopped sync.Once
		stop := func(os.Signal) error { stopped.Do(func() { close(done) }); return nil }
		return &guardianChild{Process: &os.Process{Pid: 123}, ConfirmedDone: done, Signal: stop, CloseOwner: func() error { return stop(syscall.SIGTERM) }}, nil
	}
	s.ready = func(context.Context, int, int) error { return nil }
	records := make(chan record, 3)
	failures := make(chan error, 3)
	for _, root := range []string{roots[0], roots[1], roots[0]} {
		go func() { r, err := s.ensureRecord(t.Context(), root); records <- r; failures <- err }()
	}
	first, second := mustRecv(t, started, "first owned start"), mustRecv(t, started, "independent owned start")
	require.NotEqual(t, first, second, "concurrent starts reserved the same port before registry publication")
	unblock()
	ports := make(map[string]int)
	for range 3 {
		r := mustRecv(t, records, "shared generation readiness")
		require.NoError(t, mustRecv(t, failures, "startup result"))
		if port, ok := ports[r.Worktree]; ok {
			require.Equal(t, port, r.Port)
		}
		ports[r.Worktree] = r.Port
	}
	require.EqualValues(t, 2, starts.Load())
	_, err := s.ensureRecord(t.Context(), t.TempDir())
	require.ErrorContains(t, err, "server limit")
	require.EqualValues(t, 2, starts.Load())
	for _, root := range roots {
		_, err := s.ensureRecord(t.Context(), root)
		require.NoError(t, err)
	}
	require.EqualValues(t, 2, starts.Load(), "warm reuse spawned another process at capacity")
	s.mu.Lock()
	exits := make([]<-chan struct{}, 0, len(roots))
	for _, root := range roots {
		exits = append(exits, s.generations[root].exited)
	}
	s.mu.Unlock()
	s.shutdown()
	for _, exited := range exits {
		mustRecv(t, exited, "generation cleanup after concurrent startup")
	}
	require.Empty(t, s.snapshot().Generations)
}

func TestSupervisorPartialStartKeepsLeaseUntilReconciliation(t *testing.T) {
	for _, withHandle := range []bool{false, true} {
		t.Run(strconv.FormatBool(withHandle), func(t *testing.T) {
			t.Parallel()
			s := testSupervisor(t)
			s.m.limits.SharedServers = 1
			baseline := s.snapshot().FDReserved
			dir, token := t.TempDir(), rand.Text()
			path := filepath.Join(dir, "guardian-"+token+".json")
			var lease *os.File
			s.start = func(_ context.Context, root string, port, _ int) (*guardianChild, error) {
				var err error
				lease, err = guardianAcquireLease(dir, port)
				if err != nil {
					return nil, err
				}
				err = guardianWriteRecord(path, guardianRecord{Version: guardianProtocol, Token: token, Worktree: root, Port: port, MaxOpenFiles: 512})
				if err != nil {
					return nil, err
				}
				var child *guardianChild
				if withHandle {
					uncertain := make(chan struct{})
					close(uncertain)
					child = &guardianChild{UncertainDone: uncertain, ConfirmedDone: make(chan struct{})}
				}
				return child, &guardianStartError{StatePath: path, Child: child, Err: io.ErrClosedPipe, port: port}
			}
			_, err := s.ensureRecord(t.Context(), t.TempDir())
			require.ErrorIs(t, err, io.ErrClosedPipe)
			require.NotNil(t, lease)
			t.Cleanup(func() { _ = lease.Close() })
			require.Len(t, s.snapshot().Generations, 1)
			require.Greater(t, s.snapshot().FDReserved, baseline)
			_, err = s.ensureRecord(t.Context(), t.TempDir())
			require.ErrorContains(t, err, "server limit")
			_, err = s.maintenanceSweep(t.Context())
			require.NoError(t, err)
			require.Len(t, s.snapshot().Generations, 1, "busy lifetime lease was refunded")
			require.NoError(t, lease.Close())
			_, err = s.maintenanceSweep(t.Context())
			require.NoError(t, err)
			require.Empty(t, s.snapshot().Generations)
			require.Equal(t, baseline, s.snapshot().FDReserved)
			require.NoFileExists(t, path)
		})
	}
}

func TestSupervisorObserverStopsAfterReconciledExit(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		g := readyTestGeneration(t, s, testHome)
		g.exited = make(chan struct{})
		uncertain := make(chan struct{})
		close(uncertain)
		g.child = &guardianChild{UncertainDone: uncertain, ConfirmedDone: make(chan struct{})}
		done := make(chan struct{})
		go func() { s.observeGeneration(g); close(done) }()
		synctest.Wait()
		require.Equal(t, generationTerminating, supervisorGenerationState(s, g))
		s.confirmExited(g)
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("lease reconciliation leaked the observer waiting for a guardian frame")
		}
	})
}

func TestSupervisorShutdownDuringStartCannotPublishReady(t *testing.T) {
	t.Parallel()
	s := testSupervisor(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	proof := make(chan struct{})
	s.start = func(context.Context, string, int, int) (*guardianChild, error) {
		close(entered)
		<-release
		return &guardianChild{Process: &os.Process{Pid: 123}, ConfirmedDone: proof, Signal: func(os.Signal) error { close(proof); return nil }}, nil
	}
	s.ready = func(context.Context, int, int) error { return nil }
	result := make(chan error, 1)
	go func() { _, err := s.ensureRecord(t.Context(), t.TempDir()); result <- err }()
	mustRecv(t, entered, "in-progress guardian start")
	s.shutdown()
	unblock()
	require.ErrorIs(t, mustRecv(t, result, "owner shutdown during startup"), context.Canceled)
	require.Empty(t, s.snapshot().Generations)
	select {
	case <-proof:
	default:
		t.Fatal("late startup escaped owner shutdown cleanup")
	}
}

func TestSupervisorCancelledWaiterCannotCancelSharedStart(t *testing.T) {
	t.Parallel()
	s := testSupervisor(t)
	root := t.TempDir()
	entered, release := make(chan struct{}), make(chan struct{})
	var unblocked, stopped sync.Once
	unblock := func() { unblocked.Do(func() { close(release) }) }
	proof := make(chan struct{})
	stop := func(os.Signal) error { stopped.Do(func() { close(proof) }); return nil }
	t.Cleanup(func() { unblock(); s.shutdown() })
	var starts atomic.Int32
	s.start = func(context.Context, string, int, int) (*guardianChild, error) {
		starts.Add(1)
		close(entered)
		<-release
		return &guardianChild{Process: &os.Process{Pid: 123}, ConfirmedDone: proof, Signal: stop, CloseOwner: func() error { return stop(syscall.SIGTERM) }}, nil
	}
	s.ready = func(context.Context, int, int) error { return nil }
	waiter, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := s.ensureRecord(waiter, root); first <- err }()
	mustRecv(t, entered, "shared start before caller cancellation")
	cancel()
	require.ErrorIs(t, mustRecv(t, first, "cancelled waiter"), context.Canceled)
	require.Equal(t, string(generationStarting), s.snapshot().Generations[0].State)
	second := make(chan error, 1)
	go func() { _, err := s.ensureRecord(t.Context(), root); second <- err }()
	unblock()
	require.NoError(t, mustRecv(t, second, "independent client awaiting the shared start"))
	require.EqualValues(t, 1, starts.Load())
	s.mu.Lock()
	exited := s.generations[root].exited
	s.mu.Unlock()
	s.shutdown()
	mustRecv(t, exited, "shared generation cleanup")
	require.Empty(t, s.snapshot().Generations)
}

func TestSupervisorCallerAndControlNamespacesRemainDistinct(t *testing.T) {
	for _, controlFirst := range []bool{false, true} {
		t.Run(strconv.FormatBool(controlFirst), func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				s := stateSupervisor(t)
				g := readyTestGeneration(t, s, testHome)
				wire := &fakeConn{reads: make(chan jsonrpc.Message, 1), onWrite: func(context.Context, jsonrpc.Message) error { return nil }}
				conn, err := s.connection(wire, g.record, context.Background(), func() {})
				require.NoError(t, err)
				id := mustID(t, "shared-id")
				key := operationKey{conn, id}
				state := &callState{}
				require.NoError(t, s.admit(state))
				require.NoError(t, s.bind(state, testHome))
				require.NoError(t, s.begin(state, key, testHome, nil, 0, 0))
				require.NoError(t, s.serverControl(key, g))
				require.Equal(t, 2, s.snapshot().Generations[0].Operations)
				other := readyTestGeneration(t, s, testHome+"/other")
				snapshot := s.snapshot()
				require.Equal(t, 2, snapshot.Operations)
				counts := make(map[string]int)
				for _, generation := range snapshot.Generations {
					counts[generation.Generation] = generation.Operations
				}
				require.Equal(t, map[string]int{g.id: 2, other.id: 0}, counts)
				answerCaller := func() {
					wire.reads <- &jsonrpc.Response{ID: id}
					_, err := conn.Read(t.Context())
					require.NoError(t, err)
				}
				answerControl := func() { require.NoError(t, conn.Write(t.Context(), &jsonrpc.Response{ID: id})) }
				if controlFirst {
					answerControl()
					require.Len(t, s.operations, 1)
					require.Empty(t, s.controls)
					answerCaller()
				} else {
					answerCaller()
					require.Empty(t, s.operations)
					require.Len(t, s.controls, 1)
					answerControl()
				}
				require.Zero(t, s.snapshot().Operations)
			})
		})
	}
}

func TestSupervisorExitCleanupHasOneOwner(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.m.limits.SharedServers = 1
		g := readyTestGeneration(t, s, testHome)
		g.exited = make(chan struct{})
		baseline := s.fdReserved
		g.fdQuota = 512
		s.fdReserved += g.fdQuota + guardianFDAllowance
		persist := s.updateRecords
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		s.updateRecords = func(ctx context.Context, update func([]record) ([]record, error)) ([]record, error) {
			close(entered)
			<-release
			return persist(ctx, update)
		}
		go func() { s.confirmExited(g); close(done) }()
		<-entered
		s.confirmExited(g)
		require.True(t, g.cleaning)
		require.Len(t, s.snapshot().Generations, 1)
		require.Greater(t, s.snapshot().FDReserved, baseline)
		_, err := s.ensureRecord(t.Context(), "/replacement")
		require.ErrorContains(t, err, "server limit")
		select {
		case <-g.exited:
			t.Fatal("blocked durable cleanup released the worktree")
		default:
		}
		unblock()
		<-done
		require.Empty(t, s.snapshot().Generations)
		require.Equal(t, baseline, s.snapshot().FDReserved)
		select {
		case <-g.exited:
		default:
			t.Fatal("durable cleanup did not release its waiters")
		}
		s.confirmExited(g)
		require.Equal(t, baseline, s.snapshot().FDReserved, "duplicate exit refunded capacity twice")
	})
}

func TestSupervisorLateResponseCannotCompleteReplacement(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		old := readyTestGeneration(t, s, testHome)
		oldConn := readyTestConnection(t, s, old)
		id := mustID(t, "reused")
		require.NoError(t, s.begin(&callState{}, operationKey{oldConn, id}, testHome, nil, 0, 0))
		s.confirmExited(old)
		time.Sleep(time.Hour)
		next := readyTestGeneration(t, s, testHome)
		nextConn := readyTestConnection(t, s, next)
		key := operationKey{nextConn, id}
		require.NoError(t, s.begin(&callState{}, key, testHome, nil, 0, 0))
		completed := next.completed
		time.Sleep(time.Hour)
		s.answered(operationKey{oldConn, id})
		require.Contains(t, s.operations, remoteKeyOf(key), "old response released the replacement's credit")
		require.Equal(t, completed, next.completed, "old response refreshed replacement idle time")
		s.idleSweep()
		require.Equal(t, generationReady, supervisorGenerationState(s, next), "idle retirement interrupted active replacement work")
		s.answered(key)
		require.Empty(t, s.operations)
		require.Equal(t, s.now(), next.completed)
	})
}
func TestResponseReadCannotReleaseIDBeforeWaiterClaim(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		g := readyTestGeneration(t, s, testHome)
		r := newRouter(t.Context(), s, testHome)
		defer r.clearRequests()
		wire := newFakeConn()
		wire.reads = make(chan jsonrpc.Message, 1)
		conn, err := s.connection(wire, g.record, t.Context(), func() {})
		require.NoError(t, err)
		id := mustID(t, "same")
		first, err := r.admit(id, nil)
		require.NoError(t, err)
		require.NoError(t, r.beginOperation(id, first, conn, testHome, func() {}, nil))
		_, claimed := r.finish(id, nil, first)
		require.True(t, claimed)
		wire.reads <- &jsonrpc.Response{ID: id, Result: jsontext.Value(`{}`)}
		msg, err := conn.Read(t.Context())
		require.NoError(t, err)
		next, err := r.admit(id, nil)
		require.NoError(t, err)
		require.ErrorContains(t, r.beginOperation(id, next, conn, testHome, func() {}, nil), "unresolved upstream operation")
		_, claimed = r.finish(msg.(*jsonrpc.Response).ID, conn, nil)
		require.False(t, claimed, "old response claimed the replacement waiter")
		require.NoError(t, r.beginOperation(id, next, conn, testHome, func() {}, nil))
		r.clearRequests()
		require.Equal(t, 1, r.requestUsage().AbandonedOperations)
		s.confirmExited(g)
		require.Zero(t, r.requestUsage().UpstreamOperations)
		require.Zero(t, r.requestUsage().AbandonedOperations)
		require.Empty(t, r.remote.perWorktree)
	})
}
