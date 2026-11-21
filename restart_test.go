package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
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
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestartRestoresInitializedStickySession(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	owner, starts := supervisorTransportFixture(t, &m)
	home, sticky := supervisorWorktree(t), supervisorWorktree(t)
	initialize := &jsonrpc.Request{ID: mustID(t, "original-initialize"), Method: "initialize", Params: jsontext.Value(`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"restore-test","version":"1"}}`)}
	data, err := jsonrpc.EncodeMessage(initialize)
	require.NoError(t, err)
	frontend, lifetime := inheritHandoff(t, &m, restartCheckpoint{Deadline: time.Now().Add(time.Minute),
		Sessions: []restartSessionSnapshot{{Home: home, Sticky: sticky, Initialize: data, Pause: 1}}})
	require.NoError(t, lifetime.Close())
	require.NoError(t, owner.restoreRestartSessions())
	supervisorStdioCall(t, frontend, sticky)
	require.Equal(t, int32(1), starts.Load())
	require.NoError(t, frontend.Write(t.Context(), initialize))
	message, err := frontend.Read(t.Context())
	require.NoError(t, err)
	require.ErrorContains(t, message.(*jsonrpc.Response).Error, "already initialized")
	require.NoError(t, frontend.Close())
	require.Eventually(t, func() bool {
		owner.frontends.mu.Lock()
		defer owner.frontends.mu.Unlock()
		return len(owner.frontends.sessions) == 0
	}, time.Second, time.Millisecond)
	owner.mu.Lock()
	require.Zero(t, owner.retainedBytes)
	owner.mu.Unlock()
}

func TestRestartCommandAndRejectedUpgradeKeepClientsUsable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	m, err := newManager()
	require.NoError(t, err)
	owner, starts := supervisorTransportFixture(t, m)
	home := supervisorWorktree(t)
	client := supervisorStdioClient(t, m, home)
	var output bytes.Buffer
	require.NoError(t, restartCommand([]string{"--timeout=10s"}, false, &output))
	var receipt restartReceipt
	require.NoError(t, json.Unmarshal(output.Bytes(), &receipt))
	require.Equal(t, owner.boot, receipt.Boot)
	require.Equal(t, int32(2), starts.Load())
	lock, err := os.OpenFile(m.mapPath+".owner.lock", os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer func() { _ = lock.Close() }()
	m.restart.lock = lock
	for _, binary := range []string{"/bin/echo", "/nonexistent/manager"} {
		output.Reset()
		require.Error(t, restartCommand([]string{"--hot", "--binary=" + binary, "--timeout=5s"}, false, &output))
		require.Empty(t, output.String())
		require.Equal(t, int32(2), starts.Load())
		supervisorStdioCall(t, client, home)
	}

	t.Setenv("GOPLS_RESTART_TEST_FAIL_CHILD", "1")
	executable, err := os.Executable()
	require.NoError(t, err)
	output.Reset()
	require.ErrorContains(t, restartCommand([]string{"--hot", "--binary=" + executable, "--timeout=10s"}, false, &output), "readiness")
	require.Empty(t, output.String())
	require.False(t, m.restart.isTransferred())
	supervisorStdioCall(t, client, home)
	require.Equal(t, int32(3), starts.Load(), "a failed successor must resume the existing session without replay")
}

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && (os.Args[1] == "internal-restart-check" || os.Args[1] == "internal-restart-child") {
		watchdog := time.AfterFunc(5*time.Minute, func() { os.Exit(124) })
		defer watchdog.Stop()
		if os.Args[1] == "internal-restart-child" && os.Getenv("GOPLS_RESTART_TEST_FAIL_CHILD") == "1" {
			os.Exit(3)
		}
		if err := run(os.Args, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func requireBlocked[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("%s: received %v", what, v)
	default:
	}
}

func unixConnPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	listener, err := net.Listen("unix", filepath.Join("/tmp", "gopls-restart-"+ownerNonce()+".sock"))
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	client, err = net.Dial("unix", listener.Addr().String())
	require.NoError(t, err)
	server, err = listener.Accept()
	require.NoError(t, err)
	return client, server
}

// inheritHandoff stages what a committed parent passes to restoreRestartSessions:
// one paused frontend session plus ready, commit and lifetime pipes.
func inheritHandoff(t *testing.T, m *manager, checkpoint restartCheckpoint) (frontend *restartPeer, lifetime *os.File) {
	t.Helper()
	committed := make(chan struct{})
	t.Cleanup(func() { <-committed }) // registered first, so it runs after the pipes close
	conn, accepted := unixConnPair(t)
	file, err := restartConnFile(accepted)
	require.NoError(t, err)
	require.NoError(t, accepted.Close())
	frontend = newRestartPeer(t.Context(), conn, conn, m.limits.MessageBytes, true)
	frontend.paused = &restartPauseState{id: 1, resume: make(chan struct{}), cancel: func() {}}
	frontend.start()
	t.Cleanup(func() { _ = frontend.Close() })
	readyRead, readyWrite, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = readyRead.Close(); _ = readyWrite.Close() })
	commitRead, commitWrite, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = commitRead.Close(); _ = commitWrite.Close() })
	lifetimeRead, lifetime, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = lifetime.Close() })
	m.restart.inherited = &restartInheritance{ready: readyWrite, commit: commitRead, lifetime: lifetimeRead, files: []*os.File{file}, checkpoint: checkpoint}
	go func() {
		defer close(committed)
		if _, err := bufio.NewReader(readyRead).ReadBytes('\n'); assert.NoError(t, err) {
			_, err = commitWrite.Write([]byte{1})
			assert.NoError(t, err)
		}
	}()
	return frontend, lifetime
}

func restartPeerPair(t *testing.T) (*restartPeer, *restartPeer) {
	t.Helper()
	a, b := net.Pipe()
	owner := newRestartPeer(t.Context(), a, a, 4096, false)
	frontend := newRestartPeer(t.Context(), b, b, 4096, true)
	owner.start()
	frontend.start()
	t.Cleanup(func() { _ = owner.Close(); _ = frontend.Close() })
	return owner, frontend
}

func TestRestartPeerPreservesMessagesAcrossRepeatedPauses(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		owner, frontend := restartPeerPair(t)
		for range 3 {
			paused := make(chan error, 1)
			go func() { _, err := owner.pause(t.Context()); paused <- err }()
			marker, err := owner.Read(t.Context())
			require.NoError(t, err)
			barrier := owner.barrier(marker)
			require.NotNil(t, barrier)
			close(barrier)
			require.NoError(t, <-paused)
			written := make(chan error, 1)
			request := &jsonrpc.Request{ID: mustID(t, "call"), Method: "tools/list"}
			go func() { written <- frontend.Write(t.Context(), request) }()
			synctest.Wait()
			requireBlocked(t, written, "message crossed the pause")
			require.NoError(t, owner.resume(t.Context()))
			message, err := owner.Read(t.Context())
			require.NoError(t, err)
			require.Equal(t, request, message)
			require.NoError(t, <-written)
			response := &jsonrpc.Response{ID: request.ID, Result: jsontext.Value(`{"ok":true}`)}
			go func() { written <- owner.Write(t.Context(), response) }()
			message, err = frontend.Read(t.Context())
			require.NoError(t, err)
			require.Equal(t, response, message)
			require.NoError(t, <-written)
		}
	})
}

func TestRestartAbortedBarrierCannotBecomeClientTraffic(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		owner, _ := restartPeerPair(t)
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { _, err := owner.pause(ctx); result <- err }()
		marker, err := owner.Read(t.Context())
		require.NoError(t, err)
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		require.NoError(t, owner.resume(t.Context()))
		barrier := owner.barrier(marker)
		require.NotNil(t, barrier, "a late barrier must remain private after the pause state is cleared")
		close(barrier)
	})
}

func TestRestartControlFrameFinishesAfterCommandCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		peer := newRestartPeer(t.Context(), a, a, 4096, false)
		defer func() { _ = peer.Close(); _ = b.Close() }()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- peer.write(ctx, restartResume, restartToken(17)) }()
		var header [5]byte
		_, err := io.ReadFull(b, header[:])
		require.NoError(t, err)
		cancel()
		var payload [8]byte
		_, err = io.ReadFull(b, payload[:])
		require.NoError(t, err)
		require.NoError(t, <-result)
		require.Equal(t, restartResume, header[0])
		require.Equal(t, uint64(17), binary.BigEndian.Uint64(payload[:]))
	})
}

func TestRestartPauseAbortAndHTTPDrain(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		owner, frontend := restartPeerPair(t)
		gate := &restartHTTPGate{}
		frontend.onPause, frontend.onResume = gate.pause, gate.resume
		require.NoError(t, gate.enter(t.Context()))
		require.NoError(t, gate.enter(t.Context()))
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		result := make(chan error, 1)
		go func() { _, err := owner.pause(ctx); result <- err }()
		synctest.Wait()
		gate.leave()
		synctest.Wait()
		requireBlocked(t, result, "pause completed with an active HTTP request")
		cancelled, cancelWait := context.WithCancel(t.Context())
		cancelWait()
		require.ErrorIs(t, gate.enter(cancelled), context.Canceled)
		require.ErrorIs(t, <-result, context.DeadlineExceeded)
		require.NoError(t, owner.resume(t.Context()))
		synctest.Wait()
		gate.leave()
		require.NoError(t, gate.enter(t.Context()))
		gate.leave()
		go func() { _, err := owner.pause(t.Context()); result <- err }()
		marker, err := owner.Read(t.Context())
		require.NoError(t, err)
		close(owner.barrier(marker))
		require.NoError(t, <-result)
		require.NoError(t, owner.resume(t.Context()))
	})
}

func TestRestartWireRejectsInvalidAndOversizedFrames(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		kind byte
		size uint32
		body string
	}{
		{"oversized", restartData, 4097, ""},
		{"control length", restartPause, 9, ""},
		{"unknown control", 99, 8, "12345678"},
		{"wrong direction", restartResume, 8, "12345678"},
		{"invalid json", restartData, 1, "!"},
		{"truncated payload", restartData, 4, "{}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				a, b := net.Pipe()
				peer := newRestartPeer(t.Context(), a, a, 4096, false)
				peer.start()
				t.Cleanup(func() { _ = peer.Close(); _ = b.Close() })
				go func() {
					defer func() { _ = b.Close() }()
					var header [5]byte
					header[0] = tc.kind
					binary.BigEndian.PutUint32(header[1:], tc.size)
					_, _ = b.Write(append(header[:], tc.body...))
				}()
				_, err := peer.Read(t.Context())
				require.Error(t, err)
				require.Error(t, peer.Write(t.Context(), &jsonrpc.Request{Method: strings.Repeat("x", 4097)}))
			})
		})
	}
}

func TestRestartCommandGrammarAndHTTPValidation(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"--timeout=0"}, {"--timeout=2h"}, {"--binary=/tmp/no"}, {"extra"}, {"--unknown"}} {
		require.Error(t, restartCommand(args, false, io.Discard))
	}
	s := stateSupervisor(t)
	s.initializeFrontends()
	for _, body := range []string{"!", "{}", strings.Repeat("x", 4097), `{"Hot":true,"Binary":"relative","Timeout":1}`} {
		req := httptest.NewRequest(http.MethodPost, "/restart", strings.NewReader(body))
		require.NoError(t, ownerHeaders(req, s.m, ""))
		response := httptest.NewRecorder()
		s.handler().ServeHTTP(response, req)
		require.Equal(t, http.StatusBadRequest, response.Code)
	}
	s.restart.active = true
	_, err := s.restartAll(t.Context(), restartRequest{})
	require.ErrorContains(t, err, "already in progress")
	done, ok := s.frontendAdmission()
	require.True(t, ok, "draining keeps admission open until the owner is quiet")
	done()
	s.restart.closed = true
	_, ok = s.frontendAdmission()
	require.False(t, ok)
	require.True(t, s.restartStatus().Active)
	s.restart.active, s.restart.closed = false, false
	_, err = s.restartAll(t.Context(), restartRequest{Hot: true})
	require.ErrorContains(t, err, "transferable singleton")
	require.False(t, s.restartStatus().Active)
}

func TestRestartAllGoplsPreservesStdioSessions(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	owner, starts := supervisorTransportFixture(t, &m)
	home := supervisorWorktree(t)
	first := supervisorStdioClient(t, &m, home)
	second := supervisorStdioClient(t, &m, home)
	supervisorStdioCall(t, first, home)
	supervisorStdioCall(t, second, home)
	supervisorHTTPCall(t, &m, home)
	require.Equal(t, int32(1), starts.Load())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	receipt, err := owner.restartAll(ctx, restartRequest{})
	require.NoError(t, err)
	require.Equal(t, owner.boot, receipt.Boot)
	require.Equal(t, int32(2), starts.Load())
	supervisorStdioCall(t, first, home)
	supervisorStdioCall(t, second, home)
	supervisorHTTPCall(t, &m, home)
	require.Equal(t, int32(2), starts.Load())
}

func TestRestartDrainsAdmittedCallWithoutReplay(t *testing.T) {
	for _, cancelCall := range []bool{false, true} {
		t.Run(strconv.FormatBool(cancelCall), func(t *testing.T) {
			t.Parallel()
			m := newTestManager(t)
			started, release := make(chan struct{}), make(chan struct{})
			var executions atomic.Int32
			owner, starts := supervisorTransportFixture(t, &m, func(server *mcp.Server) {
				server.AddTool(&mcp.Tool{Name: "hold", InputSchema: map[string]any{"type": "object"}},
					func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
						executions.Add(1)
						close(started)
						select {
						case <-release:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
						return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "finished"}}}, nil
					})
			})
			home := supervisorWorktree(t)
			client := supervisorStdioClient(t, &m, home)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			request := &jsonrpc.Request{ID: mustID(t, "held"), Method: "tools/call", Params: jsontext.Value(`{"name":"hold","arguments":{}}`)}
			require.NoError(t, client.Write(ctx, request))
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("tool was not admitted")
			}
			restarted := make(chan error, 1)
			go func() { _, err := owner.restartAll(ctx, restartRequest{}); restarted <- err }()
			require.Eventually(t, func() bool { return owner.restartStatus().Active }, time.Second, time.Millisecond)
			peer := client.(*restartPeer)
			paused := peer.pausedState() != nil
			require.False(t, paused, "cancellation must remain live while admitted calls drain")
			require.Equal(t, int32(1), starts.Load())
			requireBlocked(t, restarted, "restart did not drain the call")
			if cancelCall {
				require.NoError(t, client.Write(ctx, &jsonrpc.Request{Method: "notifications/cancelled", Params: jsontext.Value(`{"requestId":"held"}`)}))
			} else {
				close(release)
			}
			message, err := client.Read(ctx)
			require.NoError(t, err)
			response := message.(*jsonrpc.Response)
			require.Equal(t, request.ID, response.ID)
			if cancelCall {
				require.NotNil(t, response.Error)
				remote, ok := errors.AsType[*jsonrpc.Error](response.Error)
				require.True(t, ok)
				require.Equal(t, int64(-32800), remote.Code)
			} else {
				require.Nil(t, response.Error)
				require.Contains(t, string(response.Result), "finished")
			}
			err = <-restarted
			if cancelCall && err != nil {
				require.Contains(t, err.Error(), "unconfirmed")
			} else {
				require.NoError(t, err)
				require.Equal(t, int32(2), starts.Load())
			}
			require.Equal(t, int32(1), executions.Load())
			supervisorStdioCall(t, client, home)
		})
	}
}

func TestRestartRefusesUnconfirmedWorkAndResumesClients(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.initializeFrontends()
		g := readyTestGeneration(t, s, testHome)
		conn := readyTestConnection(t, s, g)
		r := newRouter(t.Context(), s, testHome)
		id := mustID(t, "lost")
		state, err := r.admit(id, func() {})
		require.NoError(t, err)
		require.NoError(t, r.beginOperation(id, state, conn, testHome, func() {}, nil))
		r.finish(id, nil, state)
		_, err = s.restartAll(t.Context(), restartRequest{})
		require.ErrorContains(t, err, "unconfirmed abandoned")
		require.Equal(t, generationReady, supervisorGenerationState(s, g))
		require.False(t, s.restartStatus().Active)
		require.Len(t, s.operations, 1)
	})
}

func TestRestartPreflightLeavesHealthyGenerationsReady(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.initializeFrontends()
		healthy := readyTestGeneration(t, s, testHome)
		upstream := readyTestConnection(t, s, healthy).(*ownedUpstream)
		first := readyTestGeneration(t, s, "/uncertain")
		second := readyTestGeneration(t, s, "/draining")
		first.state, second.state = generationTerminating, generationDraining
		conn, remote := net.Pipe()
		peer := newRestartPeer(s.ctx, conn, conn, 1024, false)
		defer func() { _ = peer.Close(); _ = remote.Close() }()
		s.frontends.sessions[&restartSession{peer: peer}] = struct{}{}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		_, err := s.restartAll(ctx, restartRequest{})
		require.ErrorContains(t, err, `"/draining" (draining), "/uncertain" (terminating)`)
		require.Zero(t, peer.sequence, "preflight must run before pausing the frontend")
		require.False(t, s.restartStatus().Active)

		worktrees, err := s.drainRestartGenerations(ctx)
		require.ErrorContains(t, err, "cannot be safely drained")
		require.Empty(t, worktrees)
		require.Equal(t, generationReady, supervisorGenerationState(s, healthy))
		require.True(t, upstream.available())
	})
}

func TestRestartWaitsForPendingFrontendReservation(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.initializeFrontends()
		running, unblock := make(chan struct{}), make(chan struct{})
		defer close(unblock)
		go func() {
			_, _ = ownerFilesystemCall(t.Context(), s, func() struct{} {
				close(running)
				<-unblock
				return struct{}{}
			})
		}()
		<-running

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		req := httptest.NewRequestWithContext(ctx, http.MethodConnect, "/bridge", nil)
		response, attached := httptest.NewRecorder(), make(chan struct{})
		go func() {
			defer close(attached)
			s.attachFrontend(response, req, testHome)
		}()
		synctest.Wait()
		require.Equal(t, 1, s.snapshot().Sessions)
		s.closeFrontends() // The charged reservation has no socket to close.

		restarted := make(chan error, 1)
		go func() {
			_, err := s.restartAll(t.Context(), restartRequest{})
			restarted <- err
		}()
		synctest.Wait()
		require.True(t, s.restart.closed)
		require.Empty(t, restarted, "restart crossed a pending frontend admission")

		cancel()
		<-attached
		require.Equal(t, http.StatusBadRequest, response.Code)
		require.NoError(t, <-restarted)
		require.Zero(t, s.snapshot().Sessions)
	})
}

func TestRestartWaitsForStartingGeneration(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.initializeFrontends()
		g := readyTestGeneration(t, s, testHome)
		g.state = generationStarting
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		checked := make(chan error, 1)
		go func() { checked <- restartWait(ctx, s.restartQuiescent) }()
		synctest.Wait()
		requireBlocked(t, checked, "restart should wait for startup")

		s.mu.Lock()
		g.state = generationReady
		s.mu.Unlock()
		require.NoError(t, <-checked)
	})
}

func TestRestartWaitsForUpstreamWriteWithoutCredit(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.initializeFrontends()
		g := readyTestGeneration(t, s, testHome)
		healthy := readyTestGeneration(t, s, testWorktree)
		entered, release := make(chan struct{}), make(chan struct{})
		raw := newFakeConn()
		raw.onWrite = func(ctx context.Context, _ jsonrpc.Message) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		conn, err := s.connection(raw, g.record, t.Context(), func() {})
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		written := make(chan error, 1)
		go func() { written <- conn.Write(ctx, &jsonrpc.Request{Method: "notifications/cancelled"}) }()
		<-entered
		require.Zero(t, s.snapshot().Operations)

		checked := make(chan error, 1)
		go func() { checked <- restartWait(ctx, s.restartQuiescent) }()
		synctest.Wait()
		requireBlocked(t, checked, "restart should wait for the write to return")
		_, err = s.drainRestartGenerations(ctx)
		require.ErrorContains(t, err, "new work arrived before generation drain")
		require.Equal(t, generationReady, supervisorGenerationState(s, healthy))

		close(release)
		require.NoError(t, <-written)
		require.NoError(t, <-checked)
	})
}

func TestRestartRechecksAfterHTTPBackendTeardown(t *testing.T) {
	for _, name := range []string{"cancelled", "abandoned control"} {
		t.Run(name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				s := stateSupervisor(t)
				s.initializeFrontends()
				s.m.limits.Idle = time.Millisecond
				healthy := readyTestGeneration(t, s, testHome)
				other := readyTestGeneration(t, s, testWorktree)
				conn := readyTestConnection(t, s, other)
				defer func() { _ = conn.Close() }()
				backend := newHTTPBackend(t.Context(), s, testHome)
				closing, ready := make(chan struct{}), make(chan struct{})
				close(ready)
				cancelBackend := backend.cancel
				backend.cancel = func() {
					cancelBackend()
					close(closing)
				}
				s.frontends.backends[testHome] = &supervisorHTTPBackend{backend: backend, ready: ready}
				finishClose := sync.OnceFunc(func() { close(backend.done) })
				defer finishClose()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				restarted := make(chan error, 1)
				go func() {
					_, err := s.restartAll(ctx, restartRequest{})
					restarted <- err
				}()
				mustRecv(t, closing, "restart to begin HTTP backend teardown")
				if name == "cancelled" {
					cancel()
				} else {
					require.NoError(t, s.serverControl(operationKey{conn, mustID(t, "late-roots")}, other))
					require.NoError(t, conn.Close())
					time.Sleep(s.m.limits.Idle)
				}
				finishClose()

				err := <-restarted
				if name == "cancelled" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorContains(t, err, "unconfirmed abandoned work")
					require.Equal(t, 1, s.snapshot().Operations)
				}
				require.Equal(t, generationReady, supervisorGenerationState(s, healthy))
				require.Equal(t, generationReady, supervisorGenerationState(s, other))
			})
		})
	}
}

func TestRestartHTTPGateWaitsForEveryActiveRequest(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		g := &restartHTTPGate{}
		require.NoError(t, g.enter(t.Context()))
		require.NoError(t, g.enter(t.Context()))
		ready := g.pause()
		g.leave()
		requireBlocked(t, ready, "released before the last HTTP request")
		g.leave()
		<-ready
		entered := make(chan error, 1)
		go func() { entered <- g.enter(t.Context()) }()
		synctest.Wait()
		requireBlocked(t, entered, "entered while paused")
		g.resume()
		require.NoError(t, <-entered)
		g.leave()
	})
}

func TestRestartReopensAdmissionAfterPeerResumeSettles(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.restart.active, s.restart.closed, s.restart.blockHTTP = true, true, true
		conn, remote := net.Pipe()
		peer := newRestartPeer(s.ctx, conn, conn, 1024, false)
		defer func() { _ = peer.Close(); _ = remote.Close() }()
		peer.paused = &restartPauseState{id: 1, resume: make(chan struct{})}
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.resumeAfterRestart([]*restartSession{{peer: peer}}, errors.New("worktree start failed"), true)
		}()
		synctest.Wait()
		require.False(t, s.restartHTTPBlocked())
		_, admitted := s.frontendAdmission()
		require.False(t, admitted, "admission opened while peer resumption was blocked")
		requireBlocked(t, done, "blocked peer resume completed")

		require.NoError(t, remote.Close())
		mustRecv(t, done, "peer resumption to settle")
		release, admitted := s.frontendAdmission()
		require.True(t, admitted)
		release()
		status := s.restartStatus()
		require.False(t, status.Active)
		require.Contains(t, status.Error, "worktree start failed")
		require.Contains(t, status.Error, io.ErrClosedPipe.Error())
	})
}

func TestRestartCheckpointRefusesUnsupportedListener(t *testing.T) {
	t.Parallel()
	s := stateSupervisor(t)
	s.initializeFrontends()
	_, err := s.forkReplacement(t.Context(), "unused", nil, nil)
	require.ErrorContains(t, err, "Unix listener")
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	_, err = restartConnFile(a)
	require.ErrorContains(t, err, "descriptor inheritance")
}

func TestRestartWaitHonorsCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.ErrorIs(t, restartWait(ctx, func() (bool, error) { return false, nil }), context.DeadlineExceeded)
		require.ErrorIs(t, restartWait(t.Context(), func() (bool, error) { return false, io.EOF }), io.EOF)
	})
}

func TestRestartPreviousOwnerReservationRequiresEOF(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"exit", "unexpected data", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := newSupervisor(ctx, &manager{limits: config.Default()})
			read, write, err := os.Pipe()
			require.NoError(t, err)
			defer func() { _ = write.Close() }()
			// Like an ExtraFiles descriptor: blocking until the child wraps it.
			fd, err := syscall.Dup(int(read.Fd()))
			require.NoError(t, err)
			require.NoError(t, read.Close())
			lifetime, err := pollableFile(fd, "restart-lifetime")
			require.NoError(t, err)
			s.m.restart.inherited = &restartInheritance{lifetime: lifetime}
			reserve, err := restartFDReserve(s.m)
			require.NoError(t, err)
			baseline := s.fdReserved
			s.fdReserved += reserve
			s.restart.previousOwner = true
			result := s.awaitPreviousOwner(reserve)
			s.mu.Lock()
			require.Equal(t, baseline+reserve, s.fdReserved)
			s.mu.Unlock()
			switch mode {
			case "exit":
				require.NoError(t, write.Close())
			case "unexpected data":
				_, err := write.Write([]byte{1})
				require.NoError(t, err)
			case "cancel":
				cancel()
			}
			select {
			case err := <-result:
				if mode == "exit" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "unconfirmed")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("previous-owner observer did not stop")
			}
			s.mu.Lock()
			if mode == "exit" {
				require.Equal(t, baseline, s.fdReserved)
			} else {
				require.Equal(t, baseline+reserve, s.fdReserved)
			}
			s.mu.Unlock()
			s.restart.mu.Lock()
			require.Equal(t, mode != "exit", s.restart.previousOwner)
			s.restart.mu.Unlock()
		})
	}
}

func TestRestartRefusesInsufficientHandoffBudget(t *testing.T) {
	t.Parallel()
	s := stateSupervisor(t)
	s.initializeFrontends()
	base, err := supervisorFDAllowance(s.m.limits)
	require.NoError(t, err)
	s.m.limits.OpenFiles = base
	_, err = s.restartAll(t.Context(), restartRequest{Hot: true})
	require.ErrorContains(t, err, "FD budget")
	require.False(t, s.restartStatus().Active)
	s.restart.previousOwner = true
	_, err = s.restartAll(t.Context(), restartRequest{})
	require.ErrorContains(t, err, "previous owner")
}

// A real fork/exec runs this same instrumented executable through TestMain.
// Its empty generation ledger exercises descriptor and singleton transfer
// without touching the developer's gopls processes or toolchain.
func TestRestartNativeForkExecPreservesAttachmentAndLock(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	m, err := newManager()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var child int
	t.Cleanup(func() {
		if child != 0 {
			process, err := os.FindProcess(child)
			if assert.NoError(t, err) {
				_ = process.Signal(os.Interrupt)
			}
		}
	})
	err = withSupervisorSingleton(ctx, m, func() error {
		path, err := supervisorSocket(m)
		require.NoError(t, err)
		listener, err := m.restartListener(path)
		require.NoError(t, err)
		defer m.closeRestartListener(listener, path)
		s := newSupervisor(ctx, m)
		s.initializeFrontends()
		defer s.shutdown()
		serveDone := make(chan error, 1)
		s.bindListener(listener)
		go func() { serveDone <- s.serve() }()
		require.Eventually(t, func() bool { return supervisorPing(t.Context(), m) == nil }, time.Second, time.Millisecond)
		clientConn, serverConn := unixConnPair(t)
		client := newRestartPeer(t.Context(), clientConn, clientConn, m.limits.MessageBytes, true)
		client.start()
		defer func() { _ = client.Close() }()
		session := &restartSession{peer: newRestartPeer(ctx, serverConn, serverConn, m.limits.MessageBytes, false), home: t.TempDir()}
		s.frontends.mu.Lock()
		s.frontends.sessions[session] = struct{}{}
		s.frontends.mu.Unlock()
		go s.runRestartSession(session)
		binary, err := os.Executable()
		require.NoError(t, err)
		restartCtx, stop := context.WithTimeout(t.Context(), 15*time.Second)
		defer stop()
		receipt, err := s.restartAll(restartCtx, restartRequest{Hot: true, Binary: binary})
		require.NoError(t, err)
		child = receipt.PID
		require.NotEqual(t, os.Getpid(), receipt.PID)
		require.NotEqual(t, s.boot, receipt.Boot)
		close(s.restart.committed)
		_ = listener.Close()
		require.NoError(t, <-serveDone)
		cancel()
		// This fixture keeps the test runner alive. Release its lifetime pipe
		// explicitly after teardown to simulate the old owner's OS exit.
		require.NoError(t, restartProcessLifetime.Close())
		var status bytes.Buffer
		require.NoError(t, querySupervisor(restartCtx, m, "/status", "", &status))
		var snapshot supervisorSnapshot
		require.NoError(t, json.Unmarshal(status.Bytes(), &snapshot))
		require.Equal(t, receipt.Boot, snapshot.Boot)
		require.Equal(t, 1, snapshot.Sessions)
		// A new independently-opened lock must remain excluded after the old
		// owner's descriptor closes. It must not unlock the inherited OFD.
		err = withSupervisorSingleton(t.Context(), m, func() error { return errors.New("unexpected acquired lock") })
		require.ErrorContains(t, err, "another supervisor")
		return nil
	})
	require.NoError(t, err)

	err = withSupervisorSingleton(t.Context(), m, func() error { return errors.New("successor singleton lock was lost") })
	require.ErrorContains(t, err, "another supervisor", "successor must retain exclusivity after the parent closes its original descriptor")
}

func TestRestartFailedResumeReleasesPausedSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.initializeFrontends()
		a, b := net.Pipe()
		owner := newRestartPeer(s.ctx, a, a, 4096, false)
		frontend := newRestartPeer(s.ctx, b, b, 4096, true)
		defer func() { _ = owner.Close(); _ = frontend.Close(); synctest.Wait() }()
		frontend.start()
		session := &restartSession{peer: owner, home: "/fixture"}
		s.frontends.sessions[session] = struct{}{}
		done := make(chan struct{})
		go func() { defer close(done); s.runRestartSession(session) }()
		_, err := owner.pause(t.Context())
		require.NoError(t, err)
		require.NoError(t, frontend.Close())
		require.Error(t, owner.resume(t.Context()))
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("disconnected paused session remains registered")
		}
		require.Empty(t, s.frontends.sessions)
	})
}

func TestRestartCommandRetriesStatusConnectionClosedDuringHandoff(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	m, err := newManager()
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(m.mapPath), 0o700))
	path, err := supervisorSocket(m)
	require.NoError(t, err)
	listener, err := m.restartListener(path)
	require.NoError(t, err)
	defer m.closeRestartListener(listener, path)
	boot := strings.Repeat("a", 32)
	var polls atomic.Int32
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/restart":
			_, _ = io.Copy(io.Discard, r.Body)
			assert.NoError(t, json.MarshalWrite(w, restartReceipt{Boot: boot, PID: os.Getpid()}))
		case "/status":
			if polls.Add(1) == 1 {
				conn, _, err := w.(http.Hijacker).Hijack()
				if assert.NoError(t, err) {
					_ = conn.Close()
				}
				return
			}
			assert.NoError(t, json.MarshalWrite(w, supervisorSnapshot{Boot: boot, PID: os.Getpid()}))
		}
	})}
	defer func() { _ = server.Close() }()
	go func() { _ = server.Serve(listener) }()
	var receipt bytes.Buffer
	require.NoError(t, restartCommand([]string{"--hot", "--binary=/bin/true", "--timeout=5s"}, false, &receipt))
	require.Equal(t, int32(2), polls.Load())
}

func TestServeReportsUncommittedListenerClose(t *testing.T) {
	t.Parallel()
	s := stateSupervisor(t)
	s.initializeFrontends()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s.bindListener(listener)
	require.NoError(t, listener.Close())
	require.ErrorIs(t, s.serve(), net.ErrClosed)
}

func TestRestartHandoffFailsWithoutPreviousOwnerExit(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"deadline", "unexpected data", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := newSupervisor(ctx, &manager{limits: config.Default()})
			useMemoryRegistry(s)
			var starts atomic.Int32
			s.start = func(context.Context, string, int, int) (*guardianChild, error) {
				starts.Add(1)
				return nil, errors.New("no gopls may start before previous-owner exit")
			}
			s.initializeFrontends()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() { _ = listener.Close() }()
			s.bindListener(listener)
			frontend, lifetime := inheritHandoff(t, s.m, restartCheckpoint{Deadline: time.Now().Add(300 * time.Millisecond), Worktrees: []string{t.TempDir()},
				Sessions: []restartSessionSnapshot{{Home: t.TempDir(), Pause: 1, HTTP: true}}})
			baseline := s.fdReserved
			reserve, err := restartFDReserve(s.m)
			require.NoError(t, err)
			require.NoError(t, s.restoreRestartSessions())
			switch mode {
			case "unexpected data":
				_, err := lifetime.Write([]byte{1})
				require.NoError(t, err)
			case "cancel":
				cancel()
			}
			select {
			case <-s.ctx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("owner kept running without previous-owner exit proof")
			}
			if mode == "cancel" {
				require.ErrorIs(t, context.Cause(s.ctx), context.Canceled)
			} else {
				require.ErrorContains(t, context.Cause(s.ctx), "hot restart handoff failed")
			}
			require.Eventually(t, func() bool { return s.restartStatus().Error != "" }, 5*time.Second, time.Millisecond)
			require.True(t, s.restartStatus().Active)
			require.NotNil(t, frontend.pausedState(), "frontend must not be resumed")
			_, admitted := s.frontendAdmission()
			require.False(t, admitted)
			require.True(t, s.restartHTTPBlocked())
			require.Zero(t, starts.Load())
			s.mu.Lock()
			require.Equal(t, baseline+reserve, s.fdReserved, "reservation must not be refunded without EOF")
			s.mu.Unlock()
			s.restart.mu.Lock()
			require.True(t, s.restart.previousOwner)
			s.restart.mu.Unlock()
		})
	}
}

func TestServeReturnsAbortCause(t *testing.T) {
	t.Parallel()
	s := stateSupervisor(t)
	s.initializeFrontends()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s.bindListener(listener)
	failure := errors.New("handoff failed")
	s.restart.abort(failure)
	require.ErrorIs(t, s.serve(), failure)
}

func TestRestartBridgeReportsCancellation(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	m, err := newManager()
	require.NoError(t, err)
	owner, _ := supervisorTransportFixture(t, m)
	home := supervisorWorktree(t)
	for range 20 {
		input, inputWrite := io.Pipe()
		output, outputWrite := io.Pipe()
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- restartBridge(ctx, m, home, input, outputWrite) }()
		// One answered request proves the bridge is past attachment and inside
		// its relay loop, where cancellation must win over the derived EOF.
		_, err := inputWrite.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"cancel-test","version":"1"}}}` + "\n"))
		require.NoError(t, err)
		_, err = bufio.NewReader(output).ReadBytes('\n')
		require.NoError(t, err)
		go func() { _, _ = io.Copy(io.Discard, output) }()
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("bridge did not stop on cancellation")
		}
		_ = inputWrite.Close()
		_ = outputWrite.Close()
		require.Eventually(t, func() bool { return owner.snapshot().Sessions == 0 }, 5*time.Second, time.Millisecond)
	}
}
