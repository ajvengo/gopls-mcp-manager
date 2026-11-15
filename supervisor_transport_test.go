package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
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

func TestSupervisorRetiringBackendKeepsCapacityUntilCloseCompletes(t *testing.T) {
	bubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		m := &manager{limits: config.Default()}
		m.limits.OwnerSessions = 1
		s := &supervisor{ctx: ctx, m: m, now: time.Now, filesystem: make(chan ownerFilesystemJob)}

		s.initializeFrontends()
		closing, release, retired := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var closes atomic.Int32
		backend := &httpBackend{cancel: func() { closes.Add(1); close(closing) }, done: release, r: &router{}}
		ready := make(chan struct{})
		close(ready)
		entry := &supervisorHTTPBackend{backend: backend, ready: ready, lastUsed: time.Now().Add(-2 * time.Hour)}
		s.frontends.backends["/old"] = entry
		go func() { s.retireHTTPBackends(); close(retired) }()
		<-closing
		require.Equal(t, 1, s.snapshot().Sessions, "physical teardown still owns its session/FD capacity")
		for _, home := range []string{"/old", "/new"} {
			requestContext, cancelRequest := context.WithCancel(t.Context())
			cancelRequest()
			request := httptest.NewRequestWithContext(requestContext, http.MethodPost, "/mcp", nil)
			require.NoError(t, ownerHeaders(request, m, home))
			response := httptest.NewRecorder()
			s.handler().ServeHTTP(response, request)
			require.Equal(t, http.StatusServiceUnavailable, response.Code)
		}
		s.retireHTTPBackends()
		s.closeFrontends()
		require.Equal(t, int32(1), closes.Load(), "concurrent sweep/shutdown claimed the same teardown twice")
		require.Same(t, entry, s.frontends.backends["/old"])
		close(release)
		<-retired
		require.Zero(t, s.snapshot().Sessions)
		requestContext, cancelRequest := context.WithCancel(t.Context())
		cancelRequest()
		request := httptest.NewRequestWithContext(requestContext, http.MethodPost, "/mcp", nil)
		require.NoError(t, ownerHeaders(request, m, "/new"))
		response := httptest.NewRecorder()
		s.handler().ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code, "capacity was not reusable after actual teardown")
		require.Contains(t, s.frontends.backends, "/new")
		// The fake filesystem worker accepts no real I/O. Cancel the newly
		// admitted initializer and then retire its failed entry deterministically.
		cancel()
		synctest.Wait()
		s.retireHTTPBackends()
		require.Zero(t, s.snapshot().Sessions)
	})
}

func TestSupervisorProxyExecutionFollowsCallerBudget(t *testing.T) {
	for _, deadline := range []time.Duration{0, 5 * time.Second} {
		t.Run(deadline.String(), func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				// net.Pipe is in-memory I/O; all execution/cancellation timing is
				// fake time, including response headers delayed beyond40seconds.
				clientSide, serverSide := net.Pipe()
				client := supervisorHTTPClient(&manager{limits: config.Default()})
				defer client.CloseIdleConnections()
				client.Transport.(*http.Transport).DialContext = func(context.Context, string, string) (net.Conn, error) { return clientSide, nil }
				done := make(chan struct{})
				go func() {
					defer close(done)
					request, err := http.ReadRequest(bufio.NewReader(serverSide))
					if !assert.NoError(t, err) {
						return
					}
					_, err = io.Copy(io.Discard, request.Body)
					assert.NoError(t, err)
					assert.NoError(t, request.Body.Close())
					time.Sleep(45 * time.Second)
					body := `{"jsonrpc":"2.0","id":"slow","result":{}}`
					_, _ = fmt.Fprintf(serverSide, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
				}()
				defer func() { _ = clientSide.Close(); _ = serverSide.Close(); <-done }()
				ctx := t.Context()
				if deadline != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, deadline)
					defer cancel()
				}
				var failure error
				proxy := &httputil.ReverseProxy{Transport: client.Transport, Rewrite: func(*httputil.ProxyRequest) {}, ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
					failure = err
					http.Error(w, "expired", http.StatusBadGateway)
				}}
				request := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://127.0.0.1/mcp", nil)
				response := httptest.NewRecorder()
				proxy.ServeHTTP(response, request)
				if deadline == 0 {
					require.NoError(t, failure, "unlimited execution acquired a hidden transport ceiling")
					require.Equal(t, http.StatusOK, response.Code)
					require.Contains(t, response.Body.String(), `"result":{}`)
				} else {
					require.ErrorIs(t, failure, context.DeadlineExceeded)
					require.Equal(t, http.StatusBadGateway, response.Code)
				}
			})
		})
	}
}

func TestSupervisorFailedHTTPBackendRetiresDespiteRecentUse(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.initializeFrontends()
		backend := newHTTPBackend(s.ctx, s, testHome)
		backend.start()
		backend.cancel()
		<-backend.done
		ready := make(chan struct{})
		close(ready)
		entry := &supervisorHTTPBackend{backend: backend, ready: ready, lastUsed: time.Now()}
		s.frontends.backends[testHome] = entry
		s.retireHTTPBackends()
		require.Empty(t, s.frontends.backends, "dead backend waited for idle rather than failure retirement")
		require.Zero(t, s.snapshot().Sessions)
	})
}

func TestSupervisorFailedHTTPBackendKeepsCapacityUntilActiveHandlersFinish(t *testing.T) {
	bubble(t, func(t *testing.T) {
		s := stateSupervisor(t)
		s.initializeFrontends()
		backend := newHTTPBackend(s.ctx, s, testHome)
		backend.start()
		ready := make(chan struct{})
		close(ready)
		entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
		entry := &supervisorHTTPBackend{backend: backend, ready: ready, lastUsed: time.Now(), handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(entered)
			<-release
			w.WriteHeader(http.StatusNoContent)
		})}
		s.frontends.backends[testHome] = entry
		request := httptest.NewRequest(http.MethodPost, "/mcp", nil)
		require.NoError(t, ownerHeaders(request, s.m, testHome))
		go func() { defer close(finished); s.handler().ServeHTTP(httptest.NewRecorder(), request) }()
		<-entered
		backend.cancel()
		<-backend.done
		failedResponse := httptest.NewRecorder()
		s.handler().ServeHTTP(failedResponse, request)
		require.Equal(t, http.StatusServiceUnavailable, failedResponse.Code)
		s.retireHTTPBackends()
		require.Equal(t, 1, s.snapshot().Sessions, "active old handler lost its backend reservation")
		require.Same(t, entry, s.frontends.backends[testHome])
		close(release)
		<-finished
		synctest.Wait()
		require.Empty(t, s.frontends.backends)
		require.Zero(t, s.snapshot().Sessions)
	})
}

// This real Unix/HTTP/SSE path uses a completed production backend/router and
// SDK child fixture; no real listener or process is put in a fake-time bubble.
func TestSupervisorFailedHTTPBackendReinitializesOnIndependentRequest(t *testing.T) {
	root := supervisorWorktree(t)
	m := newTestManager(t)
	s, starts := supervisorTransportFixture(t, &m)
	supervisorHTTPCall(t, &m, root)
	s.frontends.mu.Lock()
	old := s.frontends.backends[root]
	lastUsed := old.lastUsed
	s.frontends.mu.Unlock()
	old.backend.cancel()
	<-old.backend.done
	client := supervisorHTTPClient(&m)
	defer client.CloseIdleConnections()
	request := mcpRequest(t, t.Context(), "http://127.0.0.1/mcp", `{"jsonrpc":"2.0","id":"failed-backend","method":"tools/call","params":{"name":"where","arguments":{}}}`)
	require.NoError(t, ownerHeaders(request, &m, root))
	response, err := client.Do(request)
	require.NoError(t, err)
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode, "%s", body)
	require.Eventually(t, func() bool {
		s.frontends.mu.Lock()
		defer s.frontends.mu.Unlock()
		return s.frontends.backends[root] == nil
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, lastUsed, old.lastUsed, "failed retry refreshed the broken backend's idle clock")
	supervisorHTTPCall(t, &m, root)
	s.frontends.mu.Lock()
	fresh := s.frontends.backends[root]
	s.frontends.mu.Unlock()
	require.NotSame(t, old, fresh)
	require.NoError(t, fresh.backend.r.ctx.Err())
	require.Equal(t, int32(1), starts.Load(), "backend replacement unnecessarily replaced healthy shared gopls")
}

// Enumerate the policy surface independently of the encoder. Adding a future
// field automatically requires that changing it changes the compatibility hash.
func TestSupervisorConfigurationFingerprintIncludesEveryPolicyField(t *testing.T) {
	t.Parallel()
	baseline := manager{limits: config.Default()}
	baseline.limits.DirectoryFilters = []string{"-docs", "+docs/go"}
	want, err := supervisorConfig(&baseline)
	require.NoError(t, err)
	require.Len(t, want, 64)
	copy := baseline
	copy.limits.DirectoryFilters = slices.Clone(baseline.limits.DirectoryFilters)
	for range 10 {
		got, err := supervisorConfig(&copy)
		require.NoError(t, err)
		require.Equal(t, want, got, "equivalent policy is not deterministic")
	}
	policyType := reflect.TypeFor[config.Limits]()
	for index := range policyType.NumField() {
		t.Run(policyType.Field(index).Name, func(t *testing.T) {
			t.Parallel()
			changed := baseline
			field := reflect.ValueOf(&changed.limits).Elem().Field(index)
			switch field.Kind() {
			case reflect.Int, reflect.Int64:
				field.SetInt(field.Int() + 1)
			case reflect.Bool:
				field.SetBool(!field.Bool())
			case reflect.Slice:
				require.Equal(t, reflect.TypeFor[[]string](), field.Type(), "extend independent policy mutation for new field type")
				field.Set(reflect.ValueOf([]string{"-other"}))
			default:
				t.Fatalf("extend independent policy mutation for new field type %v", field.Type())
			}
			got, err := supervisorConfig(&changed)
			require.NoError(t, err)
			require.NotEqual(t, want, got, "policy change is invisible to compatibility handshake")
			req := httptest.NewRequest(http.MethodGet, "/ping", nil)
			require.NoError(t, ownerHeaders(req, &changed, ""))
			response := httptest.NewRecorder()
			(&supervisor{m: &baseline}).handler().ServeHTTP(response, req)
			require.Equal(t, http.StatusConflict, response.Code, "owner accepted a different policy")
		})
	}
	copy.limits.DirectoryFilters = []string{"+docs/go", "-docs"}
	reordered, err := supervisorConfig(&copy)
	require.NoError(t, err)
	require.NotEqual(t, want, reordered, "filter order changes policy and must not be sorted away")
}

func TestSupervisorConfigurationEncodingErrorNeverProducesFingerprintOrTraffic(t *testing.T) {
	t.Parallel()
	m := manager{limits: config.Default()}
	m.limits.DirectoryFilters = []string{"-\xff"}
	fingerprint, err := supervisorConfig(&m)
	require.ErrorContains(t, err, "encode supervisor configuration")
	require.Empty(t, fingerprint, "partial bytes produced a usable fingerprint")
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	require.Error(t, ownerHeaders(req, &m, ""))
	require.Empty(t, req.Header.Get(supervisorVersionHeader))
	require.Empty(t, req.Header.Get(supervisorConfigHeader))
	require.ErrorContains(t, supervisorPing(t.Context(), &m), "encode supervisor configuration")
	require.ErrorContains(t, querySupervisor(t.Context(), &m, "/status", "", io.Discard), "encode supervisor configuration")
	conn, reader, err := openSupervisorAttachment(t.Context(), &m, testHome, "/bridge")
	require.ErrorContains(t, err, "encode supervisor configuration")
	require.Nil(t, conn)
	require.Nil(t, reader)
	require.ErrorContains(t, proxyHTTPViaSupervisor(t.Context(), &m, testHome, "127.0.0.1:0", io.Discard), "encode supervisor configuration")
	response := httptest.NewRecorder()
	s := &supervisor{m: &m}
	s.handler().ServeHTTP(response, req)
	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.Nil(t, s.generations)
	require.Nil(t, s.frontends)
}

// Real Unix/HTTP/SSE transports and SDK sessions, with a synthetic owned child
// whose TERM closes its SDK listener. No OS signal targets a test/developer PID.
// The guardian integration suite separately proves actual process/group exit.
func supervisorTransportFixture(t testing.TB, m *manager, configure ...func(*mcp.Server)) (*supervisor, *atomic.Int32) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	var starts atomic.Int32
	owner := newSupervisor(ctx, m)
	owner.start = func(ctx context.Context, worktree string, port, _ int) (*guardianChild, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		listener, err := net.Listen("tcp", mcpAddress(port))
		if err != nil {
			return nil, err
		}
		starts.Add(1)
		sdk := mcp.NewServer(&mcp.Implementation{Name: "owned-fixture", Version: "1"}, nil)
		sdk.AddTool(&mcp.Tool{Name: "where", InputSchema: map[string]any{"type": "object"}},
			func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: worktree}}}, nil
			})
		for _, configureServer := range configure {
			configureServer(sdk)
		}
		server := &http.Server{Handler: mcp.NewSSEHandler(func(*http.Request) *mcp.Server { return sdk }, nil)}
		done := make(chan struct{})
		var once sync.Once
		go func() {
			err := server.Serve(listener)
			assert.True(t, errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed), "fixture server: %v", err)
		}()
		stop := func(os.Signal) error {
			once.Do(func() { _ = server.Close(); close(done) })
			return nil
		}
		return &guardianChild{Process: &os.Process{Pid: os.Getpid()}, ConfirmedDone: done, Signal: stop, CloseOwner: func() error { return stop(syscall.SIGTERM) }}, nil
	}
	owner.initializeFrontends()
	require.NoError(t, os.MkdirAll(filepath.Dir(m.mapPath), 0o700))
	path, err := supervisorSocket(m)
	require.NoError(t, err)
	require.NoError(t, prepareSupervisorRuntime(path))
	listener, err := net.Listen("unix", path)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, 0o600))
	done := make(chan error, 1)
	owner.bindListener(listener)
	go func() { done <- owner.serve() }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
		_ = listener.Close()
		_ = os.Remove(path)
		_ = os.Remove(filepath.Dir(path))
		require.Eventually(t, func() bool { return len(owner.snapshot().Generations) == 0 }, 5*time.Second, time.Millisecond)
	})
	require.Eventually(t, func() bool { return supervisorPing(t.Context(), m) == nil }, time.Second, time.Millisecond)
	return owner, &starts
}

func supervisorWorktree(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	runGit(t, "init", root)
	physical, err := worktreePath(t.Context(), root)
	require.NoError(t, err)
	return physical
}

func supervisorStdioClient(t *testing.T, m *manager, home string) mcp.Connection {
	t.Helper()
	conn, reader, err := openSupervisorAttachment(t.Context(), m, home, "/bridge")
	require.NoError(t, err)
	client := newRestartPeer(t.Context(), conn, reader, m.limits.MessageBytes, true)
	client.start()
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, client.Write(ctx, &jsonrpc.Request{ID: mustID(t, "initialize"), Method: "initialize",
		Params: jsontext.Value(`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"owner-test","version":"1"}}`)}))
	msg, err := client.Read(ctx)
	require.NoError(t, err)
	require.Nil(t, msg.(*jsonrpc.Response).Error)
	require.NoError(t, client.Write(ctx, &jsonrpc.Request{Method: "notifications/initialized"}))
	return client
}

func supervisorStdioCall(t *testing.T, client mcp.Connection, home string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, client.Write(ctx, &jsonrpc.Request{ID: mustID(t, "where"), Method: "tools/call", Params: jsontext.Value(`{"name":"where","arguments":{}}`)}))
	msg, err := client.Read(ctx)
	require.NoError(t, err)
	response := msg.(*jsonrpc.Response)
	require.Nil(t, response.Error, "stdio tool failed: %+v", response)
	require.Contains(t, string(response.Result), home)
}

func supervisorHTTPCall(t *testing.T, m *manager, home string) {
	t.Helper()
	client := supervisorHTTPClient(m)
	defer client.CloseIdleConnections()
	req := mcpRequest(t, t.Context(), "http://127.0.0.1/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":{}}}`)
	require.NoError(t, ownerHeaders(req, m, home))
	response, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, "HTTP tool: %s", body)
	msg, err := jsonrpc.DecodeMessage(body)
	require.NoError(t, err)
	require.Nil(t, msg.(*jsonrpc.Response).Error)
	require.Contains(t, string(body), home)
}

func TestSupervisorTwoBridgesAndHTTPShareAndReconnectAfterIdle(t *testing.T) {
	root := supervisorWorktree(t)
	m := newTestManager(t)
	s, starts := supervisorTransportFixture(t, &m)
	first, second := supervisorStdioClient(t, &m, root), supervisorStdioClient(t, &m, root)
	supervisorStdioCall(t, first, root)
	supervisorStdioCall(t, second, root)
	supervisorHTTPCall(t, &m, root)
	require.Equal(t, int32(1), starts.Load(), "independent frontends did not share one owned generation")
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		g := s.generations[root]
		if g.writes != 0 || s.operationUsageLocked() != 0 {
			return false
		}
		g.completed = time.Now().Add(-2 * time.Hour)
		return true
	}, 5*time.Second, time.Millisecond, "upstream delivery did not quiesce before idle sweep")
	s.idleSweep()
	require.Eventually(t, func() bool { return len(s.snapshot().Generations) == 0 }, 5*time.Second, time.Millisecond,
		"quiet connected clients prevented child cleanup")
	// The first new call, with the same public request ID, must redial before
	// delivery; requiring the user to call twice would hide a stale cache bug.
	supervisorStdioCall(t, first, root)
	supervisorHTTPCall(t, &m, root)
	require.Equal(t, int32(2), starts.Load())
}

func supervisorRunFixture(t *testing.T) (string, int) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	root := supervisorWorktree(t)
	m, err := newManager()
	require.NoError(t, err)
	owner, _ := supervisorTransportFixture(t, m)
	r, err := owner.ensureRecord(t.Context(), root)
	require.NoError(t, err)
	return root, r.Port
}

func TestSupervisorSocketUsesShortStablePrivateIdentity(t *testing.T) {
	dir := filepath.Join(t.TempDir(), strings.Repeat("long-state-", 15))
	require.NoError(t, os.MkdirAll(dir, 0o700))
	m := &manager{mapPath: filepath.Join(dir, "gopls-ports.map")}
	path, err := supervisorSocket(m)
	require.NoError(t, err)
	require.Less(t, len(path), 100)
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "different-tmp"))
	again, err := supervisorSocket(m)
	require.NoError(t, err)
	require.Equal(t, path, again)
	alias := filepath.Join(t.TempDir(), "state-alias")
	require.NoError(t, os.Symlink(dir, alias))
	viaAlias, err := supervisorSocket(&manager{mapPath: filepath.Join(alias, "gopls-ports.map")})
	require.NoError(t, err)
	require.Equal(t, path, viaAlias)
	require.NoError(t, prepareSupervisorRuntime(path))
	t.Cleanup(func() { _ = os.Remove(filepath.Dir(path)) })
	info, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

func TestSupervisorProtocolAndConfigMismatchFailBeforeAllocation(t *testing.T) {
	t.Parallel()
	s := testSupervisor(t)
	s.initializeFrontends()
	for _, version := range []string{"", "old"} {
		req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1/mcp", strings.NewReader("{}"))
		require.NoError(t, err)
		require.NoError(t, ownerHeaders(req, s.m, testHome))
		req.Header.Set(supervisorVersionHeader, version)
		response := httptest.NewRecorder()
		s.handler().ServeHTTP(response, req)
		require.Equal(t, http.StatusConflict, response.Code)
		require.Empty(t, s.generations)
		require.Empty(t, s.frontends.backends)
	}
}

func BenchmarkSupervisorHTTPRoundTrip(b *testing.B) {
	root := supervisorWorktree(b)
	m := newTestManager(b)
	supervisorTransportFixture(b, &m)
	endpoint, _ := supervisorHTTPFrontend(b, &m, root)
	const body = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":{}}}`
	require.NoError(b, postMCP(b, endpoint, body).Error)
	b.ReportAllocs()
	for b.Loop() {
		require.NoError(b, postMCP(b, endpoint, body).Error)
	}
}

// This is intentionally serial: runSupervisor changes the calling process's
// soft RLIMIT_NOFILE. Every test saves/restores it; HOME is never changed here.
func TestSupervisorNativeBootstrapAndSingleton(t *testing.T) {
	var original syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &original))
	t.Cleanup(func() { assert.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &original)) })
	m := newTestManager(t)
	m.limits.OwnerSessions, m.limits.Lanes, m.limits.HTTPConnections, m.limits.SharedServers = 2, 2, 8, 2
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runSupervisor(ctx, &m) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				require.FailNow(t, "private supervisor did not shut down")
			}
		})
	}
	t.Cleanup(stop)
	require.Eventually(t, func() bool { return supervisorPing(t.Context(), &m) == nil }, 5*time.Second, time.Millisecond)
	path, err := supervisorSocket(&m)
	require.NoError(t, err)
	t.Cleanup(func() { stop(); assert.NoError(t, os.Remove(filepath.Dir(path))) })
	info, err := os.Lstat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.NotZero(t, info.Mode()&os.ModeSocket)
	var actual syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &actual))
	reservation, err := supervisorFDAllowance(m.limits)
	require.NoError(t, err)
	require.Equal(t, uint64(reservation), actual.Cur)
	require.Equal(t, original.Max, actual.Max, "bootstrap lowered the hard limit")

	// The live singleton must reject a contender without reaching its callback.
	err = withSupervisorSingleton(t.Context(), &m, func() error {
		t.Error("second supervisor passed the lifetime lock")
		return nil
	})
	require.ErrorContains(t, err, "another supervisor already owns this registry")
	require.NoError(t, ensureSupervisor(t.Context(), &m), "existing owner must not attempt a child process")
	var first, second bytes.Buffer
	require.NoError(t, querySupervisor(t.Context(), &m, "/status", "", &first))
	require.NoError(t, querySupervisor(t.Context(), &m, "/status", "", &second))
	var snapshot supervisorSnapshot
	require.NoError(t, json.Unmarshal(first.Bytes(), &snapshot))
	require.NotEmpty(t, snapshot.Boot)
	require.Empty(t, snapshot.Generations)
	require.Zero(t, snapshot.Queued)
	require.Zero(t, snapshot.Operations)
	require.Zero(t, snapshot.Sessions)
	require.Equal(t, reservation, snapshot.FDReserved)
	require.Equal(t, first.String(), second.String(), "passive status changed idle/accounting state")
	var listed bytes.Buffer
	require.NoError(t, querySupervisor(t.Context(), &m, "/list", "", &listed))
	require.Equal(t, "PORT\tPID\tWORKTREE\n", listed.String())
	require.ErrorIs(t, querySupervisor(t.Context(), &m, "/status", "", &failingWriter{}), io.ErrClosedPipe)
	mismatch := m
	mismatch.limits.OwnerSessions++
	firstConfig, err := supervisorConfig(&m)
	require.NoError(t, err)
	mismatchedConfig, err := supervisorConfig(&mismatch)
	require.NoError(t, err)
	require.NotEqual(t, firstConfig, mismatchedConfig, "different owner resource policies share a fingerprint")
	require.ErrorContains(t, supervisorPing(t.Context(), &mismatch), "protocol/configuration rejected")
	_, _, err = openSupervisorAttachment(t.Context(), &m, "relative", "/bridge")
	require.ErrorContains(t, err, "HTTP 400")

	stop()
	_, err = os.Lstat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, withSupervisorSingleton(t.Context(), &m, func() error { return nil }), "shutdown retained singleton lock")
	require.True(t, m.ownerLogs.Snapshot().Closed)
}

func TestSupervisorNativeSingletonCancellationAndFailureRelease(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, withSupervisorSingleton(cancelled, &m, func() error {
		t.Error("cancelled singleton executed body")
		return nil
	}), context.Canceled)
	failure := errors.New("native bootstrap failure")
	require.ErrorIs(t, withSupervisorSingleton(t.Context(), &m, func() error { return failure }), failure)
	require.NoError(t, withSupervisorSingleton(t.Context(), &m, func() error { return nil }), "failed bootstrap retained lock")
}

func TestSupervisorNativeBootstrapRefusesLiveForeignRegistry(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	foreign := record{Worktree: t.TempDir(), PID: os.Getpid(), Port: firstPort, OwnerBoot: "previous-boot", Generation: "foreign-generation"}
	mustWriteMap(t, m.mapPath, []record{foreign})
	require.ErrorContains(t, runSupervisor(t.Context(), &m), "cannot adopt live or uncertain gopls")
	socket, socketErr := supervisorSocket(&m)
	require.NoError(t, socketErr)
	require.NoFileExists(t, socket, "foreign reservation reached process ownership")
	require.Nil(t, m.ownerLogs, "foreign reservation allocated log resources")
	require.Equal(t, []record{foreign}, mustReadMap(t, m.mapPath), "refused bootstrap rewrote ownership")
	require.NoError(t, withSupervisorSingleton(t.Context(), &m, func() error { return nil }))
}

func TestSupervisorNativeManagedToolchainContract(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "go.mod"), "module example.com/native-toolchain\n\ngo 1.27\n")
	require.NoError(t, m.validateManagedToolchain(t.Context(), root))
	require.ErrorContains(t, m.validateManagedToolchain(t.Context(), filepath.Join(root, "missing")), "validate managed Go telemetry contract")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, m.validateManagedToolchain(ctx, root), context.Canceled)
}

func TestSupervisorNativeRuntimeRefusesForeignPathObjects(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"public directory", "symlink directory", "regular endpoint", "symlink endpoint", "fifo endpoint", "stale socket"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			base, err := os.MkdirTemp("/tmp", "gopls-native-")
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, os.RemoveAll(base)) })
			dir := filepath.Join(base, "runtime")
			require.NoError(t, os.Mkdir(dir, 0o700))
			path := filepath.Join(dir, "owner.sock")
			switch kind {
			case "public directory":
				require.NoError(t, os.Chmod(dir, 0o755))
			case "symlink directory":
				alias := filepath.Join(t.TempDir(), "runtime-alias")
				require.NoError(t, os.Symlink(dir, alias))
				path = filepath.Join(alias, "owner.sock")
			case "regular endpoint":
				mustWriteFile(t, path, "keep unrelated bytes")
			case "symlink endpoint":
				target := filepath.Join(t.TempDir(), "target")
				mustWriteFile(t, target, "keep unrelated bytes")
				require.NoError(t, os.Symlink(target, path))
			case "fifo endpoint":
				require.NoError(t, syscall.Mkfifo(path, 0o600))
			case "stale socket":
				listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
				require.NoError(t, err)
				listener.SetUnlinkOnClose(false)
				require.NoError(t, listener.Close())
			}
			err = prepareSupervisorRuntime(path)
			if kind == "stale socket" {
				require.NoError(t, err)
				_, err = os.Lstat(path)
				require.ErrorIs(t, err, os.ErrNotExist)
			} else {
				require.Error(t, err)
				if strings.HasSuffix(kind, "endpoint") {
					_, err := os.Lstat(path)
					require.NoError(t, err, "refusal removed unrelated object")
				}
			}
		})
	}
}

// All path and socket operations are real OS I/O and deliberately outside
// synctest. The SDK fixture replaces only the owned child, never the proxy.
func TestSupervisorNativeHTTPProxyLifetimeAndValidation(t *testing.T) {
	root := nativeAuditWorktree(t)
	m := newTestManager(t)
	s, starts := supervisorTransportFixture(t, &m)
	endpoint, stop := supervisorHTTPFrontend(t, &m, root)
	require.True(t, strings.HasPrefix(endpoint, "http://127.0.0.1:"))
	req := mcpRequest(t, t.Context(), endpoint, `{"jsonrpc":"2.0","id":"native-proxy","method":"tools/call","params":{"name":"where","arguments":{}}}`)
	// The public client cannot choose another owner/worktree via forged headers.
	req.Header.Set(supervisorHomeHeader, "/foreign/forged")
	req.Header.Set(supervisorVersionHeader, "old")
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	resp, err := client.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
	require.Contains(t, string(body), root)
	require.NotContains(t, string(body), "forged")
	require.Equal(t, int32(1), starts.Load())
	stop()
	require.Eventually(t, func() bool {
		s.frontends.mu.Lock()
		defer s.frontends.mu.Unlock()
		return len(s.frontends.sessions) == 0
	}, 5*time.Second, time.Millisecond, "closed frontend retained control attachment")
	_, err = client.Get(endpoint)
	require.Error(t, err, "cancelled frontend listener stayed live")
	for _, tc := range []struct {
		name, address string
		writer        io.Writer
		want          string
	}{
		{"nonloopback", "0.0.0.0:0", io.Discard, "loopback"},
		{"invalid address", "127.0.0.1:invalid", io.Discard, "unknown port"},
		{"output failure", "127.0.0.1:0", &failingWriter{}, "closed pipe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, proxyHTTPViaSupervisor(t.Context(), &m, root, tc.address, tc.writer), tc.want)
		})
	}
}

type nativeAuditAnnouncement struct{ output chan<- string }

func (w nativeAuditAnnouncement) Write(p []byte) (int, error) {
	w.output <- string(p)
	return len(p), nil
}

func nativeAuditWorktree(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	out, err := exec.CommandContext(t.Context(), "but", "-C", root, "init").CombinedOutput()
	require.NoError(t, err, "%s", out)
	return root
}

func TestSupervisorNativeFDPreflightAndAggregateReservation(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	s := newSupervisor(t.Context(), &m)
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "go.mod"), "module example.com/fd\n\ngo 1.27\n")
	require.NoError(t, os.Mkdir(filepath.Join(root, "sub"), 0o700))
	mustWriteFile(t, filepath.Join(root, "sub", "a.go"), "package sub\n")
	// Exact count: root + go.mod + sub + a.go, plus fixed child headroom.
	first := &ownedGeneration{worktree: root}
	baseline := s.snapshot().FDReserved
	require.NoError(t, s.reserveGenerationFDs(t.Context(), first))
	require.Equal(t, childFDHeadroom+4, first.fdQuota)
	reserved := s.snapshot().FDReserved
	require.Equal(t, baseline+first.fdQuota+guardianFDAllowance, reserved)
	s.m.limits.OpenFiles = reserved + first.fdQuota + guardianFDAllowance - 1
	second := &ownedGeneration{worktree: root}
	require.ErrorContains(t, s.reserveGenerationFDs(t.Context(), second), "reservation exhausted")
	require.Zero(t, second.fdQuota)
	require.Equal(t, reserved, s.snapshot().FDReserved, "rejected generation consumed capacity")
	s.m.limits.OpenFiles++
	require.NoError(t, s.reserveGenerationFDs(t.Context(), second), "exact aggregate boundary must fit")
	require.Equal(t, s.m.limits.OpenFiles, s.snapshot().FDReserved)
	missing := &ownedGeneration{worktree: filepath.Join(t.TempDir(), "missing")}
	require.Error(t, s.reserveGenerationFDs(t.Context(), missing))
	require.Zero(t, missing.fdQuota)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, s.reserveGenerationFDs(cancelled, &ownedGeneration{worktree: root}), context.Canceled)
}

func TestSupervisorNativeFDDirectoryBudgetsAndPruning(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for i := range watchDirectoryBatch + 3 {
		mustWriteFile(t, filepath.Join(root, fmt.Sprintf("file-%03d.go", i)), "package fixture\n")
	}
	count := childFDHeadroom
	budget := &watchBudget{remaining: defaultWatchLimits()}
	require.NoError(t, countWatchDescriptors(t.Context(), root, 0, budget, &count, 10000))
	require.Equal(t, childFDHeadroom+watchDirectoryBatch+3, count, "batched scan missed a page")

	for _, tc := range []struct {
		name string
		run  func() error
		want string
	}{
		{"entry cap", func() error {
			budget := &watchBudget{remaining: defaultWatchLimits()}
			budget.remaining.entries = watchDirectoryBatch
			count := 0
			return countWatchDescriptors(t.Context(), root, 0, budget, &count, 10000)
		}, "entry count"},
		{"name bytes", func() error {
			budget := &watchBudget{remaining: defaultWatchLimits()}
			budget.remaining.nameBytes = 1
			count := 0
			return countWatchDescriptors(t.Context(), root, 0, budget, &count, 10000)
		}, "bytes"},
		{"depth", func() error {
			count := 0
			return countWatchDescriptors(t.Context(), root, maxWatchDepth+1, &watchBudget{remaining: defaultWatchLimits()}, &count, 10000)
		}, "depth"},
		{"descriptor quota", func() error {
			count := childFDHeadroom
			return countWatchDescriptors(t.Context(), root, 0, &watchBudget{remaining: defaultWatchLimits()}, &count, childFDHeadroom+guardianFDAllowance)
		}, "cannot fit"},
	} {
		t.Run(tc.name, func(t *testing.T) { require.ErrorContains(t, tc.run(), tc.want) })
	}
	for _, name := range []string{".git", "_private", "testdata"} {
		dir := filepath.Join(root, name)
		require.NoError(t, os.Mkdir(dir, 0o700))
		mustWriteFile(t, filepath.Join(dir, "hidden.go"), "package hidden\n")
	}
	count = 0
	require.NoError(t, countWatchDescriptors(t.Context(), root, 0, &watchBudget{remaining: defaultWatchLimits()}, &count, 10000))
	require.Equal(t, watchDirectoryBatch+3, count, "watcher-excluded subtree consumed descriptors")
	fifo := filepath.Join(t.TempDir(), "replaced-directory")
	require.NoError(t, syscall.Mkfifo(fifo, 0o600))
	require.Error(t, countWatchDescriptors(t.Context(), fifo, 0, &watchBudget{remaining: defaultWatchLimits()}, &count, 10000), "directory-only nonblocking open must refuse FIFO")
}

func TestSupervisorNativeFDAllowanceValidation(t *testing.T) {
	t.Parallel()
	limits := config.Default()
	needed, err := supervisorFDAllowance(limits)
	require.NoError(t, err)
	limits.OpenFiles = needed
	got, err := supervisorFDAllowance(limits)
	require.NoError(t, err)
	require.Equal(t, needed, got)
	limits.OpenFiles--
	_, err = supervisorFDAllowance(limits)
	require.ErrorContains(t, err, "cannot hold")
	for _, value := range []int{0, -1, 1<<20 + 1} {
		for _, field := range []string{"sessions", "lanes", "servers", "connections"} {
			t.Run(fmt.Sprintf("%s/%d", field, value), func(t *testing.T) {
				t.Parallel()
				limits := config.Default()
				switch field {
				case "sessions":
					limits.OwnerSessions = value
				case "lanes":
					limits.Lanes = value
				case "servers":
					limits.SharedServers = value
				case "connections":
					limits.HTTPConnections = value
				}
				_, err := supervisorFDAllowance(limits)
				require.ErrorContains(t, err, "between 1 and 1048576")
			})
		}
	}
}

// CLI subprocesses alone receive private HOME. They never auto-start an owner:
// list/status/trim are observational and the missing worktree fails earlier.
func TestSupervisorNativeCLICommandsUsePrivateState(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		args []string
		want string
		err  bool
	}{
		{"list", []string{"list"}, "PORT\tPID\tWORKTREE\n", false},
		{"status", []string{"status"}, `"RecordedServers":0`, false},
		{"trim", []string{"trim-logs"}, `"Files":0`, false},
		{"http grammar", []string{"http", "a", "b", "c"}, "usage:", true},
		{"supervisor grammar", []string{"supervisor", "unexpected"}, "usage:", true},
		{"guardian grammar", []string{"internal-guardian", "unexpected"}, "accepts no arguments", true},
		{"missing worktree", []string{"ensure", "/nonexistent/gopls-audit-path"}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			executable, err := os.Executable()
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			args := append([]string{"-test.run=^TestSupervisorNativeCLIProcess$", "--"}, tc.args...)
			cmd := exec.CommandContext(ctx, executable, args...)
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(), "GOPLS_NATIVE_AUDIT_CLI=1"}
			cmd.WaitDelay = 250 * time.Millisecond
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err = cmd.Run()
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err, "%s", stderr.String())
			}
			require.NoError(t, ctx.Err(), "CLI hung rather than completing its branch")
			require.Contains(t, stdout.String()+stderr.String(), tc.want)
			if tc.name == "status" {
				var value struct {
					RecordedServers int
					RegistryIntact  bool
					Owner           *supervisorSnapshot
					OwnerError      string
				}
				require.NoError(t, json.Unmarshal(stdout.Bytes(), &value), "protocol stdout: %s; diagnostic stderr: %s", stdout.String(), stderr.String())
				require.True(t, value.RegistryIntact)
				require.Nil(t, value.Owner, "observational status started an owner")
				require.NotEmpty(t, value.OwnerError)
			}
		})
	}
}

func TestSupervisorNativeCLIProcess(t *testing.T) {
	if os.Getenv("GOPLS_NATIVE_AUDIT_CLI") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			args := append([]string{"native-audit"}, os.Args[i+1:]...)
			if err := run(args, os.Stdout); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			os.Exit(0)
		}
	}
	t.Fatal("native CLI helper lacks explicit argument separator")
}

func TestSupervisorHTTPRejectsUntrustedBrowserRequests(t *testing.T) {
	root := nativeAuditWorktree(t)
	m := newTestManager(t)
	s, starts := supervisorTransportFixture(t, &m)
	endpoint, _ := supervisorHTTPFrontend(t, &m, root)
	address, err := url.Parse(endpoint)
	require.NoError(t, err)
	before := s.snapshot()
	require.Len(t, before.Generations, 1)
	initialStarts := starts.Load()
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	for _, test := range []struct{ name, host, origin, site string }{
		{"rebound hostname", "attacker.invalid", "http://attacker.invalid", "same-origin"},
		{"cross-origin browser", address.Host, "https://attacker.invalid", "cross-site"},
		{"legacy cross-origin browser", address.Host, "https://attacker.invalid", ""},
		{"opaque origin", address.Host, "null", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := mcpRequest(t, t.Context(), endpoint, `{"jsonrpc":"2.0","id":"browser","method":"tools/call","params":{"name":"where","arguments":{}}}`)
			req.Host = test.host
			req.Header.Set("Origin", test.origin)
			req.Header.Set("Sec-Fetch-Site", test.site)
			response, err := client.Do(req)
			require.NoError(t, err)
			body, err := io.ReadAll(response.Body)
			require.NoError(t, response.Body.Close())
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, response.StatusCode, "%s", body)
		})
	}
	require.Equal(t, initialStarts, starts.Load())
	after := s.snapshot()
	require.Len(t, after.Generations, 1)
	require.Equal(t, before.Generations[0].LastCompletion, after.Generations[0].LastCompletion,
		"rejected browser requests reached upstream work")
	s.frontends.mu.Lock()
	backends := len(s.frontends.backends)
	s.frontends.mu.Unlock()
	require.Zero(t, backends, "rejected browser requests allocated an HTTP backend")
	req := mcpRequest(t, t.Context(), endpoint, `{"jsonrpc":"2.0","id":"local","method":"tools/call","params":{"name":"where","arguments":{}}}`)
	req.Header.Set("Origin", address.Scheme+"://"+address.Host)
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	response, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.EqualValues(t, 1, starts.Load())
}

func supervisorHTTPFrontend(t testing.TB, m *manager, home string) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	output := make(chan string, 1)
	done := make(chan error, 1)
	go func() { done <- proxyHTTPViaSupervisor(ctx, m, home, "127.0.0.1:0", nativeAuditAnnouncement{output}) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				require.FailNow(t, "HTTP proxy did not exit")
			}
		})
	}
	t.Cleanup(stop)
	select {
	case line := <-output:
		return strings.TrimSpace(strings.TrimPrefix(line, "MCP endpoint: ")), stop
	case <-time.After(5 * time.Second):
		require.FailNow(t, "HTTP proxy did not announce loopback address")
		return "", stop
	}
}

func TestSupervisorSingletonReleasesLockHeldByForkedDescendant(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	var child *exec.Cmd
	require.NoError(t, withSupervisorSingleton(t.Context(), &m, func() error {
		// A descendant between fork and exec shares the lock's open file
		// description; closing the owner's descriptor alone keeps the lock.
		child = exec.Command("sleep", "30")
		child.ExtraFiles = []*os.File{m.restart.lock}
		return child.Start()
	}))
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	require.NoError(t, withSupervisorSingleton(t.Context(), &m, func() error { return nil }))
}
