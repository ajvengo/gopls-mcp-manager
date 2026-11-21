package main

import (
	"bufio"
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPBodyBudgetIncludesSlowReaders(t *testing.T) {
	bubble(t, func(t *testing.T) {
		b := newHTTPBackend(t.Context(), routerTestOwner(t, nil), testHome)
		defer b.cancel()
		b.r.limits.Session = 10
		b.r.limits.MessageBytes = 64
		b.r.limits.HTTPBytes = 128
		handler := newHTTPHandler(b, "")
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		done := make(chan struct{}, 2)
		for range 2 {
			body := &gatedBody{Reader: strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`), entered: make(chan struct{}), release: release}
			req := httptest.NewRequest(http.MethodPost, "/mcp", body)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")
			go func() { handler.ServeHTTP(httptest.NewRecorder(), req); done <- struct{}{} }()
			mustRecv(t, body.entered, "body reader did not start")
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{}`)))
		unblock()
		for range 2 {
			mustRecv(t, done, "HTTP exchange did not release")
		}
		require.Equalf(t, http.StatusServiceUnavailable, response.Code, "budget overload = %d, want 503", response.Code)
	})
}

func TestHTTPRejectsOversizeAndReleasesReservation(t *testing.T) {
	t.Parallel()
	b := newHTTPBackend(t.Context(), routerTestOwner(t, nil), testHome)
	defer b.cancel()
	b.r.limits.MessageBytes = 64
	b.r.limits.HTTPBytes = 64
	handler := newHTTPHandler(b, "")
	for _, tc := range []struct {
		body   string
		status int
	}{
		{strings.Repeat(" ", 65), http.StatusRequestEntityTooLarge},
		{`{"jsonrpc":"2.0","id":1,"method":"ping"}`, http.StatusOK},
	} {
		req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		require.Equalf(t, tc.status, response.Code, "HTTP status = %d, want %d: %s", response.Code, tc.status, response.Body)
	}
}

func BenchmarkHTTPRoundTrip(b *testing.B) {
	_, endpoint, _, _, _ := httpFixture(b)
	const body = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":{}}}`
	b.ReportAllocs()
	for b.Loop() {
		response := postMCP(b, endpoint, body)
		require.NoError(b, response.Error)
	}
}

func TestHTTPWarmCallsBypassBlockedResolution(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := newHTTPBackend(t.Context(), routerTestOwner(t, nil), testHome)
		entered := make(chan struct{})
		b.r.lookupCold = func(ctx context.Context, _ jsontext.Value) []string {
			close(entered)
			<-ctx.Done()
			return nil
		}
		b.r.dial = func(context.Context, string, int) (mcp.Connection, error) { return loopbackConn(), nil }
		b.start()
		defer b.close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		coldDone := make(chan error, 1)
		go func() {
			var result jsontext.Value
			coldDone <- b.call(ctx, "tools/call", fileCallParams("/cold/file.go"), &result)
		}()
		mustRecv(t, entered, "cold lookup did not start")
		start := time.Now()
		var result jsontext.Value
		require.NoError(t, b.call(t.Context(), "tools/call", jsontext.Value(`{"arguments":{}}`), &result))
		elapsed := time.Since(start)
		require.Zero(t, elapsed, "warm HTTP call waited for the cold lookup budget")
		cancel()
		err := mustRecv(t, coldDone, "cancelled cold call did not finish")
		require.ErrorIsf(t, err, context.Canceled, "cold error = %v", err)
	})
}

func TestHTTPBackendShutdownReleasesColdQueue(t *testing.T) {
	bubble(t, func(t *testing.T) {
		owner := routerTestOwner(t, nil)
		b := newHTTPBackend(t.Context(), owner, testHome)
		entered := make(chan struct{})
		b.r.lookupCold = func(ctx context.Context, _ jsontext.Value) []string {
			close(entered)
			<-ctx.Done()
			return nil
		}
		b.start()
		t.Cleanup(b.close)
		results := make(chan error, 2)
		call := func() {
			results <- b.call(t.Context(), "tools/call", fileCallParams("/cold/file.go"), new(jsontext.Value))
		}
		go call()
		mustRecv(t, entered, "cold resolution in progress")
		go call()
		synctest.Wait()
		state := owner.snapshot()
		require.Equal(t, 2, state.Queued)
		require.Equal(t, 2*b.r.limits.MessageBytes, state.RetainedMessageBytes)

		b.close()
		state = owner.snapshot()
		require.Zero(t, state.Queued)
		require.Zero(t, state.RetainedMessageBytes)
		for range 2 {
			require.ErrorIs(t, mustRecv(t, results, "cancelled cold call"), context.Canceled)
		}
	})
}

func TestEndToEndHTTPServerLifecycle(t *testing.T) {
	t.Parallel()
	m, _, _, _ := sseFixture(t, testHome)
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = reader.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	done := inBackground(func() error {
		defer func() { _ = writer.Close() }()
		return serveHTTP(ctx, m, testHome, "127.0.0.1:0", writer)
	})
	t.Cleanup(func() {
		cancel()
		assert.NoErrorf(t, mustRecv(t, done, "HTTP server shutdown"), "shutdown")
	})
	line, err := bufio.NewReader(reader).ReadString('\n')
	require.NoError(t, err)
	endpoint := strings.TrimSpace(strings.TrimPrefix(line, "MCP endpoint: "))
	response := postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":{}}}`)
	ok := response.Error == nil && strings.Contains(string(response.Result), testHome)
	require.Truef(t, ok, "production HTTP response: %+v", response)
	resp, err := http.Get(strings.TrimSuffix(endpoint, "/mcp") + "/missing")
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equalf(t, http.StatusNotFound, resp.StatusCode, "unexpected route status")
}

func TestHTTPServerStartupErrors(t *testing.T) {
	t.Parallel()
	owner := routerTestOwner(t, nil)
	startError := errors.New("home gopls failed to start")
	owner.start = func(context.Context, string, int, int) (*guardianChild, error) {
		return nil, startError
	}
	for _, address := range []string{"invalid", "0.0.0.0:0", "127.0.0.1:0"} {
		t.Run(address, func(t *testing.T) {
			t.Parallel()
			err := serveHTTP(t.Context(), owner, testHome, address, io.Discard)
			require.Error(t, err, "invalid address or missing home server accepted")
			if address == "127.0.0.1:0" {
				require.ErrorContains(t, err, startError.Error())
			}
		})
	}
}

func TestHTTPServerOutputFailure(t *testing.T) {
	t.Parallel()
	m, _, _, _ := sseFixture(t, testHome)
	want := io.ErrClosedPipe
	err := serveHTTP(t.Context(), m, testHome, "127.0.0.1:0", &failingWriter{})
	require.ErrorIsf(t, err, want, "output failure = %v, want %v", err, want)
}

// Serial: replace only the Go stderr variable, restoring it after the output
// worker has returned. Real pipe writes stay outside synctest bubbles.
func TestHTTPErrorCancelsBeforeDiagnosticWrite(t *testing.T) {
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	oldStderr := os.Stderr
	os.Stderr = writer
	defer func() { os.Stderr = oldStderr }()
	defer func() { _ = reader.Close(); _ = writer.Close() }()
	b := newHTTPBackend(t.Context(), routerTestOwner(t, nil), testHome)
	b.start()
	// This exceeds pipe capacity, making the diagnostic block until we close
	// the pipe. Backend cancellation must precede that write.
	b.r.errs <- errors.New(strings.Repeat("diagnostic", 128<<10))
	var first [1]byte
	_, err = reader.Read(first[:]) // diagnostic has captured os.Stderr
	require.NoError(t, err)
	cancelled := false
	select {
	case <-b.r.ctx.Done():
		cancelled = true
	case <-time.After(time.Second):
	}
	// Drain to completion before restoring the process-global variable.
	drained := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, reader); close(drained) }()
	b.close()
	require.NoError(t, writer.Close())
	<-drained
	require.True(t, cancelled, "blocked diagnostics delayed cancellation after terminal output loss")
}

// sseFixture supplies SDK upstreams and a private registry for either frontend.
// Servers occupy real allocation-range ports, so production manager dialing is
// usable without replacing process lifecycle or transport behavior.
func sseFixture(t testing.TB, worktrees ...string) (*supervisor, map[string]string, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	fixtureCtx, stopFixture := context.WithCancel(t.Context())
	started, cancelled := make(chan struct{}, 8), make(chan struct{}, 8)
	m := newTestManager(t)
	owner := newSupervisor(fixtureCtx, &m)
	t.Cleanup(func() {
		stopFixture()
		owner.shutdown()
		require.Eventually(t, func() bool { return len(owner.snapshot().Generations) == 0 }, 5*time.Second, time.Millisecond)
	})
	owner.validateToolchain = func(context.Context, string) error { return nil }
	endpoints := make(map[string]string)
	var records []record
	for _, worktree := range worktrees {
		sdk := mcp.NewServer(&mcp.Implementation{Name: "test-gopls", Version: "1"}, nil)
		sdk.AddTool(&mcp.Tool{Name: "where", InputSchema: map[string]any{"type": "object"}}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var args struct {
				Block bool `json:"block"`
			}
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				return nil, err
			}
			if args.Block {
				started <- struct{}{}
				select {
				case <-ctx.Done():
					cancelled <- struct{}{}
					return nil, ctx.Err()
				case <-fixtureCtx.Done():
					return nil, fixtureCtx.Err()
				}
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: worktree}}}, nil
		})
		upstream := httptest.NewUnstartedServer(mcp.NewSSEHandler(func(*http.Request) *mcp.Server { return sdk }, nil))
		_ = upstream.Listener.Close()
		listener, port := listenInAllocationRange(t, worktree)
		upstream.Listener = listener
		upstream.Start()
		t.Cleanup(upstream.Close)
		endpoints[worktree] = upstream.URL
		ready, exited, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		close(ready)
		var once sync.Once
		stop := func(os.Signal) error { once.Do(func() { upstream.Close(); close(done) }); return nil }
		g := &ownedGeneration{id: ownerNonce(), worktree: worktree, state: generationReady, ready: ready, exited: exited, completed: time.Now(), connections: make(map[*ownedUpstream]struct{})}
		g.record = record{Worktree: worktree, Port: port, PID: os.Getpid(), OwnerBoot: owner.boot, Generation: g.id}
		g.child = &guardianChild{ConfirmedDone: done, Signal: stop, CloseOwner: func() error { return stop(syscall.SIGTERM) }}
		owner.generations[worktree] = g
		go owner.observeGeneration(g)
		records = append(records, g.record)
	}
	t.Cleanup(func() {

		owner.shutdown()
		require.Eventually(t, func() bool { return len(owner.snapshot().Generations) == 0 }, 5*time.Second, time.Millisecond)
	})
	mustWriteMap(t, m.mapPath, records)
	return owner, endpoints, started, cancelled
}

// Exercise both real transports: stateless HTTP outside, bounded SSE inside.
func httpFixture(t testing.TB, capacity ...int) (*httpBackend, string, *atomic.Int32, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	_, endpoints, started, cancelled := sseFixture(t, "/home", "/other")
	var dials atomic.Int32
	b := newHTTPBackend(t.Context(), routerTestOwner(t, nil), "/home")
	if len(capacity) > 0 {
		b.r.limits.Session = capacity[0]
	}
	b.r.paths["/other/file.go"] = pathMemo{worktree: "/other"}
	b.r.dial = func(ctx context.Context, worktree string, port int) (mcp.Connection, error) {
		dials.Add(1)
		return transport.ConnectSSE(ctx, endpoints[worktree], b.r.limits.MessageBytes, b.r.sseBudget)
	}
	b.start()
	t.Cleanup(b.close)
	instructions, err := b.initialize(t.Context())
	require.NoError(t, err)
	server := httptest.NewServer(newHTTPHandler(b, instructions))
	t.Cleanup(server.Close)
	return b, server.URL, &dials, started, cancelled
}

// mcpRequest is a POST the streamable-HTTP handler will accept: the SDK rejects
// a body whose headers do not negotiate the protocol, so every test that sends
// one sets the same three.
func mcpRequest(t testing.TB, ctx context.Context, endpoint, body string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Protocol-Version", "2025-06-18")
	return req
}

func postMCP(t testing.TB, endpoint, body string) *jsonrpc.Response {
	t.Helper()
	req := mcpRequest(t, t.Context(), endpoint, body)
	// Even a supplied session identifier must not create or select a session.
	req.Header.Set("Mcp-Session-Id", "same-client-supplied-id")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Truef(t, resp.StatusCode == http.StatusOK && resp.Header.Get("Mcp-Session-Id") == "",
		"HTTP %d, session %q: %s", resp.StatusCode, resp.Header.Get("Mcp-Session-Id"), raw)
	msg, err := jsonrpc.DecodeMessage(raw)
	require.NoErrorf(t, err, "decode %s", raw)
	result, ok := msg.(*jsonrpc.Response)
	require.Truef(t, ok, "got %T, want response", msg)
	return result
}

func TestHTTPStatelessRoutingAndSSEReuse(t *testing.T) {
	t.Parallel()
	b, endpoint, dials, _, _ := httpFixture(t)
	for _, tc := range []struct{ args, want string }{
		{`{"file":"/other/file.go"}`, "/other"},
		{`{}`, "/home"},
		{`{"file":"/other/file.go"}`, "/other"},
	} {
		resp := postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":`+tc.args+`}}`)
		ok := resp.Error == nil && strings.Contains(string(resp.Result), tc.want)
		require.Truef(t, ok, "got %+v, want %s", resp, tc.want)
	}
	// All concurrent clients deliberately use the same external request ID.
	for i := range 12 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			resp := postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":{}}}`)
			ok := resp.Error == nil && strings.Contains(string(resp.Result), "/home")
			require.Truef(t, ok, "concurrent response: %+v", resp)
		})
	}
	require.Equalf(t, int32(2), dials.Load(), "got %d SSE connections, want two reused lanes", dials.Load())
	require.Emptyf(t, b.r.sticky, "stateless routing retained sticky state")
}

func TestHTTPProtocolAndErrors(t *testing.T) {
	t.Parallel()
	_, endpoint, _, _, _ := httpFixture(t)
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		req, err := http.NewRequestWithContext(t.Context(), method, endpoint, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equalf(t, http.StatusMethodNotAllowed, resp.StatusCode, "%s: %d", method, resp.StatusCode)
	}
	resp := postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`)
	require.NoError(t, resp.Error)
	resp = postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	ok := resp.Error == nil && strings.Contains(string(resp.Result), `"where"`)
	require.Truef(t, ok, "tools/list: %+v", resp)
	// The SDK marshals the embedded Cacheable with no omitempty, so an unset scope
	// ships as "" and a client validating the "public"/"private" enum drops the
	// whole tool list. Asserted on the wire: an SDK client parses either happily.
	require.Containsf(t, string(resp.Result), `"cacheScope":"public"`, "tools/list must carry a valid cacheScope: %s", resp.Result)
	resp = postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":{}}}`)
	ok = resp.Error == nil && strings.Contains(string(resp.Result), `"resultType":"complete"`)
	require.Truef(t, ok, "tools/call must carry a complete resultType: %+v", resp)
	resp = postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"missing","arguments":{}}}`)
	require.Error(t, resp.Error, "upstream error was lost")
}

func TestHTTPRejectsAmbiguousJSON(t *testing.T) {
	t.Parallel()
	b := newHTTPBackend(t.Context(), routerTestOwner(t, nil), testHome)
	defer b.cancel()
	handler := newHTTPHandler(b, "")
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"ping","method":"tools/call"}`,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"arguments":{"file":"/a","file":"/b"}}}`,
		"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"\xff\"}",
	} {
		t.Run(body, func(t *testing.T) {
			t.Parallel()
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, mcpRequest(t, t.Context(), "/mcp", body))
			require.Equal(t, http.StatusBadRequest, response.Code)
		})
	}
}

func TestHTTPCurrentSDKClient(t *testing.T) {
	t.Parallel()
	_, endpoint, _, _, _ := httpFixture(t)
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: endpoint}, nil)
	require.NoError(t, err)
	defer func() { _ = session.Close() }()
	listed, err := session.ListTools(t.Context(), &mcp.ListToolsParams{})
	require.NoError(t, err)
	ok := len(listed.Tools) == 1 && listed.Tools[0].Name == "where"
	require.Truef(t, ok, "tools: %+v", listed)
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "where", Arguments: map[string]any{}})
	require.NoError(t, err)
	ok = len(result.Content) == 1 && result.Content[0].(*mcp.TextContent).Text == "/home"
	require.Truef(t, ok, "result: %+v", result)
}

func TestHTTPDisconnectCancelsLegacySSECall(t *testing.T) {
	t.Parallel()
	b, endpoint, _, started, cancelled := httpFixture(t, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req := mcpRequest(t, ctx, endpoint,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":{"block":true}}}`)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()
	mustRecv(t, started, "upstream call did not start")
	rejected, err := http.DefaultClient.Do(mcpRequest(t, t.Context(), endpoint, `{}`))
	require.NoError(t, err)
	_ = rejected.Body.Close()
	require.Equalf(t, http.StatusServiceUnavailable, rejected.StatusCode, "overload status: %d", rejected.StatusCode)
	cancel()
	mustRecv(t, done, "HTTP request did not stop")
	mustRecv(t, cancelled, "SSE cancellation did not arrive")
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for b.r.requestUsage().Outstanding != 0 {
		select {
		case <-deadline.C:
			require.FailNow(t, "cancelled HTTP call retained its outstanding slot")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestHTTPBackendShutdownReleasesCalls(t *testing.T) {
	t.Parallel()
	b, _, _, started, _ := httpFixture(t)
	done := make(chan error, 1)
	go func() {
		var result mcp.CallToolResult
		done <- b.call(t.Context(), "tools/call", jsontext.Value(`{"name":"where","arguments":{"block":true}}`), &result)
	}()
	mustRecv(t, started, "upstream call did not start")
	b.close()
	err := mustRecv(t, done, "shutdown did not release HTTP caller")
	require.Error(t, err, "shutdown returned success")
	got := b.r.requestUsage().Outstanding
	require.Zerof(t, got, "shutdown retained %d calls", got)
}

func TestHTTPServerBoundsSlowHeaderConnections(t *testing.T) {
	t.Parallel()
	m, _, _, _ := sseFixture(t, testHome)
	m.m.limits.HTTPConnections = 2
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = reader.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	done := inBackground(func() error {
		defer func() { _ = writer.Close() }()
		return serveHTTP(ctx, m, testHome, "127.0.0.1:0", writer)
	})
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, mustRecv(t, done, "HTTP admission shutdown"))
	})
	line, err := bufio.NewReader(reader).ReadString('\n')
	require.NoError(t, err)
	address := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(line), "MCP endpoint: http://"), "/mcp")
	var clients []net.Conn
	for range m.m.limits.HTTPConnections {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		// A completed response proves this socket was admitted before the next
		// one connects. Then leave its next request partway through the headers.
		require.NoError(t, httpAdmissionRoundTrip(conn))
		_, err = fmt.Fprint(conn, "POST /mcp HTTP/1.1\r\nHost: ")
		require.NoError(t, err)
		clients = append(clients, conn)
	}
	rejected, err := net.DialTimeout("tcp", address, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rejected.Close() })
	require.NoError(t, rejected.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = rejected.Read(make([]byte, 1))
	require.Error(t, err)
	var timeout net.Error
	require.False(t, (errors.As(err, &timeout) && timeout.Timeout()), "over-capacity socket was retained for header timeout: %v", err)

	// Closing an incomplete peer request must make room without waiting for
	// the configured five-second header timeout.
	require.NoError(t, clients[0].Close())
	var replacement net.Conn
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err != nil {
			return false
		}
		if err := httpAdmissionRoundTrip(conn); err != nil {
			_ = conn.Close()
			return false
		}
		replacement = conn
		return true
	}, time.Second, time.Millisecond, "peer closure did not return its socket permit")
	t.Cleanup(func() { _ = replacement.Close() })
	cancel()
	for _, conn := range []net.Conn{clients[1], replacement} {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
		_, err := conn.Read(make([]byte, 1))
		require.Error(t, err)
		require.False(t, (errors.As(err, &timeout) && timeout.Timeout()), "shutdown retained an admitted socket: %v", err)
	}
}

func httpAdmissionRoundTrip(conn net.Conn) error {
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	if _, err := fmt.Fprint(conn, "GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		return err
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return conn.SetDeadline(time.Time{})
}

func TestHTTPConnectionPermitReleasedOnce(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	limited := &httpConnectionListener{Listener: listener, slots: make(chan struct{}, 1)}
	accepted := make(chan net.Conn, 3)
	done := inBackground(func() error {
		var conns []net.Conn
		defer func() {
			for _, conn := range conns {
				_ = conn.Close()
			}
		}()
		for {
			conn, err := limited.Accept()
			if err != nil {
				return err
			}
			conns = append(conns, conn)
			accepted <- conn
		}
	})
	t.Cleanup(func() {
		_ = limited.Close()
		assert.ErrorIs(t, mustRecv(t, done, "limited listener exit"), net.ErrClosed)
	})
	dial := func() net.Conn {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	_ = dial()
	first := mustRecv(t, accepted, "first admitted socket")
	require.NoError(t, first.Close())
	_ = dial()
	second := mustRecv(t, accepted, "reused socket permit")
	// A duplicate Close on the previous generation must not return the permit
	// that now belongs to another socket.
	require.NoError(t, first.Close())
	rejected := dial()
	require.NoError(t, rejected.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = rejected.Read(make([]byte, 1))
	require.Error(t, err)
	var timeout net.Error
	require.False(t, errors.As(err, &timeout) && timeout.Timeout(), "duplicate close released another socket's permit")
	require.NoError(t, second.Close())
	_ = dial()
	third := mustRecv(t, accepted, "permit after rejection")
	require.NoError(t, third.Close())
}

func TestLoopbackHTTPProtection(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, host, origin, site string
		allowed                  bool
	}{
		{"numeric IPv4", "127.0.0.1:6099", "", "", true},
		{"IPv4 loopback range", "127.42.0.1:6099", "", "", true},
		{"IPv4 without port", "127.0.0.1", "", "", true},
		{"IPv6", "[::1]:6099", "", "", true},
		{"IPv6 without port", "[::1]", "", "", true},
		{"localhost", "localhost:6099", "", "", true},
		{"localhost without port", "localhost", "", "", true},
		{"same origin", "127.0.0.1:6099", "http://127.0.0.1:6099", "same-origin", true},
		{"legacy same origin", "localhost:6099", "http://localhost:6099", "", true},
		{"browser direct navigation", "127.0.0.1:6099", "", "none", true},
		{"foreign hostname", "attacker.invalid:6099", "", "", false},
		{"hostname suffix", "localhost.attacker.invalid", "", "", false},
		{"foreign address", "192.0.2.1:6099", "", "", false},
		{"missing host", "", "", "", false},
		{"userinfo", "attacker@localhost:6099", "", "", false},
		{"path", "127.0.0.1:6099/ignored", "", "", false},
		{"invalid port", "127.0.0.1:bad", "", "", false},
		{"invalid URL", "[::1", "", "", false},
		{"cross-site without origin", "127.0.0.1:6099", "", "cross-site", false},
		{"same-site", "127.0.0.1:6099", "http://127.0.0.1:8000", "same-site", false},
		{"foreign origin", "127.0.0.1:6099", "https://attacker.invalid", "", false},
		{"opaque origin", "127.0.0.1:6099", "null", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			called := false
			handler := protectLoopbackHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			}))
			req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/mcp", nil)
			req.Host = test.host
			req.Header.Set("Origin", test.origin)
			req.Header.Set("Sec-Fetch-Site", test.site)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			require.Equal(t, test.allowed, called)
			status := http.StatusForbidden
			if test.allowed {
				status = http.StatusNoContent
			}
			require.Equal(t, status, response.Code)
		})
	}
}

func serveHTTP(ctx context.Context, owner *supervisor, home, address string, output io.Writer) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	if !listener.Addr().(*net.TCPAddr).IP.IsLoopback() {
		return fmt.Errorf("HTTP MCP must listen on a loopback address")
	}
	b := newHTTPBackend(ctx, owner, home)
	b.start()
	defer b.close()
	instructions, err := b.initialize(ctx)
	if err != nil {
		return fmt.Errorf("initialize home gopls: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/mcp", newHTTPHandler(b, instructions))
	server := &http.Server{Handler: protectLoopbackHTTP(mux), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
		BaseContext: func(net.Listener) context.Context { return b.r.ctx }}
	stop := context.AfterFunc(b.r.ctx, func() { _ = server.Close() })
	defer stop()
	if _, err := fmt.Fprintf(output, "MCP endpoint: http://%s/mcp\n", listener.Addr()); err != nil {
		return err
	}
	err = server.Serve(&httpConnectionListener{Listener: listener, slots: make(chan struct{}, b.r.limits.HTTPConnections)})
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
