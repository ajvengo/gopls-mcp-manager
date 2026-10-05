package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/transport"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

// sseFixture supplies SDK upstreams and a private registry for either frontend.
// Servers occupy real allocation-range ports, so production manager dialing is
// usable without replacing process lifecycle or transport behavior.
func sseFixture(t testing.TB, worktrees ...string) (*manager, map[string]string, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	fixtureCtx, stopFixture := context.WithCancel(t.Context())
	started, cancelled := make(chan struct{}, 8), make(chan struct{}, 8)
	m := newTestManager(t)
	endpoints := make(map[string]string)
	var records []record
	for _, worktree := range worktrees {
		s := mcp.NewServer(&mcp.Implementation{Name: "test-gopls", Version: "1"}, nil)
		s.AddTool(&mcp.Tool{Name: "where", InputSchema: map[string]any{"type": "object"}},
			func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
		upstream := httptest.NewUnstartedServer(mcp.NewSSEHandler(func(*http.Request) *mcp.Server { return s }, nil))
		_ = upstream.Listener.Close()
		listener, port := listenInAllocationRange(t, worktree)
		upstream.Listener = listener
		upstream.Start()
		t.Cleanup(upstream.Close)
		endpoints[worktree] = upstream.URL
		records = append(records, record{Worktree: worktree, Port: port, PID: os.Getpid()})
	}
	t.Cleanup(stopFixture)
	mustWriteMap(t, m.mapPath, records)
	return &m, endpoints, started, cancelled
}

// Exercise both real transports: stateless HTTP outside, bounded SSE inside.
func httpFixture(t testing.TB, capacity ...int) (*httpBackend, string, *atomic.Int32, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	_, endpoints, started, cancelled := sseFixture(t, "/home", "/other")
	var dials atomic.Int32
	b := newHTTPBackend(t.Context(), nil, "/home")
	if len(capacity) > 0 {
		b.r.limits.Session = capacity[0]
	}
	b.r.paths["/other/file.go"] = pathMemo{worktree: "/other"}
	b.r.dial = func(ctx context.Context, worktree string) (mcp.Connection, error) {
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
		done <- b.call(t.Context(), "tools/call", json.RawMessage(`{"name":"where","arguments":{"block":true}}`), &result)
	}()
	mustRecv(t, started, "upstream call did not start")
	b.close()
	err := mustRecv(t, done, "shutdown did not release HTTP caller")
	require.Error(t, err, "shutdown returned success")
	got := b.r.requestUsage().Outstanding
	require.Zerof(t, got, "shutdown retained %d calls", got)
}
