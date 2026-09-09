package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/transport"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Exercise both real transports: stateless HTTP outside, bounded SSE against the SDK server inside.
func httpFixture(t testing.TB, capacity ...int) (*httpBackend, string, *atomic.Int32, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	fixtureCtx, stopFixture := context.WithCancel(t.Context())
	started, cancelled := make(chan struct{}, 8), make(chan struct{}, 8)
	var dials atomic.Int32
	endpoints := make(map[string]string)
	for _, worktree := range []string{"/home", "/other"} {
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
		upstream := httptest.NewServer(mcp.NewSSEHandler(func(*http.Request) *mcp.Server { return s }, nil))
		t.Cleanup(upstream.Close)
		endpoints[worktree] = upstream.URL
	}
	t.Cleanup(stopFixture)
	b := newHTTPBackend(t.Context(), nil, "/home")
	if len(capacity) > 0 {
		b.r.limits.Session = capacity[0]
	}
	b.r.paths["/other/file.go"] = "/other"
	b.r.dial = func(ctx context.Context, worktree string) (mcp.Connection, error) {
		dials.Add(1)
		return transport.ConnectSSE(ctx, endpoints[worktree], b.r.limits.MessageBytes, b.r.sseBudget)
	}
	b.start()
	t.Cleanup(b.close)
	instructions, err := b.initialize(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(newHTTPHandler(b, instructions))
	t.Cleanup(server.Close)
	return b, server.URL, &dials, started, cancelled
}

func postMCP(t testing.TB, endpoint, body string) *jsonrpc.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Protocol-Version", "2025-06-18")
	// Even a supplied session identifier must not create or select a session.
	req.Header.Set("Mcp-Session-Id", "same-client-supplied-id")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Mcp-Session-Id") != "" {
		t.Fatalf("HTTP %d, session %q: %s", resp.StatusCode, resp.Header.Get("Mcp-Session-Id"), raw)
	}
	msg, err := jsonrpc.DecodeMessage(raw)
	if err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	result, ok := msg.(*jsonrpc.Response)
	if !ok {
		t.Fatalf("got %T, want response", msg)
	}
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
		if resp.Error != nil || !strings.Contains(string(resp.Result), tc.want) {
			t.Fatalf("got %+v, want %s", resp, tc.want)
		}
	}
	// All concurrent clients deliberately use the same external request ID.
	for i := range 12 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			resp := postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":{}}}`)
			if resp.Error != nil || !strings.Contains(string(resp.Result), "/home") {
				t.Fatalf("concurrent response: %+v", resp)
			}
		})
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("got %d SSE connections, want two reused lanes", got)
	}
	if b.r.sticky != "" {
		t.Fatal("stateless routing retained sticky state")
	}
}

func TestHTTPProtocolAndErrors(t *testing.T) {
	t.Parallel()
	_, endpoint, _, _, _ := httpFixture(t)
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		req, err := http.NewRequestWithContext(t.Context(), method, endpoint, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s: %d", method, resp.StatusCode)
		}
	}
	resp := postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`)
	if resp.Error != nil {
		t.Fatal(resp.Error)
	}
	resp = postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	if resp.Error != nil || !strings.Contains(string(resp.Result), `"where"`) {
		t.Fatalf("tools/list: %+v", resp)
	}
	resp = postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"missing","arguments":{}}}`)
	if resp.Error == nil {
		t.Fatal("upstream error was lost")
	}
}

func TestHTTPCurrentSDKClient(t *testing.T) {
	t.Parallel()
	_, endpoint, _, _, _ := httpFixture(t)
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: endpoint}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	listed, err := session.ListTools(t.Context(), &mcp.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Tools) != 1 || listed.Tools[0].Name != "where" {
		t.Fatalf("tools: %+v", listed)
	}
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "where", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Content) != 1 || result.Content[0].(*mcp.TextContent).Text != "/home" {
		t.Fatalf("result: %+v", result)
	}
}

func TestHTTPDisconnectCancelsLegacySSECall(t *testing.T) {
	t.Parallel()
	b, endpoint, _, started, cancelled := httpFixture(t, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":{"block":true}}}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Protocol-Version", "2025-06-18")
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()
	mustRecv(t, started, "upstream call did not start")
	overload, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := http.DefaultClient.Do(overload)
	if err != nil {
		t.Fatal(err)
	}
	_ = rejected.Body.Close()
	if rejected.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("overload status: %d", rejected.StatusCode)
	}
	cancel()
	mustRecv(t, done, "HTTP request did not stop")
	mustRecv(t, cancelled, "SSE cancellation did not arrive")
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for b.r.requestUsage().Outstanding != 0 {
		select {
		case <-deadline.C:
			t.Fatal("cancelled HTTP call retained its outstanding slot")
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
	if err := mustRecv(t, done, "shutdown did not release HTTP caller"); err == nil {
		t.Fatal("shutdown returned success")
	}
	if got := b.r.requestUsage().Outstanding; got != 0 {
		t.Fatalf("shutdown retained %d calls", got)
	}
}
