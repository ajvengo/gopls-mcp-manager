package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A full stdio session: real newline framing, manager/registry dialing, and
// real SSE upstreams. Setup and handshakes are excluded from warm benchmarks.
func stdioFixture(tb testing.TB, m *manager, home string) mcp.Connection {
	tb.Helper()
	left, right := net.Pipe()
	client, err := (&mcp.IOTransport{Reader: left, Writer: left}).Connect(tb.Context())
	require.NoError(tb, err)
	server := transport.NewStdio(right, right, m.limits.MessageBytes)
	ctx, cancel := context.WithCancel(tb.Context())
	done := inBackground(func() error { return serve(ctx, m, home, server) })
	tb.Cleanup(func() {
		cancel()
		_ = client.Close()
		select {
		case err := <-done:
			assert.NoErrorf(tb, err, "stdio shutdown")
		case <-time.After(5 * time.Second):
			assert.Fail(tb, "stdio server did not stop")
		}
	})
	init := &jsonrpc.Request{ID: mustID(tb, "initialize"), Method: "initialize", Params: json.RawMessage(`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}`)}
	response := stdioRoundTrip(tb, tb.Context(), client, init)
	require.Nilf(tb, response.Error, "initialize: %+v", response)
	require.NoError(tb, client.Write(tb.Context(), &jsonrpc.Request{Method: "notifications/initialized"}))
	return client
}

// stdioRoundTrip verifies transport success and response identity; callers own
// the assertions on the result or protocol error.
func stdioRoundTrip(tb testing.TB, ctx context.Context, client mcp.Connection, request *jsonrpc.Request) *jsonrpc.Response {
	tb.Helper()
	require.NoError(tb, client.Write(ctx, request))
	msg, err := client.Read(ctx)
	require.NoError(tb, err)
	return wantResponse(tb, msg, request.ID)
}

func TestEndToEndStdioRouting(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	m, _, _, _ := sseFixture(t, root, linked)
	client := stdioFixture(t, m, root)
	for _, tc := range []struct {
		name  string
		args  map[string]any
		want  string
		fails bool
	}{
		{name: "home", args: map[string]any{}, want: root},
		{name: "linked", args: map[string]any{"file": filepath.Join(linked, "main.go")}, want: linked},
		{name: "sticky", args: map[string]any{}, want: linked},
		{name: "mixed worktrees", args: map[string]any{"files": []string{filepath.Join(root, "main.go"), filepath.Join(linked, "main.go")}}, fails: true},
		{name: "sticky after refusal", args: map[string]any{}, want: linked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params, err := json.Marshal(map[string]any{"name": "where", "arguments": tc.args})
			require.NoError(t, err)
			id := mustID(t, tc.name)
			response := stdioRoundTrip(t, t.Context(), client, &jsonrpc.Request{ID: id, Method: "tools/call", Params: params})
			ok := (response.Error != nil) == tc.fails && (tc.fails || strings.Contains(string(response.Result), tc.want))
			require.Truef(t, ok, "response = %+v, want %s, failure %v", response, tc.want, tc.fails)
		})
	}
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
	m := newTestManager(t)
	for _, address := range []string{"invalid", "0.0.0.0:0", "127.0.0.1:0"} {
		t.Run(address, func(t *testing.T) {
			t.Parallel()
			err := serveHTTP(t.Context(), &m, testHome, address, io.Discard)
			require.Error(t, err, "invalid address or missing home server accepted")
		})
	}
}

func BenchmarkStdioRoundTrip(b *testing.B) {
	m, _, _, _ := sseFixture(b, testHome)
	client := stdioFixture(b, m, testHome)
	request := &jsonrpc.Request{ID: mustID(b, "call"), Method: "tools/call", Params: json.RawMessage(`{"name":"where","arguments":{}}`)}
	b.ReportAllocs()
	for b.Loop() {
		response := stdioRoundTrip(b, b.Context(), client, request)
		require.Nilf(b, response.Error, "tool response: %+v", response)
	}
}

func TestHTTPServerOutputFailure(t *testing.T) {
	t.Parallel()
	m, _, _, _ := sseFixture(t, testHome)
	want := io.ErrClosedPipe
	err := serveHTTP(t.Context(), m, testHome, "127.0.0.1:0", &failingWriter{})
	require.ErrorIsf(t, err, want, "output failure = %v, want %v", err, want)
}
