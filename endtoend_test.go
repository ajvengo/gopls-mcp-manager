package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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
)

// A full stdio session: real newline framing, manager/registry dialing, and
// real SSE upstreams. Setup and handshakes are excluded from warm benchmarks.
func stdioFixture(tb testing.TB, m *manager, home string) mcp.Connection {
	tb.Helper()
	left, right := net.Pipe()
	client, err := (&mcp.IOTransport{Reader: left, Writer: left}).Connect(tb.Context())
	if err != nil {
		tb.Fatal(err)
	}
	server := transport.NewStdio(right, right, m.limits.MessageBytes)
	ctx, cancel := context.WithCancel(tb.Context())
	done := inBackground(func() error { return serve(ctx, m, home, server) })
	tb.Cleanup(func() {
		cancel()
		_ = client.Close()
		select {
		case err := <-done:
			if err != nil {
				tb.Errorf("stdio shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			tb.Error("stdio server did not stop")
		}
	})
	init := &jsonrpc.Request{ID: mustID(tb, "initialize"), Method: "initialize", Params: json.RawMessage(`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}`)}
	if response := stdioRoundTrip(tb, tb.Context(), client, init); response.Error != nil {
		tb.Fatalf("initialize: %+v", response)
	}
	if err := client.Write(tb.Context(), &jsonrpc.Request{Method: "notifications/initialized"}); err != nil {
		tb.Fatal(err)
	}
	return client
}

// stdioRoundTrip verifies transport success and response identity; callers own
// the assertions on the result or protocol error.
func stdioRoundTrip(tb testing.TB, ctx context.Context, client mcp.Connection, request *jsonrpc.Request) *jsonrpc.Response {
	tb.Helper()
	if err := client.Write(ctx, request); err != nil {
		tb.Fatal(err)
	}
	msg, err := client.Read(ctx)
	if err != nil {
		tb.Fatal(err)
	}
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
			if err != nil {
				t.Fatal(err)
			}
			id := mustID(t, tc.name)
			response := stdioRoundTrip(t, t.Context(), client, &jsonrpc.Request{ID: id, Method: "tools/call", Params: params})
			if (response.Error != nil) != tc.fails || (!tc.fails && !strings.Contains(string(response.Result), tc.want)) {
				t.Fatalf("response = %+v, want %s, failure %v", response, tc.want, tc.fails)
			}
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
		if err := mustRecv(t, done, "HTTP server shutdown"); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	endpoint := strings.TrimSpace(strings.TrimPrefix(line, "MCP endpoint: "))
	response := postMCP(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":{}}}`)
	if response.Error != nil || !strings.Contains(string(response.Result), testHome) {
		t.Fatalf("production HTTP response: %+v", response)
	}
	resp, err := http.Get(strings.TrimSuffix(endpoint, "/mcp") + "/missing")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected route status: %d", resp.StatusCode)
	}
}

func TestHTTPServerStartupErrors(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	for _, address := range []string{"invalid", "0.0.0.0:0", "127.0.0.1:0"} {
		t.Run(address, func(t *testing.T) {
			t.Parallel()
			if err := serveHTTP(t.Context(), &m, testHome, address, io.Discard); err == nil {
				t.Fatal("invalid address or missing home server accepted")
			}
		})
	}
}

func BenchmarkStdioRoundTrip(b *testing.B) {
	m, _, _, _ := sseFixture(b, testHome)
	client := stdioFixture(b, m, testHome)
	request := &jsonrpc.Request{ID: mustID(b, "call"), Method: "tools/call", Params: json.RawMessage(`{"name":"where","arguments":{}}`)}
	b.ReportAllocs()
	for b.Loop() {
		if response := stdioRoundTrip(b, b.Context(), client, request); response.Error != nil {
			b.Fatalf("tool response: %+v", response)
		}
	}
}

func TestHTTPServerOutputFailure(t *testing.T) {
	t.Parallel()
	m, _, _, _ := sseFixture(t, testHome)
	want := io.ErrClosedPipe
	if err := serveHTTP(t.Context(), m, testHome, "127.0.0.1:0", &failingWriter{}); !errors.Is(err, want) {
		t.Fatalf("output failure = %v, want %v", err, want)
	}
}
