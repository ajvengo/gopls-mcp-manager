package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestBudgetReservations(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{-1, 0, 10} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			t.Parallel()
			b := NewBudget(limit)
			if used, peak := b.Snapshot(); used != 0 || peak != 0 {
				t.Fatalf("new budget = %d/%d", used, peak)
			}
			if got := b.acquire(6); got != (limit > 0) {
				t.Fatalf("reservation = %v, limit %d", got, limit)
			}
			if limit <= 0 {
				return
			}
			if b.acquire(5) {
				t.Fatal("over-capacity reservation accepted")
			}
			b.release(6)
			if used, peak := b.Snapshot(); used != 0 || peak != 6 {
				t.Fatalf("released budget = %d/%d, want 0/6", used, peak)
			}
		})
	}
}

func TestSSEReadContracts(t *testing.T) {
	t.Parallel()
	const response = `{"jsonrpc":"2.0","id":1,"result":{"text":"owned"}}`
	for _, tc := range []struct {
		name, frame string
		fails       bool
	}{
		{name: "response after keepalive", frame: ": keepalive\n\nid: 2\nretry: 100\n\ndata: " + response + "\n\n"},
		{name: "request", frame: "data: {\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"roots/list\"}\n\n"},
		{name: "invalid JSON", frame: "data: not json\n\n", fails: true},
		{name: "invalid protocol", frame: "data: {\"jsonrpc\":\"1.0\",\"id\":1,\"result\":{}}\n\n", fails: true},
		{name: "malformed field", frame: "broken\n\n", fails: true},
		{name: "closed stream", fails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, "event: endpoint\ndata: /message\n\n", tc.frame)
			}))
			t.Cleanup(server.Close)
			budget := NewBudget(4096)
			conn, err := ConnectSSE(t.Context(), server.URL, 1024, budget)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			if conn.SessionID() != "" {
				t.Fatal("legacy SSE transport has an HTTP session ID")
			}
			msg, err := conn.Read(t.Context())
			if (err != nil) != tc.fails || (!tc.fails && msg == nil) {
				t.Fatalf("Read = %v, %v; want failure %v", msg, err, tc.fails)
			}
			if tc.fails {
				if used, _ := budget.Snapshot(); used != 0 {
					t.Fatalf("failed read retained %d bytes", used)
				}
			}
		})
	}
}

func TestSSEWriteContracts(t *testing.T) {
	t.Parallel()
	id, _ := jsonrpc.MakeID("call")
	for _, tc := range []struct {
		name                     string
		status                   int
		endpoint                 string
		params                   json.RawMessage
		closed, cancelled, fails bool
	}{
		{name: "accepted", status: http.StatusAccepted},
		{name: "rejected", status: http.StatusServiceUnavailable, fails: true},
		{name: "invalid endpoint", endpoint: ":invalid", fails: true},
		{name: "encoding failure", params: json.RawMessage(`{`), fails: true},
		{name: "cancelled", cancelled: true, fails: true},
		{name: "closed", closed: true, fails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			posted := make(chan string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				body, _ := io.ReadAll(req.Body)
				posted <- req.Header.Get("Content-Type") + ":" + string(body)
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(server.Close)
			endpoint := server.URL
			if tc.endpoint != "" {
				endpoint = tc.endpoint
			}
			c := &sseConn{endpoint: endpoint, maxBytes: 1024, done: make(chan struct{}), body: io.NopCloser(strings.NewReader(""))}
			if tc.closed {
				_ = c.Close()
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			err := c.Write(ctx, &jsonrpc.Request{ID: id, Method: "ping", Params: tc.params})
			if (err != nil) != tc.fails {
				t.Fatalf("Write = %v, want failure %v", err, tc.fails)
			}
			if tc.status != 0 {
				if wire := <-posted; !strings.HasPrefix(wire, "application/json:") || !strings.Contains(wire, `"id":"call"`) {
					t.Fatalf("POST lost headers or identity: %s", wire)
				}
			}
		})
	}
}

func TestTransportReadCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reader, writer := io.Pipe()
	defer func() { _ = writer.Close() }()
	stdio := NewStdio(reader, io.Discard, 1024)
	defer func() { _ = stdio.Close() }()
	if stdio.SessionID() != "" {
		t.Fatal("stdio session ID must be empty")
	}
	sse := &sseConn{done: make(chan struct{})}
	for name, conn := range map[string]mcp.Connection{"stdio": stdio, "SSE": sse} {
		if _, err := conn.Read(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("%s read cancellation = %v", name, err)
		}
	}
}
