package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestBudgetReservations(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{-1, 0, 10} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			t.Parallel()
			b := NewBudget(limit)
			used, peak := b.Snapshot()
			require.Falsef(t, used != 0 || peak != 0, "new budget = %d/%d", used, peak)
			got := b.acquire(6)
			require.Equalf(t, limit > 0, got, "reservation = %v, limit %d", got, limit)
			if limit <= 0 {
				return
			}
			require.False(t, b.acquire(5), "over-capacity reservation accepted")
			b.release(6)
			used, peak = b.Snapshot()
			require.Falsef(t, used != 0 || peak != 6, "released budget = %d/%d, want 0/6", used, peak)
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
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			require.Empty(t, conn.SessionID(), "legacy SSE transport has an HTTP session ID")
			msg, err := conn.Read(t.Context())
			require.Falsef(t, (err != nil) != tc.fails || (!tc.fails && msg == nil), "Read = %v, %v; want failure %v", msg, err, tc.fails)
			if tc.fails {
				used, _ := budget.Snapshot()
				require.Equalf(t, 0, used, "failed read retained %d bytes", used)
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
			require.Equalf(t, tc.fails, err != nil, "Write = %v, want failure %v", err, tc.fails)
			if tc.status != 0 {
				wire := <-posted
				require.Falsef(t, !strings.HasPrefix(wire, "application/json:") || !strings.Contains(wire, `"id":"call"`), "POST lost headers or identity: %s", wire)
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
	require.Empty(t, stdio.SessionID(), "stdio session ID must be empty")
	sse := &sseConn{done: make(chan struct{})}
	for name, conn := range map[string]mcp.Connection{"stdio": stdio, "SSE": sse} {
		_, err := conn.Read(ctx)
		require.ErrorIsf(t, err, context.Canceled, "%s read cancellation = %v", name, err)
	}
}
