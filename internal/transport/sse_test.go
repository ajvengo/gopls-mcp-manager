package transport

import (
	"bufio"
	"context"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestSSELifetimeCancellationReleasesUnreadFrame(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "event: endpoint\ndata: /message\n\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notification\"}\n\n")
		w.(http.Flusher).Flush()
		<-req.Context().Done()
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	budget := NewBudget(4096)
	conn, err := ConnectSSE(ctx, server.URL, 1024, budget)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.Eventually(t, func() bool {
		used, _ := budget.Snapshot()
		return used > 0
	}, time.Second, time.Millisecond, "the unread frame never reached its handoff")

	cancel()
	require.Eventually(t, func() bool {
		used, _ := budget.Snapshot()
		return used == 0
	}, time.Second, time.Millisecond, "lifetime cancellation retained an unread frame without Close")
}

func TestSSECloseCancelsInProgressPost(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			_, _ = io.Copy(io.Discard, req.Body)
			close(entered)
			<-req.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "event: endpoint\ndata: /message\n\n")
		w.(http.Flusher).Flush()
		<-req.Context().Done()
	}))
	t.Cleanup(server.Close)
	conn, err := ConnectSSE(t.Context(), server.URL, 1024, NewBudget(4096))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	writeCtx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- conn.Write(writeCtx, &jsonrpc.Request{Method: "notifications/initialized"}) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		require.FailNow(t, "POST never reached the server")
	}

	require.NoError(t, conn.Close())
	select {
	case err := <-done:
		require.Error(t, err, "closing the connection must end an in-progress POST")
	case <-time.After(time.Second):
		cancel()
		require.Error(t, <-done)
		require.FailNow(t, "Close retained an in-progress POST until its independent caller cancelled")
	}
}

func TestSSEWriteCancellationPreservesConnection(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			_, _ = io.Copy(io.Discard, req.Body)
			if posts.Add(1) == 1 {
				close(entered)
				<-req.Context().Done()
				return
			}
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "event: endpoint\ndata: /message\n\n")
		w.(http.Flusher).Flush()
		<-req.Context().Done()
	}))
	t.Cleanup(server.Close)
	conn, err := ConnectSSE(t.Context(), server.URL, 1024, NewBudget(4096))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	writeCtx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- conn.Write(writeCtx, &jsonrpc.Request{Method: "first"}) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		require.FailNow(t, "POST never reached the server")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		require.FailNow(t, "write cancellation did not release the POST")
	}
	require.NoError(t, conn.Write(t.Context(), &jsonrpc.Request{Method: "second"}))
	require.Equal(t, int64(2), posts.Load())
}

func TestSSERejectsEndpointEscape(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []string{"http://example.invalid/message", "//example.invalid/message", "file:///tmp/message", "http://user@127.0.0.1/message"} {
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "event: endpoint\ndata: %s\n\n", endpoint)
				w.(http.Flusher).Flush()
				<-req.Context().Done()
			}))
			defer server.Close()
			budget := NewBudget(4096)
			conn, err := ConnectSSE(t.Context(), server.URL, 1024, budget)
			if conn != nil {
				_ = conn.Close()
			}
			require.Error(t, err, "endpoint event must stay on its original origin")
			used, _ := budget.Snapshot()
			require.Zero(t, used)
		})
	}
}

func TestSSERejectsRedirects(t *testing.T) {
	t.Parallel()
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			var escaped atomic.Int64
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				escaped.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, "event: endpoint\ndata: /message\n\n")
			}))
			defer target.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method == method {
					http.Redirect(w, req, target.URL, http.StatusTemporaryRedirect)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, "event: endpoint\ndata: /message\n\n")
				w.(http.Flusher).Flush()
				<-req.Context().Done()
			}))
			defer server.Close()
			conn, err := ConnectSSE(t.Context(), server.URL, 1024, NewBudget(4096))
			if conn != nil {
				defer func() { _ = conn.Close() }()
			}
			if method == http.MethodGet {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Error(t, conn.Write(context.Background(), &jsonrpc.Request{Method: "notifications/initialized"}))
			}
			require.Zero(t, escaped.Load(), "redirect sent a request outside the recorded endpoint")
		})
	}
}

func FuzzSSEFrameBudget(f *testing.F) {
	f.Add("event: message\ndata: {\"jsonrpc\":\"2.0\",\ndata: \"id\":1,\"result\":{}}\n\n: tail without newline")
	f.Add("id: 1\nretry: 0\n\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n")
	f.Add("event: endpoint\ndata: //example.invalid/message\n\n")
	f.Add("data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"x\":1,\"x\":2}}\n\n")
	for _, seed := range []string{"data: {}\n\n", ": comment\n\n", "event: message\r\ndata: {\r\ndata: }\r\n\r\n", strings.Repeat("x", 4097)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		budget := NewBudget(8192)
		conn := &sseConn{budget: budget, maxBytes: 4096}
		frame, err := conn.readFrame(bufio.NewReader(strings.NewReader(input)))
		if err == nil {
			require.LessOrEqual(t, len(frame), 4096)
			_, _, _ = parseSSEFrame(frame)
			budget.release(cap(frame))
		}
		used, peak := budget.Snapshot()
		require.Zero(t, used)
		require.LessOrEqual(t, peak, 8192)
	})
}

func TestSSEFrameBoundsAndOwnership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, input, want string
		limit, budget     int
		fails             bool
	}{
		{"exact", "data: 1\n\n", "1", 9, 9, false},
		{"multiline", "event: message\r\ndata: {\r\ndata: \"id\":1}\r\n\r\n", "{\n\"id\":1}", 128, 128, false},
		{"incomplete", "data: 1", "1", 7, 7, false},
		{"oversize", "data: 12\n\n", "", 9, 9, true},
		{"unterminated", strings.Repeat("x", 8193), "", 8192, 32768, true},
		{"aggregate exhausted", "data: 1\n\n", "", 128, 127, true},
		{"growth", "data: " + strings.Repeat("x", 6000) + "\n\n", strings.Repeat("x", 6000), 8192, 16384, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			budget := &Budget{limit: tc.budget}
			c := &sseConn{budget: budget, maxBytes: tc.limit}
			frame, err := c.readFrame(bufio.NewReader(strings.NewReader(tc.input)))
			require.Equalf(t, tc.fails, err != nil, "read error = %v, want failure %v", err, tc.fails)
			if err == nil {
				_, data, err := parseSSEFrame(frame)
				require.Truef(t, err == nil && string(data) == tc.want, "data = %q, error %v", data, err)
				require.Equalf(t, cap(frame), budget.used, "charged %d, frame capacity %d", budget.used, cap(frame))
				budget.release(cap(frame))
			}
			require.Falsef(t, budget.used != 0 || budget.peak > budget.limit, "budget after release: %+v", budget)
		})
	}
}

func TestSSEReadAheadAndSlowConsumerClose(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, ": comment\n\nevent: endpoint\ndata: /message\n\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n\n")
		w.(http.Flusher).Flush()
		<-req.Context().Done()
	}))
	t.Cleanup(server.Close)
	budget := &Budget{limit: 4096}
	conn, err := ConnectSSE(t.Context(), server.URL, 1024, budget)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	msg, err := conn.Read(t.Context())
	require.NoError(t, err)
	response, ok := msg.(*jsonrpc.Response)
	wantID, _ := jsonrpc.MakeID(float64(1))
	require.Truef(t, ok && response.ID == wantID, "read-ahead response: %v", msg)
	// The second frame is retained by an unbuffered handoff, even though no
	// consumer is reading. Close must return its charge without needing a Read.
	deadline := time.Now().Add(time.Second)
	for used, _ := budget.Snapshot(); used <= 0; used, _ = budget.Snapshot() {
		if time.Now().After(deadline) {
			require.FailNow(t, "second frame did not arrive")
		}
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, conn.Close())
	for used, _ := budget.Snapshot(); used != 0; used, _ = budget.Snapshot() {
		if time.Now().After(deadline) {
			require.FailNow(t, "Close retained frame capacity")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSSEOversizeBeforePost(t *testing.T) {
	t.Parallel()
	c := &sseConn{maxBytes: 8, done: make(chan struct{})}
	id, _ := jsonrpc.MakeID("1")
	err := c.Write(context.Background(), &jsonrpc.Request{ID: id, Method: "too large"})
	require.ErrorIsf(t, err, ErrMessageTooLarge, "write: %v", err)
}

func BenchmarkSSEFrameDecode(b *testing.B) {
	for _, size := range []int{128, 64 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			input := "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"" + strings.Repeat("x", size) + "\"}\n\n"
			c := &sseConn{maxBytes: 4 << 20, budget: &Budget{limit: 64 << 20}}
			b.ReportAllocs()
			b.SetBytes(int64(len(input)))
			for b.Loop() {
				frame, err := c.readFrame(bufio.NewReader(strings.NewReader(input)))
				require.NoError(b, err)
				_, data, err := parseSSEFrame(frame)
				require.NoError(b, err)
				_, err = jsonrpc.DecodeMessage(data)
				require.NoError(b, err)
				c.budget.release(cap(frame))
			}
		})
	}
}

func BenchmarkSSETransportRoundTrip(b *testing.B) {
	for _, transport := range []string{"SDK", "bounded"} {
		b.Run(transport, func(b *testing.B) {
			server := mcp.NewServer(&mcp.Implementation{Name: "fixture", Version: "1"}, nil)
			upstream := httptest.NewServer(mcp.NewSSEHandler(func(*http.Request) *mcp.Server { return server }, nil))
			b.Cleanup(upstream.Close)
			var conn mcp.Connection
			var err error
			if transport == "SDK" {
				conn, err = (&mcp.SSEClientTransport{Endpoint: upstream.URL}).Connect(b.Context())
			} else {
				conn, err = ConnectSSE(b.Context(), upstream.URL, 4<<20, &Budget{limit: 64 << 20})
			}
			require.NoError(b, err)
			b.Cleanup(func() { _ = conn.Close() })
			id, _ := jsonrpc.MakeID("1")
			err = conn.Write(b.Context(), &jsonrpc.Request{ID: id, Method: "initialize", Params: []byte(`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"bench","version":"1"}}`)})
			require.NoError(b, err)
			_, err = conn.Read(b.Context())
			require.NoError(b, err)
			err = conn.Write(b.Context(), &jsonrpc.Request{Method: "notifications/initialized"})
			require.NoError(b, err)
			request := &jsonrpc.Request{ID: id, Method: "ping"}
			b.ReportAllocs()
			for b.Loop() {
				err := conn.Write(b.Context(), request)
				require.NoError(b, err)
				msg, err := conn.Read(b.Context())
				require.NoError(b, err)
				response, ok := msg.(*jsonrpc.Response)
				require.Truef(b, ok && response.Error == nil && response.ID == id, "ping response: %+v", msg)
			}
		})
	}
}

// The shipped message ceiling, for the tests that are not about the limit.
const defaultTestLimit = 4 << 20

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
		{name: "duplicate names", frame: "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"x\":1,\"x\":2}}\n\n", fails: true},
		{name: "invalid UTF-8", frame: "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"\xff\"}\n\n", fails: true},
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
		params                   jsontext.Value
		closed, cancelled, fails bool
	}{
		{name: "accepted", status: http.StatusAccepted},
		{name: "rejected", status: http.StatusServiceUnavailable, fails: true},
		{name: "invalid endpoint", endpoint: ":invalid", fails: true},
		{name: "encoding failure", params: jsontext.Value(`{`), fails: true},
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
			lifetime, closeLifetime := context.WithCancel(t.Context())
			defer closeLifetime()
			c := &sseConn{ctx: lifetime, cancel: closeLifetime, endpoint: endpoint, maxBytes: 1024, done: make(chan struct{}), body: io.NopCloser(strings.NewReader(""))}
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
