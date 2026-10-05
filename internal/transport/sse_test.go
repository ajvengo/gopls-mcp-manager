package transport

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

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
	for {
		budget.mu.Lock()
		used := budget.used
		budget.mu.Unlock()
		if used > 0 {
			break
		}
		if time.Now().After(deadline) {
			require.FailNow(t, "second frame did not arrive")
		}
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, conn.Close())
	for {
		budget.mu.Lock()
		used := budget.used
		budget.mu.Unlock()
		if used == 0 {
			break
		}
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
