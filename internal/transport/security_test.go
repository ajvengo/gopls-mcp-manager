package transport

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/stretchr/testify/require"
)

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
