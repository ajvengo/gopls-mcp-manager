package main

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

// Serial so the profiles do not include concurrently running package tests.
// Each cycle exercises a backend/router, private handshake, roots worker,
// successful call, cancelled unresolved call and shutdown against one manager.
func TestAuditSessionChurn(t *testing.T) {
	m := &manager{pending: newPendingCalls()}
	profileDir := os.Getenv("GOPLS_MANAGER_AUDIT_PROFILES")
	snapshot := func(batch int) {
		t.Helper()
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		t.Logf("batch=%d heap_bytes=%d heap_objects=%d goroutines=%d", batch, mem.HeapAlloc, mem.HeapObjects, runtime.NumGoroutine())
		if profileDir == "" {
			return
		}
		require.True(t, filepath.IsAbs(profileDir))
		require.NoError(t, os.MkdirAll(profileDir, 0o700))
		for _, name := range []string{"heap", "goroutine"} {
			f, err := os.Create(filepath.Join(profileDir, fmt.Sprintf("batch-%02d.%s", batch, name)))
			require.NoError(t, err)
			require.NoError(t, pprof.Lookup(name).WriteTo(f, 0))
			require.NoError(t, f.Close())
		}
	}
	snapshot(0)
	for batch := 1; batch <= 10; batch++ {
		for range 100 {
			ctx, cancel := context.WithCancel(t.Context())
			b := newHTTPBackend(ctx, m, testHome)
			b.r.plan = func(worktree string, _ []string) watchPlan { return watchPlan{roots: []string{worktree}} }
			b.r.dial = func(context.Context, string) (mcp.Connection, error) {
				conn := &fakeConn{reads: make(chan jsonrpc.Message, 4)}
				conn.onWrite = func(ctx context.Context, msg jsonrpc.Message) error {
					req, ok := msg.(*jsonrpc.Request)
					if !ok || !req.ID.IsValid() || req.Method == "tools/call" {
						return nil
					}
					result := jsontext.Value(`{}`)
					if req.Method == "initialize" {
						conn.reads <- &jsonrpc.Request{ID: initID, Method: "roots/list"}
					}
					select {
					case conn.reads <- &jsonrpc.Response{ID: req.ID, Result: result}:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return conn, nil
			}
			b.start()
			_, err := b.initialize(ctx)
			require.NoError(t, err)
			// Expiring work retains its uncertain operation until backend close.
			callCtx, stopCall := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- b.call(callCtx, "tools/call", map[string]any{}, new(jsontext.Value)) }()
			require.Eventually(t, func() bool { return b.r.requestUsage().UpstreamOperations == 1 }, time.Second, time.Millisecond)
			stopCall()
			require.ErrorIs(t, <-done, context.Canceled)
			b.close()
			cancel()
			m.pending.mu.Lock()
			remaining := len(m.pending.ops)
			m.pending.mu.Unlock()
			require.Zero(t, remaining)
		}
		require.Eventually(t, func() bool {
			var stacks bytes.Buffer
			_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
			for _, symbol := range []string{"(*httpBackend).start.func", "(*router).readFromUpstream", "(*router).answerRootsAsks", "(*lane).runControls", "(*router).planRoots.func"} {
				if strings.Contains(stacks.String(), symbol) {
					return false
				}
			}
			return true
		}, 2*time.Second, time.Millisecond, "session-owned goroutines survived shutdown")
		snapshot(batch)
	}
}

func TestAuditDecodedBackpressure(t *testing.T) {
	runtime.GC()
	var before, full, after runtime.MemStats
	runtime.ReadMemStats(&before)
	func() {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r := newRouter(ctx, nil, testHome)
		conn := &fakeConn{reads: make(chan jsonrpc.Message, cap(r.out)+1)}
		for range cap(conn.reads) {
			params := jsontext.Value(fmt.Sprintf(`{"text":%q}`, strings.Repeat("x", 256<<10)))
			conn.reads <- &jsonrpc.Request{Method: "notifications/progress", Params: params}
		}
		done := make(chan struct{})
		go func() { defer close(done); r.readFromUpstream(conn, testHome) }()
		require.Eventually(t, func() bool { return len(r.out) == cap(r.out) }, time.Second, time.Millisecond)
		runtime.GC()
		runtime.ReadMemStats(&full)
		used, _ := r.sseBudget.Snapshot()
		require.Zero(t, used, "decoded output retention is outside the SSE wire budget")
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			require.FailNow(t, "backpressured reader survived shutdown")
		}
	}()
	// A second collection also clears pool victims and avoids attributing a
	// just-completed collection's profile lag to retained session messages.
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	t.Logf("decoded_queue=64 payload_bytes=%d heap_before=%d heap_full=%d heap_after=%d", 256<<10, before.HeapAlloc, full.HeapAlloc, after.HeapAlloc)
}
