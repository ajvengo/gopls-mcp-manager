package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestSweepBoundsConcurrencyAndCancelsBeforeMutation(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	records := testRecords(64)
	mustWriteMap(t, m.mapPath, records)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{}, len(records))
	var active, peak atomic.Int32
	m.alive = func(ctx context.Context, _ record) probeVerdict {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		entered <- struct{}{}
		<-ctx.Done()
		return probeGone
	}
	done := make(chan error, 1)
	go func() {
		_, err := m.withRecords(ctx, func(rs []record) ([]record, error) { return rs, nil })
		done <- err
	}()
	for range 8 {
		mustRecv(t, entered, "probe worker did not start")
	}
	cancel()
	if err := mustRecv(t, done, "cancelled sweep did not stop"); !errors.Is(err, context.Canceled) {
		t.Fatalf("sweep error = %v, want cancellation", err)
	}
	if got := peak.Load(); got != 8 {
		t.Fatalf("peak probes = %d, want 8", got)
	}
	wantRecords(t, m.mapPath, "cancelled sweep changed registry", records...)
}

type gatedBody struct {
	io.Reader
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (b *gatedBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered); <-b.release })
	return b.Reader.Read(p)
}

func TestHTTPBodyBudgetIncludesSlowReaders(t *testing.T) {
	t.Parallel()
	b := newHTTPBackend(t.Context(), nil, testHome)
	defer b.cancel()
	b.r.limits.Session = 10
	b.r.limits.MessageBytes = 64
	b.r.limits.HTTPBytes = 128
	handler := newHTTPHandler(b, "")
	release := make(chan struct{})
	done := make(chan struct{}, 2)
	for range 2 {
		body := &gatedBody{Reader: strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`), entered: make(chan struct{}), release: release}
		req := httptest.NewRequest(http.MethodPost, "/mcp", body)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		go func() { handler.ServeHTTP(httptest.NewRecorder(), req); done <- struct{}{} }()
		mustRecv(t, body.entered, "body reader did not start")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{}`)))
	close(release)
	for range 2 {
		mustRecv(t, done, "HTTP exchange did not release")
	}
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("budget overload = %d, want 503", response.Code)
	}
}

func TestHTTPRejectsOversizeAndReleasesReservation(t *testing.T) {
	t.Parallel()
	b := newHTTPBackend(t.Context(), nil, testHome)
	defer b.cancel()
	b.r.limits.MessageBytes = 64
	b.r.limits.HTTPBytes = 64
	handler := newHTTPHandler(b, "")
	for _, tc := range []struct {
		body   string
		status int
	}{
		{strings.Repeat(" ", 65), http.StatusRequestEntityTooLarge},
		{`{"jsonrpc":"2.0","id":1,"method":"ping"}`, http.StatusOK},
	} {
		req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != tc.status {
			t.Fatalf("HTTP status = %d, want %d: %s", response.Code, tc.status, response.Body)
		}
	}
}

func BenchmarkHTTPRoundTrip(b *testing.B) {
	_, endpoint, _, _, _ := httpFixture(b)
	const body = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"where","arguments":{}}}`
	b.ReportAllocs()
	for b.Loop() {
		if response := postMCP(b, endpoint, body); response.Error != nil {
			b.Fatal(response.Error)
		}
	}
}

func TestHTTPWarmCallsBypassBlockedResolution(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := newHTTPBackend(t.Context(), nil, testHome)
		entered, release := make(chan struct{}), make(chan struct{})
		b.r.resolver = &pathResolver{jobs: make(chan resolution), lookup: func(context.Context, json.RawMessage) []string {
			close(entered)
			<-release
			return []string{testHome}
		}}
		go b.r.resolver.run(b.r.ctx)
		b.r.dial = func(context.Context, string) (mcp.Connection, error) { return loopbackConn(), nil }
		b.start()
		defer b.close()
		defer close(release)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		coldDone := make(chan error, 1)
		go func() {
			var result json.RawMessage
			coldDone <- b.call(ctx, "tools/call", fileCallParams("/cold/file.go"), &result)
		}()
		mustRecv(t, entered, "cold lookup did not start")
		start := time.Now()
		var result json.RawMessage
		if err := b.call(t.Context(), "tools/call", json.RawMessage(`{"arguments":{}}`), &result); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 0 {
			t.Fatal("warm HTTP call waited for the cold lookup budget")
		}
		cancel()
		if err := mustRecv(t, coldDone, "cancelled cold call did not finish"); !errors.Is(err, context.Canceled) {
			t.Fatalf("cold error = %v", err)
		}
	})
}

func TestMemoExpiryRevalidatesRetargetedSymlink(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	alias := symlinkAt(t, root)
	r := newTestRouter(t, root)
	path := filepath.Join(alias, "missing.go")
	if got := r.worktreeOf(r.ctx, path); got != root {
		t.Fatalf("initial target = %s, want %s", got, root)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(linked, alias); err != nil {
		t.Fatal(err)
	}
	r.memo.expires = time.Now().Add(-time.Second)
	if got := r.worktreeOf(r.ctx, path); got != linked {
		t.Fatalf("expired target = %s, want %s", got, linked)
	}
}

func TestMemoEpochExpiresBothLevels(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits.CacheTTL = time.Second
		r.paths["/old/file.go"] = pathMemo{worktree: testWorktree}
		r.worktrees["/old"] = testWorktree
		_, hit := r.cachedWorktrees(fileCallParams("/old/file.go"))
		if !hit {
			t.Fatal("seeded memo missed")
		}
		time.Sleep(time.Second)
		_, hit = r.cachedWorktrees(fileCallParams("/old/file.go"))
		usage := r.requestUsage()
		if hit || usage.Paths != 0 || usage.Directories != 0 || usage.MemoExpirations != 1 {
			t.Fatalf("expired memo retained state: hit=%v usage=%+v", hit, usage)
		}
		if usage.MemoHits != 1 || usage.MemoMisses != 1 {
			t.Fatalf("memo observations = %+v", usage)
		}
	})
}

func TestMemoRefreshAtCapacityDoesNotEvict(t *testing.T) {
	t.Parallel()
	r := newTestRouter(t, testHome)
	r.limits.CacheEntries = 1
	memoize(r, r.paths, "/a", pathMemo{worktree: testHome})
	memoize(r, r.paths, "/a", pathMemo{worktree: testWorktree})
	if r.memo.rollovers != 0 || r.paths["/a"].worktree != testWorktree {
		t.Fatal("updating an existing key rolled the memo over")
	}
	memoize(r, r.paths, "/b", pathMemo{worktree: testHome})
	if r.memo.rollovers != 1 || len(r.paths) != 1 {
		t.Fatal("new key did not enforce capacity")
	}
}

func TestLaneChurnRemainsAtRetentionCeiling(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.limits.Lanes = 4
		handshakeReady(r)
		var dials int
		r.dial = func(context.Context, string) (mcp.Connection, error) { dials++; return loopbackConn(), nil }
		defer r.closeLanes()
		for i := range 1000 {
			worktree := fmt.Sprintf("/worktree-%d", i)
			file := worktree + "/file.go"
			r.paths[file] = pathMemo{worktree: worktree}
			r.route(&jsonrpc.Request{ID: mustID(t, float64(i)), Method: "tools/call", Params: fileCallParams(file)})
			msg := mustRecv(t, r.out, "churn request did not finish")
			response, ok := msg.(*jsonrpc.Response)
			if !ok || (response.Error != nil) != (i >= 4) {
				t.Fatalf("request %d: %+v", i, msg)
			}
		}
		usage := r.requestUsage()
		if dials != 4 || usage.Lanes != 4 || usage.Outstanding != 0 || usage.UpstreamOperations != 0 {
			t.Fatalf("churn escaped bounds: dials=%d usage=%+v", dials, usage)
		}
	})
}
