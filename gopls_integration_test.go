//go:build integration

package main

import (
	"context"
	"encoding/json/jsontext"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Opt-in because this starts real gopls processes and requires gopls on PATH.
// Every process uses a disposable repository and private registry, and is
// explicitly reaped; integration runs never touch the user's shared servers.
func realGoplsFixture(t testing.TB) (*manager, string, string) {
	t.Helper()
	_, err := exec.LookPath(goplsBinary)
	require.NoError(t, err, "integration tests require gopls on PATH")
	root, linked := newLinkedWorktree(t)
	for dir, marker := range map[string]string{root: "RootMarker", linked: "LinkedMarker"} {
		mustWriteFile(t, filepath.Join(dir, "go.mod"), "module example.com/fixture\n\ngo 1.27\n")
		mustWriteFile(t, filepath.Join(dir, "marker.go"), "package fixture\n\nconst "+marker+" = 1\n")
		mustWriteFile(t, filepath.Join(dir, "fixture.go"), "package fixture\n\nvar _ = "+marker+"\n")
	}
	m := newTestManager(t)
	m.start = func(ctx context.Context, worktree string, port int) (*childProcess, error) {
		child, err := m.startGopls(ctx, worktree, port)
		if err == nil {
			t.Cleanup(func() {
				assert.NoErrorf(t, child.stop(), "gopls cleanup")
			})
		}
		return child, err
	}
	return &m, root, linked
}

func TestEndToEndRealGopls(t *testing.T) {
	t.Parallel()
	m, root, linked := realGoplsFixture(t)
	for _, frontend := range []string{"HTTP", "stdio"} {
		t.Run(frontend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			var call func(jsontext.Value) *jsonrpc.Response
			wantUnqualifiedWorktree := root
			if frontend == "HTTP" {
				b := newHTTPBackend(ctx, m, root)
				b.start()
				defer b.close()
				instructions, err := b.initialize(ctx)
				require.NoError(t, err)
				server := httptest.NewServer(newHTTPHandler(b, instructions))
				defer server.Close()
				call = func(params jsontext.Value) *jsonrpc.Response {
					return postMCP(t, server.URL, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":`+string(params)+`}`)
				}
			} else {
				wantUnqualifiedWorktree = linked
				client := stdioFixture(t, m, root)
				call = func(params jsontext.Value) *jsonrpc.Response {
					id := mustID(t, "call")
					return stdioRoundTrip(t, ctx, client, &jsonrpc.Request{ID: id, Method: "tools/call", Params: params})
				}
			}
			for _, tc := range []struct{ name, params, want string }{
				{"home workspace", `{"name":"go_workspace","arguments":{}}`, root},
				{"linked file", `{"name":"go_file_context","arguments":{"file":` + strconv.Quote(filepath.Join(linked, "fixture.go")) + `}}`, "LinkedMarker"},
				{"unqualified workspace after linked call", `{"name":"go_workspace","arguments":{}}`, wantUnqualifiedWorktree},
			} {
				response := call(jsontext.Value(tc.params))
				ok := response.Error == nil && !strings.Contains(string(response.Result), `"isError":true`) && strings.Contains(string(response.Result), tc.want)
				require.Truef(t, ok, "%s: real gopls response to %s = %+v; want %s", tc.name, tc.params, response, tc.want)
			}
		})
	}
	records, _, err := readMap(m.mapPath)
	require.Truef(t, err == nil && len(records) == 2, "two frontends must share two worktree servers: records=%+v, error=%v", records, err)
}

// Measures real gopls tool execution as well as HTTP/SSE routing. Child CPU and
// allocations are outside this process's pprof profile; their latency is included.
func BenchmarkRealGoplsRoundTrip(b *testing.B) {
	m, root, _ := realGoplsFixture(b)
	backend := newHTTPBackend(b.Context(), m, root)
	backend.start()
	b.Cleanup(backend.close)
	instructions, err := backend.initialize(b.Context())
	require.NoError(b, err)
	server := httptest.NewServer(newHTTPHandler(backend, instructions))
	b.Cleanup(server.Close)
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"go_file_context","arguments":{"file":` + strconv.Quote(filepath.Join(root, "fixture.go")) + `}}}`
	// Load/index the fixture before timing steady-state tool calls.
	response := postMCP(b, server.URL, body)
	require.NoError(b, response.Error)
	b.ReportAllocs()
	for b.Loop() {
		response := postMCP(b, server.URL, body)
		ok := response.Error == nil && strings.Contains(string(response.Result), "RootMarker")
		require.Truef(b, ok, "real gopls tool response: %+v", response)
	}
}

func TestAuditRealGoplsSharedClientsAndWatcherRestart(t *testing.T) {
	t.Parallel()
	m, root, linked := realGoplsFixture(t)
	m.pending = newPendingCalls()
	m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	m.limits.DirectoryFilters = []string{"-excluded"}
	require.NoError(t, os.Mkdir(filepath.Join(root, "excluded"), 0o700))
	other := *m
	other.pending = newPendingCalls()
	other.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	first := newHTTPBackend(ctx, m, root)
	second := newHTTPBackend(ctx, &other, root)
	first.start()
	second.start()
	defer first.close()
	defer second.close()
	initialized := make(chan error, 2)
	for _, b := range []*httpBackend{first, second} {
		go func() { _, err := b.initialize(ctx); initialized <- err }()
	}
	require.NoError(t, <-initialized)
	require.NoError(t, <-initialized)
	records := mustReadMap(t, m.mapPath)
	require.Len(t, records, 1, "simultaneous clients must share one child")
	old := records[0]
	call := func(b *httpBackend, file, marker string) {
		t.Helper()
		var result jsontext.Value
		require.NoError(t, b.call(ctx, "tools/call", map[string]any{"name": "go_file_context", "arguments": map[string]any{"file": file}}, &result))
		require.NotContains(t, string(result), `"isError":true`)
		require.Contains(t, string(result), marker)
	}
	call(first, filepath.Join(root, "fixture.go"), "RootMarker")
	call(second, filepath.Join(linked, "fixture.go"), "LinkedMarker")
	require.Len(t, mustReadMap(t, m.mapPath), 2, "linked worktree must get its own shared child")
	require.Eventually(t, func() bool {
		m.watch.mu.Lock()
		defer m.watch.mu.Unlock()
		return len(m.watch.plans[root].partial) > 0
	}, time.Second, time.Millisecond)
	// Exercise the real start-grace policy rather than rewriting process identity.
	require.NoError(t, waitContext(ctx, time.Until(time.Unix(old.StartedAt, 0).Add(startGrace))))
	mustWriteFile(t, filepath.Join(root, "added.go"), "package fixture\nvar _ = RootMarker\n")
	_, err := m.sweepMaintenance(ctx)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		if _, err := m.withRecords(ctx, keepRecords); err != nil {
			return false
		}
		records, _, err := readMap(m.mapPath)
		if err != nil {
			return false
		}
		for _, r := range records {
			if r.Worktree == root {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	call(first, filepath.Join(root, "added.go"), "RootMarker")
	for _, r := range mustReadMap(t, m.mapPath) {
		if r.Worktree == root {
			require.NotEqual(t, old.PID, r.PID)
		}
	}
}

func TestAuditRealGoplsFailedStartReapsChild(t *testing.T) {
	t.Parallel()
	m, root, _ := realGoplsFixture(t)
	start := m.start
	var child *childProcess
	m.start = func(ctx context.Context, worktree string, port int) (*childProcess, error) {
		var err error
		child, err = start(ctx, worktree, port)
		return child, err
	}
	m.ready = func(context.Context, int) error { return context.DeadlineExceeded }
	_, err := m.ensure(t.Context(), root)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotNil(t, child)
	select {
	case <-child.done:
	default:
		require.FailNow(t, "failed child was not reaped")
	}
	require.Empty(t, mustReadMap(t, m.mapPath))
}

// Hold one actual upstream result until local cancellation completes. This
// tests real SSE delivery and late-result cleanup, without claiming the tool
// was interrupted after gopls had already computed its answer.
type auditHeldResponseConn struct {
	mcp.Connection
	id       jsonrpc.ID
	received chan struct{}
	release  <-chan struct{}
}

func (c *auditHeldResponseConn) Read(ctx context.Context) (jsonrpc.Message, error) {
	msg, err := c.Connection.Read(ctx)
	if resp, ok := msg.(*jsonrpc.Response); ok && resp.ID == c.id {
		select {
		case c.received <- struct{}{}:
		default:
		}
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return msg, err
}

func (c *auditHeldResponseConn) server() serverID { return serverOf(c.Connection) }

func TestAuditRealGoplsCancellationAndWorktreeChurn(t *testing.T) {
	t.Parallel()
	m, root, linked := realGoplsFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	b := newHTTPBackend(ctx, m, root)
	dial := b.r.dial
	received, release := make(chan struct{}, 1), make(chan struct{})
	heldID := mustID(t, "2")
	b.r.dial = func(ctx context.Context, worktree string) (mcp.Connection, error) {
		conn, err := dial(ctx, worktree)
		if err != nil {
			return nil, err
		}
		return &auditHeldResponseConn{Connection: conn, id: heldID, received: received, release: release}, nil
	}
	b.start()
	defer b.close()
	_, err := b.initialize(ctx)
	require.NoError(t, err)
	params := func(dir string) map[string]any {
		return map[string]any{"name": "go_file_context", "arguments": map[string]any{"file": filepath.Join(dir, "fixture.go")}}
	}
	callCtx, stopCall := context.WithCancel(ctx)
	defer stopCall()
	done := make(chan error, 1)
	go func() { done <- b.call(callCtx, "tools/call", params(root), new(jsontext.Value)) }()
	select {
	case <-received:
	case <-ctx.Done():
		require.FailNow(t, "real upstream did not produce the held response")
	}
	stopCall()
	require.ErrorIs(t, <-done, context.Canceled)
	close(release)
	require.Eventually(t, func() bool { return b.r.requestUsage().UpstreamOperations == 0 }, 5*time.Second, time.Millisecond)

	churn := filepath.Join(t.TempDir(), "churn")
	runGit(t, "-C", root, "worktree", "add", "--detach", churn, "HEAD")
	mustWriteFile(t, filepath.Join(churn, "go.mod"), "module example.com/churn\n\ngo 1.27\n")
	mustWriteFile(t, filepath.Join(churn, "fixture.go"), "package fixture\nconst ChurnMarker = 1\n")
	for _, dir := range []string{root, linked, churn} {
		var result jsontext.Value
		require.NoError(t, b.call(ctx, "tools/call", params(dir), &result))
		require.NotContains(t, string(result), `"isError":true`)
	}
	records := mustReadMap(t, m.mapPath)
	require.Len(t, records, 3)
	m.openFiles = countOpenFiles
	var status strings.Builder
	require.NoError(t, m.status(ctx, &status))
	t.Logf("owned real-gopls resources before removal: %s", &status)
	latest := time.Time{}
	for _, r := range records {
		if start := time.Unix(r.StartedAt, 0); start.After(latest) {
			latest = start
		}
	}
	for _, dir := range []string{linked, churn} {
		runGit(t, "-C", root, "worktree", "remove", "--force", dir)
	}
	require.NoError(t, waitContext(ctx, time.Until(latest.Add(startGrace))))
	require.Eventually(t, func() bool {
		_, err := m.sweepMaintenance(ctx)
		if err != nil {
			return false
		}
		records, _, err := readMap(m.mapPath)
		return err == nil && len(records) == 1 && records[0].Worktree == root
	}, 5*time.Second, 20*time.Millisecond, "removed worktree servers retained their records")
}
