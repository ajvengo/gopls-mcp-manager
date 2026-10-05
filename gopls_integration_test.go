//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
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
			var call func(json.RawMessage) *jsonrpc.Response
			wantUnqualifiedWorktree := root
			if frontend == "HTTP" {
				b := newHTTPBackend(ctx, m, root)
				b.start()
				defer b.close()
				instructions, err := b.initialize(ctx)
				require.NoError(t, err)
				server := httptest.NewServer(newHTTPHandler(b, instructions))
				defer server.Close()
				call = func(params json.RawMessage) *jsonrpc.Response {
					return postMCP(t, server.URL, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":`+string(params)+`}`)
				}
			} else {
				wantUnqualifiedWorktree = linked
				client := stdioFixture(t, m, root)
				call = func(params json.RawMessage) *jsonrpc.Response {
					id := mustID(t, "call")
					return stdioRoundTrip(t, ctx, client, &jsonrpc.Request{ID: id, Method: "tools/call", Params: params})
				}
			}
			for _, tc := range []struct{ name, params, want string }{
				{"home workspace", `{"name":"go_workspace","arguments":{}}`, root},
				{"linked file", `{"name":"go_file_context","arguments":{"file":` + strconv.Quote(filepath.Join(linked, "fixture.go")) + `}}`, "LinkedMarker"},
				{"unqualified workspace after linked call", `{"name":"go_workspace","arguments":{}}`, wantUnqualifiedWorktree},
			} {
				response := call(json.RawMessage(tc.params))
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
