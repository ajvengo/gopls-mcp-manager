//go:build integration

package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
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
func realGoplsFixture(t testing.TB) (*supervisor, string, string) {
	t.Helper()
	binary, err := exec.LookPath(goplsBinary)
	require.NoError(t, err, "integration tests require gopls on PATH")
	require.NoError(t, guardianCheckGopls(binary))
	root, linked := newLinkedWorktree(t)
	for dir, marker := range map[string]string{root: "RootMarker", linked: "LinkedMarker"} {
		mustWriteFile(t, filepath.Join(dir, "go.mod"), "module example.com/fixture\n\ngo 1.27\n")
		mustWriteFile(t, filepath.Join(dir, "marker.go"), "package fixture\n\nconst "+marker+" = 1\n")
		mustWriteFile(t, filepath.Join(dir, "fixture.go"), "package fixture\n\nvar _ = "+marker+"\n")
	}
	m := newTestManager(t)
	m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	logs, err := newOwnerLog(t.Context(), filepath.Dir(m.logPath("")), m.limits.SharedServers+1, int64(m.limits.OwnerLogBytes))
	require.NoError(t, err)
	m.ownerLogs = logs
	t.Cleanup(logs.Close)
	ctx, cancel := context.WithCancel(t.Context())
	owner := newSupervisor(ctx, &m)
	owner.start = func(ctx context.Context, worktree string, port, quota int) (*guardianChild, error) {
		if err := m.validateManagedToolchain(ctx, worktree); err != nil {
			return nil, err
		}
		config := guardianConfig{Version: guardianProtocol, Worktree: worktree, Port: port, Token: rand.Text(), Command: binary,
			Args: []string{"mcp", "-listen", mcpAddress(port)}, MaxOpenFiles: quota,
			ExecArgs: []string{"-test.run=^TestGuardianHelperProcess$", "--", "guardian-child"}}
		config.StatePath = filepath.Join(guardianStateDir(&m), "guardian-"+config.Token+".json")
		command := exec.Command(os.Args[0], "-test.run=^TestGuardianHelperProcess$", "--", "guardian")
		command.Env = append(os.Environ(), "GOPLS_GUARDIAN_TEST_HELPER=1")
		sink, err := logs.Sink(m.logPath(worktree))
		if err != nil {
			return nil, err
		}
		return startGuardianCommand(ctx, config, sink, command)
	}
	t.Cleanup(func() {
		cancel()
		owner.shutdown()
		require.Eventually(t, func() bool { return len(owner.snapshot().Generations) == 0 }, 15*time.Second, 10*time.Millisecond, "owned real gopls/guardian cleanup did not finish")
	})
	return owner, root, linked
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
	records, _, err := readMap(m.m.mapPath)
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

	m.m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	m.m.limits.DirectoryFilters = []string{"-excluded"}
	require.NoError(t, os.Mkdir(filepath.Join(root, "excluded"), 0o700))
	other := m

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	first := newHTTPBackend(ctx, m, root)
	second := newHTTPBackend(ctx, other, root)
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
	records := mustReadMap(t, m.m.mapPath)
	require.Len(t, records, 1, "simultaneous clients must share one child")
	old := records[0]
	call := func(b *httpBackend, file, marker string) error {
		t.Helper()
		var result jsontext.Value
		if err := b.call(ctx, "tools/call", map[string]any{"name": "go_file_context", "arguments": map[string]any{"file": file}}, &result); err != nil {
			return err
		}
		require.NotContains(t, string(result), `"isError":true`)
		require.Contains(t, string(result), marker)
		return nil
	}
	require.NoError(t, call(first, filepath.Join(root, "fixture.go"), "RootMarker"))
	require.NoError(t, call(second, filepath.Join(linked, "fixture.go"), "LinkedMarker"))
	require.Len(t, mustReadMap(t, m.m.mapPath), 2, "linked worktree must get its own shared child")
	require.Eventually(t, func() bool {
		m.m.watch.mu.Lock()
		defer m.m.watch.mu.Unlock()
		return len(m.m.watch.plans[root].partial) > 0
	}, time.Second, time.Millisecond)
	// Exercise the real start-grace policy rather than rewriting process identity.

	mustWriteFile(t, filepath.Join(root, "added.go"), "package fixture\nvar _ = RootMarker\n")
	_, err := m.maintenanceSweep(ctx)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		if _, err := m.maintenanceSweep(ctx); err != nil {
			return false
		}
		records, _, err := readMap(m.m.mapPath)
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
	if err := call(first, filepath.Join(root, "added.go"), "RootMarker"); err != nil {
		// Reader failure and an uncertain Write can race on the old stream
		// (SPEC F2). Either terminal result must precede any replacement; the
		// next independent call must reconnect and see the new file.
		var remote *jsonrpc.Error
		require.ErrorAs(t, err, &remote)
		require.EqualValues(t, jsonrpc.CodeInternalError, remote.Code)
		require.True(t,
			strings.HasPrefix(remote.Message, "gopls for "+root+" went away mid-call:") ||
				strings.HasPrefix(remote.Message, "gopls for "+root+": upstream write failed; request may have executed:"),
			"unexpected terminal failure: %s", remote.Message)
		for _, r := range mustReadMap(t, m.m.mapPath) {
			require.NotEqual(t, root, r.Worktree, "a replacement failed instead of the old stream")
		}
		require.NoError(t, call(first, filepath.Join(root, "added.go"), "RootMarker"))
	}
	for _, r := range mustReadMap(t, m.m.mapPath) {
		if r.Worktree == root {
			require.NotEqual(t, old.PID, r.PID)
		}
	}
}

func TestAuditRealGoplsFailedStartReapsChild(t *testing.T) {
	t.Parallel()
	m, root, _ := realGoplsFixture(t)
	owner := m
	start := owner.start
	var child *guardianChild
	owner.start = func(ctx context.Context, worktree string, port, quota int) (*guardianChild, error) {
		var err error
		child, err = start(ctx, worktree, port, quota)
		return child, err
	}
	owner.ready = func(context.Context, int, int) error { return context.DeadlineExceeded }
	_, err := owner.ensureRecord(t.Context(), root)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotNil(t, child)
	select {
	case <-child.ConfirmedDone:
	default:
		t.Fatal("failed guardian/child was not reaped")
	}
	require.Empty(t, mustReadMap(t, m.m.mapPath))
	require.Empty(t, owner.snapshot().Generations)
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
	b.r.dial = func(ctx context.Context, worktree string, port int) (mcp.Connection, error) {
		conn, err := dial(ctx, worktree, port)
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
	records := mustReadMap(t, m.m.mapPath)
	require.Len(t, records, 3)
	m.m.openFiles = countOpenFiles
	var status strings.Builder
	require.NoError(t, m.m.status(ctx, &status))
	t.Logf("owned real-gopls resources before removal: %s", &status)

	for _, dir := range []string{linked, churn} {
		runGit(t, "-C", root, "worktree", "remove", "--force", dir)
	}

	require.Eventually(t, func() bool {
		_, err := m.maintenanceSweep(ctx)
		if err != nil {
			return false
		}
		records, _, err := readMap(m.m.mapPath)
		return err == nil && len(records) == 1 && records[0].Worktree == root
	}, 5*time.Second, 20*time.Millisecond, "removed worktree servers retained their records")
}

// This pins the actual installed gopls telemetry contract, including lack of
// telemetry descriptors/sidecars. All managed state and filecache are private;
// no user telemetry mode file is changed.
func TestGuardianRealGoplsTelemetrySuppressed(t *testing.T) {
	t.Parallel()
	binary, err := exec.LookPath(goplsBinary)
	require.NoError(t, err)
	require.NoError(t, guardianCheckGopls(binary))
	lsof, err := exec.LookPath("lsof")
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module guardian.test\n\ngo 1.27\n"), 0o600))
	port, err := allocatePort(dir, nil, portUnavailable)
	require.NoError(t, err)
	executable, err := os.Executable()
	require.NoError(t, err)
	config := guardianConfig{Version: guardianProtocol, Worktree: dir, Port: port,
		Token: rand.Text(), Command: binary, MaxOpenFiles: 512,
		ExecArgs: []string{"-test.run=^TestGuardianHelperProcess$", "--", "guardian-child"}, Args: []string{"mcp", "-listen", mcpAddress(port)}}
	config.StatePath = filepath.Join(dir, "guardians", "guardian-"+config.Token+".json")
	command := exec.Command(executable, "-test.run=^TestGuardianHelperProcess$", "--", "guardian")
	command.Env = append(os.Environ(), "GOPLS_GUARDIAN_TEST_HELPER=1", "GOPLSCACHE="+filepath.Join(dir, "cache"))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	child, err := startGuardianCommand(ctx, config, io.Discard, command)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, child.CloseOwner())
		select {
		case <-child.ConfirmedDone:
		case <-time.After(10 * time.Second):
			assert.Fail(t, "real gopls group did not exit")
		}
	})
	require.NoError(t, awaitReady(ctx, port, child.Process.Pid))
	out, err := exec.CommandContext(ctx, lsof, "-n", "-P", "-F", "n", "-p", strconv.Itoa(child.Process.Pid)).Output()
	require.NoError(t, err)
	require.NotContains(t, string(out), "/telemetry/", "counter files must not be opened")
	rows, err := guardianSnapshot()
	require.NoError(t, err)
	require.False(t, guardianEscaped(rows, child.Process.Pid, child.GuardianPID))
	require.Equal(t, 1, guardianGroupMembers(rows, child.GuardianPID), "only the actual gopls belongs to the anchored group")
	require.NoError(t, child.Signal(syscall.SIGTERM))
	select {
	case <-child.ConfirmedDone:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "real gopls TERM/Wait/group proof did not complete")
	}
	require.NoFileExists(t, child.StatePath)
}

func TestGuardianRealGoPrivateTelemetryMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	env, err := guardianTelemetryEnvironment(dir)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "-C", dir, "env", "GOTELEMETRY", "GOTELEMETRYDIR")
	command.Env = env
	out, err := command.Output()
	require.NoError(t, err)
	require.Equal(t, "off\n"+filepath.Join(dir, "telemetry-off")+"\n", string(out))
	entries, err := os.ReadDir(filepath.Join(dir, "telemetry-off"))
	require.NoError(t, err)
	require.Len(t, entries, 1, "off mode must create no weekly counters/upload files")
	require.Equal(t, "mode", entries[0].Name())
}

func TestAuditRealGoplsHotRestart(t *testing.T) {
	auditRealGoplsHotRestart(t, "", "")
}

func TestAuditRealGoplsReleaseUpgrade(t *testing.T) {
	original := os.Getenv("GOPLS_MANAGER_RELEASE_BINARY")
	if original == "" {
		t.Skip("set GOPLS_MANAGER_RELEASE_BINARY to a shipped manager executable")
	}
	require.True(t, filepath.IsAbs(original))
	auditRealGoplsHotRestart(t, original, "")
}

func TestAuditRealGoplsHandoffProcessDeaths(t *testing.T) {
	for _, point := range []string{"before-successor-ready", "after-owner-commit", "after-successor-commit"} {
		t.Run(point, func(t *testing.T) { auditRealGoplsHotRestart(t, "", point) })
	}
}

func auditRealGoplsHotRestart(t *testing.T, original, crash string) {
	t.Helper()
	binary, err := exec.LookPath(goplsBinary)
	require.NoError(t, err)
	require.NoError(t, guardianCheckGopls(binary))
	root, linked := newLinkedWorktree(t)
	for dir, marker := range map[string]string{root: "RootMarker", linked: "LinkedMarker"} {
		mustWriteFile(t, filepath.Join(dir, "go.mod"), "module example.com/restart\n\ngo 1.27\n")
		mustWriteFile(t, filepath.Join(dir, "marker.go"), "package fixture\nconst "+marker+" = 1\n")
		mustWriteFile(t, filepath.Join(dir, "fixture.go"), "package fixture\nvar _ = "+marker+"\n")
	}
	home := t.TempDir()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var hooks []auditHook
	switch crash {
	case "before-successor-ready":
		hooks = []auditHook{{"restart.go", "if _, err := inherit.ready.Write(", `auditCrash("before-successor-ready")`}}
	case "after-owner-commit":
		hooks = []auditHook{{"restart.go", "committed = true", `auditCrash("after-owner-commit")`}}
	case "after-successor-commit":
		hooks = []auditHook{{"restart.go", "inherit.activated = true", `auditCrash("after-successor-commit")`}}
	}
	overlay := auditOverlayFlag(t, hooks)
	if original == "" {
		original = buildAuditManager(t, ctx, overlay)
	}
	updated := buildAuditManager(t, ctx, overlay, "-ldflags=-s -w")
	marker := filepath.Join(home, "audit-crash")
	t.Setenv("GOPLS_AUDIT_CRASH", crash)
	t.Setenv("GOPLS_AUDIT_MARKER", marker)
	m := useAuditHome(t, home)
	owner := exec.CommandContext(ctx, original, "supervisor")
	require.NoError(t, owner.Start())
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- owner.Wait() }()
	t.Cleanup(func() {
		currentPID := owner.Process.Pid
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		// The handoff may commit even if the CLI fails to receive its receipt.
		if snapshot, err := auditSnapshot(cleanupCtx, m); err == nil && snapshot.PID > 0 {
			currentPID = snapshot.PID
		}
		process, err := os.FindProcess(currentPID)
		if assert.NoError(t, err) {
			_ = process.Signal(syscall.SIGTERM)
		}
		assert.Eventually(t, func() bool {
			count, err := guardianManifestCount(guardianStateDir(m))
			return err == nil && count == 0 && supervisorPing(cleanupCtx, m) != nil
		}, 15*time.Second, 10*time.Millisecond, "integration left an owner or guardian")
	})
	require.Eventually(t, func() bool { return supervisorPing(ctx, m) == nil }, 10*time.Second, 10*time.Millisecond)
	start := func(args ...string) (io.WriteCloser, io.ReadCloser) {
		command := exec.CommandContext(ctx, original, args...)
		input, err := command.StdinPipe()
		require.NoError(t, err)
		output, err := command.StdoutPipe()
		require.NoError(t, err)
		require.NoError(t, command.Start())
		t.Cleanup(func() { _ = input.Close(); _ = output.Close(); _ = command.Process.Kill(); _ = command.Wait() })
		return input, output
	}
	connect := func() (mcp.Connection, io.Writer) {
		input, output := start("bridge", root)
		client, err := (&mcp.IOTransport{Reader: output, Writer: input}).Connect(ctx)
		require.NoError(t, err)
		response := stdioRoundTrip(t, ctx, client, &jsonrpc.Request{ID: mustID(t, "init"), Method: "initialize", Params: jsontext.Value(
			`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"restart-test","version":"1"}}`)})
		require.Nil(t, response.Error)
		require.NoError(t, client.Write(ctx, &jsonrpc.Request{Method: "notifications/initialized"}))
		return client, input
	}
	first, input := connect()
	second, _ := connect()
	call := func(client mcp.Connection, params, want string) {
		response := stdioRoundTrip(t, ctx, client, &jsonrpc.Request{ID: mustID(t, "call"), Method: "tools/call", Params: jsontext.Value(params)})
		require.Nil(t, response.Error)
		require.NotContains(t, string(response.Result), `"isError":true`)
		require.Contains(t, string(response.Result), want)
	}
	call(first, `{"name":"go_file_context","arguments":{"file":`+strconv.Quote(filepath.Join(linked, "fixture.go"))+`}}`, "LinkedMarker")
	call(second, `{"name":"go_workspace","arguments":{}}`, root)
	_, httpOutput := start("http", "127.0.0.1:0", root)
	line, err := bufio.NewReader(httpOutput).ReadString('\n')
	require.NoError(t, err)
	endpoint := strings.TrimSpace(strings.TrimPrefix(line, "MCP endpoint: "))
	var dials atomic.Int32
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: 20 * time.Second}
	httpCall := func() {
		req := mcpRequest(t, ctx, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"go_workspace","arguments":{}}}`)
		response, err := httpClient.Do(req)
		require.NoError(t, err)
		data, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		require.Equal(t, http.StatusOK, response.StatusCode, "%s", data)
		require.Contains(t, string(data), root)
	}
	httpCall()
	before := mustReadMap(t, m.mapPath)
	require.Len(t, before, 2)
	split := &jsonrpc.Request{ID: mustID(t, "split"), Method: "tools/call", Params: jsontext.Value(`{"name":"go_workspace","arguments":{}}`)}
	data, err := jsonrpc.EncodeMessage(split)
	require.NoError(t, err)
	_, err = input.Write(data[:len(data)/2])
	require.NoError(t, err)
	timeout := "--timeout=60s"
	if crash == "after-successor-commit" {
		timeout = "--timeout=10s"
	}
	out, err := exec.CommandContext(ctx, updated, "reload", timeout).CombinedOutput()
	var receipt restartReceipt
	switch crash {
	case "before-successor-ready":
		require.Error(t, err)
		require.Contains(t, string(out), "replacement failed before readiness")
	case "after-owner-commit":
		require.Error(t, err, "old owner died before sending a receipt")
		require.Error(t, <-ownerDone)
		require.Equal(t, 93, owner.ProcessState.ExitCode())
		require.Eventually(t, func() bool {
			snapshot, err := auditSnapshot(ctx, m)
			if err != nil || snapshot.Restart.Active {
				return false
			}
			receipt = restartReceipt{PID: snapshot.PID, Boot: snapshot.Boot}
			return receipt.PID != owner.Process.Pid
		}, 15*time.Second, 20*time.Millisecond)
	case "after-successor-commit":
		require.Error(t, err, "successor died after receiving commit")
		require.NoError(t, <-ownerDone)
	default:
		require.NoError(t, err, "%s", out)
		require.NoError(t, json.Unmarshal(out, &receipt))
		require.NoError(t, <-ownerDone)
	}
	if crash != "" {
		require.Equal(t, crash, mustReadString(t, marker))
	}
	if crash == "after-successor-commit" {
		readCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		_, err := first.Read(readCtx)
		require.Error(t, err)
		require.False(t, errors.Is(err, context.DeadlineExceeded), "frontend remained paused")
		require.Eventually(t, func() bool {
			return withSupervisorSingleton(ctx, m, func() error { return nil }) == nil
		}, 2*time.Second, 20*time.Millisecond, "handoff left the singleton locked")
		startAuditOwner(t, ctx, updated, m)
		require.NoError(t, auditEnsure(ctx, m, root))
		return
	}
	_, err = input.Write(append(data[len(data)/2:], '\n'))
	require.NoError(t, err)
	message, err := first.Read(ctx)
	require.NoError(t, err)
	response := message.(*jsonrpc.Response)
	require.Equal(t, split.ID, response.ID)
	require.Nil(t, response.Error)
	require.Contains(t, string(response.Result), linked, "partial input and sticky state must survive replacement")
	call(first, `{"name":"go_workspace","arguments":{}}`, linked)
	call(second, `{"name":"go_workspace","arguments":{}}`, root)
	httpCall()
	require.Equal(t, int32(1), dials.Load(), "public HTTP connection was replaced")
	after := mustReadMap(t, m.mapPath)
	require.Len(t, after, 2)
	if crash == "before-successor-ready" {
		// The old owner has already drained and can start fresh generations.
		snapshot, err := auditSnapshot(ctx, m)
		require.NoError(t, err)
		require.Equal(t, owner.Process.Pid, snapshot.PID)
		return
	}
	require.NotEqual(t, owner.Process.Pid, receipt.PID)
	for _, prior := range before {
		for _, next := range after {
			if prior.Worktree == next.Worktree {
				require.NotEqual(t, prior.PID, next.PID)
				require.NotEqual(t, prior.Generation, next.Generation)
				descriptors, err := exec.CommandContext(ctx, "lsof", "-p", strconv.Itoa(next.PID), "-Fn").Output()
				require.NoError(t, err)
				require.NotContains(t, string(descriptors), ".owner.lock", "replacement gopls inherited the owner singleton lock")
			}
		}
	}
	identity, err := exec.CommandContext(ctx, "ps", "-ww", "-p", strconv.Itoa(receipt.PID), "-o", "command=").Output()
	require.NoError(t, err)
	require.Contains(t, string(identity), updated+" internal-restart-child")
}
