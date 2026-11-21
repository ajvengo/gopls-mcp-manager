//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/stretchr/testify/require"
)

func TestAuditRealGoplsNativePressure(t *testing.T) {
	binary := buildAuditManager(t)
	root, linked := auditWorktrees(t)
	for _, tc := range []struct{ name, slots, files, reject string }{
		{"FDs", "2", "760", "descriptor reservation exhausted"},
		{"processes", "1", "2048", "shared gopls server limit reached"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GOPLS_MANAGER_MAX_LANES", "2")
			t.Setenv("GOPLS_MANAGER_MAX_HTTP_CONNECTIONS", "8")
			t.Setenv("GOPLS_MANAGER_MAX_OWNER_SESSIONS", "2")
			t.Setenv("GOPLS_MANAGER_MAX_SERVERS", tc.slots)
			t.Setenv("GOPLS_MANAGER_MAX_OPEN_FILES", tc.files)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			m := useAuditHome(t, t.TempDir())
			expectAuditCleanup(t, m)
			owner := startAuditOwner(t, ctx, binary, m)
			require.NoError(t, auditTool(t, ctx, m, root))
			before, err := auditSnapshot(ctx, m)
			require.NoError(t, err)
			require.Len(t, before.Generations, 1)
			rejections := make(chan error, 4)
			for range cap(rejections) {
				go func() { rejections <- auditEnsure(ctx, m, linked) }()
			}
			for range cap(rejections) {
				require.ErrorContains(t, <-rejections, tc.reject)
			}

			path, err := supervisorSocket(m)
			require.NoError(t, err)
			var sockets []net.Conn
			for range m.limits.HTTPConnections {
				conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
				require.NoError(t, err)
				defer func() { _ = conn.Close() }()
				sockets = append(sockets, conn)
			}
			require.Eventually(t, func() bool {
				attempt, stop := context.WithTimeout(ctx, 150*time.Millisecond)
				defer stop()
				return supervisorPing(attempt, m) != nil
			}, 3*time.Second, 20*time.Millisecond, "native sockets did not exhaust the owner connection limit")
			sampleAuditProcesses(t, ctx, m, before)
			for _, conn := range sockets {
				require.NoError(t, conn.Close())
			}
			require.Eventually(t, func() bool { return supervisorPing(ctx, m) == nil }, 3*time.Second, 20*time.Millisecond)
			require.NoError(t, auditTool(t, ctx, m, root))
			require.Equal(t, owner.command.Process.Pid, before.PID)
			records := mustReadMap(t, m.mapPath)
			require.Len(t, records, 1)
			require.Equal(t, before.Generations[0].Generation, records[0].Generation)
		})
	}
}

func TestAuditRealGoplsSoak(t *testing.T) {
	mode := os.Getenv("GOPLS_MANAGER_RUNTIME_AUDIT")
	if mode == "" {
		t.Skip("set GOPLS_MANAGER_RUNTIME_AUDIT to smoke, idle-smoke, idle, suspend, 24h or 72h")
	}
	duration, interval, restartEvery := time.Duration(0), 15*time.Second, 10*time.Minute
	switch mode {
	case "smoke":
		duration, interval, restartEvery = 3*time.Second, 250*time.Millisecond, time.Second
	case "idle-smoke":
		duration, interval = 40*time.Second, time.Second
	case "idle", "suspend":
		duration = 75 * time.Minute
	case "24h":
		duration = 24 * time.Hour
	case "72h":
		duration = 72 * time.Hour
	default:
		t.Fatalf("unsupported runtime audit mode %q", mode)
	}
	if mode == "suspend" {
		require.Equal(t, "linux", runtime.GOOS, "suspend evidence uses Linux boot uptime")
	}
	binary := buildAuditManager(t)
	root, linked := auditWorktrees(t)
	idle, observe := time.Hour, 70*time.Minute
	if mode == "idle-smoke" {
		idle, observe = 2*time.Second, 0
	}
	t.Setenv("GOPLS_MANAGER_IDLE_TIMEOUT", idle.String())
	ctx, cancel := context.WithTimeout(t.Context(), duration+2*time.Minute)
	defer cancel()
	m := useAuditHome(t, t.TempDir())
	expectAuditCleanup(t, m)
	startAuditOwner(t, ctx, binary, m)
	if mode == "idle-smoke" || mode == "idle" || mode == "suspend" {
		verifyAuditIdle(t, ctx, m, root, mode == "suspend", duration, interval, observe)
		return
	}
	end, nextRestart := time.Now().Add(duration), time.Now().Add(restartEvery)
	restarts := 0
	for time.Now().Before(end) {
		for _, worktree := range []string{root, linked} {
			require.NoError(t, auditTool(t, ctx, m, worktree))
		}
		snapshot, err := auditSnapshot(ctx, m)
		require.NoError(t, err)
		require.Len(t, snapshot.Generations, 2)
		sampleAuditProcesses(t, ctx, m, snapshot)
		if time.Now().After(nextRestart) {
			out, err := exec.CommandContext(ctx, binary, "restart", "--timeout=1m").CombinedOutput()
			require.NoError(t, err, "%s", out)
			restarts++
			nextRestart = time.Now().Add(restartEvery)
		}
		require.NoError(t, waitContext(ctx, interval))
	}
	require.Positive(t, restarts)
}

func verifyAuditIdle(t *testing.T, ctx context.Context, m *manager, root string, suspend bool, duration, interval, observe time.Duration) {
	t.Helper()
	require.NoError(t, auditEnsure(ctx, m, root))
	start := time.Now()
	suspendGap := time.Duration(0)
	var uptime time.Duration
	if suspend {
		uptime = auditBootUptime(t)
	}
	retired := false
	for time.Since(start) < duration {
		if suspend {
			suspendGap = max(suspendGap, auditBootUptime(t)-uptime-time.Since(start))
		}
		snapshot, err := auditSnapshot(ctx, m)
		require.NoError(t, err)
		require.Zero(t, snapshot.Sessions, "idle trial requires zero attachments")
		if len(snapshot.Generations) == 0 {
			require.GreaterOrEqual(t, time.Since(start), m.limits.Idle-interval, "retired early")
			retired = true
		} else {
			require.False(t, retired, "idle owner started a replacement without a client")
			require.Len(t, snapshot.Generations, 1)
		}
		if retired && time.Since(start) >= observe {
			break
		}
		require.NoError(t, waitContext(ctx, interval))
	}
	require.True(t, retired, "real maintenance did not retire the idle generation")
	if suspend {
		require.GreaterOrEqual(t, suspendGap, 5*time.Second, "suspend the full Linux host or VM during the trial")
	}
	t.Logf("idle age=%s suspend observed=%s", time.Since(start), suspendGap)
	require.NoError(t, auditTool(t, ctx, m, root))
}

func auditBootUptime(t *testing.T) time.Duration {
	t.Helper()
	raw, err := os.ReadFile("/proc/uptime")
	require.NoError(t, err)
	fields := strings.Fields(string(raw))
	require.NotEmpty(t, fields)
	seconds, err := strconv.ParseFloat(fields[0], 64)
	require.NoError(t, err)
	return time.Duration(seconds * float64(time.Second))
}

func buildAuditManager(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "manager")
	out, err := exec.CommandContext(t.Context(), "go", "build", "-o", binary, ".").CombinedOutput()
	require.NoError(t, err, "%s", out)
	return binary
}

func auditWorktrees(t *testing.T) (string, string) {
	t.Helper()
	root, linked := newLinkedWorktree(t)
	for _, worktree := range []string{root, linked} {
		mustWriteFile(t, filepath.Join(worktree, "go.mod"), "module example.com/audit\n\ngo 1.27\n")
		mustWriteFile(t, filepath.Join(worktree, "audit.go"), "package audit\n")
	}
	return root, linked
}

func auditTool(t *testing.T, parent context.Context, m *manager, home string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	client := supervisorHTTPClient(m)
	defer client.CloseIdleConnections()
	request := mcpRequest(t, ctx, "http://127.0.0.1/mcp",
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"go_workspace","arguments":{}}}`)
	if err := ownerHeaders(request, m, home); err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(m.limits.MessageBytes)+1))
	if err != nil {
		return err
	}
	message, err := jsonrpc.DecodeMessage(body)
	result, ok := message.(*jsonrpc.Response)
	if err != nil || response.StatusCode != http.StatusOK || !ok || result.Error != nil ||
		strings.Contains(string(result.Result), `"isError":true`) || !strings.Contains(string(result.Result), home) {
		return fmt.Errorf("workspace MCP response HTTP %d: %s", response.StatusCode, body)
	}
	return nil
}

func sampleAuditProcesses(t *testing.T, ctx context.Context, m *manager, snapshot supervisorSnapshot) {
	t.Helper()
	require.LessOrEqual(t, snapshot.FDReserved, m.limits.OpenFiles)
	require.LessOrEqual(t, snapshot.Sessions, m.limits.OwnerSessions)
	require.LessOrEqual(t, snapshot.Queued+snapshot.Operations, m.limits.OwnerOperations)
	require.LessOrEqual(t, snapshot.RetainedMessageBytes, m.limits.OwnerRetainedBytes)
	require.LessOrEqual(t, len(snapshot.Generations), m.limits.SharedServers)
	pids := []string{strconv.Itoa(snapshot.PID)}
	for _, generation := range snapshot.Generations {
		require.Equal(t, "ready", generation.State)
		require.Positive(t, generation.GuardianPID)
		require.Positive(t, generation.PID)
		pids = append(pids, strconv.Itoa(generation.PID), strconv.Itoa(generation.GuardianPID))
	}
	selector := strings.Join(pids, ",")
	out, err := boundedCommandOutput(ctx, 1<<20, "lsof", "-n", "-P", "-w", "-Fpf", "-p", selector)
	require.NoError(t, err)
	counts := map[string]int{}
	var pid string
	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.HasPrefix(line, "p") {
			pid = line[1:]
		}
		if strings.HasPrefix(line, "f") {
			if _, err := strconv.Atoi(line[1:]); err == nil {
				counts[pid]++
			}
		}
	}
	total := 0
	for _, pid := range pids {
		require.Positive(t, counts[pid], "missing live process %s", pid)
		total += counts[pid]
	}
	require.LessOrEqual(t, total, snapshot.FDReserved)
	ownerLimit, err := supervisorFDAllowance(m.limits)
	require.NoError(t, err)
	require.LessOrEqual(t, counts[pids[0]], ownerLimit)
	rss, err := boundedCommandOutput(ctx, 4096, "ps", "-o", "pid=,rss=", "-p", selector)
	require.NoError(t, err)
	t.Logf("sessions=%d fd_reserved=%d fd_counts=%v pid_rss_kib=%q logs=%+v",
		snapshot.Sessions, snapshot.FDReserved, counts, strings.TrimSpace(string(rss)), snapshot.Logs)
}
