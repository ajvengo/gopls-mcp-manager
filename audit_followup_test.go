package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProbeDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var reached atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
	}))
	defer target.Close()
	listener, port := listenInAllocationRange(t, t.Name())
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, target.URL, http.StatusTemporaryRedirect)
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()
	alive, conclusive := endpointProbe(t.Context(), port)
	require.Zero(t, reached.Load(), "readiness probe left its recorded endpoint")
	require.False(t, alive)
	require.False(t, conclusive, "a redirect is not proof that the shared process is gone")
}

func TestCleanupLockFilesBoundedAcrossGenerations(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	for i := range 100 {
		require.NoError(t, m.signalTerminating(t.Context(), record{PID: i + 1, Port: firstPort, Terminating: true}))
	}
	locks, err := filepath.Glob(m.mapPath + ".cleanup-*.lock")
	require.NoError(t, err)
	require.Len(t, locks, 1, "historical process generations accumulated lock inodes")
}

func TestFailedUsageCommitPreservesConcurrentCounts(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions")
	}
	m := newTestManager(t)
	m.uses = &useCounts{counts: make(map[string]int)}
	r := record{Worktree: "/used", PID: 1, Port: firstPort}
	mustWriteMap(t, m.mapPath, []record{r})
	require.NoError(t, withFileLock(t.Context(), m.mapPath, func() error { return nil }))
	m.used(r.Worktree)
	dir := filepath.Dir(m.mapPath)
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(dir, 0o700)) })
	// The observation runs after the failed registry commit and before the
	// caller returns. A rollback must merge, not replace, newer deliveries.
	m.observe = func(lockTiming) { m.used(r.Worktree) }
	_, err := m.evictionVictims(context.Background())
	require.Error(t, err, "regression requires a failed persistent write")
	require.Equal(t, 2, m.uses.counts[r.Worktree], "failed commit lost delivered usage")
}

func TestProcessIdentityRejectsSubstringMatches(t *testing.T) {
	t.Parallel()
	for _, command := range []string{
		"gopls mcp -listen 127.0.0.1:611001",
		"not-gopls mcp -listen 127.0.0.1:61100",
		"editor --log=/tmp/gopls --server=127.0.0.1:61100",
		"gopls other-command 127.0.0.1:61100",
	} {
		require.False(t, matchesGopls(command, firstPort), "unrelated process matched: %s", command)
	}
	for _, command := range []string{"gopls mcp -listen 127.0.0.1:61100", "/usr/local/bin/gopls mcp -listen 127.0.0.1:61100"} {
		require.True(t, matchesGopls(command, firstPort))
	}
}

func TestProcessFixtureCleanupReapsItsChild(t *testing.T) {
	t.Parallel()
	var pid int
	t.Run("fixture", func(t *testing.T) {
		pid = startFakeGopls(t, firstPort).Process.Pid
	})
	require.Eventually(t, func() bool {
		return syscall.Kill(pid, 0) == syscall.ESRCH
	}, time.Second, time.Millisecond, "fixture left a child process or zombie behind")
}
