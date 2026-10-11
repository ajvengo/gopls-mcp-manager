package main

import (
	"bytes"
	"context"

	json "encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdmissionReapsAnsweringDeletedWorktreeBeforeCapacityCheck(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.limits.SharedServers = 1
	listener, port := listenInAllocationRange(t, t.Name())
	serveEndpoint(t, listener, 0)
	process := startFakeGopls(t, port)
	exited := make(chan struct{})
	go func() { _ = process.Wait(); close(exited) }()
	stale := record{Worktree: filepath.Join(t.TempDir(), "deleted"), PID: process.Process.Pid, Port: port}
	mustWriteMap(t, m.mapPath, []record{stale})
	verdict := recordAlive(t.Context(), stale)
	require.Equalf(t, probeLive, verdict, "regression requires an answering server, got %v", verdict)
	err := m.prepareAdmission(t.Context(), "/new/worktree")
	require.NoError(t, err)
	select {
	case <-exited:
	case <-time.After(time.Second):
		require.FailNow(t, "stale process did not exit")
	}
	m.start = func(context.Context, string, int) (*childProcess, error) {
		return &childProcess{Process: &os.Process{Pid: os.Getpid()}, done: make(chan struct{})}, nil
	}
	wanted := t.TempDir()
	claimed, started, err := m.claimPort(t.Context(), wanted)
	require.Truef(t, err == nil && started != nil && claimed.Worktree == wanted,
		"new server denied after stale slot cleanup: %+v %v", claimed, err)
}

func TestMaintenanceReapsUnrequestedRecordsAndStops(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	stale := record{Worktree: "/removed/worktree", PID: 12345, Port: firstPort}
	mustWriteMap(t, m.mapPath, []record{stale})
	probed := make(chan struct{}, 1)
	m.alive = func(context.Context, record) probeVerdict {
		select {
		case probed <- struct{}{}:
		default:
		}
		return probeGone
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); m.maintain(ctx, time.Millisecond) }()
	select {
	case <-probed:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "idle maintenance never probed the stale server")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		records, _, err := readMap(m.mapPath)
		require.NoError(t, err)
		if len(records) == 0 {
			break
		}
		if time.Now().After(deadline) {
			require.FailNow(t, "stale record was not removed")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "maintenance did not stop on cancellation")
	}
}

func TestAdmissionDoesNotWaitForUnrelatedUncertainTermination(t *testing.T) {
	t.Parallel()
	for _, reuse := range []bool{false, true} {
		t.Run(fmt.Sprint(reuse), func(t *testing.T) {
			t.Parallel()
			m := newTestManager(t)
			m.limits.SharedServers = 2
			wanted := t.TempDir()
			records := []record{{Worktree: "/uncertain", PID: 12345, Port: firstPort, Terminating: true}}
			if reuse {
				records = append(records, record{Worktree: wanted, PID: 12346, Port: firstPort + 1})
			}
			mustWriteMap(t, m.mapPath, records)
			m.alive = func(_ context.Context, r record) probeVerdict {
				if r.Terminating {
					return probeUncertain
				}
				return probeLive
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := m.prepareAdmission(ctx, wanted)
			require.NoError(t, err)
		})
	}
}

func TestLogTrimPreservesInheritedAppendDescriptor(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.limits.LogBytes = 8
	path := m.logPath("/retired-worktree")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	writer, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })
	_, err = writer.WriteString("old oversized diagnostics")
	require.NoError(t, err)
	before, err := writer.Stat()
	require.NoError(t, err)
	usage, err := m.logs(t.Context(), false)
	require.Truef(t, err == nil && usage.Files == 1 && usage.Oversized == 1 && usage.Cleared == 0 && usage.Bytes == before.Size(), "observe = %+v, %v", usage, err)
	usage, err = m.logs(t.Context(), true)
	require.Truef(t, err == nil && usage.Cleared == 1 && usage.ClearedBytes == before.Size(), "trim = %+v, %v", usage, err)
	_, err = writer.WriteString("new")
	require.NoError(t, err)
	content, err := os.ReadFile(path)
	require.Truef(t, err == nil && string(content) == "new", "append after trim = %q, %v", content, err)
	after, err := os.Stat(path)
	require.Truef(t, err == nil && os.SameFile(before, after), "trim replaced inode: %v", err)
	usage, err = m.logs(t.Context(), true)
	require.Truef(t, err == nil && usage.Cleared == 0 && usage.Bytes == 3, "small file trimmed: %+v, %v", usage, err)
}

func TestLogTrimIgnoresUnrelatedFilesAndSymlinks(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.limits.LogBytes = 1
	dir := filepath.Dir(m.logPath(""))
	require.NoError(t, os.MkdirAll(dir, 0o700))
	path := filepath.Join(dir, "unrelated.log")
	require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
	link := m.logPath("/symlink")
	require.NoError(t, os.Symlink(path, link))
	usage, err := m.logs(t.Context(), true)
	require.Truef(t, err == nil && usage.Files == 0, "unexpected managed logs: %+v, %v", usage, err)
	_, err = clearOversizedLog(link, 1)
	require.Error(t, err, "trimmer followed symlink")
	data, err := os.ReadFile(path)
	require.Truef(t, err == nil && string(data) == "keep", "unrelated file changed: %q, %v", data, err)
}

func TestRunTrimLogs(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var out bytes.Buffer
	require.NoError(t, run([]string{"manager", "trim-logs"}, &out))
	var usage logUsage
	require.NoError(t, json.Unmarshal(out.Bytes(), &usage))
	require.Truef(t, usage.Files == 0 && strings.Contains(usage.Policy, "explicit"), "empty trim: %+v", usage)
	err := run([]string{"manager", "trim-logs", "extra"}, &out)
	require.Truef(t, err != nil && strings.HasPrefix(err.Error(), "usage:"), "invalid args: %v", err)
}
