package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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
	if verdict := recordAlive(t.Context(), stale); verdict != probeLive {
		t.Fatalf("regression requires an answering server, got %v", verdict)
	}
	if err := m.prepareAdmission(t.Context(), "/new/worktree"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("stale process did not exit")
	}
	m.start = func(context.Context, string, int) (*childProcess, error) {
		return &childProcess{Process: &os.Process{Pid: os.Getpid()}, done: make(chan struct{})}, nil
	}
	wanted := t.TempDir()
	claimed, started, err := m.claimPort(t.Context(), wanted)
	if err != nil || started == nil || claimed.Worktree != wanted {
		t.Fatalf("new server denied after stale slot cleanup: %+v %v", claimed, err)
	}
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
		t.Fatal("idle maintenance never probed the stale server")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		records, _, err := readMap(m.mapPath)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stale record was not removed")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance did not stop on cancellation")
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
			if err := m.prepareAdmission(ctx, wanted); err != nil {
				t.Fatal(err)
			}
		})
	}
}
