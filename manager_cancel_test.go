package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureCancelledAtLockDoesNotSpawn(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.start = func(context.Context, string, int) (*childProcess, error) {
		assert.Fail(t, "cancelled lock waiter spawned a server")
		return nil, errors.New("unexpected spawn")
	}
	err := withFileLock(t.Context(), m.mapPath, func() error {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancel()
		_, err := m.ensure(ctx, t.TempDir())
		return err
	})
	require.ErrorIsf(t, err, context.DeadlineExceeded, "lock waiter returned %v, want deadline exceeded", err)
	wantRecords(t, m.mapPath, "cancelled waiter wrote a record")
}

func TestCancelledSweepPreservesRecords(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	r := record{Worktree: "/repo/shared", PID: os.Getpid(), Port: firstPort}
	mustWriteMap(t, m.mapPath, []record{r})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	m.alive = func(ctx context.Context, _ record) probeVerdict {
		cancel()
		<-ctx.Done()
		return probeGone
	}
	_, err := m.withRecords(ctx, func([]record) ([]record, error) {
		assert.Fail(t, "cancelled sweep entered the mutation")
		return nil, nil
	})
	require.ErrorIsf(t, err, context.Canceled, "sweep returned %v, want cancellation", err)
	wantRecords(t, m.mapPath, "cancelled probe deleted a shared record", r)
}

func TestCancelledReadinessLeavesAnotherProcessesServerAlone(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	cmd := startFakeGopls(t, firstPort)
	r := record{Worktree: "/repo/shared", PID: cmd.Process.Pid, Port: firstPort, StartedAt: time.Now().Unix()}
	mustWriteMap(t, m.mapPath, []record{r})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	m.ready = func(ctx context.Context, _ int) error {
		cancel()
		return ctx.Err()
	}
	_, err := m.ensure(ctx, r.Worktree)
	require.ErrorIsf(t, err, context.Canceled, "readiness returned %v, want cancellation", err)
	wantRunning(t, cmd, "a waiter killed another process's server")
	wantRecords(t, m.mapPath, "a waiter forgot another process's server", r)
}

func TestReadinessProbeHonoursCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := awaitReady(ctx, silentPort(t))
	require.ErrorIsf(t, err, context.DeadlineExceeded, "readiness returned %v, want deadline exceeded", err)
	elapsed := time.Since(start)
	require.Lessf(t, elapsed, time.Second, "cancelled readiness took %s", elapsed)
}

// PATH is process-wide. The marker ensures SIGTERM is ignored before the
// manager gets the child handle, so these cover escalation without a spawn race.
func TestFailedChildIsReapedBeforeReturn(t *testing.T) {
	for _, failure := range []string{"readiness", "map write", "cancelled readiness", "session shutdown"} {
		t.Run(failure, func(t *testing.T) {
			m := newTestManager(t)
			worktree := t.TempDir()
			bin := t.TempDir()
			stub := filepath.Join(bin, goplsBinary)
			err := os.WriteFile(stub, []byte("#!/bin/sh\ntrap '' TERM\necho ready > ready\nexec sleep 30\n"), 0o700)
			require.NoError(t, err)
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			var child *childProcess
			m.start = func(ctx context.Context, dir string, port int) (*childProcess, error) {
				var err error
				child, err = m.startGopls(ctx, dir, port)
				if err != nil {
					return nil, err
				}
				t.Cleanup(func() { _ = child.Kill() })
				deadline := time.Now().Add(5 * time.Second)
				for {
					if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
						break
					}
					if time.Now().After(deadline) {
						return child, errors.New("child did not install signal handler")
					}
					time.Sleep(time.Millisecond)
				}
				if failure == "map write" {
					// readMap already ran; make the atomic rename fail after spawn.
					if err := os.Mkdir(m.mapPath, 0o700); err != nil {
						return child, err
					}
				}
				return child, nil
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			m.ready = func(context.Context, int) error {
				if failure == "cancelled readiness" {
					cancel()
					return ctx.Err()
				}
				return errors.New("never bound")
			}
			if failure == "session shutdown" {
				waiting := make(chan struct{})
				m.ready = func(ctx context.Context, _ int) error {
					close(waiting)
					<-ctx.Done()
					return ctx.Err()
				}
				client := newFakeConn()
				done := inBackground(func() error { return serve(ctx, &m, worktree, client) })
				client.reads <- &jsonrpc.Request{ID: mustID(t, "init"), Method: "initialize"}
				mustRecv(t, waiting, "readiness before session shutdown")
				cancel()
				err := mustRecv(t, done, "session to finish cleanup")
				require.NoError(t, err)
			} else {
				_, err := m.ensure(ctx, worktree)
				require.Error(t, err, "failed child returned a successful ensure")
			}
			require.NotNil(t, child, "failure did not reach the child cleanup path")
			select {
			case <-child.done:
			default:
				require.FailNow(t, "ensure returned before the child was reaped")
			}
			err = child.Signal(syscall.Signal(0))
			require.ErrorIsf(t, err, os.ErrProcessDone, "child is not reaped: %v", err)
			if failure != "map write" {
				wantRecords(t, m.mapPath, "failed child was not forgotten")
			}
		})
	}
}
