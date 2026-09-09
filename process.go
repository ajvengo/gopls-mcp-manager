package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"time"
)

// goplsBinary is both what we spawn and what isOurGopls looks for in a command
// line. Named once because those two have to agree: a rename that reached only
// the spawn would leave every identity check failing, and a failing check does
// not error — it silently declines to kill a server this tool started, stranding
// its index and its port for the life of the machine.
const goplsBinary = "gopls"

// readyTimeout bounds the wait for a freshly spawned gopls to serve its
// endpoint. startGrace is the window a sweep spares that record for. Derived
// rather than written out, because it has to outlast the wait: the two are
// spent in different processes, so a start still inside its own budget must not
// be reapable by anyone else's sweep, and an edit to one number alone would
// reopen exactly the window the grace exists to close.
const (
	readyTimeout = 10 * time.Second
	startGrace   = readyTimeout + 5*time.Second
	// exitGrace bounds how long a gopls that failed to become ready is given to
	// honour its SIGTERM before it is killed outright. Short, because nothing is
	// waiting on it: the server never served, so it has no session to unwind and
	// no index worth flushing.
	exitGrace = 2 * time.Second
)

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

// A per-record action lock serializes signal attempts across managers. The
// atomic registry snapshot and process identity are checked again immediately
// before signalling, outside the global lock. PID identity still has POSIX's
// check/signal race. Cleanup lock files stay in place to preserve flock identity.
func (m *manager) signalTerminating(ctx context.Context, r record) error {
	return withFileLock(ctx, fmt.Sprintf("%s.cleanup-%d-%d", m.mapPath, r.PID, r.Port), func() error {
		current, _, err := readMap(m.mapPath)
		if err != nil || !slices.Contains(current, r) {
			return err
		}
		ours, err := isOurGopls(ctx, r.PID, r.Port)
		if err != nil || ctx.Err() != nil || !ours {
			return nil
		}
		if err := syscall.Kill(r.PID, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("terminate gopls pid %d (record retained): %w", r.PID, err)
		}
		return nil // the next sweep confirms exit; never forget on signal alone
	})
}

func (m *manager) logPath(worktree string) string {
	hash := sha256.Sum256([]byte(worktree))
	return filepath.Join(filepath.Dir(m.mapPath), "gopls-mcp-logs", fmt.Sprintf("%x.log", hash[:8]))
}

// startGopls spawns a gopls for worktree and returns as soon as the child
// exists. Whether it ever serves its endpoint is awaitReady's question, and the
// caller's to ask outside the map lock — see ensure.
func (m *manager) startGopls(ctx context.Context, worktree string, port int) (*childProcess, error) {
	logPath := m.logPath(worktree)
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() { _ = logFile.Close() }()

	// exec.Command stashes a failed PATH lookup in cmd.Err and Start returns it,
	// so a missing gopls arrives here as "start gopls: ... not found in $PATH".
	cmd := exec.Command(goplsBinary, "mcp", "-listen", mcpAddress(port))
	cmd.Dir = worktree
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	// Do not tie the child lifetime to ctx: other clients share this server.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start gopls: %w", err)
	}
	// Reap the child rather than releasing it. Setsid does not reparent, so an
	// unwaited gopls stays a zombie child of this process for the whole session
	// — and kill(pid, 0) succeeds against a zombie, which would make the PID
	// half of recordAlive report a dead server as alive.
	child := &childProcess{Process: cmd.Process, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(child.done)
	}()
	return child, nil
}

// childProcess retains both identity and proof of exit for our own starts.
type childProcess struct {
	*os.Process
	done chan struct{}
}

func (p *childProcess) stop() error {
	select {
	case <-p.done:
		return nil
	default:
	}
	_ = p.Signal(syscall.SIGTERM)
	timer := time.NewTimer(exitGrace)
	defer timer.Stop()
	select {
	case <-p.done:
		return nil
	case <-timer.C:
	}
	if err := p.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("kill gopls pid %d: %w", p.Pid, err)
	}
	timer.Reset(exitGrace)
	select {
	case <-p.done:
		return nil
	case <-timer.C:
		return fmt.Errorf("gopls pid %d has not exited after SIGKILL", p.Pid)
	}
}

// awaitReady polls until the gopls just spawned on port serves its endpoint.
// The caller signals the process it holds when this gives up, rather than the
// recorded pid — see isOurGopls for why a bare pid is not enough.
func awaitReady(ctx context.Context, port int) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	// Backed off rather than polled flat: a gopls that binds 15ms after the fork
	// is not noticed for the rest of the tick, and this wait is on the lane's
	// goroutine with the send budget running. Doubling to a 100ms ceiling costs
	// a handful of extra probes on a slow start and nothing on a failed one.
	for wait := 10 * time.Millisecond; ctx.Err() == nil; wait = min(2*wait, 100*time.Millisecond) {
		if alive, _ := endpointProbe(ctx, port); alive {
			return nil
		}
		if err := waitContext(ctx, wait); err != nil {
			break
		}
	}
	return fmt.Errorf("gopls did not become ready on %s: %w", mcpAddress(port), ctx.Err())
}
