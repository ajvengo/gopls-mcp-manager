package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const (
	firstPort = 61100
	lastPort  = 65100
	portCount = lastPort - firstPort + 1
)

type manager struct {
	mapPath string
	alive   func(context.Context, record) probeVerdict // read-only; called concurrently
	observe func(lockTiming)                           // optional, concurrency-safe observer
	limits  callLimits
	// ready is awaitReady, replaced by tests: what it waits out is readyTimeout,
	// and ensure's failure path is otherwise ten seconds of sleeping away from
	// every assertion about it.
	ready func(context.Context, int) error
	start func(context.Context, string, int) (*childProcess, error)
}

// gopls is resolved by startGopls, not here: list's whole job is to find and
// reap servers whose 1-2GB indexes are stranded, and a gopls that has since
// been removed or renamed is exactly when that matters. Resolving up front made
// every command, list included, refuse to run. bridge and ensure still fail at
// startup on a broken install, because both reach startGopls through ensure.
func newManager() (*manager, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	m := &manager{
		mapPath: filepath.Join(home, ".local", "share", "gopls-ports.map"),
		alive:   recordAlive,
		ready:   awaitReady,
	}
	m.start = m.startGopls
	m.limits, err = limitsFromEnv()
	if err != nil {
		return nil, err
	}
	if os.Getenv("GOPLS_MANAGER_METRICS") == "1" {
		m.observe = func(t lockTiming) {
			fmt.Fprintf(os.Stderr, "{\"event\":\"registry_lock\",\"wait_ns\":%d,\"held_ns\":%d}\n", t.Wait, t.Held)
		}
	}
	return m, nil
}

func basePort(worktree string) int {
	sum := sha256.Sum256([]byte(worktree))
	return firstPort + int(binary.BigEndian.Uint64(sum[:8])%uint64(portCount))
}

func mcpAddress(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// mcpURL is the endpoint the prober and the dialer must agree on: the probe is
// what decides whether to SIGTERM a server, so a scheme or host that drifted
// between the two would condemn one that is answering the dialer perfectly.
func mcpURL(port int) string {
	return "http://" + mcpAddress(port)
}

func nextPort(port int) int {
	return firstPort + (port-firstPort+1)%portCount
}

func allocatePort(worktree string, records []record, unavailable func(int) bool) (int, error) {
	used := make(map[int]bool, len(records))
	for _, r := range records {
		used[r.Port] = true
	}
	port := basePort(worktree)
	for range portCount {
		if !used[port] && !unavailable(port) {
			return port, nil
		}
		port = nextPort(port)
	}
	return 0, errors.New("no free gopls MCP port")
}

func portUnavailable(port int) bool {
	listener, err := net.Listen("tcp", mcpAddress(port))
	if err != nil {
		return true
	}
	_ = listener.Close()
	return false
}

// ensure answers with the port worktree's gopls listens on, starting one if the
// map has none.
//
// The readiness wait runs with the map lock released. Under the lock it made
// every other gopls-mcp-manager process — and so every other worktree's next
// tool call — queue behind one cold start for up to readyTimeout, which is
// precisely the case worktree isolation produces. The record written before the
// lock is dropped is what makes that safe: it reserves the port, and startGrace
// keeps a concurrent sweep from reaping a server that has not bound yet.
func (m *manager) ensure(ctx context.Context, worktree string) (int, error) {
	claimed, started, err := m.claimPort(ctx, worktree)
	if err != nil {
		return 0, err
	}
	// P6. A record inside its start grace was vouched for by L7 without being
	// probed at all, so its port can still refuse: the gopls it names was forked
	// by somebody who has not seen it bind yet. Waited for outside the flock (P4).
	//
	// Our own start is waited for on the fact rather than the clock: the grace
	// would almost always cover it too, but "almost" would make the cleanup
	// below depend on how long the sweep's probes took, and a slow one would
	// silently skip both the wait and the SIGTERM that follows it.
	if started == nil && !withinStartGrace(claimed) {
		return claimed.Port, nil
	}
	if err := m.ready(ctx, claimed.Port); err != nil {
		// Wrapped here rather than in awaitReady, which knows only a port: the
		// worktree is in hand here, and the success path still does not hash it
		// for a message nobody reads.
		err = fmt.Errorf("%w; see %s", err, m.logPath(worktree))
		if started == nil {
			// Somebody else's start, and its failure belongs to them: that
			// process signals and forgets. This only reports it.
			return 0, err
		}
		// Cleanup survives request cancellation and completes before the CLI
		// can exit. If exit cannot be confirmed, keep the record discoverable.
		if stopErr := started.stop(); stopErr != nil {
			return 0, errors.Join(err, stopErr)
		}
		cleanup, cancel := context.WithTimeout(context.Background(), exitGrace)
		defer cancel()
		return 0, errors.Join(err, m.forget(cleanup, claimed))
	}
	return claimed.Port, nil
}

// claimPort reserves worktree's record under the map lock, spawning a gopls
// when the map has none. A process comes back only when this call is what
// started it, which is how ensure tells its own readiness wait from the one it
// may still owe another process's start.
func (m *manager) claimPort(ctx context.Context, worktree string) (record, *childProcess, error) {
	var claimed record
	var started *childProcess
	_, err := m.withRecords(ctx, func(records []record) ([]record, error) {
		for _, r := range records {
			if r.Worktree == worktree {
				if r.Terminating {
					return nil, fmt.Errorf("gopls pid %d is terminating; record retained until exit", r.PID)
				}
				claimed = r
				return records, nil
			}
		}
		port, err := allocatePort(worktree, records, portUnavailable)
		if err != nil {
			return nil, err
		}
		started, err = m.start(ctx, worktree, port)
		if err != nil {
			return nil, err
		}
		claimed = record{
			Worktree:  worktree,
			Port:      port,
			PID:       started.Pid,
			StartedAt: time.Now().Unix(),
		}
		return append(records, claimed), nil
	})
	if err != nil {
		if started != nil {
			// The write is the only step that can fail past the spawn, and it
			// left the record unrecorded — so nothing would ever find it again.
			err = errors.Join(err, started.stop())
		}
		return record{}, nil, err
	}
	return claimed, started, nil
}

// forget drops started's own record. Only ensure calls it, for the gopls it
// just started and that never came up.
//
// Matched on the whole record rather than on worktree and port, because those
// two do not name a gopls: allocatePort is deterministic in the worktree, so
// the port a failed start held is the very port the next start is handed. A
// process delayed on the flock past its grace can arrive here after another
// one reaped its record, took the same port and recorded a live gopls of its
// own — and a match on the pair alone would delete that one.
// Not routed through withRecords, which sweeps: this drops exactly the record
// named and nothing else, on an error path that has already spent readyTimeout
// and should not also probe every other worktree under the lock.
func (m *manager) forget(ctx context.Context, started record) error {
	_, err := m.withMap(ctx, func(records []record) ([]record, error) {
		return slices.DeleteFunc(records, func(r record) bool { return r == started }), nil
	})
	return err
}

// list prints the surviving records. Printed after the lock is dropped, so a
// client that stopped reading cannot hold it, and so that the sweep's result is
// already on disk whatever stdout does.
func (m *manager) list(ctx context.Context, w io.Writer) error {
	records, err := m.withRecords(ctx, func(records []record) ([]record, error) { return records, nil })
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "PORT\tPID\tWORKTREE"); err != nil {
		return err
	}
	for _, r := range records {
		if _, err := fmt.Fprintf(w, "%d\t%d\t%s\n", r.Port, r.PID, r.Worktree); err != nil {
			return err
		}
	}
	return nil
}
