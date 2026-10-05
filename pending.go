package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// pendingFresh is how long a pending upstream operation keeps its server from
// eviction. Older ones are as likely to be lost connections, whose credits
// never resolve (see operation), as work gopls is still doing.
const pendingFresh = 30 * time.Second

// pendingCalls mirrors the routers' upstream operations with their start
// times, so eviction can spare a server gopls is still answering. Other
// managers learn of them through publishPending's file, since only this
// process can see its routers.
type pendingCalls struct {
	mu    sync.Mutex
	ops   map[operationKey]pendingCall
	dirty chan struct{} // one slot: a publish owed
}

type pendingCall struct {
	worktree string
	started  time.Time
}

func newPendingCalls() *pendingCalls {
	return &pendingCalls{ops: make(map[operationKey]pendingCall), dirty: make(chan struct{}, 1)}
}

// beginPending and endPending follow beginOperation and endOperationLocked,
// under the router's mu; pendingCalls never takes that lock back. Nil-safe:
// routers built in tests have no manager, or one without pending. The clock is
// read here, not taken from router.now, which stays zero without metrics.
func (m *manager) beginPending(key operationKey, worktree string) {
	if m == nil || m.pending == nil {
		return
	}
	started := time.Now()
	m.pending.update(func(ops map[operationKey]pendingCall) { ops[key] = pendingCall{worktree, started} })
}

func (m *manager) endPending(key operationKey) {
	if m == nil || m.pending == nil {
		return
	}
	m.pending.update(func(ops map[operationKey]pendingCall) { delete(ops, key) })
}

func (p *pendingCalls) update(change func(map[operationKey]pendingCall)) {
	p.mu.Lock()
	change(p.ops)
	p.mu.Unlock()
	select {
	case p.dirty <- struct{}{}:
	default:
	}
}

// newest is the start of the latest pending operation per worktree.
func (p *pendingCalls) newest() map[string]time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	newest := make(map[string]time.Time)
	for _, op := range p.ops {
		if op.started.After(newest[op.worktree]) {
			newest[op.worktree] = op.started
		}
	}
	return newest
}

func (m *manager) pendingDir() string {
	return filepath.Join(filepath.Dir(m.mapPath), "gopls-mcp-pending")
}

// pendingPath is the file the manager running as pid publishes to.
func (m *manager) pendingPath(pid int) string {
	return filepath.Join(m.pendingDir(), strconv.Itoa(pid))
}

// publishPending keeps this process's file in step with its pending
// operations until ctx ends, then removes it. Writes are coalesced off the
// routers' hot path, so a reader can lag a burst by one write.
func (m *manager) publishPending(ctx context.Context) {
	path := m.pendingPath(os.Getpid())
	defer func() { _ = os.Remove(path) }()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.pending.dirty:
			if err := writePending(path, m.pending.newest()); err != nil && ctx.Err() == nil {
				fmt.Fprintln(os.Stderr, "gopls-mcp-manager: publish pending calls:", err)
			}
		}
	}
}

// writePending replaces path with worktree -> newest start in unix
// milliseconds, or removes it when nothing is pending. Renamed into place so a
// reader never sees half a file; not synced, since it means nothing after a
// crash anyway.
func writePending(path string, newest map[string]time.Time) error {
	if len(newest) == 0 {
		if err := os.Remove(path); !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	millis := make(map[string]int64, len(newest))
	for worktree, started := range newest {
		millis[worktree] = started.UnixMilli()
	}
	data, _ := json.Marshal(millis) // strings and integers cannot fail
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".pending-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	_, err = tmp.Write(data)
	if err = errors.Join(err, tmp.Close()); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// busyWorktrees names the worktrees on which any manager has an upstream
// operation pending that started less than pendingFresh ago. Entries carry
// their own start times, so a crashed manager's file stops protecting anything
// within pendingFresh; it is removed once its pid is gone. A future start
// protects nothing, like a future StartedAt. Unparsable files are skipped.
func (m *manager) busyWorktrees(now time.Time) (map[string]bool, error) {
	busy := make(map[string]bool)
	mark := func(worktree string, started time.Time) {
		if age := now.Sub(started); age >= 0 && age < pendingFresh {
			busy[worktree] = true
		}
	}
	if m.pending != nil {
		for worktree, started := range m.pending.newest() {
			mark(worktree, started)
		}
	}
	entries, err := os.ReadDir(m.pendingDir())
	if errors.Is(err, os.ErrNotExist) {
		return busy, nil
	}
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 || pid == os.Getpid() {
			continue
		}
		path := m.pendingPath(pid)
		if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
			_ = os.Remove(path)
			continue
		}
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue // its owner found nothing pending since the listing
		}
		if err != nil {
			return nil, err
		}
		var millis map[string]int64
		if json.Unmarshal(data, &millis) != nil {
			continue
		}
		for worktree, started := range millis {
			mark(worktree, time.UnixMilli(started))
		}
	}
	return busy, nil
}
