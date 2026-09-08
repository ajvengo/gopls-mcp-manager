package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

type record struct {
	Worktree string
	Port     int
	PID      int
	// StartedAt is when this tool spawned the gopls, in unix seconds, and marks
	// the record unreapable until startGrace is up; see recordAlive. Zero in a
	// map written before the field existed, which reads as "no grace" — the
	// behaviour those records already had.
	StartedAt   int64
	Terminating bool `json:",omitempty"`
}

// readMap returns the records the file holds, and whether it holds nothing
// else. A file that is not intact has lines the read dropped, so it needs
// rewriting whatever the caller then does with the records — see withRecords.
// A file that is not there yet is intact: it already says what no records say.
func readMap(path string) (records []record, intact bool, err error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = f.Close() }()

	// A line we cannot parse names a gopls we cannot manage anyway, so it is
	// dropped rather than failed on: every entry point reads this file, so failing
	// on one bad line would leave no way to inspect or repair it from the tool
	// itself. The next write rewrites the file without it.
	//
	// The fields are checked, not just decoded, because this file is meant to be
	// editable by hand and every one of them is an argument to kill(2) or to a
	// probe that decides on a kill: pid 0 signals this process's own group, a
	// negative pid signals every process the user owns, and a port outside the
	// range we allocate from can only refuse a probe and condemn a live server.
	var lines int
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines++
		var r record
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			continue
		}
		if r.Worktree == "" || r.PID <= 0 || r.Port < firstPort || r.Port > lastPort {
			continue
		}
		records = append(records, r)
	}
	if err := scanner.Err(); err != nil {
		return nil, false, fmt.Errorf("read %s: %w", path, err)
	}
	return records, len(records) == lines, nil
}

func writeMap(path string, records []record) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".gopls-ports.map-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	// CreateTemp asks for 0600 but the kernel still applies the umask, and this
	// file's mode is asserted, not merely hoped for.
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	// bufio.Writer latches its first write error and returns it from Flush, so
	// that one check covers every line below.
	w := bufio.NewWriter(tmp)
	enc := json.NewEncoder(w)
	for _, r := range records {
		// A Unix path is arbitrary bytes, but json.Marshal replaces invalid
		// UTF-8 with U+FFFD instead of failing — so such a record would come
		// back naming a worktree nobody asked for. Refused rather than written:
		// read back, it would never match its own worktree, so every ensure
		// would start another gopls beside the last and forget would never find
		// the record to drop.
		if !utf8.ValidString(r.Worktree) {
			return fmt.Errorf("worktree path is not valid UTF-8: %q", r.Worktree)
		}
		_ = enc.Encode(r)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func withFileLock(ctx context.Context, path string, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			return err
		}
		if err := waitContext(ctx, 10*time.Millisecond); err != nil {
			return err
		}
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn()
}

// withRecords probes an atomic snapshot outside the lock and reconciles only
// unchanged records under it. Ordinary updates share one commit with the body.
// Termination is committed separately so a failed body cannot erase the evidence
// required before a signal. The caller must not return a terminating endpoint.
func (m *manager) withRecords(ctx context.Context, body func([]record) ([]record, error)) ([]record, error) {
	return m.withSelectedRecords(ctx, "", body)
}

// An empty worktree requests explicit whole-registry maintenance. Acquisition
// observes only its own worktree; unrelated records still reserve their ports.
func (m *manager) withSelectedRecords(ctx context.Context, worktree string, body func([]record) ([]record, error)) ([]record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Atomic rename makes an unlocked snapshot coherent. Probe it without
	// making another process's unrelated ensure wait for network timeouts.
	snapshot, _, err := readMap(m.mapPath)
	if err != nil {
		return nil, err
	}
	verdicts := make([]probeVerdict, len(snapshot))
	probeStart := time.Now()
	var probes sync.WaitGroup
	for i, r := range snapshot {
		if worktree != "" && r.Worktree != worktree {
			continue
		}
		probes.Go(func() { verdicts[i] = m.alive(ctx, r) })
	}
	probes.Wait()
	m.timed("probes", probeStart)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	byRecord := make(map[record]probeVerdict, len(snapshot))
	for i, r := range snapshot {
		byRecord[r] = verdicts[i]
	}
	var terminating []record
	// Commit cleanup independently of the caller's mutation. A failed spawn
	// must not undo the durable evidence required before a signal.
	updated, err := m.withMap(ctx, func(current []record) ([]record, error) {
		kept := current[:0]
		for _, r := range current {
			switch byRecord[r] { // absent/replaced records are uncertain
			case probeGone:
				continue
			case probeTerminate:
				r.Terminating = true
				terminating = append(terminating, r)
			}
			kept = append(kept, r)
		}
		if len(terminating) != 0 {
			return kept, nil
		}
		return body(kept)
	})
	if err != nil {
		return nil, err
	}
	if len(terminating) == 0 {
		return updated, nil
	}
	for _, r := range terminating {
		if err := m.signalTerminating(ctx, r); err != nil {
			return nil, err
		}
	}
	return m.withMap(ctx, body)
}

type lockTiming struct{ Wait, Held time.Duration }

func (m *manager) locked(ctx context.Context, fn func() error) error {
	start := time.Now()
	var acquired time.Time
	err := withFileLock(ctx, m.mapPath, func() error {
		acquired = time.Now()
		return fn()
	})
	if m.observe != nil {
		timing := lockTiming{Wait: time.Since(start)}
		if !acquired.IsZero() {
			timing = lockTiming{Wait: acquired.Sub(start), Held: time.Since(acquired)}
		}
		m.observe(timing)
	}
	return err
}

// withMap runs body under the map's flock, over what the file holds, and writes
// back what it returns. The sweep is withRecords' addition, not this one's:
// forget goes through here to drop one record without probing every other
// worktree under the lock.
//
// Skipped entirely when body hands back what it was given, which is the steady
// state: the write is a temp file, an fsync and a rename inside a lock every
// process on the machine shares — some forty times the rest of this function
// and up, the ratio moving with how busy the disk is (BenchmarkWithRecords).
// The intact flag is the other half of the question: the lines readMap refused
// are gone from stored already, so equal records do not mean an equal file, and
// only readMap can say whether the repair is still owed.
//
// body is handed its own copy, because what it is given is also what its result
// is compared against: a body editing in place — slices.DeleteFunc does — would
// otherwise compare its result against itself, find no change, and leave the map
// saying what it said before. Cloning here rather than asking every body to is
// what keeps that from being a rule each caller has to know and none can test.
func (m *manager) withMap(ctx context.Context, body func([]record) ([]record, error)) ([]record, error) {
	var updated []record
	err := m.locked(ctx, func() error {
		stored, intact, err := readMap(m.mapPath)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if updated, err = body(slices.Clone(stored)); err != nil {
			return err
		}
		if intact && slices.Equal(stored, updated) {
			return nil
		}
		return writeMap(m.mapPath, updated)
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}
