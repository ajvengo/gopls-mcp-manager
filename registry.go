package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"
)

const (
	firstPort = 61100
	lastPort  = 65100
	portCount = lastPort - firstPort + 1
)

type manager struct {
	diagnostics io.Writer
	ownerLogs   *ownerLog
	mapPath     string
	observe     func(lockTiming)
	measure     func(string, time.Duration)
	limits      config.Limits
	openFiles   func(context.Context, []record) (map[int]int, error)
	watch       *watchPlans
}

func (m *manager) diagnosticWriter() io.Writer {
	if m != nil && m.diagnostics != nil {
		return m.diagnostics
	}
	return os.Stderr
}

func newManager() (*manager, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	m := &manager{mapPath: filepath.Join(home, ".local", "share", "gopls-ports.map"), openFiles: countOpenFiles, watch: &watchPlans{plans: make(map[string]watchSnapshot)}}

	m.limits, err = config.FromEnv()
	if err != nil {
		return nil, err
	}
	if m.limits.Metrics {
		m.measure = func(stage string, elapsed time.Duration) {
			_, _ = fmt.Fprintf(m.diagnosticWriter(), "{\"event\":\"manager_operation\",\"stage\":%q,\"elapsed_ns\":%d}\n", stage, elapsed.Nanoseconds())
		}
		m.observe = func(t lockTiming) {
			_, _ = fmt.Fprintf(m.diagnosticWriter(), "{\"event\":\"registry_lock\",\"wait_ns\":%d,\"held_ns\":%d}\n", t.Wait, t.Held)
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

func (m *manager) timed(stage string, start time.Time) {
	if m.measure != nil {
		m.measure(stage, time.Since(start))
	}
}

func (m *manager) list(ctx context.Context, w io.Writer) error {
	records, _, err := readMap(m.mapPath)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return errors.New("process reservations require the owner or explicit offline legacy cleanup; status remains read-only")
	}
	return writeRecords(w, records)
}

func writeRecords(w io.Writer, records []record) error {
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

type record struct {
	Worktree    string
	Port        int
	PID         int
	OwnerBoot   string `json:",omitzero"`
	Generation  string `json:",omitzero"`
	GuardianPID int    `json:",omitzero"`
	// Persist seconds and milliseconds together for watch snapshot comparison.
	StartedAt int64
	// StartedAtMilli is the same instant in unix milliseconds, for comparing
	// the start with a file's mtime (see startedPrecisely). Zero in a map
	// written by a manager that predates it; that reader then falls back to
	// StartedAt.
	StartedAtMilli int64 `json:",omitzero"`
	Terminating    bool  `json:",omitzero"`
	// Retain legacy JSON fields without using them for admission or eviction.
	Uses   float64 `json:",omitzero"`
	UsedAt int64   `json:",omitzero"`
}

// startedPrecisely is when r's gopls was spawned, to the millisecond, and false
// for a record that cannot say. StartedAtMilli is trusted only when it agrees
// with StartedAt: the map is meant to be editable by hand, and a milli that
// contradicts the seconds beside it is a typo, not a better clock. A map
// rewritten by an older manager carries no milli at all, so the seconds stand
// in. A zero StartedAt is no start at all.
func (r record) startedPrecisely() (time.Time, bool) {
	if r.StartedAt == 0 {
		return time.Time{}, false
	}
	if r.StartedAtMilli != 0 && r.StartedAtMilli/1000 == r.StartedAt {
		return time.UnixMilli(r.StartedAtMilli), true
	}
	return time.Unix(r.StartedAt, 0), true
}

// serverID names one gopls process: its pid and the unix second it started. A
// pid alone is reused; the start tells a restart on the same pid from the same
// server. Whole seconds, not StartedAtMilli: an older manager rewriting the map
// drops the milli, and an identity that changed under it would orphan the
// server's snapshot. The zero value is "unknown", which is what a connection
// that did not come from ensure — a test's stub — carries.
type serverID struct {
	pid     int
	started int64
}

func (id serverID) known() bool { return id.pid != 0 }

// server is the identity of the gopls r describes, unknown when r has no start.
func (r record) server() serverID {
	if r.StartedAt == 0 {
		return serverID{}
	}
	return serverID{pid: r.PID, started: r.StartedAt}
}

const (
	maxRegistryBytes = 4 << 20
	maxProcessID     = 1<<31 - 1 // POSIX pid_t is signed; never allow integer truncation into a group signal.
)

// Blank formatting may be normalised; malformed nonblank state is never
// exposed as a partial mutable snapshot.
// A file that is not there yet is intact: it already says what no records say.
func readMap(path string) (records []record, intact bool, err error) {
	f, err := openStateFile(path, maxRegistryBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = f.Close() }()

	// Unknown/corrupt nonblank records may still reserve a live process. Refuse
	// the whole observation rather than silently rewrite away its only identity.
	// Explicit operator repair is required; blank formatting is harmless.
	//
	// The fields are checked, not just decoded, because this file is meant to be
	// editable by hand and every one of them is an argument to kill(2) or to a
	// probe that decides on a kill: pid 0 signals this process's own group, a
	// negative pid signals every process the user owns, and a port outside the
	// range we allocate from can only refuse a probe and condemn a live server.
	var lines int
	// Include invalid/blank lines in the count: skipped lines must not bypass
	// the aggregate work bound. Keep a byte ceiling too, including concurrent
	// growth after Stat. A failed read returns no partial registry to mutate.
	limited := &io.LimitedReader{R: f, N: maxRegistryBytes + 1}
	scanner := bufio.NewScanner(limited)
	for scanner.Scan() {
		lines++
		if lines > portCount {
			return nil, false, fmt.Errorf("read %s: registry exceeds %d lines", path, portCount)
		}
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		var r record
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			return nil, false, fmt.Errorf("read %s: invalid registry record on line %d; explicit repair required", path, lines)
		}
		if r.Worktree == "" || r.PID <= 0 || r.PID > maxProcessID || r.Port < firstPort || r.Port > lastPort {
			return nil, false, fmt.Errorf("read %s: invalid registry fields on line %d; explicit repair required", path, lines)
		}
		records = append(records, r)
	}
	if err := scanner.Err(); err != nil {
		return nil, false, fmt.Errorf("read %s: %w", path, err)
	}
	if limited.N == 0 {
		return nil, false, fmt.Errorf("read %s: registry exceeds %d bytes", path, maxRegistryBytes)
	}
	return records, len(records) == lines, nil
}

func writeMap(path string, records []record) error {
	if len(records) > portCount {
		return fmt.Errorf("write %s: registry exceeds %d records", path, portCount)
	}
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
	w := bufio.NewWriter(&stateBudgetWriter{Writer: tmp, remaining: maxRegistryBytes})
	for _, r := range records {
		if r.Worktree == "" || r.PID <= 0 || r.PID > maxProcessID || r.Port < firstPort || r.Port > lastPort {
			return fmt.Errorf("write %s: invalid registry fields", path)
		}
		// A Unix path is arbitrary bytes, but JSON strings require UTF-8.
		// Refuse an unrepresentable path with a worktree-specific error:
		// read back, it would never match its own worktree, so every ensure
		// would start another gopls beside the last and forget would never find
		// the record to drop.
		if !utf8.ValidString(r.Worktree) {
			return fmt.Errorf("worktree path is not valid UTF-8: %q", r.Worktree)
		}
		data, err := json.Marshal(r)
		if err != nil {
			return err
		}
		// Scanner must hold the full value and its newline simultaneously.
		// Enforce its token boundary before committing a replacement, too.
		if len(data) >= bufio.MaxScanTokenSize {
			return fmt.Errorf("write %s: registry record exceeds %d bytes", path, bufio.MaxScanTokenSize-1)
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
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

// Clone transaction input so in-place edits cannot hide a required write.
// Unchanged records skip replacement only when the source file was intact.
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

// Open before inspecting the descriptor: Lstat followed by Open would permit a
// file replacement to turn the read into a FIFO/device open. Nonblocking open
// and O_NOFOLLOW refuse that race, including a symlink to an otherwise regular
// file. Private state is writable by cooperating managers, never a symlink API.
func openStateFile(path string, maxBytes int64) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err == nil && (!info.Mode().IsRegular() || info.Size() > maxBytes) {
		err = fmt.Errorf("state file must be regular and at most %d bytes", maxBytes)
	}
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return f, nil
}

// Reject an oversized replacement before it reaches the atomic rename. A
// buffered encoder may otherwise produce state the bounded reader cannot read.
type stateBudgetWriter struct {
	io.Writer
	remaining int
}

func (w *stateBudgetWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, fmt.Errorf("state replacement exceeds byte limit")
	}
	n, err := w.Writer.Write(p)
	w.remaining -= n
	return n, err
}
