package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	ownerLogQueueBytes   = 1 << 20
	ownerLogQueueJobs    = 128
	ownerLogMaxOwners    = 64
	ownerLogMaxFiles     = 4096
	ownerLogScanEntries  = 100_000
	ownerLogScanBatch    = 128
	ownerLogMaxPathBytes = 4096
	ownerLogInitTimeout  = 5 * time.Second
)

var (
	errOwnerLogClosed   = errors.New("owner log is closed")
	errOwnerLogReleased = errors.New("owner log sink is released")
	errOwnerLogQueue    = errors.New("owner log queue capacity reached")
	errOwnerLogLimit    = errors.New("owner log resource limit reached")
	errOwnerLogChanged  = errors.New("owner log storage changed outside its owner")
)

// A single disk worker survives helper/sink churn. A stuck filesystem call may
// retain this one worker and the bounded queue, never replacement workers.
var ownerLogIO = struct {
	sync.Mutex
	once         sync.Once
	jobs         chan ownerLogTask
	pendingJobs  int
	pendingBytes int
	owners       map[string]*ownerLog
}{jobs: make(chan ownerLogTask, ownerLogQueueJobs), owners: make(map[string]*ownerLog)}

type ownerLogTask struct {
	run   func()
	bytes int
}

func enqueueOwnerLog(bytes int, run func()) error {
	ownerLogIO.Lock()
	if ownerLogIO.pendingJobs >= ownerLogQueueJobs || bytes > ownerLogQueueBytes-ownerLogIO.pendingBytes {
		ownerLogIO.Unlock()
		return errOwnerLogQueue
	}
	ownerLogIO.pendingJobs++
	ownerLogIO.pendingBytes += bytes
	ownerLogIO.Unlock()
	ownerLogIO.once.Do(func() {
		go func() {
			for task := range ownerLogIO.jobs {
				task.run()
				// Drop captured payload/helper references before admitting their
				// replacement capacity, including when no further task arrives.
				task.run = nil
				ownerLogIO.Lock()
				ownerLogIO.pendingJobs--
				ownerLogIO.pendingBytes -= task.bytes
				ownerLogIO.Unlock()
			}
		}()
	})
	// Reservations count active, queued and not-yet-sent tasks, so there is
	// always a channel slot. Admission never waits for the disk worker.
	select {
	case ownerLogIO.jobs <- ownerLogTask{run: run, bytes: bytes}:
		return nil
	default:
		ownerLogIO.Lock()
		ownerLogIO.pendingJobs--
		ownerLogIO.pendingBytes -= bytes
		ownerLogIO.Unlock()
		return errOwnerLogQueue
	}
}

type ownerLogFile struct {
	name  string
	size  int64
	info  os.FileInfo
	order uint64
}

type ownerLogStorage interface {
	Scan(string) ([]ownerLogFile, error)
	Append(string, []byte, ownerLogFile) (int, os.FileInfo, error)
	Remove(string, ownerLogFile) error
}

type ownerLog struct {
	dir      string
	maxFiles int
	maxBytes int64
	storage  ownerLogStorage
	// directory is guarded by ownerLogIO and identifies the physical directory,
	// including ancestor-symlink or mount aliases of another registered path.
	directory os.FileInfo
	mu        sync.Mutex // never held across filesystem I/O
	close     sync.Once
	sinks     map[string]*ownerLogSink
	closed    bool
	failure   error
	failures  chan error
	stop      func() bool
	observed  ownerLogSnapshot
	// The process-global disk worker alone owns these fields.
	files    map[string]ownerLogFile
	bytes    int64
	sequence uint64
}

type ownerLogSink struct {
	owner    *ownerLog
	name     string
	released bool // owner.mu
	pending  int  // owner.mu
}

// Files/Bytes are last completed worker observations, not admission authority
// or a durability promise. PendingBytes includes an active write's payload.
type ownerLogSnapshot struct {
	Files              int
	Bytes              int64
	Sinks              int
	PendingBytes       int
	GlobalPendingBytes int
	PendingJobs        int
	Closed             bool
	Failure            string
}

func newOwnerLog(ctx context.Context, dir string, maxFiles int, maxBytes int64) (*ownerLog, error) {
	return newOwnerLogWithStorage(ctx, dir, maxFiles, maxBytes, ownerLogOSStorage{})
}

func newOwnerLogWithStorage(ctx context.Context, dir string, maxFiles int, maxBytes int64, storage ownerLogStorage) (*ownerLog, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(dir) > ownerLogMaxPathBytes || !filepath.IsAbs(dir) || maxFiles <= 0 || maxFiles > ownerLogMaxFiles || maxBytes <= 0 {
		return nil, fmt.Errorf("%w: invalid log directory or limits", errOwnerLogLimit)
	}
	dir = filepath.Clean(dir)
	l := &ownerLog{dir: dir, maxFiles: maxFiles, maxBytes: maxBytes, storage: storage,
		sinks: make(map[string]*ownerLogSink), failures: make(chan error, 1), files: make(map[string]ownerLogFile)}
	ownerLogIO.Lock()
	if ownerLogIO.owners[dir] != nil || len(ownerLogIO.owners) >= ownerLogMaxOwners {
		ownerLogIO.Unlock()
		return nil, fmt.Errorf("%w: directory already owned or owner count reached", errOwnerLogLimit)
	}
	ownerLogIO.owners[dir] = l
	ownerLogIO.Unlock()
	l.mu.Lock()
	l.stop = context.AfterFunc(ctx, l.Close)
	l.mu.Unlock()
	ready := make(chan error, 1)
	if err := enqueueOwnerLog(0, func() {
		err := l.initialize()
		if err != nil {
			l.fail(err)
		}
		ready <- err
	}); err != nil {
		l.Close()
		return nil, err
	}
	timer := time.NewTimer(ownerLogInitTimeout)
	defer timer.Stop()
	select {
	case err := <-ready:
		if err := ctx.Err(); err != nil {
			l.Close()
			return nil, err
		}
		if err != nil {
			l.Close()
			return nil, err
		}
		return l, nil
	case <-ctx.Done():
		l.Close()
		return nil, ctx.Err()
	case <-timer.C:
		l.Close()
		return nil, fmt.Errorf("owner log initialization: %w", context.DeadlineExceeded)
	}
}

func (l *ownerLog) initialize() error {
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return errOwnerLogClosed
	}
	files, err := l.storage.Scan(l.dir)
	if err != nil {
		return err
	}
	directory, err := os.Lstat(l.dir)
	if err != nil {
		return err
	}
	if !directory.IsDir() {
		return errOwnerLogChanged
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return errOwnerLogClosed
	}
	ownerLogIO.Lock()
	for _, other := range ownerLogIO.owners {
		if other != l && other.directory != nil && os.SameFile(directory, other.directory) {
			ownerLogIO.Unlock()
			l.mu.Unlock()
			return fmt.Errorf("%w: physical log directory already owned", errOwnerLogLimit)
		}
	}
	l.directory = directory
	ownerLogIO.Unlock()
	l.mu.Unlock()
	// Keep the newest files that fit. Ties use names, making startup retention
	// independent of the directory iteration order. Never read file contents.
	slices.SortFunc(files, func(a, b ownerLogFile) int {
		if order := b.info.ModTime().Compare(a.info.ModTime()); order != 0 {
			return order
		}
		return strings.Compare(a.name, b.name)
	})
	l.sequence = uint64(len(files))
	for i, file := range files {
		l.mu.Lock()
		closed := l.closed
		l.mu.Unlock()
		if closed {
			return errOwnerLogClosed
		}
		if len(l.files) >= l.maxFiles || file.size > l.maxBytes-l.bytes {
			if err := l.storage.Remove(filepath.Join(l.dir, file.name), file); err != nil {
				return err
			}
			continue
		}
		file.order = uint64(len(files) - i)
		l.files[file.name] = file
		l.bytes += file.size
	}
	l.publish()
	return nil
}

func managedOwnerLogName(name string) bool {
	if name == "manager.log" {
		return true
	}
	if len(name) != 20 || !strings.HasSuffix(name, ".log") {
		return false
	}
	for _, c := range name[:16] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (l *ownerLog) sinkName(path string) (string, error) {
	if len(path) > ownerLogMaxPathBytes || filepath.Clean(path) != path || filepath.Dir(path) != l.dir || !managedOwnerLogName(filepath.Base(path)) {
		return "", fmt.Errorf("%w: invalid managed log path", errOwnerLogLimit)
	}
	return filepath.Base(path), nil
}

func (l *ownerLog) Sink(path string) (io.Writer, error) {
	name, err := l.sinkName(path)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failure != nil {
		return nil, l.failure
	}
	if l.closed {
		return nil, errOwnerLogClosed
	}
	if l.sinks[name] != nil || len(l.sinks) >= l.maxFiles {
		return nil, fmt.Errorf("%w: sink already registered or sink count reached", errOwnerLogLimit)
	}
	sink := &ownerLogSink{owner: l, name: name}
	l.sinks[name] = sink
	return sink, nil
}

// Release is idempotent and does not wait for disk. Queued writes finish before
// its metadata is removed, so a stalled sink cannot be replaced without bound.
func (l *ownerLog) Release(path string) {
	name, err := l.sinkName(path)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if sink := l.sinks[name]; sink != nil {
		sink.released = true
		if sink.pending == 0 {
			delete(l.sinks, name)
		}
	}
}

func (s *ownerLogSink) Write(p []byte) (int, error) {
	l := s.owner
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failure != nil {
		return 0, l.failure
	}
	if l.closed {
		return 0, errOwnerLogClosed
	}
	if s.released || l.sinks[s.name] != s {
		return 0, errOwnerLogReleased
	}
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > l.maxBytes || len(p) > ownerLogQueueBytes {
		err := fmt.Errorf("%w: write exceeds log/queue byte ceiling", errOwnerLogLimit)
		l.failLocked(err)
		return 0, err
	}
	// Reservation happens before allocating the private payload copy. The
	// exact-length copy is charged until the task releases all references.
	var data []byte
	err := enqueueOwnerLog(len(p), func() {
		// Write still owns mu until its reserved payload has been copied.
		l.mu.Lock()
		payload := data
		l.mu.Unlock()
		l.write(s.name, payload)
		l.mu.Lock()
		s.pending--
		l.observed.PendingBytes -= len(data)
		l.observed.PendingJobs--
		if s.released && s.pending == 0 {
			delete(l.sinks, s.name)
		}
		l.mu.Unlock()
	})
	if err != nil {
		l.failLocked(err)
		return 0, err
	}
	data = make([]byte, len(p))
	copy(data, p)
	s.pending++
	l.observed.PendingBytes += len(data)
	l.observed.PendingJobs++
	return len(p), nil
}

func (l *ownerLog) write(name string, data []byte) {
	l.mu.Lock()
	failed := l.failure != nil
	l.mu.Unlock()
	if failed {
		return
	}
	if err := l.makeRoom(name, int64(len(data))); err != nil {
		l.fail(err)
		return
	}
	file := l.files[name]
	n, info, err := l.storage.Append(filepath.Join(l.dir, name), data, file)
	file.size += int64(n)
	file.info = info
	l.sequence++
	file.order = l.sequence
	l.files[name] = file
	l.bytes += int64(n)
	l.publish()
	if err != nil {
		l.fail(fmt.Errorf("owner log %s: %w", name, err))
	}
}

func (l *ownerLog) makeRoom(name string, bytes int64) error {
	_, exists := l.files[name]
	for bytes > l.maxBytes-l.bytes || !exists && len(l.files) >= l.maxFiles {
		var oldest ownerLogFile
		found := false
		for _, file := range l.files {
			if !found || file.order < oldest.order || file.order == oldest.order && file.name < oldest.name {
				oldest, found = file, true
			}
		}
		if !found {
			return errOwnerLogLimit
		}
		if err := l.storage.Remove(filepath.Join(l.dir, oldest.name), oldest); err != nil {
			return err
		}
		delete(l.files, oldest.name)
		l.bytes -= oldest.size
		_, exists = l.files[name]
		l.publish()
	}
	if !exists {
		l.files[name] = ownerLogFile{name: name}
	}
	return nil
}

func (l *ownerLog) publish() {
	l.mu.Lock()
	l.observed.Files, l.observed.Bytes = len(l.files), l.bytes
	l.mu.Unlock()
}

func (l *ownerLog) failLocked(err error) {
	if l.failure == nil {
		l.failure = err
		l.failures <- err // exactly one terminal failure, buffered once
	}
}

func (l *ownerLog) fail(err error) {
	l.mu.Lock()
	l.failLocked(err)
	l.mu.Unlock()
}

func (l *ownerLog) Failures() <-chan error { return l.failures }

func (l *ownerLog) Snapshot() ownerLogSnapshot {
	l.mu.Lock()
	result := l.observed
	result.Sinks, result.Closed = len(l.sinks), l.closed
	if l.failure != nil {
		text := l.failure.Error()
		if len(text) > 1024 {
			text = text[:1024]
		}
		result.Failure = strings.ToValidUTF8(text, "?")
	}
	l.mu.Unlock()
	ownerLogIO.Lock()
	result.GlobalPendingBytes = ownerLogIO.pendingBytes
	ownerLogIO.Unlock()
	return result
}

// Close stops admission immediately; it never waits for the disk worker. Work
// already admitted may finish asynchronously. Snapshot exposes unflushed bytes;
// successful Write means queue admission, not durable delivery across exit.
func (l *ownerLog) Close() {
	l.close.Do(func() {
		l.mu.Lock()
		l.closed = true
		stop := l.stop
		l.stop = nil
		for name, sink := range l.sinks {
			sink.released = true
			if sink.pending == 0 {
				delete(l.sinks, name)
			}
		}
		l.mu.Unlock()
		if stop != nil {
			stop()
		}
		ownerLogIO.Lock()
		if ownerLogIO.owners[l.dir] == l {
			delete(ownerLogIO.owners, l.dir)
		}
		ownerLogIO.Unlock()
	})
}

type ownerLogOSStorage struct{}

func singleOwnerLogFile(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return info.Mode().IsRegular() && ok && stat.Nlink == 1
}

func (ownerLogOSStorage) Scan(dir string) ([]ownerLogFile, error) {
	return scanOwnerLogs(dir, ownerLogScanEntries, ownerLogMaxFiles)
}

func scanOwnerLogs(dir string, maxEntries, maxFiles int) ([]ownerLogFile, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("owner log directory must be a real directory")
	}
	directory, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = directory.Close() }()
	var files []ownerLogFile
	for {
		entries, err := directory.ReadDir(ownerLogScanBatch)
		for _, entry := range entries {
			maxEntries--
			if maxEntries < 0 {
				return nil, fmt.Errorf("%w: directory scan entries", errOwnerLogLimit)
			}
			if !managedOwnerLogName(entry.Name()) {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			if !singleOwnerLogFile(info) {
				return nil, errors.New("managed owner log must be a singly-linked regular file")
			}
			if len(files) >= maxFiles {
				return nil, fmt.Errorf("%w: historical log scan files", errOwnerLogLimit)
			}
			files = append(files, ownerLogFile{name: entry.Name(), size: info.Size(), info: info})
		}
		if errors.Is(err, io.EOF) {
			return files, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func (ownerLogOSStorage) Append(path string, data []byte, want ownerLogFile) (int, os.FileInfo, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return 0, nil, err
	}
	info, err := f.Stat()
	if err == nil && (!singleOwnerLogFile(info) || info.Size() != want.size || want.info != nil && !os.SameFile(info, want.info)) {
		err = errOwnerLogChanged
	}
	if err != nil {
		return 0, info, errors.Join(err, f.Close())
	}
	n, err := f.Write(data)
	return n, info, errors.Join(err, f.Close())
}

func (ownerLogOSStorage) Remove(path string, want ownerLogFile) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !singleOwnerLogFile(info) || info.Size() != want.size || want.info != nil && !os.SameFile(info, want.info) {
		return errOwnerLogChanged
	}
	return os.Remove(path)
}
