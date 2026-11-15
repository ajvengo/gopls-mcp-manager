package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testOwnerLog(t *testing.T, files int, size int64) (*ownerLog, string) {
	t.Helper()
	dir := t.TempDir()
	log, err := newOwnerLog(t.Context(), dir, files, size)
	require.NoError(t, err)
	t.Cleanup(log.Close)
	return log, dir
}

func waitOwnerLog(t *testing.T, log *ownerLog) {
	t.Helper()
	require.Eventually(t, func() bool { return log.Snapshot().PendingJobs == 0 }, 3*time.Second, time.Millisecond)
	require.Empty(t, log.Snapshot().Failure)
}

func physicalOwnerLogs(t *testing.T, dir string) (int, int64, []string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var count int
	var size int64
	var names []string
	for _, entry := range entries {
		if managedOwnerLogName(entry.Name()) {
			info, err := entry.Info()
			require.NoError(t, err)
			require.True(t, info.Mode().IsRegular())
			count++
			size += info.Size()
			names = append(names, entry.Name())
		}
	}
	return count, size, names
}

func TestOwnerLogRetentionBoundsRealFilesAcrossChurn(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		files int
		bytes int64
		chunk string
	}{
		{name: "file count", files: 2, bytes: 64, chunk: "x"},
		{name: "byte count", files: 3, bytes: 10, chunk: "data"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log, dir := testOwnerLog(t, tc.files, tc.bytes)
			unrelated := filepath.Join(dir, "unrelated.log")
			lock := filepath.Join(dir, "gopls-ports.map.lock")
			require.NoError(t, os.WriteFile(unrelated, []byte("keep"), 0o600))
			require.NoError(t, os.WriteFile(lock, []byte("lock"), 0o600))
			for i := range 20 {
				path := filepath.Join(dir, fmt.Sprintf("%016x.log", i))
				sink, err := log.Sink(path)
				require.NoError(t, err)
				n, err := io.WriteString(sink, tc.chunk)
				require.NoError(t, err)
				require.Equal(t, len(tc.chunk), n)
				waitOwnerLog(t, log)
				count, size, _ := physicalOwnerLogs(t, dir)
				require.LessOrEqual(t, count, tc.files)
				require.LessOrEqual(t, size, tc.bytes)
				require.Equal(t, count, log.Snapshot().Files)
				require.Equal(t, size, log.Snapshot().Bytes)
				log.Release(path)
				require.Zero(t, log.Snapshot().Sinks, "historical sink metadata accumulated")
			}
			data, err := os.ReadFile(unrelated)
			require.NoError(t, err)
			require.Equal(t, "keep", string(data))
			data, err = os.ReadFile(lock)
			require.NoError(t, err)
			require.Equal(t, "lock", string(data))
		})
	}
}

func TestOwnerLogStartupRetentionIsDeterministic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	base := time.Now().Add(-time.Hour)
	for i, size := range []int{4, 8, 8} {
		path := filepath.Join(dir, fmt.Sprintf("%016x.log", i))
		require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{'x'}, size), 0o600))
		stamp := base.Add(time.Duration(i) * time.Second)
		require.NoError(t, os.Chtimes(path, stamp, stamp))
	}
	log, err := newOwnerLog(t.Context(), dir, 2, 12)
	require.NoError(t, err)
	t.Cleanup(log.Close)
	count, size, names := physicalOwnerLogs(t, dir)
	require.Equal(t, 2, count)
	require.Equal(t, int64(12), size)
	require.Equal(t, []string{"0000000000000000.log", "0000000000000002.log"}, names)
	require.Equal(t, count, log.Snapshot().Files)
	require.Equal(t, size, log.Snapshot().Bytes)
}

func TestOwnerLogReleaseReopenAndClose(t *testing.T) {
	t.Parallel()
	log, dir := testOwnerLog(t, 2, 32)
	path := filepath.Join(dir, "manager.log")
	first, err := log.Sink(path)
	require.NoError(t, err)
	_, err = io.WriteString(first, "first")
	require.NoError(t, err)
	log.Release(path)
	require.Eventually(t, func() bool { return log.Snapshot().Sinks == 0 }, time.Second, time.Millisecond)
	_, err = first.Write([]byte("obsolete"))
	require.ErrorIs(t, err, errOwnerLogReleased)
	second, err := log.Sink(path)
	require.NoError(t, err)
	_, err = io.WriteString(second, "+second")
	require.NoError(t, err)
	waitOwnerLog(t, log)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "first+second", string(data))
	log.Close()
	log.Close()
	_, err = second.Write([]byte("closed"))
	require.ErrorIs(t, err, errOwnerLogClosed)
	require.True(t, log.Snapshot().Closed)
	require.Zero(t, log.Snapshot().Sinks)
	reopened, err := newOwnerLog(t.Context(), dir, 2, 32)
	require.NoError(t, err)
	t.Cleanup(reopened.Close)
}

func TestOwnerLogRejectsInvalidPathsAndRegistrationOverflow(t *testing.T) {
	t.Parallel()
	log, dir := testOwnerLog(t, 1, 64)
	for _, path := range []string{"relative.log", filepath.Join(dir, "other.log"), filepath.Join(dir, "AAAAAAAAAAAAAAAA.log"), filepath.Join(dir, "..", "0000000000000000.log")} {
		_, err := log.Sink(path)
		require.ErrorIs(t, err, errOwnerLogLimit)
	}
	path := filepath.Join(dir, "0000000000000000.log")
	_, err := log.Sink(path)
	require.NoError(t, err)
	_, err = log.Sink(path)
	require.ErrorIs(t, err, errOwnerLogLimit)
	_, err = log.Sink(filepath.Join(dir, "0000000000000001.log"))
	require.ErrorIs(t, err, errOwnerLogLimit)
	log.Release(path)
	log.Release(path)
	_, err = log.Sink(filepath.Join(dir, "0000000000000001.log"))
	require.NoError(t, err)
}

type blockedOwnerLogStorage struct {
	ownerLogOSStorage
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int64
}

func (s *blockedOwnerLogStorage) Append(path string, data []byte, want ownerLogFile) (int, os.FileInfo, error) {
	s.calls.Add(1)
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.ownerLogOSStorage.Append(path, data, want)
}

type countedOwnerLogStorage struct {
	ownerLogOSStorage
	calls atomic.Int64
}

func (s *countedOwnerLogStorage) Scan(dir string) ([]ownerLogFile, error) {
	s.calls.Add(1)
	return s.ownerLogOSStorage.Scan(dir)
}

func TestOwnerLogStalledWorkerBoundsQueueAndMetadataAcrossChurn(t *testing.T) {
	// This test intentionally occupies the process-global disk worker; keep it
	// serial and drain it in cleanup before parallel tests can start.
	dir := t.TempDir()
	storage := &blockedOwnerLogStorage{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	log, err := newOwnerLogWithStorage(t.Context(), dir, 2, 2<<20, storage)
	require.NoError(t, err)
	t.Cleanup(func() {
		release.Do(func() { close(storage.release) })
		log.Close()
		require.Eventually(t, func() bool {
			ownerLogIO.Lock()
			defer ownerLogIO.Unlock()
			return ownerLogIO.pendingJobs == 0 && ownerLogIO.pendingBytes == 0
		}, 3*time.Second, time.Millisecond)
	})
	firstPath := filepath.Join(dir, "0000000000000000.log")
	first, err := log.Sink(firstPath)
	require.NoError(t, err)
	payload := bytes.Repeat([]byte{'x'}, ownerLogQueueBytes/4)
	_, err = first.Write(payload)
	require.NoError(t, err)
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		require.FailNow(t, "disk worker did not enter the blocked append")
	}
	payload[0] = 'z' // queued writes must own their exact charged copy.
	log.Release(firstPath)
	second, err := log.Sink(filepath.Join(dir, "0000000000000001.log"))
	require.NoError(t, err)
	for i := 2; i < 100; i++ {
		_, err := log.Sink(filepath.Join(dir, fmt.Sprintf("%016x.log", i)))
		require.ErrorIs(t, err, errOwnerLogLimit)
	}
	require.Equal(t, 2, log.Snapshot().Sinks)
	for range 3 {
		_, err := second.Write(payload)
		require.NoError(t, err)
	}
	require.Equal(t, ownerLogQueueBytes, log.Snapshot().GlobalPendingBytes)
	_, err = second.Write([]byte{'!'})
	require.ErrorIs(t, err, errOwnerLogQueue)
	require.ErrorIs(t, <-log.Failures(), errOwnerLogQueue)
	for range 100 {
		_, err := second.Write([]byte{'!'})
		require.ErrorIs(t, err, errOwnerLogQueue)
	}
	require.Empty(t, log.failures, "terminal failure notifications accumulated")
	started := time.Now()
	log.Close()
	require.Less(t, time.Since(started), 250*time.Millisecond, "shutdown waited for blocked disk I/O")
	// New helper construction cannot create replacement workers behind a stuck
	// disk. Even cancelled construction churn has a bounded global task count.
	queuedStorage := &countedOwnerLogStorage{}
	for i := range ownerLogQueueJobs + 2 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		other, err := newOwnerLogWithStorage(ctx, filepath.Join(dir, fmt.Sprint(i)), 1, 64, queuedStorage)
		cancel()
		require.Error(t, err)
		require.Nil(t, other)
	}
	ownerLogIO.Lock()
	jobs, queued := ownerLogIO.pendingJobs, ownerLogIO.pendingBytes
	ownerLogIO.Unlock()
	require.LessOrEqual(t, jobs, ownerLogQueueJobs)
	require.Equal(t, ownerLogQueueBytes, queued)
	require.Equal(t, int64(1), storage.calls.Load())
	require.Zero(t, queuedStorage.calls.Load())
	release.Do(func() { close(storage.release) })
	require.Eventually(t, func() bool { return log.Snapshot().PendingJobs == 0 }, 3*time.Second, time.Millisecond)
	data, err := os.ReadFile(firstPath)
	require.NoError(t, err)
	require.Equal(t, byte('x'), data[0], "caller mutation corrupted an admitted payload")
}

type partialFailureOwnerLogStorage struct{ ownerLogOSStorage }

func (s partialFailureOwnerLogStorage) Append(path string, data []byte, want ownerLogFile) (int, os.FileInfo, error) {
	n, info, err := s.ownerLogOSStorage.Append(path, data[:2], want)
	return n, info, errors.Join(err, syscall.ENOSPC)
}

type scanFailureOwnerLogStorage struct{ ownerLogOSStorage }

func (scanFailureOwnerLogStorage) Scan(string) ([]ownerLogFile, error) {
	return nil, os.ErrPermission
}

type removeFailureOwnerLogStorage struct{ ownerLogOSStorage }

func (removeFailureOwnerLogStorage) Remove(string, ownerLogFile) error { return os.ErrPermission }

func TestOwnerLogPartialWriteFailureIsAccountedAndObservable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	log, err := newOwnerLogWithStorage(t.Context(), dir, 2, 8, partialFailureOwnerLogStorage{})
	require.NoError(t, err)
	t.Cleanup(log.Close)
	sink, err := log.Sink(filepath.Join(dir, "manager.log"))
	require.NoError(t, err)
	n, err := sink.Write([]byte("four"))
	require.NoError(t, err, "successful Write promises queue admission, not disk durability")
	require.Equal(t, 4, n)
	select {
	case err := <-log.Failures():
		require.ErrorIs(t, err, syscall.ENOSPC)
	case <-time.After(time.Second):
		require.FailNow(t, "disk failure was not delivered to the owner")
	}
	require.Eventually(t, func() bool { return log.Snapshot().PendingJobs == 0 }, time.Second, time.Millisecond)
	count, size, _ := physicalOwnerLogs(t, dir)
	require.Equal(t, 1, count)
	require.Equal(t, int64(2), size)
	require.Equal(t, size, log.Snapshot().Bytes, "partial writes disappeared from disk accounting")
	require.NotEmpty(t, log.Snapshot().Failure)
	_, err = sink.Write([]byte("more"))
	require.ErrorIs(t, err, syscall.ENOSPC)
}

func TestOwnerLogScanAndRetentionErrorsFailExplicitly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, err := newOwnerLogWithStorage(t.Context(), dir, 1, 8, scanFailureOwnerLogStorage{})
	require.ErrorIs(t, err, os.ErrPermission)
	path := filepath.Join(dir, "0000000000000000.log")
	require.NoError(t, os.WriteFile(path, []byte("oversized"), 0o600))
	_, err = newOwnerLogWithStorage(t.Context(), dir, 1, 8, removeFailureOwnerLogStorage{})
	require.ErrorIs(t, err, os.ErrPermission)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "oversized", string(data))
}

func TestOwnerLogRefusesSymlinksAndNonregularFiles(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"symlink", "directory", "FIFO"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "0000000000000000.log")
			target := filepath.Join(dir, "unrelated")
			require.NoError(t, os.WriteFile(target, []byte("untouched"), 0o600))
			switch kind {
			case "symlink":
				require.NoError(t, os.Symlink(target, path))
			case "directory":
				require.NoError(t, os.Mkdir(path, 0o700))
			case "FIFO":
				require.NoError(t, syscall.Mkfifo(path, 0o600))
			}
			_, err := newOwnerLog(t.Context(), dir, 2, 64)
			require.Error(t, err)
			data, err := os.ReadFile(target)
			require.NoError(t, err)
			require.Equal(t, "untouched", string(data))
		})
	}
}

func TestOwnerLogDoesNotFollowAReplacementSymlink(t *testing.T) {
	t.Parallel()
	log, dir := testOwnerLog(t, 2, 64)
	target := filepath.Join(dir, "unrelated")
	require.NoError(t, os.WriteFile(target, []byte("untouched"), 0o600))
	path := filepath.Join(dir, "0000000000000000.log")
	require.NoError(t, os.Symlink(target, path))
	sink, err := log.Sink(path)
	require.NoError(t, err)
	_, err = sink.Write([]byte("attack"))
	require.NoError(t, err)
	select {
	case err := <-log.Failures():
		require.ErrorIs(t, err, syscall.ELOOP)
	case <-time.After(time.Second):
		require.FailNow(t, "replacement symlink failure was not reported")
	}
	data, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "untouched", string(data))
}

func TestOwnerLogScanBoundsAndInvalidConfiguration(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for i := range ownerLogScanBatch + 1 {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%016x.log", i)), nil, 0o600))
	}
	_, err := scanOwnerLogs(dir, ownerLogScanBatch, ownerLogMaxFiles)
	require.ErrorIs(t, err, errOwnerLogLimit)
	_, err = scanOwnerLogs(dir, ownerLogScanEntries, 2)
	require.ErrorIs(t, err, errOwnerLogLimit)
	files, err := scanOwnerLogs(dir, ownerLogScanBatch+1, ownerLogScanBatch+1)
	require.NoError(t, err)
	require.Len(t, files, ownerLogScanBatch+1)
	for _, tc := range []struct {
		dir   string
		files int
		bytes int64
	}{
		{dir: "relative", files: 1, bytes: 1},
		{dir: dir, files: 0, bytes: 1},
		{dir: dir, files: ownerLogMaxFiles + 1, bytes: 1},
		{dir: dir, files: 1, bytes: 0},
		{dir: "/" + strings.Repeat("x", ownerLogMaxPathBytes), files: 1, bytes: 1},
	} {
		_, err := newOwnerLog(t.Context(), tc.dir, tc.files, tc.bytes)
		require.ErrorIs(t, err, errOwnerLogLimit)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = newOwnerLog(ctx, t.TempDir(), 1, 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestOwnerLogContextCancellationReleasesDirectoryOwnership(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	log, err := newOwnerLog(ctx, dir, 1, 64)
	require.NoError(t, err)
	t.Cleanup(log.Close)
	_, err = newOwnerLog(t.Context(), dir, 1, 64)
	require.ErrorIs(t, err, errOwnerLogLimit)
	cancel()
	require.Eventually(t, func() bool { return log.Snapshot().Closed }, time.Second, time.Millisecond)
	log.Close()
	reopened, err := newOwnerLog(t.Context(), dir, 1, 64)
	require.NoError(t, err)
	t.Cleanup(reopened.Close)
}

func TestOwnerLogRejectsAnAliasedOwnedDirectory(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	dir := filepath.Join(parent, "logs")
	first, err := newOwnerLog(t.Context(), dir, 1, 4)
	require.NoError(t, err)
	t.Cleanup(first.Close)
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(parent, alias))
	second, err := newOwnerLog(t.Context(), filepath.Join(alias, "logs"), 1, 4)
	if second != nil {
		t.Cleanup(second.Close)
	}
	require.ErrorIs(t, err, errOwnerLogLimit, "an ancestor symlink must not create a second independent disk budget")
	require.Nil(t, second)
}

func TestOwnerLogRefusesHardLinkedManagedFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "unrelated")
	require.NoError(t, os.WriteFile(target, []byte("untouched"), 0o600))
	require.NoError(t, os.Link(target, filepath.Join(dir, "0000000000000000.log")))
	log, err := newOwnerLog(t.Context(), dir, 2, 64)
	if log != nil {
		t.Cleanup(log.Close)
	}
	require.Error(t, err, "shared log inodes must not bypass owned-file byte accounting")
	data, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "untouched", string(data))
}

type cancelingOwnerLogStorage struct {
	ownerLogOSStorage
	cancel context.CancelFunc
}

func (s cancelingOwnerLogStorage) Scan(dir string) ([]ownerLogFile, error) {
	files, err := s.ownerLogOSStorage.Scan(dir)
	s.cancel()
	return files, err
}

func TestOwnerLogCanceledInitializationCannotReturnSuccess(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for i := range 20 {
		ctx, cancel := context.WithCancel(t.Context())
		log, err := newOwnerLogWithStorage(ctx, filepath.Join(root, fmt.Sprint(i)), 1, 64,
			cancelingOwnerLogStorage{cancel: cancel})
		cancel()
		if log != nil {
			log.Close()
		}
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, log)
	}
}

func TestOwnerLogRejectsUnstorableWritesBeforeQueueing(t *testing.T) {
	t.Parallel()
	for _, limit := range []int64{4, 2 * ownerLogQueueBytes} {
		log, dir := testOwnerLog(t, 1, limit)
		sink, err := log.Sink(filepath.Join(dir, "manager.log"))
		require.NoError(t, err)
		size := min(int(limit), ownerLogQueueBytes) + 1
		n, err := sink.Write(bytes.Repeat([]byte{'x'}, size))
		require.ErrorIs(t, err, errOwnerLogLimit)
		require.Zero(t, n)
		require.Zero(t, log.Snapshot().PendingBytes)
		require.Zero(t, log.Snapshot().PendingJobs)
		require.ErrorIs(t, <-log.Failures(), errOwnerLogLimit)
		count, sizeOnDisk, _ := physicalOwnerLogs(t, dir)
		require.Zero(t, count)
		require.Zero(t, sizeOnDisk)
	}
}

func TestOwnerLogDetectsExternalFileMutation(t *testing.T) {
	t.Parallel()
	log, dir := testOwnerLog(t, 1, 64)
	path := filepath.Join(dir, "manager.log")
	sink, err := log.Sink(path)
	require.NoError(t, err)
	_, err = io.WriteString(sink, "owned")
	require.NoError(t, err)
	waitOwnerLog(t, log)
	require.NoError(t, os.WriteFile(path, []byte("external"), 0o600))
	_, err = io.WriteString(sink, "must not append")
	require.NoError(t, err)
	select {
	case err := <-log.Failures():
		require.ErrorIs(t, err, errOwnerLogChanged)
	case <-time.After(time.Second):
		require.FailNow(t, "unowned file mutation was not reported")
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "external", string(data))
}

func TestOwnerLogDoesNotRetainAppendDescriptors(t *testing.T) {
	// Process-wide descriptor counts cannot overlap parallel tests.
	log, dir := testOwnerLog(t, 2, 64)
	initial, err := os.ReadDir("/dev/fd")
	require.NoError(t, err)
	for i := range 20 {
		path := filepath.Join(dir, fmt.Sprintf("%016x.log", i))
		sink, err := log.Sink(path)
		require.NoError(t, err)
		_, err = io.WriteString(sink, "line")
		require.NoError(t, err)
		waitOwnerLog(t, log)
		log.Release(path)
	}
	log.Close()
	require.Eventually(t, func() bool {
		files, err := os.ReadDir("/dev/fd")
		return err == nil && len(files) <= len(initial)
	}, time.Second, time.Millisecond, "sink churn retained append or directory descriptors")
	require.Zero(t, log.Snapshot().Sinks)
}
