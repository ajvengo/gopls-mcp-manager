package main

import (
	"bufio"
	"bytes"
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditResourceRegistryFailuresReleaseDescriptors(t *testing.T) {
	dir := t.TempDir()
	mapPath := filepath.Join(dir, "registry")
	valid := []record{{Worktree: "/repo", PID: 123, Port: firstPort}}
	require.NoError(t, writeMap(mapPath, valid))
	malformed := filepath.Join(dir, "oversized-line")
	require.NoError(t, os.WriteFile(malformed, []byte(strings.Repeat("x", 128<<10)), 0o600))
	renameTarget := filepath.Join(dir, "directory")
	require.NoError(t, os.Mkdir(renameTarget, 0o700))
	refused := errors.New("audit callback refused mutation")
	check := func() {
		t.Helper()
		require.ErrorIs(t, withFileLock(t.Context(), mapPath, func() error { return refused }), refused)
		require.Error(t, writeMap(mapPath, []record{{Worktree: string([]byte{0xff}), PID: 123, Port: firstPort}}))
		require.Error(t, writeMap(renameTarget, valid))
		_, _, err := readMap(malformed)
		require.Error(t, err)
	}
	check() // Warm lazy runtime resources before taking the baseline.
	before := auditResourceFDCount(t)
	for range 256 {
		check()
	}
	require.LessOrEqual(t, auditResourceFDCount(t), before, "failed registry operations retained descriptors")
	got, intact, err := readMap(mapPath)
	require.NoError(t, err)
	require.True(t, intact)
	require.Equal(t, valid, got, "a failed replacement changed the committed registry")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.False(t, strings.HasPrefix(entry.Name(), ".gopls-ports.map-"), "failed writes left temporary files")
	}
}

func TestAuditResourceRegistryEncodedLineBoundary(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	r := record{Worktree: "/", PID: 123, Port: firstPort}
	base, err := json.Marshal(r)
	require.NoError(t, err)
	r.Worktree = "/" + strings.Repeat("x", bufio.MaxScanTokenSize-1-len(base))
	encoded, err := json.Marshal(r)
	require.NoError(t, err)
	require.Len(t, encoded, bufio.MaxScanTokenSize-1)
	require.NoError(t, writeMap(m.mapPath, []record{r}))
	got, intact, err := readMap(m.mapPath)
	require.NoError(t, err, "writer committed a boundary record its reader cannot read")
	require.True(t, intact)
	require.Equal(t, []record{r}, got)
	committed, err := os.ReadFile(m.mapPath)
	require.NoError(t, err)
	r.Worktree += "x"
	require.Error(t, writeMap(m.mapPath, []record{r}))
	after, err := os.ReadFile(m.mapPath)
	require.NoError(t, err)
	require.Equal(t, committed, after, "rejected line replacement changed committed state")
}

func TestAuditResourceRegistryInputBoundaries(t *testing.T) {
	t.Parallel()
	line := fmt.Sprintf("{\"Worktree\":\"/repo\",\"Port\":%d,\"PID\":123}\n", firstPort)
	for _, tc := range []struct {
		name    string
		data    []byte
		intact  bool
		records int
	}{
		{name: "valid line count", data: []byte(strings.Repeat(line, portCount)), intact: true, records: portCount},
		{name: "blank line count", data: bytes.Repeat([]byte("\n"), portCount)},
		{name: "maximum bytes", data: bytes.Repeat([]byte(strings.Repeat(" ", (64<<10)-1)+"\n"), maxRegistryBytes/(64<<10))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "registry")
			require.NoError(t, os.WriteFile(path, tc.data, 0o600))
			records, intact, err := readMap(path)
			require.NoError(t, err)
			require.Len(t, records, tc.records)
			require.Equal(t, tc.intact, intact)
		})
	}
}

func TestAuditResourceRegistryOverflowPreservesCommittedState(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "discarded lines", data: bytes.Repeat([]byte("\n"), portCount+1)},
		{name: "bytes", data: bytes.Repeat([]byte(" "), maxRegistryBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newTestManager(t)
			require.NoError(t, os.WriteFile(m.mapPath, tc.data, 0o600))

			_, err := m.withMap(t.Context(), func([]record) ([]record, error) {
				t.Fatal("overflow reached registry mutation")
				return nil, nil
			})
			require.Error(t, err)

			got, err := os.ReadFile(m.mapPath)
			require.NoError(t, err)
			require.Equal(t, tc.data, got, "overflow silently repaired or dropped registry contents")
		})
	}
}

func TestAuditResourceRegistryWriteOverflowPreservesCommittedState(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	valid := []record{{Worktree: "/original", PID: 123, Port: firstPort}}
	require.NoError(t, writeMap(m.mapPath, valid))
	for _, records := range [][]record{
		make([]record, portCount+1),
		bytesOversizedRegistry(),
	} {
		require.Error(t, writeMap(m.mapPath, records))
		got, intact, err := readMap(m.mapPath)
		require.NoError(t, err)
		require.True(t, intact)
		require.Equal(t, valid, got)
	}
	entries, err := os.ReadDir(filepath.Dir(m.mapPath))
	require.NoError(t, err)
	require.Len(t, entries, 1, "failed writes left temporary files")
}

func bytesOversizedRegistry() []record {
	records := make([]record, 256)
	for i := range records {
		records[i] = record{Worktree: "/" + strings.Repeat("x", 32<<10), PID: i + 1, Port: firstPort}
	}
	return records
}

func TestAuditResourceRegistryCardinalityHasHardLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry")
	line := fmt.Sprintf("{\"Worktree\":\"/repo\",\"Port\":%d,\"PID\":123}\n", firstPort)
	// More records than there are assignable ports, all individually valid and
	// well below the scanner's per-line byte limit.
	require.NoError(t, os.WriteFile(path, []byte(strings.Repeat(line, portCount+1)), 0o600))
	_, _, err := readMap(path)
	require.Error(t, err, "registry read has no total-record bound")
}

// The map file under its flock: what every ensure pays, and pays serially with
// every other process on the machine, since the lock is one file for all of
// them. Both rows run the same read and the same sweep; they differ only in
// whether the body hands back what it was given, which is the steady state — a
// warm worktree whose record is already there and still alive.
//
// The gap between them is the write: a temp file, an fsync and a rename inside
// the critical section. Nothing here forks or probes, so what is left is the
// syscalls the lock actually holds.
func BenchmarkWithRecords(b *testing.B) {
	records := testRecords(16)
	benches := []struct {
		name string
		body func([]record) ([]record, error)
	}{
		{name: "unchanged", body: func(rs []record) ([]record, error) { return rs, nil }},
		{name: "rewritten", body: func(rs []record) ([]record, error) {
			// One record changed, not one added. The map is the same file across
			// iterations, so a body that appended would report the average over a
			// file that kept growing rather than the cost of one write. The change
			// has to be a real one, or the skip above eats it and both rows measure
			// the same thing. Edited in place because withMap hands every body a
			// copy of its own.
			rs[len(rs)-1].PID++
			return rs, nil
		}},
	}
	for _, bench := range benches {
		b.Run(bench.name, func(b *testing.B) {
			m := newTestManager(b)
			// Every record answers alive without a syscall: the probes are another
			// benchmark's subject, and a real one here would drown the file work.

			require.NoError(b, writeMap(m.mapPath, records))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := m.withMap(b.Context(), bench.body); err != nil {
					require.NoError(b, err)
				}
			}
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

// cleanRecords probes its records at once, so what it hands back is assembled
// from a slice several goroutines wrote into by index. Three properties say
// that assembly is right at any length: every record offered, none offered
// twice, and the order preserved — a reordered result would still round-trip
// through the file and still be wrong, since forget matches by value. What each

// The map file is meant to be editable by hand, so readMap's input is arbitrary
// bytes. Nothing it returns may violate the checks it makes, because every
// field it hands back is an argument to kill(2) or to a probe that decides on
// one: a pid of 0 signals this process's own group, a negative pid signals
// every process the user owns, and a port outside our range can only refuse a
// probe and so condemn a live server.
func FuzzReadMap(f *testing.F) {
	f.Add([]byte(`{"Worktree":"/repo","Port":62001,"PID":42,"Terminating":true,"Uses":1e309}`))
	f.Add([]byte(`{"Worktree":"/repo","Port":62001,"PID":42,"StartedAt":-1,"StartedAtMilli":1}`))
	f.Add([]byte(`{"Worktree":"/repo","Port":62001,"PID":42,"Terminating":true}`))
	f.Add([]byte(`{"Worktree":"/repo","Port":62001,"PID":42,"PID":0}`))
	f.Add([]byte(`{"Worktree":"/repo","Port":62001,"PID":42,"StartedAtMilli":9223372036854775807}`))
	f.Add([]byte(""))
	f.Add([]byte("\n\n\n"))
	f.Add([]byte(`{"worktree":"/repo","port":62001,"pid":42}`))
	f.Add([]byte(`{"worktree":"/repo","port":62001,"pid":42,"startedAt":1700000000}`))
	f.Add([]byte(`{"worktree":"","port":62001,"pid":42}`))   // empty worktree
	f.Add([]byte(`{"worktree":"/r","port":1,"pid":42}`))     // port under range
	f.Add([]byte(`{"worktree":"/r","port":70000,"pid":42}`)) // port over range
	f.Add([]byte(`{"worktree":"/r","port":62001,"pid":0}`))  // pid 0: our own group
	f.Add([]byte(`{"worktree":"/r","port":62001,"pid":-1}`)) // pid -1: everything
	f.Add([]byte("not json\n{\"worktree\":\"/r\",\"port\":62001,\"pid\":7}"))
	f.Add([]byte(`{"worktree":"/r","port":62001.9,"pid":7}`))

	f.Fuzz(func(t *testing.T, content []byte) {
		path := newTestManager(t).mapPath
		mustWriteFile(t, path, string(content))
		records, intact, err := readMap(path)
		if err != nil {
			return // a read that failed hands nothing back to act on
		}
		for _, r := range records {
			assert.NotEmptyf(t, r.Worktree, "readMap kept a record with no worktree: %#v", r)
			assert.Greaterf(t, r.PID, 0, "readMap kept pid %d, which kill(2) would aim at a group: %#v", r.PID, r)
			assert.Truef(t, r.Port >= firstPort && r.Port <= lastPort, "readMap kept port %d, outside %d-%d: %#v", r.Port, firstPort, lastPort, r)
		}

		// The repair has to converge, because withMap skips the write only when
		// the records are unchanged and the file is intact. A file that rewrote to
		// something this read would drop again, or that still reported damaged after
		// its own repair, would be rewritten and fsynced under the machine-wide lock
		// on every ensure for as long as it sits there.
		require.NoErrorf(t, writeMap(path, records), "writeMap refused what readMap handed back")
		again, againIntact, err := readMap(path)
		require.NoErrorf(t, err, "readMap failed on a file it wrote itself")
		require.Truef(t, slices.Equal(again, records), "readMap is not idempotent: %#v, then %#v", records, again)
		require.Truef(t, againIntact, "readMap called its own rewritten file damaged: %#v", again)
		// Damaged has to mean there is something to repair. The converse is not
		// claimed: a hand-edited file may order its keys differently from writeMap
		// and still have lost nothing, so intact does not promise equal bytes.
		require.Falsef(t, !intact && string(content) == mustReadString(t, path), "readMap called %q damaged, but rewriting it changed nothing", content)
	})
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

func TestOfflineListRetainsLegacyReservations(t *testing.T) {
	t.Parallel()
	for _, ownerBoot := range []string{"", "previous-owner"} {
		t.Run(ownerBoot, func(t *testing.T) {
			t.Parallel()
			m := newTestManager(t)
			child := startFakeGopls(t, firstPort)
			records := []record{{Worktree: "/live", PID: child.Process.Pid, Port: firstPort, OwnerBoot: ownerBoot}, {Worktree: "/dead", PID: maxProcessID, Port: firstPort + 1, OwnerBoot: ownerBoot}}
			mustWriteMap(t, m.mapPath, records)
			before, err := os.ReadFile(m.mapPath)
			require.NoError(t, err)
			var out bytes.Buffer
			require.ErrorContains(t, m.list(t.Context(), &out), "offline legacy cleanup")
			require.Empty(t, out.String())
			after, err := os.ReadFile(m.mapPath)
			require.NoError(t, err)
			require.Equal(t, before, after, "offline list rewrote uncertain reservations")
			wantRunning(t, child, "offline list signalled a legacy process")
			if ownerBoot == "" {
				mustWriteMap(t, m.mapPath, []record{{Worktree: "/dead-legacy", PID: maxProcessID, Port: firstPort}})
				require.ErrorContains(t, validateFrontendRegistry(&m), "offline cleanup")
				require.ErrorContains(t, validateOwnerRegistry(t.Context(), &m), "offline cleanup")
				require.Len(t, mustReadMap(t, m.mapPath), 1, "dead legacy metadata was silently reconciled")
			}
		})
	}
}

func TestBasePortIsStableAndInRange(t *testing.T) {
	t.Parallel()
	const worktree = "/Users/example/src/repository"

	got := basePort(worktree)
	require.Falsef(t, got < firstPort || got > lastPort, "basePort(%q) = %d, outside %d-%d", worktree, got, firstPort, lastPort)
	again := basePort(worktree)
	require.Equalf(t, got, again, "basePort(%q) changed from %d to %d", worktree, got, again)
}

func TestAllocatePortProbesPastMappedAndOccupiedPorts(t *testing.T) {
	t.Parallel()
	worktree := "/repo/a"
	start := basePort(worktree)
	records := []record{{Worktree: "/repo/b", Port: start, PID: 10}}
	unavailable := func(port int) bool { return port == nextPort(start) }

	got, err := allocatePort(worktree, records, unavailable)
	require.NoError(t, err)
	want := nextPort(nextPort(start))
	require.Equalf(t, want, got, "allocatePort() = %d, want %d", got, want)
}

// Every record must be offered to alive(), which kills what it rejects. A
// second record for a worktree is a second gopls, and skipping it would leave

// The probes must run at once — see cleanRecords for what that buys. Under the
// fake clock the difference is exact rather than a race: three probes of one
func mustWriteMap(t testing.TB, path string, records []record) {
	t.Helper()
	err := writeMap(path, records)
	require.NoError(t, err)
}

// appendToMap puts text after whatever the map already holds, which is the only
// way these tests can produce a file readMap will drop lines from: writeMap can
// only write one it would accept entirely.
func appendToMap(t *testing.T, path string, text string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = f.WriteString(text)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)
}

func mustReadMap(t *testing.T, path string) []record {
	t.Helper()
	records, _, err := readMap(path)
	require.NoError(t, err)
	return records
}

// wantRecords reads the map back and insists it holds exactly want, in order.
// The file is the durable claim, so most tests here assert on it rather than on
// what a call happened to return.
func wantRecords(t *testing.T, path string, whatWouldBeWrong string, want ...record) {
	t.Helper()
	got := mustReadMap(t, path)
	require.Truef(t, slices.Equal(got, want), "%s: records = %#v, want %#v", whatWouldBeWrong, got, want)
}

// newTestManager returns a manager over a map file of its own. Its limits are
// the shipped ones, because newManager's are: a partially filled Limits is not
// a state the program can be in, and nothing below it checks for one.
func newTestManager(tb testing.TB) manager {
	tb.Helper()
	m := manager{mapPath: filepath.Join(tb.TempDir(), "gopls-ports.map"), limits: config.Default()}

	return m
}

func TestMapRoundTripEscapesPaths(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	// One record per line, so a path containing a newline has to survive as one.
	want := []record{{Worktree: "/repo/with spaces\tand\na quote\"", Port: 63001, PID: 42}}

	mustWriteMap(t, m.mapPath, want)
	wantRecords(t, m.mapPath, "the map did not round-trip", want...)
	info, err := os.Stat(m.mapPath)
	require.NoError(t, err)
	require.Equalf(t, os.FileMode(0o600), info.Mode().Perm(), "map permissions = %o, want 600", info.Mode().Perm())
}

// idleAs runs a process whose command line is exactly args, which is what
// isOurGopls() inspects.
//
// A shell builtin waits on stdin without spawning a grandchild or replacing
// the argv that identity tests inspect. Cleanup kills and reaps the direct child.
func idleAs(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sh", append([]string{"-c", "read hold"}, args...)...)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	err = cmd.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	return cmd
}

// startFakeGopls idles under the command line this tool spawns its own gopls
// with, so isOurGopls() recognises it as ours.
func startFakeGopls(t *testing.T, port int) *exec.Cmd {
	t.Helper()
	return idleAs(t, goplsBinary, "mcp", "-listen", mcpAddress(port))
}

// wantRunning fails when cmd was signalled. Every recordAlive test that expects
// a verdict short of "provably dead and ours" asserts this: the cost of getting
// it wrong is a full re-index of a tree nobody was asking about, or somebody
// else's process killed outright.
func wantRunning(t *testing.T, cmd *exec.Cmd, whatWouldBeWrong string) {
	t.Helper()
	err := cmd.Process.Signal(syscall.Signal(0))
	require.NoErrorf(t, err, "%s", whatWouldBeWrong)
}

// listenInAllocationRange holds a port from inside the range ports are
// allocated from, which is where a record has to name one: readMap rejects
// anything outside it, so an ephemeral port from deadPort would be dropped
// before any test using the map file could see it.
//
// The availability test is the bind, and the listener it opens is the one kept.
// Asking portUnavailable and binding afterwards leaves a window, and the sibling
// tests spend it taking ephemeral ports — a range macOS overlaps with this one —
// so the port this walked to is occasionally gone by the time it is claimed.
func listenInAllocationRange(t testing.TB, worktree string) (net.Listener, int) {
	t.Helper()
	var listener net.Listener
	port, err := allocatePort(worktree, nil, func(port int) bool {
		held, err := net.Listen("tcp", mcpAddress(port))
		if err != nil {
			return true
		}
		listener = held
		return false
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return listener, port
}

// listenLocal holds an ephemeral local port for as long as the test runs.
func listenLocal(tb testing.TB) (net.Listener, int) {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = listener.Close() })
	return listener, listener.Addr().(*net.TCPAddr).Port
}

// eventStreamHandler answers the way our endpoint does, which is the whole of
// what a probe reads before calling a port alive.
func eventStreamHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
	})
}

// serveEndpoint answers on a listener already bound, which is how a caller keeps
// binding and answering as two separate events — after delays the answer without
// delaying the bind, the way a gopls between fork and listen looks to a probe.
//
// Named rather than an http.Serve call, so that Close can end it: left running,
// the server and its goroutine outlive the test and race the listener's own
// cleanup. Not httptest, whose unstarted server insists on opening an ephemeral
// listener of its own before this one can be substituted in — and macOS hands
// those out of a range that overlaps the one ports are allocated from, so it
// would occasionally take the port another test was about to claim.
func serveEndpoint(tb testing.TB, listener net.Listener, after time.Duration) {
	tb.Helper()
	endpoint := &http.Server{Handler: eventStreamHandler()}
	tb.Cleanup(func() { _ = endpoint.Close() })
	go func() {
		time.Sleep(after)
		_ = endpoint.Serve(listener)
	}()
}

// livePort returns a port answering the way our endpoint does, which is the
// only answer endpointProbe accepts as alive. Distinct from answeringPort,
// whose server answers but not like ours — that one reaches the inconclusive
// verdict, and so pays the ps fork this one skips.

// silentPort returns a port held by something that takes the connection and
// then says nothing — §8's server, the one a probe can only time out against.
// Accepted connections are kept rather than closed, since closing is what would
// give the prober its conclusive answer.
//
// Accepted at all, rather than left to pile up in the kernel's backlog, because
// a full backlog starts refusing — which is that same conclusive answer by
// another road. BenchmarkSweepProbes probes eight records an iteration, so the
// backlog would not last a second of it. The cost is one held descriptor per
// probe until the listener closes, which bounds it by the caller's own run.
func silentPort(tb testing.TB) int {
	tb.Helper()
	listener, port := listenLocal(tb)
	go func() {
		var held []net.Conn
		defer func() {
			for _, c := range held {
				_ = c.Close()
			}
		}()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			held = append(held, conn)
		}
	}()
	return port
}

// deadPort returns a port nothing listens on, so a probe of it is refused: it
// takes an ephemeral one and gives it straight back. Closing here rather than
// leaving it to the cleanup is the whole point, and the cleanup's own close then
// finds it shut — which is why that one discards its error and this one does not.

// refusedPort is the table row for "nothing is listening": a dead port, and so
// no answered check to make, since a refusal is conclusive on its own.

// answeringPort returns a port held by something that answers, but not the way
// our endpoint does, along with a way to ask whether the probe actually reached
// it. Every caller cares: a probe that merely timed out reaches the same verdict
// by the other route, so without the check the test would pass unchanged on any
// run slow enough that nothing answered inside probeClient's 500ms.

// wantSignalled fails unless cmd was terminated by SIGTERM. The Wait also
// reaps, so what it asserts on is the signal rather than the exit.

// verdict is what must have become of the process holding a record's pid once
// recordAlive has had its say.

// The whole of recordAlive's decision: what is on the port, what holds the pid,
// and what the timestamp says — against the verdict and, just as much, what
// becomes of the process. The two halves belong in one table because they are
// one decision: every row that spares a server is a row where killing it would
// be the expensive mistake, and the one row that kills is the one where not

// A damaged record may still reserve a live process. Refuse partial snapshots
// so subsequent mutations cannot silently strand its only process identity.
func TestReadMapRefusesUncertainProcessReservations(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	good := record{Worktree: "/repo/good", Port: 62001, PID: 11}
	mustWriteMap(t, m.mapPath, []record{good})
	// Not just unparseable lines: valid JSON whose fields would be handed to
	// kill(2). A missing pid decodes to 0, which signals our own process group.
	appendToMap(t, m.mapPath, "garbage\t\tnot-a-port\tx\n\"/repo/b\"\tnope\t12\n"+
		`{"Worktree":"/repo/no-pid","Port":62002}`+"\n"+
		`{"Worktree":"/repo/every-process","Port":62003,"PID":-1}`+"\n"+
		`{"Worktree":"/repo/bad-port","Port":0,"PID":13}`+"\n")

	got, intact, err := readMap(m.mapPath)
	require.Error(t, err)
	require.Nil(t, got, "damaged map exposed a partial snapshot for mutation")
	require.False(t, intact)
}

// The write is what costs: a temp file, an fsync and a rename inside a lock
// every process on the machine shares, on a path a warm tool call takes every
// time. Skipping it turns on two questions rather than one, and the halves fail
// in opposite directions — keyed on the records alone, the repair M3 leaves to
// the next write is owed forever; never skipping pays the fsync for nothing.
//
// Asserted on the mtime because the skip is the absence of a write, which the
// records cannot show: both rows end holding exactly the same one.
func TestWithRecordsWritesOnlyWhenTheFileWouldChange(t *testing.T) {
	t.Parallel()
	kept := record{Worktree: "/repo/kept", Port: 62001, PID: 11}
	for _, tc := range []struct {
		name      string
		damage    string
		wantWrite bool
	}{
		{name: "records unchanged and the read dropped nothing", wantWrite: false},
		{name: "records unchanged but the read dropped blank formatting", damage: " \t\n", wantWrite: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newTestManager(t)
			// Every record alive, so the sweep hands back what it was given and the
			// records are equal in both rows — leaving the intact flag as the only
			// thing that differs between them.

			mustWriteMap(t, m.mapPath, []record{kept})
			if tc.damage != "" {
				appendToMap(t, m.mapPath, tc.damage)
			}
			// Far enough back that the fresh mtime a rename leaves cannot be mistaken
			// for it, whatever the filesystem's timestamp resolution.
			stale := time.Now().Add(-time.Hour)
			err := os.Chtimes(m.mapPath, stale, stale)
			require.NoError(t, err)

			_, err = m.withMap(t.Context(), func(rs []record) ([]record, error) { return rs, nil })
			require.NoError(t, err)

			info, err := os.Stat(m.mapPath)
			require.NoError(t, err)
			written := !info.ModTime().Equal(stale)
			require.Equalf(t, tc.wantWrite, written, "withMap() rewrote the map = %t, want %t", written, tc.wantWrite)
			// Either way the file ends up saying the same thing: the row above is
			// about what it cost to get there, not about what it holds.
			wantRecords(t, m.mapPath, "the map lost the record it should have kept", kept)
		})
	}
}

// stubGopls puts a gopls on PATH that starts, ignores its arguments and never

// claimPort must not wait for the gopls it spawned. ensure does that with the
// map lock released, so a wait here would put a cold start back under the lock
// and make every other process queue behind it for up to readyTimeout.
//

// Several processes reach ensure for one worktree at once — worktree isolation
// produces exactly that — and only the flock stops each of them starting a
// gopls of its own. Goroutines stand in for the processes: withFileLock opens
// its own descriptor per call, so they contend the way separate processes do.
//

// A record inside its start grace is vouched for without being probed at all,
// so its port can still refuse: the gopls it names was forked by another
// process that has not seen it bind yet. The caller that did not start it has
// to wait for it anyway — that is what holding the flock used to do — or the
// very next dial gets ECONNREFUSED and the client is handed an error for a

// A gopls this process started and that never became ready is this process's to
// clean up, and both halves of that are load-bearing. The record is the only
// handle anyone has on the process, so dropping it without the signal strands a
// 1-2GB index holding its port until the machine reboots; signalling without
// dropping it leaves a record naming a port with nothing on it, which its own
// start grace would spare from every sweep that came looking.
//

// Another process's start that never became ready is reported and nothing else:
// that process is the one holding the handle, and it signals and forgets its own
// child. Reaping it from here would race a starter that is still inside its own

// A gopls that never came up leaves a record naming a port with nothing on it,
// and its own grace would spare that record from the next sweep. ensure drops
// it instead — and drops only it.
//
// The successor row is the one that matters: allocatePort hands the next start
// the same port, so a record with this worktree and this port can already name
// somebody else's live gopls by the time forget runs. Matching the pair would

// failingWriter is a client that stopped reading — a closed pipe on the other
// end of stdout, which is what list writes to.
type failingWriter struct{ after int }

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.after <= 0 {
		return 0, io.ErrClosedPipe
	}
	w.after--
	return len(p), nil
}

// A client that stopped reading must not take the failure quietly: list is how
// a person finds the stranded 1-2GB indexes this tool exists to reap, and a
// truncated listing reported as success is one they would never come looking
// for again. Both writes are rows, because the header and the records are two
// separate calls and only the second is in a loop that could swallow it.
func TestListReportsAWriterThatStoppedReading(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		after int
	}{
		{name: "header", after: 0},
		{name: "record", after: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newTestManager(t)
			live := record{Worktree: "/repo/live", Port: 62001, PID: 11}
			mustWriteMap(t, m.mapPath, []record{live})

			err := writeRecords(&failingWriter{after: tc.after}, []record{live})
			require.ErrorIsf(t, err, io.ErrClosedPipe, "list() = %v for a writer that stopped reading, want its error", err)
		})
	}
}

// A map file that cannot be read is not the same as one that is not there yet,
// and the difference decides whether this tool may write. An unreadable file
// still names live servers, so treating it as empty would allocate their ports
// to somebody else and lose every record it holds on the next write.
func TestReadMapReportsAFileItCannotRead(t *testing.T) {
	t.Parallel()
	// A directory, which opens and then refuses to be read — the closest thing
	// to an unreadable file that does not depend on running as an unprivileged
	// user, since root reads a 0000 file perfectly well.
	records, intact, err := readMap(t.TempDir())
	require.Errorf(t, err, "readMap() = %#v, intact %v for a path it cannot read, want an error", records, intact)
	require.Falsef(t, intact, "readMap() called a file it could not read intact, which would let the next write skip its repair")
}

// withMap hands the body's own refusal back rather than writing what it
// returned alongside it: the body is what decides the map's next contents, and
// one that failed has not decided anything.
func TestWithRecordsWritesNothingWhenTheBodyRefuses(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	stored := record{Worktree: "/repo/live", Port: 62001, PID: 11}
	mustWriteMap(t, m.mapPath, []record{stored})

	refused := errors.New("no port left")

	got, err := m.withMap(t.Context(), func([]record) ([]record, error) { return nil, refused })
	require.ErrorIsf(t, err, refused, "withMap() = %v, want the body's own error", err)
	require.Nilf(t, got, "withMap() = %#v alongside an error, want nothing", got)
	wantRecords(t, m.mapPath, "withMap wrote what a failed body returned", stored)
}

func TestStatusMeasuresWithoutChangingRegistry(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	cmd := startFakeGopls(t, firstPort)
	r := record{Worktree: "/repo/status", PID: cmd.Process.Pid, Port: firstPort, Terminating: true}
	mustWriteMap(t, m.mapPath, []record{r})
	before, err := os.ReadFile(m.mapPath)
	require.NoError(t, err)
	var out bytes.Buffer
	err = m.status(t.Context(), &out)
	require.NoError(t, err)
	var report struct {
		RecordedServers int
		RegistryIntact  bool
		Servers         []serverUsage
	}
	err = json.Unmarshal(out.Bytes(), &report)
	require.NoError(t, err)
	require.Falsef(t, report.RecordedServers != 1 || !report.RegistryIntact || len(report.Servers) != 1, "bad status: %s", &out)
	usage := report.Servers[0]
	require.Falsef(t, usage.Identity != "matched" || usage.RSSKiB == nil || *usage.RSSKiB < 0 || usage.ActiveClients != nil, "bad process accounting: %s", &out)
	after, err := os.ReadFile(m.mapPath)
	require.NoError(t, err)
	require.Truef(t, bytes.Equal(before, after), "status mutated registry")
	wantRunning(t, cmd, "status signalled the server")
	appendToMap(t, m.mapPath, "broken record\n")
	corrupt, err := os.ReadFile(m.mapPath)
	require.NoError(t, err)
	out.Reset()
	require.Error(t, m.status(t.Context(), &out))
	require.Empty(t, out.String(), "corrupt registry produced a partial process report")
	after, err = os.ReadFile(m.mapPath)
	require.NoError(t, err)
	require.Equal(t, corrupt, after, "status silently repaired corrupt process reservations")
}

func TestParseOpenFiles(t *testing.T) {
	t.Parallel()
	got, err := parseOpenFiles("p10\nfcwd\nftxt\nf0\np11\nf3\nfmem\n")
	require.NoError(t, err)
	want := map[int]int{10: 3, 11: 2}
	require.Truef(t, maps.Equal(got, want), "parseOpenFiles = %v, want %v", got, want)
	for _, malformed := range []string{"p10\nf0\nnoise\n", "p\nf9\n", "p-1\nf0\n", "f0\n", "p1\nf\n"} {
		got, err := parseOpenFiles(malformed)
		require.Error(t, err)
		require.Nil(t, got)
	}
}

func TestCountOpenFilesToleratesMissingPID(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("lsof"); err != nil {
		t.Skip("lsof not installed")
	}
	got, err := countOpenFiles(t.Context(), []record{{PID: os.Getpid()}, {PID: 2147483647}})
	require.Falsef(t, err != nil || got[os.Getpid()] == 0, "countOpenFiles = %v, %v; want this process counted", got, err)
}

func TestParseOpenFilesRejectsDescriptorErrors(t *testing.T) {
	t.Parallel()
	for _, fd := range []string{"err", "NOFD"} {
		t.Run(fd, func(t *testing.T) {
			t.Parallel()
			got, err := parseOpenFiles("p123\nf1\nf" + fd + "\np456\nf0\n")
			require.Error(t, err)
			require.Nil(t, got, "descriptor error must invalidate the whole census")
		})
	}
}

func censusCommand(t *testing.T, name, script string) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+script), 0o700))
	t.Setenv("PATH", dir)
}

func TestCountOpenFilesRejectsMalformedSnapshot(t *testing.T) {
	censusCommand(t, "lsof", "printf 'p123\\nf0\\ninvalid\\n'\n")
	got, err := countOpenFiles(t.Context(), []record{{PID: 123}})
	require.Error(t, err)
	require.Nil(t, got, "a valid prefix must not become a complete measurement")
}

func TestGuardianSnapshotRequiresObserverIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, output string
		valid        bool
	}{
		{name: "empty"},
		{name: "blank rows", output: " \n\t\n"},
		{name: "unrelated process", output: "1 0 1\n"},
		{name: "observer", output: strconv.Itoa(os.Getpid()) + " " + strconv.Itoa(os.Getppid()) + " " + strconv.Itoa(syscall.Getpgrp()) + "\n", valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GPM_TEST_PS_OUTPUT", tc.output)
			censusCommand(t, "ps", "printf '%s' \"$GPM_TEST_PS_OUTPUT\"\n")
			rows, err := guardianSnapshot()
			if tc.valid {
				require.NoError(t, err)
				require.Len(t, rows, 1)
			} else {
				require.Error(t, err, "missing observer cannot prove group absence")
				require.Nil(t, rows)
			}
		})
	}
}

func TestCountOpenFilesAcceptsValidMissingPIDSnapshot(t *testing.T) {
	for _, output := range []string{"", "p123\\nfcwd\\nftxt\\nf0\\nfmem\\n"} {
		t.Run(output, func(t *testing.T) {
			censusCommand(t, "lsof", "printf '"+output+"'\nexit 1\n")
			got, err := countOpenFiles(t.Context(), []record{{PID: 123}, {PID: 2147483647}})
			require.NoError(t, err)
			if output == "" {
				require.Empty(t, got)
			} else {
				require.Equal(t, map[int]int{123: 4}, got)
			}
		})
	}
}

func TestStatusRejectsFailedProcessSnapshot(t *testing.T) {
	censusCommand(t, "ps", "printf '123 42 /tmp/gopls mcp -listen 127.0.0.1:61100\\n'\nexit 2\n")
	m := newTestManager(t)
	r := record{PID: 123, Port: firstPort, Worktree: "/fixture"}
	mustWriteMap(t, m.mapPath, []record{r})
	measured := false
	m.openFiles = func(context.Context, []record) (map[int]int, error) {
		measured = true
		return map[int]int{r.PID: 7}, nil
	}
	var out bytes.Buffer
	require.NoError(t, m.status(t.Context(), &out))
	require.True(t, measured, "status must attempt file measurement")
	var got struct{ Servers []serverUsage }
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	require.Len(t, got.Servers, 1)
	require.Equal(t, "unknown", got.Servers[0].Identity)
	require.Nil(t, got.Servers[0].RSSKiB)
	require.Nil(t, got.Servers[0].OpenFiles)
	wantRecords(t, m.mapPath, "status changed its registry", r)
}

// The fake ps exits while a finite descendant retains its output pipe. Real
// processes stay outside synctest, and the marker keeps fixture state until
// the descendant finishes even when the deadline assertion fails.
func TestGuardianSnapshotBoundsInheritedOutputPipe(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "holder-exited")
	t.Setenv("GPM_CENSUS_HOLDER_EXIT", marker)
	censusCommand(t, "ps", "(/bin/sleep 3; : > \"$GPM_CENSUS_HOLDER_EXIT\") &\nexit 0\n")
	defer func() {
		require.Eventually(t, func() bool { _, err := os.Stat(marker); return err == nil }, 5*time.Second, 10*time.Millisecond)
	}()
	start := time.Now()
	rows, err := guardianSnapshot()
	require.Error(t, err)
	require.Nil(t, rows)
	require.Less(t, time.Since(start), time.Second)
}

func TestProbeDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var reached atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
	}))
	defer target.Close()
	listener, port := listenInAllocationRange(t, t.Name())
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, target.URL, http.StatusTemporaryRedirect)
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()
	alive, conclusive := endpointProbe(t.Context(), port)
	require.Zero(t, reached.Load(), "readiness probe left its recorded endpoint")
	require.False(t, alive)
	require.False(t, conclusive, "a redirect is not proof that the shared process is gone")
}

func TestProcessIdentityRejectsSubstringMatches(t *testing.T) {
	t.Parallel()
	for _, command := range []string{
		"gopls mcp -listen 127.0.0.1:611001",
		"not-gopls mcp -listen 127.0.0.1:61100",
		"editor --log=/tmp/gopls --server=127.0.0.1:61100",
		"gopls other-command 127.0.0.1:61100",
	} {
		require.False(t, matchesGopls(command, firstPort), "unrelated process matched: %s", command)
	}
	for _, command := range []string{"gopls mcp -listen 127.0.0.1:61100", "/usr/local/bin/gopls mcp -listen 127.0.0.1:61100"} {
		require.True(t, matchesGopls(command, firstPort))
	}
}

func TestProcessFixtureCleanupReapsItsChild(t *testing.T) {
	t.Parallel()
	var pid int
	t.Run("fixture", func(t *testing.T) {
		pid = startFakeGopls(t, firstPort).Process.Pid
	})
	require.Eventually(t, func() bool {
		return syscall.Kill(pid, 0) == syscall.ESRCH
	}, time.Second, time.Millisecond, "fixture left a child process or zombie behind")
}

const subprocessFixtureMarker = "gopls-manager-bounded-output-fixture"

func subprocessFixtureArgs(mode string, args ...string) []string {
	return append([]string{"-test.run=^TestBoundedCommandFixture$", "--", subprocessFixtureMarker, mode}, args...)
}

func TestBoundedCommandFixture(t *testing.T) {
	marker := slices.Index(os.Args, subprocessFixtureMarker)
	if marker < 0 {
		return
	}
	args := os.Args[marker+1:]
	require.NotEmpty(t, args)
	switch args[0] {
	case "env":
		require.Len(t, args, 3)
		_, _ = fmt.Fprint(os.Stdout, os.Getenv(args[1]), "\n", os.Getenv(args[2]))
		os.Exit(0)
	case "emit":
		require.Len(t, args, 4)
		stdout, err := strconv.Atoi(args[1])
		require.NoError(t, err)
		stderr, err := strconv.Atoi(args[2])
		require.NoError(t, err)
		exit, err := strconv.Atoi(args[3])
		require.NoError(t, err)
		_, _ = io.WriteString(os.Stdout, strings.Repeat("o", stdout))
		_, _ = io.WriteString(os.Stderr, strings.Repeat("e", stderr))
		os.Exit(exit)
	case "flood":
		require.Len(t, args, 3)
		require.NoError(t, os.WriteFile(args[2], []byte(strconv.Itoa(os.Getpid())), 0o600))
		stream := os.Stdout
		if args[1] == "stderr" {
			stream = os.Stderr
		}
		chunk := strings.Repeat("x", 16<<10)
		for {
			if _, err := io.WriteString(stream, chunk); err != nil {
				os.Exit(0)
			}
		}
	case "sleep":
		require.Len(t, args, 2)
		require.NoError(t, os.WriteFile(args[1], []byte(strconv.Itoa(os.Getpid())), 0o600))
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "hold-pipe":
		require.Len(t, args, 2)
		child := exec.Command(os.Args[0], subprocessFixtureArgs("sleep", args[1])...)
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		require.NoError(t, child.Start())
		require.Eventually(t, func() bool {
			data, err := os.ReadFile(args[1])
			return err == nil && len(data) > 0
		}, 2*time.Second, time.Millisecond, "fixture descendant did not become ready")
		os.Exit(0)
	default:
		t.Fatalf("unknown subprocess fixture mode %q", args[0])
	}
}

func TestBoundedCommandOutputEnvironmentIsolation(t *testing.T) {
	const inherited = "GOPLS_MANAGER_TEST_CAPTURE_INHERITED"
	const explicit = "GOPLS_MANAGER_TEST_CAPTURE_EXPLICIT"
	t.Setenv(inherited, "parent-only")
	t.Setenv(explicit, "")
	args := subprocessFixtureArgs("env", inherited, explicit)
	out, err := boundedCommandOutput(t.Context(), 64, os.Args[0], args...)
	require.NoError(t, err)
	require.Equal(t, "parent-only\n", string(out), "nil environment must preserve inherited semantics")
	out, err = boundedCommandOutputEnv(t.Context(), []string{explicit + "=child-only"}, 64, os.Args[0], args...)
	require.NoError(t, err)
	require.Equal(t, "\nchild-only", string(out), "explicit environment must exclude unlisted parent values")
	out, err = boundedCommandOutputEnv(t.Context(), []string{}, 64, os.Args[0], args...)
	require.NoError(t, err)
	require.Equal(t, "\n", string(out), "an explicitly empty environment must remain distinct from nil")
}

func TestBoundedCommandOutputBoundariesAndExitStatus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                 string
		stdout, stderr, exit int
		overflow             bool
	}{
		{name: "empty"},
		{name: "exact stdout", stdout: 64},
		{name: "stdout overflow", stdout: 65, overflow: true},
		{name: "exact stderr", stdout: 1, stderr: maxCommandStderrBytes},
		{name: "stderr overflow", stdout: 1, stderr: maxCommandStderrBytes + 1, overflow: true},
		{name: "ordinary exit one", stdout: 64, stderr: 32, exit: 1},
		{name: "overflow exiting one", stdout: 65, exit: 1, overflow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			out, err := boundedCommandOutput(ctx, 64, os.Args[0], subprocessFixtureArgs("emit",
				strconv.Itoa(tc.stdout), strconv.Itoa(tc.stderr), strconv.Itoa(tc.exit))...)
			var exit *exec.ExitError
			if tc.overflow {
				require.ErrorIs(t, err, errCommandOutputLimit)
				require.False(t, errors.As(err, &exit), "overflow cannot qualify for the lsof exit-1 exception")
				require.Nil(t, out)
				return
			}
			require.Equal(t, strings.Repeat("o", tc.stdout), string(out))
			if tc.exit == 0 {
				require.NoError(t, err)
				return
			}
			require.ErrorAs(t, err, &exit)
			require.Equal(t, tc.exit, exit.ExitCode())
			require.Equal(t, strings.Repeat("e", tc.stderr), string(exit.Stderr))
			require.LessOrEqual(t, cap(exit.Stderr), maxCommandStderrBytes)
		})
	}
}

func TestBoundedCommandOutputOverflowKillsAndReaps(t *testing.T) {
	t.Parallel()
	for _, stream := range []string{"stdout", "stderr"} {
		t.Run(stream, func(t *testing.T) {
			t.Parallel()
			pidPath := filepath.Join(t.TempDir(), "pid")
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			started := time.Now()
			out, err := boundedCommandOutput(ctx, 64, os.Args[0], subprocessFixtureArgs("flood", stream, pidPath)...)
			require.ErrorIs(t, err, errCommandOutputLimit)
			require.Nil(t, out)
			require.Less(t, time.Since(started), 2*time.Second, "overflow waited for the caller's ten-second deadline")
			pid := subprocessFixturePID(t, pidPath)
			require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "the direct child was not reaped")
		})
	}
}

func TestBoundedCommandOutputParentCancellation(t *testing.T) {
	t.Parallel()
	pidPath := filepath.Join(t.TempDir(), "pid")
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	type result struct {
		output []byte
		err    error
	}
	done := make(chan result, 1)
	go func() {
		out, err := boundedCommandOutput(ctx, 64, os.Args[0], subprocessFixtureArgs("sleep", pidPath)...)
		done <- result{out, err}
	}()
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(pidPath)
		return err == nil && len(data) > 0
	}, 2*time.Second, time.Millisecond)
	pid := subprocessFixturePID(t, pidPath)
	cancel()
	select {
	case result := <-done:
		require.ErrorIs(t, result.err, context.Canceled)
		require.Nil(t, result.output)
		require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "cancelled subprocess did not return")
	}
}

func TestBoundedCommandOutputWaitDelayClosesDescendantPipes(t *testing.T) {
	t.Parallel()
	pidPath := filepath.Join(t.TempDir(), "descendant-pid")
	t.Cleanup(func() {
		// The application owns only the direct child. Assert its bounded return
		// before this harness cleanup terminates the fixture's surviving child.
		if data, err := os.ReadFile(pidPath); err == nil {
			if pid, err := strconv.Atoi(string(data)); err == nil {
				_ = syscall.Kill(pid, syscall.SIGKILL)
				require.Eventually(t, func() bool {
					return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
				}, 2*time.Second, time.Millisecond, "fixture descendant was not reaped after harness cleanup")
			}
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	started := time.Now()
	out, err := boundedCommandOutput(ctx, 64, os.Args[0], subprocessFixtureArgs("hold-pipe", pidPath)...)
	require.ErrorIs(t, err, exec.ErrWaitDelay)
	require.Nil(t, out)
	require.Less(t, time.Since(started), 3*time.Second, "descendant-held pipes outlived WaitDelay")
	pid := subprocessFixturePID(t, pidPath)
	require.NoError(t, syscall.Kill(pid, 0), "the application must return before harness descendant cleanup")
}

func subprocessFixturePID(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	pid, err := strconv.Atoi(string(data))
	require.NoError(t, err)
	require.Positive(t, pid)
	return pid
}

func TestCommandOutputBufferCapacityAndOverflow(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	buffer := commandOutputBuffer{limit: 8, cancel: cancel}
	for range 8 {
		n, err := buffer.Write([]byte{'x'})
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.LessOrEqual(t, cap(buffer.data), buffer.limit, "capture allocation exceeded its cap")
	}
	n, err := buffer.Write([]byte{'x'})
	require.ErrorIs(t, err, errCommandOutputLimit)
	require.Zero(t, n)
	require.Len(t, buffer.data, 8)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.True(t, buffer.overflow)
}

func TestBoundedCommandOutputInvalidLimitAndMissingExecutable(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{0, -1} {
		out, err := boundedCommandOutput(t.Context(), limit, "unreachable")
		require.ErrorContains(t, err, "must be positive")
		require.Nil(t, out)
	}
	out, err := boundedCommandOutput(t.Context(), 64, filepath.Join(t.TempDir(), "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, out)
}

func TestBoundedCommandOutputChurnReleasesResources(t *testing.T) {
	// Global FD/goroutine measurements must not overlap parallel tests.
	run := func() {
		out, err := boundedCommandOutput(t.Context(), 64, os.Args[0], subprocessFixtureArgs("emit", "8", "0", "0")...)
		require.NoError(t, err)
		require.Equal(t, "oooooooo", string(out))
	}
	run() // warm runtime polling and subprocess paths before the baseline.
	countFDs := func() (int, error) {
		entries, err := os.ReadDir("/dev/fd")
		return len(entries), err
	}
	initialFDs, err := countFDs()
	require.NoError(t, err)
	initialGoroutines := runtime.NumGoroutine()
	for batch := range 3 {
		for range 4 {
			run()
			out, err := boundedCommandOutput(t.Context(), 64, os.Args[0], subprocessFixtureArgs("emit", "65", "0", "0")...)
			require.ErrorIs(t, err, errCommandOutputLimit)
			require.Nil(t, out)
		}
		require.Eventually(t, func() bool {
			fds, err := countFDs()
			return err == nil && fds <= initialFDs && runtime.NumGoroutine() <= initialGoroutines+1
		}, time.Second, time.Millisecond, fmt.Sprintf("batch %d retained process pipes/copying goroutines", batch))
	}
}
