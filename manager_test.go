package main

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestBasePortIsStableAndInRange(t *testing.T) {
	t.Parallel()
	const worktree = "/Users/example/src/repository"

	got := basePort(worktree)
	if got < firstPort || got > lastPort {
		t.Fatalf("basePort(%q) = %d, outside %d-%d", worktree, got, firstPort, lastPort)
	}
	if again := basePort(worktree); again != got {
		t.Fatalf("basePort(%q) changed from %d to %d", worktree, got, again)
	}
}

func TestAllocatePortProbesPastMappedAndOccupiedPorts(t *testing.T) {
	t.Parallel()
	worktree := "/repo/a"
	start := basePort(worktree)
	records := []record{{Worktree: "/repo/b", Port: start, PID: 10}}
	unavailable := func(port int) bool { return port == nextPort(start) }

	got, err := allocatePort(worktree, records, unavailable)
	if err != nil {
		t.Fatal(err)
	}
	want := nextPort(nextPort(start))
	if got != want {
		t.Fatalf("allocatePort() = %d, want %d", got, want)
	}
}

// Every record must be offered to alive(), which kills what it rejects. A
// second record for a worktree is a second gopls, and skipping it would leave
// nobody holding its only handle.
func TestCleanRecordsOffersEveryRecordAndDropsOnlyTheDead(t *testing.T) {
	t.Parallel()
	records := []record{
		{Worktree: "/repo/live", Port: 62001, PID: 11},
		{Worktree: "/repo/dead", Port: 62002, PID: 12},
		{Worktree: "/repo/live", Port: 62003, PID: 13},
	}
	var offered atomic.Int64 // the probes run at once, so this count is shared
	alive := func(r record) bool {
		offered.Add(1)
		return r.Port != 62002
	}
	want := []record{records[0], records[2]}

	got := cleanRecords(records, alive)
	if offered.Load() != int64(len(records)) {
		t.Fatalf("alive() saw %d of %d records", offered.Load(), len(records))
	}
	if !slices.Equal(got, want) {
		t.Fatalf("cleanRecords() = %#v, want %#v", got, want)
	}
}

// The probes must run at once — see cleanRecords for what that buys. Under the
// fake clock the difference is exact rather than a race: three probes of one
// tick each cost one tick together, and three apart.
func TestCleanRecordsProbesEveryRecordAtOnce(t *testing.T) {
	bubble(t, func(t *testing.T) {
		const probe = time.Second
		records := []record{
			{Worktree: "/repo/a", Port: 62001, PID: 11},
			{Worktree: "/repo/b", Port: 62002, PID: 12},
			{Worktree: "/repo/c", Port: 62003, PID: 13},
		}

		start := time.Now()
		cleanRecords(records, func(record) bool {
			time.Sleep(probe)
			return true
		})
		if elapsed := time.Since(start); elapsed != probe {
			t.Fatalf("%d probes of %s each took %s, want them run at once", len(records), probe, elapsed)
		}
	})
}

// mustWriteMap and mustReadMap keep the map file itself out of the way: every
// test below either seeds records or reads them back, and for all but
// TestReadMapSkipsUnparseableLines — which is about the read failing or not —
// neither step is what the test is asking about.
func mustWriteMap(t *testing.T, path string, records []record) {
	t.Helper()
	if err := writeMap(path, records); err != nil {
		t.Fatal(err)
	}
}

// appendToMap puts text after whatever the map already holds, which is the only
// way these tests can produce a file readMap will drop lines from: writeMap can
// only write one it would accept entirely.
func appendToMap(t *testing.T, path string, text string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(text); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func mustReadMap(t *testing.T, path string) []record {
	t.Helper()
	records, _, err := readMap(path)
	if err != nil {
		t.Fatal(err)
	}
	return records
}

// wantRecords reads the map back and insists it holds exactly want, in order.
// The file is the durable claim, so most tests here assert on it rather than on
// what a call happened to return.
func wantRecords(t *testing.T, path string, whatWouldBeWrong string, want ...record) {
	t.Helper()
	if got := mustReadMap(t, path); !slices.Equal(got, want) {
		t.Fatalf("%s: records = %#v, want %#v", whatWouldBeWrong, got, want)
	}
}

// newTestManager returns a manager over a map file of its own, which is what
// every test below reads and writes through.
func newTestManager(tb testing.TB) manager {
	tb.Helper()
	return manager{mapPath: filepath.Join(tb.TempDir(), "gopls-ports.map"), alive: recordAlive, ready: awaitReady}
}

// newStubbedManager adds what the claimPort tests need on top: a gopls stub on
// PATH, and a worktree to spawn it in — gopls is started with cwd set to it, so
// it must exist. Its callers cannot be parallel, because PATH is process-wide.
func newStubbedManager(t *testing.T) (m manager, worktree string) {
	t.Helper()
	stubGopls(t)
	return newTestManager(t), t.TempDir()
}

func TestMapRoundTripEscapesPaths(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	// One record per line, so a path containing a newline has to survive as one.
	want := []record{{Worktree: "/repo/with spaces\tand\na quote\"", Port: 63001, PID: 42}}

	mustWriteMap(t, m.mapPath, want)
	wantRecords(t, m.mapPath, "the map did not round-trip", want...)
	info, err := os.Stat(m.mapPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("map permissions = %o, want 600", info.Mode().Perm())
	}
}

// idleAs runs a process whose command line is exactly args, which is what
// isOurGopls() inspects.
//
// The trailing ";:" matters: given a single simple command, sh execs it in
// place and the process loses this argv along with the shell.
func idleAs(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sh", append([]string{"-c", "sleep 10; :"}, args...)...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
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
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("%s: %v", whatWouldBeWrong, err)
	}
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
func listenInAllocationRange(t *testing.T, worktree string) (net.Listener, int) {
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
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener, port
}

// listenLocal holds an ephemeral local port for as long as the test runs.
func listenLocal(tb testing.TB) (net.Listener, int) {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
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
func livePort(tb testing.TB) int {
	tb.Helper()
	listener, port := listenLocal(tb)
	serveEndpoint(tb, listener, 0)
	return port
}

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
func deadPort(tb testing.TB) int {
	tb.Helper()
	listener, port := listenLocal(tb)
	if err := listener.Close(); err != nil {
		tb.Fatal(err)
	}
	return port
}

// refusedPort is the table row for "nothing is listening": a dead port, and so
// no answered check to make, since a refusal is conclusive on its own.
func refusedPort(t *testing.T) (int, func() bool) { return deadPort(t), nil }

// answeringPort returns a port held by something that answers, but not the way
// our endpoint does, along with a way to ask whether the probe actually reached
// it. Every caller cares: a probe that merely timed out reaches the same verdict
// by the other route, so without the check the test would pass unchanged on any
// run slow enough that nothing answered inside probeClient's 500ms.
func answeringPort(tb testing.TB, status int) (port int, answered func() bool) {
	tb.Helper()
	var hit atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit.Store(true)
		http.Error(w, http.StatusText(status), status)
	}))
	tb.Cleanup(server.Close)
	return server.Listener.Addr().(*net.TCPAddr).Port, hit.Load
}

// wantSignalled fails unless cmd was terminated by SIGTERM. The Wait also
// reaps, so what it asserts on is the signal rather than the exit.
func wantSignalled(t *testing.T, cmd *exec.Cmd, whatWouldBeWrong string) {
	t.Helper()
	err := cmd.Wait()
	if err == nil {
		t.Fatal(whatWouldBeWrong)
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGTERM {
		t.Fatalf("server exited with %v, want SIGTERM", err)
	}
}

// verdict is what must have become of the process holding a record's pid once
// recordAlive has had its say.
type verdict int

const (
	// spared is every verdict short of "provably dead and ours" — see
	// wantRunning for what getting it wrong costs.
	spared verdict = iota
	// signalled: the record is the only handle anyone has on its gopls, so
	// dropping one must terminate the process. Otherwise a 1-2GB index survives
	// unreferenced, still holding its port, until the machine reboots.
	signalled
	// alreadyGone: the process died before the call, so there is nothing to say
	// about what happened to it.
	alreadyGone
)

// The whole of recordAlive's decision: what is on the port, what holds the pid,
// and what the timestamp says — against the verdict and, just as much, what
// becomes of the process. The two halves belong in one table because they are
// one decision: every row that spares a server is a row where killing it would
// be the expensive mistake, and the one row that kills is the one where not
// killing strands an index for the life of the machine.
func TestRecordAlive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// port yields the port under test and, where something answers there, a
		// way to ask whether the probe actually reached it — see answeringPort.
		// nil when nothing is listening.
		port func(t *testing.T) (port int, answered func() bool)
		// proc holds the record's pid.
		proc func(t *testing.T, port int) *exec.Cmd
		// startedAt stamps the record; nil leaves it unset, which is no grace.
		startedAt func() int64
		wantAlive bool
		want      verdict
	}{
		{
			name: "a process with no MCP endpoint is killed",
			port: refusedPort,
			proc: startFakeGopls,
			want: signalled,
		},
		{
			// Every caller sweeps every record, so a gopls too busy indexing to
			// answer in time must survive a probe run on some other worktree's
			// behalf. Listening but never accepting: the connection sits in the
			// backlog, so the probe hangs to its deadline instead of being refused.
			name: "a server that only timed out is spared",
			port: func(t *testing.T) (int, func() bool) {
				_, port := listenLocal(t)
				return port, nil
			},
			proc:      startFakeGopls,
			wantAlive: true,
			want:      spared,
		},
		{
			// An answer we did not want is not a death certificate either:
			// whatever sent it is holding that port, and if it is our gopls —
			// briefly overloaded, or a newer one serving MCP from somewhere else —
			// killing it costs the same full re-index as killing a busy one.
			// Retrying would not help: a server answering wrongly answers wrongly
			// again.
			name: "a server answering 503 is spared",
			port: func(t *testing.T) (int, func() bool) {
				return answeringPort(t, http.StatusServiceUnavailable)
			},
			proc:      startFakeGopls,
			wantAlive: true,
			want:      spared,
		},
		{
			// Sparing a server on an inconclusive probe must not make the record
			// immortal: nothing revisits the verdict, so a record kept on that
			// alone would send every later ensure to a port its gopls no longer
			// holds. A pid that is not ours — after a reboot recycled it, and
			// something else took the port — is dropped on identity, without a
			// signal, and the worktree gets a fresh port next time.
			name: "an inconclusive record that is no longer ours is dropped unsignalled",
			port: func(t *testing.T) (int, func() bool) { return answeringPort(t, http.StatusNotFound) },
			proc: func(t *testing.T, _ int) *exec.Cmd { return idleAs(t, "some-unrelated-process") },
			want: spared,
		},
		{
			// What StartedAt buys a record on a port that refuses every probe —
			// the one conclusive verdict recordAlive acts on. ensure releases the
			// map lock before waiting for readiness, so a gopls between fork and
			// bind is reachable by another process's sweep, and the grace is what
			// stops that sweep shooting a server starting exactly as it should.
			name:      "a gopls still inside its start grace is spared",
			port:      refusedPort,
			proc:      startFakeGopls,
			startedAt: func() int64 { return time.Now().Unix() },
			wantAlive: true,
			want:      spared,
		},
		{
			// The grace is for a process that exists and has not bound its port
			// yet. A start that crashed instead is not that, and sparing it would
			// answer every ensure for its worktree with a dead port until the
			// timestamp expired.
			name: "a start that died inside its grace is not spared",
			port: refusedPort,
			proc: func(t *testing.T, port int) *exec.Cmd {
				cmd := startFakeGopls(t, port)
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				_ = cmd.Wait() // reaped, so the pid is gone rather than a zombie
				return cmd
			},
			startedAt: func() int64 { return time.Now().Unix() },
			want:      alreadyGone,
		},
		{
			// The map is meant to be editable by hand, and this field is the one
			// way to make a record immortal: a timestamp that has not happened yet
			// buys no grace.
			name:      "a record dated in the future gets no grace",
			port:      refusedPort,
			proc:      func(t *testing.T, _ int) *exec.Cmd { return idleAs(t, "some-unrelated-process") },
			startedAt: func() int64 { return time.Now().Add(time.Hour).Unix() },
			want:      spared,
		},
		// A record proved dead is still only a pid, and the pid may have been
		// recycled since — after a reboot, every pid in the map has been. The
		// process most likely to have taken it and still answer to "gopls" is the
		// one the user's editor runs, which serves no listen address of ours. Such
		// a record is forgotten, but never signalled.
		{
			name: "not a gopls at all",
			port: refusedPort,
			proc: func(t *testing.T, _ int) *exec.Cmd { return idleAs(t, "sleep") },
			want: spared,
		},
		{
			name: "a gopls we did not run",
			port: refusedPort,
			proc: func(t *testing.T, _ int) *exec.Cmd { return idleAs(t, goplsBinary, "serve") },
			want: spared,
		},
		{
			name: "a gopls on another port",
			port: refusedPort,
			proc: func(t *testing.T, port int) *exec.Cmd {
				return idleAs(t, goplsBinary, "mcp", "-listen", mcpAddress(port+1))
			},
			want: spared,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			port, answered := test.port(t)
			cmd := test.proc(t, port)

			r := record{Worktree: "/repo/" + test.name, Port: port, PID: cmd.Process.Pid}
			if test.startedAt != nil {
				r.StartedAt = test.startedAt()
			}
			if got := recordAlive(r); got != test.wantAlive {
				t.Errorf("recordAlive() = %v, want %v", got, test.wantAlive)
			}
			if answered != nil && !answered() {
				t.Fatal("the probe never reached the server, so this row's verdict was reached by the wrong route")
			}
			switch test.want {
			case spared:
				wantRunning(t, cmd, "a server that should have been spared was signalled")
			case signalled:
				wantSignalled(t, cmd, "the server declared dead was left running")
			case alreadyGone:
			}
		})
	}
}

// Every entry point reads the map, so a line this parser rejects outright used
// to leave no way to inspect or repair the file from the tool itself.
func TestReadMapSkipsUnparseableLines(t *testing.T) {
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
	if err != nil {
		t.Fatalf("readMap() failed on a damaged file: %v", err)
	}
	if !slices.Equal(got, []record{good}) {
		t.Fatalf("readMap() = %#v, want just %#v", got, good)
	}
	// The damage is only ever repaired by the next write, and withRecords skips
	// that write when the records come back unchanged — so a read that dropped
	// lines and reported the file intact would leave them there for good.
	if intact {
		t.Fatal("readMap() called a file it dropped four lines from intact, so nothing would rewrite it")
	}
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
		{name: "records unchanged but the read dropped a line", damage: "garbage\n", wantWrite: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newTestManager(t)
			// Every record alive, so the sweep hands back what it was given and the
			// records are equal in both rows — leaving the intact flag as the only
			// thing that differs between them.
			m.alive = func(record) bool { return true }
			mustWriteMap(t, m.mapPath, []record{kept})
			if tc.damage != "" {
				appendToMap(t, m.mapPath, tc.damage)
			}
			// Far enough back that the fresh mtime a rename leaves cannot be mistaken
			// for it, whatever the filesystem's timestamp resolution.
			stale := time.Now().Add(-time.Hour)
			if err := os.Chtimes(m.mapPath, stale, stale); err != nil {
				t.Fatal(err)
			}

			if _, err := m.withRecords(func(rs []record) ([]record, error) { return rs, nil }); err != nil {
				t.Fatal(err)
			}

			info, err := os.Stat(m.mapPath)
			if err != nil {
				t.Fatal(err)
			}
			if written := !info.ModTime().Equal(stale); written != tc.wantWrite {
				t.Fatalf("withRecords() rewrote the map = %t, want %t", written, tc.wantWrite)
			}
			// Either way the file ends up saying the same thing: the row above is
			// about what it cost to get there, not about what it holds.
			wantRecords(t, m.mapPath, "the map lost the record it should have kept", kept)
		})
	}
}

// stubGopls puts a gopls on PATH that starts, ignores its arguments and never
// binds anything, which is what leaves claimPort's child provably not ready.
func stubGopls(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\nsleep 30\n"
	// Named from the constant the manager spawns and matches on: a stub under any
	// other name would be invisible to both.
	if err := os.WriteFile(filepath.Join(dir, goplsBinary), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// claimPort must not wait for the gopls it spawned. ensure does that with the
// map lock released, so a wait here would put a cold start back under the lock
// and make every other process queue behind it for up to readyTimeout.
//
// Not parallel: PATH is process-wide.
func TestClaimPortReturnsBeforeItsGoplsIsReady(t *testing.T) {
	m, worktree := newStubbedManager(t)

	start := time.Now()
	claimed, started, err := m.claimPort(worktree)
	if err != nil {
		t.Fatal(err)
	}
	if started == nil {
		t.Fatal("claimPort() started nothing for a worktree the map had no record of")
	}
	t.Cleanup(func() { _ = started.Signal(syscall.SIGTERM) })
	if elapsed := time.Since(start); elapsed >= readyTimeout {
		t.Errorf("claimPort() took %s, want a return long before the %s readiness budget", elapsed, readyTimeout)
	}

	// The reservation is what makes releasing the lock safe: it holds the port
	// and dates the record, so another process's sweep spares the starting gopls.
	records := mustReadMap(t, m.mapPath)
	if !slices.Equal(records, []record{claimed}) || claimed.PID != started.Pid {
		t.Fatalf("records = %#v, want just the claim %#v for pid %d", records, claimed, started.Pid)
	}
	if !withinStartGrace(records[0]) {
		t.Errorf("record stamped %d is outside its own start grace, want one a sweep would spare", records[0].StartedAt)
	}
}

// Several processes reach ensure for one worktree at once — worktree isolation
// produces exactly that — and only the flock stops each of them starting a
// gopls of its own. Goroutines stand in for the processes: withFileLock opens
// its own descriptor per call, so they contend the way separate processes do.
//
// Not parallel: PATH is process-wide.
func TestConcurrentClaimPortSpawnsOnce(t *testing.T) {
	m, worktree := newStubbedManager(t)

	const callers = 16
	claims := make([]record, callers)
	spawned := make([]*os.Process, callers)
	errs := make([]error, callers)
	var racing sync.WaitGroup
	for i := range callers {
		racing.Go(func() { claims[i], spawned[i], errs[i] = m.claimPort(worktree) })
	}
	racing.Wait()

	var starter *os.Process
	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d: claimPort() = %v", i, errs[i])
		}
		if claims[i] != claims[0] {
			t.Errorf("caller %d claimed %#v, want the one claim %#v they all share", i, claims[i], claims[0])
		}
		if spawned[i] == nil {
			continue
		}
		if starter != nil {
			t.Errorf("two callers both started a gopls: pids %d and %d", starter.Pid, spawned[i].Pid)
			_ = spawned[i].Signal(syscall.SIGTERM)
			continue
		}
		starter = spawned[i]
		t.Cleanup(func() { _ = starter.Signal(syscall.SIGTERM) })
	}
	if starter == nil {
		t.Fatal("no caller started a gopls, though the map held no record for the worktree")
	}

	// The map is the durable claim, and where a lost write or a second spawn
	// shows even when the returned values happen to agree.
	wantRecords(t, m.mapPath, "a write was lost or a second gopls spawned", claims[0])
}

// A record inside its start grace is vouched for without being probed at all,
// so its port can still refuse: the gopls it names was forked by another
// process that has not seen it bind yet. The caller that did not start it has
// to wait for it anyway — that is what holding the flock used to do — or the
// very next dial gets ECONNREFUSED and the client is handed an error for a
// server that was coming up perfectly.
func TestEnsureWaitsForAGoplsAnotherProcessIsStillStarting(t *testing.T) {
	t.Parallel()
	const binding = 300 * time.Millisecond
	const worktree = "/repo/starting"
	m := newTestManager(t)
	// Bound but not answering, which is what a gopls between fork and bind
	// looks like to a probe; it starts serving the endpoint only after binding.
	listener, port := listenInAllocationRange(t, worktree)
	cmd := startFakeGopls(t, port)
	mustWriteMap(t, m.mapPath, []record{{
		Worktree:  worktree,
		Port:      port,
		PID:       cmd.Process.Pid,
		StartedAt: time.Now().Unix(),
	}})

	serveEndpoint(t, listener, binding)

	start := time.Now()
	got, err := m.ensure(worktree)
	if err != nil {
		t.Fatal(err)
	}
	if got != port {
		t.Fatalf("ensure() = %d, want the recorded port %d", got, port)
	}
	if elapsed := time.Since(start); elapsed < binding {
		t.Errorf("ensure() answered after %s, before the endpoint came up at %s", elapsed, binding)
	}
}

// A gopls this process started and that never became ready is this process's to
// clean up, and both halves of that are load-bearing. The record is the only
// handle anyone has on the process, so dropping it without the signal strands a
// 1-2GB index holding its port until the machine reboots; signalling without
// dropping it leaves a record naming a port with nothing on it, which its own
// start grace would spare from every sweep that came looking.
//
// Not parallel: PATH is process-wide.
func TestEnsureSignalsAndForgetsAGoplsOfItsOwnThatNeverBecameReady(t *testing.T) {
	m, worktree := newStubbedManager(t)
	neighbour := record{Worktree: "/repo/other", Port: 62001, PID: 11}
	mustWriteMap(t, m.mapPath, []record{neighbour})
	m.alive = func(record) bool { return true }

	// The pid is read while the readiness wait is still running, which is the
	// last moment the map still names it: forget runs on the way out of the very
	// call this hook is standing in for.
	var pid int
	m.ready = func(int) error {
		for _, r := range mustReadMap(t, m.mapPath) {
			if r.Worktree == worktree {
				pid = r.PID
			}
		}
		return errors.New("never bound")
	}

	if port, err := m.ensure(worktree); err == nil {
		t.Fatalf("ensure() = %d for a gopls that never became ready, want an error", port)
	}

	if pid == 0 {
		t.Fatal("no record named the worktree while its start was still being waited for")
	}
	// Polled rather than asked once: the signal is delivered synchronously but
	// the process leaves a zombie behind until startGopls' reaper collects it,
	// and kill(pid, 0) succeeds against a zombie.
	deadline := time.Now().Add(5 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("pid %d still exists, want the gopls that never served signalled", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	wantRecords(t, m.mapPath, "ensure left the failed start in the map", neighbour)
}

// Another process's start that never became ready is reported and nothing else:
// that process is the one holding the handle, and it signals and forgets its own
// child. Reaping it from here would race a starter that is still inside its own
// readiness wait, and dropping its record would take away the only handle it has.
func TestEnsureLeavesAnotherProcessesFailedStartAlone(t *testing.T) {
	t.Parallel()
	const worktree = "/repo/theirs"
	m := newTestManager(t)
	// Inside its start grace, which is what makes ensure wait for a record it did
	// not create rather than answer with its port straight away.
	theirs := record{Worktree: worktree, Port: 62001, PID: os.Getpid(), StartedAt: time.Now().Unix()}
	mustWriteMap(t, m.mapPath, []record{theirs})
	m.alive = func(record) bool { return true }
	m.ready = func(int) error { return errors.New("never bound") }

	if port, err := m.ensure(worktree); err == nil {
		t.Fatalf("ensure() = %d for a gopls that never became ready, want an error", port)
	}

	wantRecords(t, m.mapPath, "ensure dropped a record belonging to another process's start", theirs)
}

// A gopls that never came up leaves a record naming a port with nothing on it,
// and its own grace would spare that record from the next sweep. ensure drops
// it instead — and drops only it.
//
// The successor row is the one that matters: allocatePort hands the next start
// the same port, so a record with this worktree and this port can already name
// somebody else's live gopls by the time forget runs. Matching the pair would
// orphan it.
func TestForgetDropsOnlyTheNamedRecord(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	failed := record{Worktree: "/repo/failed", Port: 62002, PID: 12, StartedAt: 1000}
	kept := []record{
		{Worktree: "/repo/other", Port: 62001, PID: 11},
		{Worktree: "/repo/failed", Port: 62003, PID: 13},                  // same worktree, another port
		{Worktree: "/repo/failed", Port: 62002, PID: 14, StartedAt: 2000}, // the successor
	}
	mustWriteMap(t, m.mapPath, []record{kept[0], failed, kept[1], kept[2]})

	if err := m.forget(failed); err != nil {
		t.Fatal(err)
	}

	wantRecords(t, m.mapPath, "forget dropped the wrong records", kept...)
}

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
			m.alive = func(record) bool { return true }

			if err := m.list(&failingWriter{after: tc.after}); !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("list() = %v for a writer that stopped reading, want its error", err)
			}
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
	if err == nil {
		t.Fatalf("readMap() = %#v, intact %v for a path it cannot read, want an error", records, intact)
	}
	if intact {
		t.Fatal("readMap() called a file it could not read intact, which would let the next write skip its repair")
	}
}

// withRecords hands the body's own refusal back rather than writing what it
// returned alongside it: the body is what decides the map's next contents, and
// one that failed has not decided anything.
func TestWithRecordsWritesNothingWhenTheBodyRefuses(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	stored := record{Worktree: "/repo/live", Port: 62001, PID: 11}
	mustWriteMap(t, m.mapPath, []record{stored})
	m.alive = func(record) bool { return true }
	refused := errors.New("no port left")

	got, err := m.withRecords(func([]record) ([]record, error) { return nil, refused })
	if !errors.Is(err, refused) {
		t.Fatalf("withRecords() = %v, want the body's own error", err)
	}
	if got != nil {
		t.Fatalf("withRecords() = %#v alongside an error, want nothing", got)
	}
	wantRecords(t, m.mapPath, "withRecords wrote what a failed body returned", stored)
}

func TestListShowsLiveRecordsAndCleansDeadRecords(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	live := record{Worktree: "/repo/live", Port: 62001, PID: 11}
	dead := record{Worktree: "/repo/dead", Port: 62002, PID: 12}
	mustWriteMap(t, m.mapPath, []record{live, dead})

	var output bytes.Buffer
	m.alive = func(r record) bool { return r == live }
	if err := m.list(&output); err != nil {
		t.Fatal(err)
	}
	const wantOutput = "PORT\tPID\tWORKTREE\n62001\t11\t/repo/live\n"
	if output.String() != wantOutput {
		t.Fatalf("list output = %q, want %q", output.String(), wantOutput)
	}
	wantRecords(t, m.mapPath, "list did not persist its sweep", live)
}
