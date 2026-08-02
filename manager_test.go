package main

import (
	"bytes"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
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
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
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

func TestMapRoundTripEscapesPaths(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "gopls-ports.map")
	// One record per line, so a path containing a newline has to survive as one.
	want := []record{{Worktree: "/repo/with spaces\tand\na quote\"", Port: 63001, PID: 42}}

	if err := writeMap(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := readMap(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("readMap() = %#v, want %#v", got, want)
	}
	info, err := os.Stat(path)
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
	return idleAs(t, "gopls", "mcp", "-listen", mcpAddress(port))
}

// deadPort returns a port nothing listens on, so a probe of it is refused.
func deadPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

// answeringPort returns a port held by something that answers, but not the way
// our endpoint does, along with a way to ask whether the probe actually reached
// it. Every caller cares: a probe that merely timed out reaches the same verdict
// by the other route, so without the check the test would pass unchanged on any
// run slow enough that nothing answered inside probeClient's 500ms.
func answeringPort(t *testing.T, status int) (port int, answered func() bool) {
	t.Helper()
	var hit atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit.Store(true)
		http.Error(w, http.StatusText(status), status)
	}))
	t.Cleanup(server.Close)
	return server.Listener.Addr().(*net.TCPAddr).Port, hit.Load
}

// A record is the only handle anyone has on its gopls, so dropping one must
// terminate the process: otherwise a 1-2GB index survives unreferenced, still
// holding its port, until the machine reboots.
func TestRecordAliveKillsTheServerItDeclaresDead(t *testing.T) {
	t.Parallel()

	port := deadPort(t)
	cmd := startFakeGopls(t, port)
	if recordAlive(record{Worktree: "/repo/x", Port: port, PID: cmd.Process.Pid}) {
		t.Fatal("recordAlive() = true for a process with no MCP endpoint")
	}
	err := cmd.Wait() // also reaps, so the assertion below is about the signal
	if err == nil {
		t.Fatal("the server declared dead was left running")
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGTERM {
		t.Fatalf("server exited with %v, want SIGTERM", err)
	}
}

// Every caller sweeps every record, so a gopls too busy indexing to answer in
// time must survive a probe run on some other worktree's behalf. Killing it
// costs a full re-index of a tree nobody asked about.
func TestRecordAliveSparesAServerThatIsMerelyBusy(t *testing.T) {
	t.Parallel()

	// Listening but never accepting: the connection sits in the backlog, so the
	// probe hangs to its deadline instead of being refused.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	port := listener.Addr().(*net.TCPAddr).Port
	cmd := startFakeGopls(t, port)
	if !recordAlive(record{Worktree: "/repo/busy", Port: port, PID: cmd.Process.Pid}) {
		t.Error("recordAlive() = false for a server that only timed out")
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("the busy server was killed: %v", err)
	}
}

// An answer we did not want is not a death certificate either: whatever sent it
// is holding that port, and if it is our gopls — briefly overloaded, or a newer
// one serving MCP from somewhere else — killing it costs the same full re-index
// as killing a busy one. Retrying would not help: a server answering wrongly
// answers wrongly again.
func TestRecordAliveSparesAServerAnsweringSomethingElse(t *testing.T) {
	t.Parallel()

	port, answered := answeringPort(t, http.StatusServiceUnavailable)
	cmd := startFakeGopls(t, port)
	if !recordAlive(record{Worktree: "/repo/loud", Port: port, PID: cmd.Process.Pid}) {
		t.Error("recordAlive() = false for a server that answered 503")
	}
	if !answered() {
		t.Fatal("the probe never reached the server, so the 503 verdict was never exercised")
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("the server was killed for answering 503: %v", err)
	}
}

// Sparing a server on an inconclusive probe must not make the record immortal:
// nothing revisits the verdict, so a record kept on that alone would send every
// later ensure to a port its gopls no longer holds. A pid that is not ours —
// after a reboot recycled it, and something else took the port — is dropped on
// identity, without a signal, and the worktree gets a fresh port next time.
func TestRecordAliveDropsAnInconclusiveRecordThatIsNoLongerOurs(t *testing.T) {
	t.Parallel()

	port, answered := answeringPort(t, http.StatusNotFound)
	cmd := idleAs(t, "some-unrelated-process")
	if recordAlive(record{Worktree: "/repo/stale", Port: port, PID: cmd.Process.Pid}) {
		t.Error("recordAlive() = true for a pid that is no longer our gopls")
	}
	if !answered() {
		t.Fatal("the probe never reached the server, so the record went for the wrong reason")
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("the unrelated process was signalled: %v", err)
	}
}

// A record proved dead is still only a pid, and the pid may have been recycled
// since — after a reboot, every pid in the map has been. The process most likely
// to have taken it and still answer to "gopls" is the one the user's editor
// runs, which serves no listen address of ours. Such a record is forgotten, but
// never signalled.
func TestRecordAliveSparesAProcessThatIsNotOurGopls(t *testing.T) {
	t.Parallel()

	port := deadPort(t)
	strangers := map[string][]string{
		"not a gopls at all":      {"sleep"},
		"a gopls we did not run":  {"gopls", "serve"},
		"a gopls on another port": {"gopls", "mcp", "-listen", mcpAddress(port + 1)},
	}
	for name, args := range strangers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cmd := idleAs(t, args...)

			if recordAlive(record{Worktree: "/repo/stranger", Port: port, PID: cmd.Process.Pid}) {
				t.Errorf("recordAlive() = true for %s on a refused port", name)
			}
			if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("recordAlive() signalled %s: %v", name, err)
			}
		})
	}
}

// Every entry point reads the map, so a line this parser rejects outright used
// to leave no way to inspect or repair the file from the tool itself.
func TestReadMapSkipsUnparseableLines(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "gopls-ports.map")
	good := record{Worktree: "/repo/good", Port: 62001, PID: 11}
	if err := writeMap(path, []record{good}); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	// Not just unparseable lines: valid JSON whose fields would be handed to
	// kill(2). A missing pid decodes to 0, which signals our own process group.
	damage := "garbage\t\tnot-a-port\tx\n\"/repo/b\"\tnope\t12\n" +
		`{"Worktree":"/repo/no-pid","Port":62002}` + "\n" +
		`{"Worktree":"/repo/every-process","Port":62003,"PID":-1}` + "\n" +
		`{"Worktree":"/repo/bad-port","Port":0,"PID":13}` + "\n"
	if _, err := f.WriteString(damage); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := readMap(path)
	if err != nil {
		t.Fatalf("readMap() failed on a damaged file: %v", err)
	}
	if len(got) != 1 || got[0] != good {
		t.Fatalf("readMap() = %#v, want just %#v", got, good)
	}
}

func TestListShowsLiveRecordsAndCleansDeadRecords(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "gopls-ports.map")
	live := record{Worktree: "/repo/live", Port: 62001, PID: 11}
	dead := record{Worktree: "/repo/dead", Port: 62002, PID: 12}
	if err := writeMap(path, []record{live, dead}); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	m := manager{mapPath: path, alive: func(r record) bool { return r == live }}
	if err := m.list(&output); err != nil {
		t.Fatal(err)
	}
	const wantOutput = "PORT\tPID\tWORKTREE\n62001\t11\t/repo/live\n"
	if output.String() != wantOutput {
		t.Fatalf("list output = %q, want %q", output.String(), wantOutput)
	}
	got, err := readMap(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != live {
		t.Fatalf("records after list = %#v, want %#v", got, []record{live})
	}
}
