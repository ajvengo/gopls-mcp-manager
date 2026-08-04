package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	firstPort = 61100
	lastPort  = 65100
	portCount = lastPort - firstPort + 1
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

type record struct {
	Worktree string
	Port     int
	PID      int
	// StartedAt is when this tool spawned the gopls, in unix seconds, and marks
	// the record unreapable until startGrace is up; see recordAlive. Zero in a
	// map written before the field existed, which reads as "no grace" — the
	// behaviour those records already had.
	StartedAt int64
}

type manager struct {
	mapPath string
	alive   func(record) bool // recordAlive; replaced by tests, and called concurrently — see cleanRecords
	// ready is awaitReady, replaced by tests: what it waits out is readyTimeout,
	// and ensure's failure path is otherwise ten seconds of sleeping away from
	// every assertion about it.
	ready func(port int) error
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
	return &manager{
		mapPath: filepath.Join(home, ".local", "share", "gopls-ports.map"),
		alive:   recordAlive,
		ready:   awaitReady,
	}, nil
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

// cleanRecords drops the records whose server is gone. Every record is offered
// to alive, which reaps what it rejects, so none may be skipped — including a
// second record for a worktree ensure will never answer with, since the record
// is the only handle on that process and its port really is taken.
//
// The probes run at once, so alive has to be safe to call from several
// goroutines. Every caller holds the map's exclusive flock across this and a
// probe waits out its own timeout, so in sequence a few servers too busy to
// answer would keep every other invocation of this tool — and so every other
// worktree's next tool call — waiting for the sum of them. Nothing bounds the
// fan-out because nothing needs to: the records are one per live gopls, and a
// machine that could run enough of those to matter here has already run out of
// memory.
func cleanRecords(records []record, alive func(record) bool) []record {
	live := make([]bool, len(records))
	var probes sync.WaitGroup
	for i, r := range records {
		probes.Go(func() { live[i] = alive(r) })
	}
	probes.Wait()

	kept := make([]record, 0, len(records))
	for i, r := range records {
		if live[i] {
			kept = append(kept, r)
		}
	}
	return kept
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

func withFileLock(path string, fn func() error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	return fn()
}

// Short, because waiting longer buys nothing: an answer that never comes is not
// a verdict either way, and a port nobody listens on refuses instantly.
//
// ponytail: this is the per-sweep tax on the wedged server of §10. Count
// consecutive inconclusive sweeps in the record and drop it after a few if that
// ever shows up in practice.
var probeClient = &http.Client{Timeout: 500 * time.Millisecond}

// endpointProbe reports whether the MCP endpoint on port answered, and whether
// a negative answer is conclusive. Only a refused connection is, because only
// that one means nobody is listening; §8 has the rest of the argument.
func endpointProbe(port int) (alive, conclusive bool) {
	// The url is ours and fixed, so NewRequest cannot reject it.
	req, _ := http.NewRequest(http.MethodGet, mcpURL(port), nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := probeClient.Do(req)
	if err != nil {
		// Refusal is named, not inferred from "not a timeout": every other way
		// a dial can fail — descriptors gone, host unreachable, our own context
		// cancelled — says something about this process, not about whether a
		// gopls is listening, and inferring death from it condemns live servers.
		return false, errors.Is(err, syscall.ECONNREFUSED)
	}
	defer func() { _ = resp.Body.Close() }()
	alive = resp.StatusCode == http.StatusOK && strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
	return alive, false
}

// withinStartGrace reports whether r names a gopls forked so recently that it
// may not have bound its port yet. Two callers need to know: a sweep, which
// must not reap it (L7), and an ensure that did not start it, which must not
// hand its caller a port that still refuses (P6). `ensure` holds the map lock
// only up to the fork now (§9), which is what puts this window in another
// process's reach at all.
//
// A timestamp in the future gets no grace, since the file is meant to be
// editable by hand and this field is the one way to make a record immortal.
func withinStartGrace(r record) bool {
	age := time.Since(time.Unix(r.StartedAt, 0))
	return age >= 0 && age < startGrace
}

// recordAlive reports whether the gopls named by r is still usable, and kills
// it when it is provably not.
//
// The map record is the only handle anyone has on that process: dropping the
// record without killing it strands a full workspace index — routinely 1-2GB
// resident — that no longer appears in list, still holds its port, and lives
// until the machine reboots. The next ensure for that worktree then finds the
// port taken and starts a second one beside it. The one record dropped without
// a signal is the one whose pid is not ours at all: there is no index behind it
// to strand, and the process belongs to someone else.
//
// Every caller sweeps every record, so this runs against other worktrees'
// servers too, and an unsure verdict keeps its server rather than killing it: a
// wrong kill costs a full re-index of a tree nobody was even asking about.
func recordAlive(r record) bool {
	err := syscall.Kill(r.PID, 0)
	if err != nil && !errors.Is(err, syscall.EPERM) {
		return false // already gone, nothing to kill
	}
	// A gopls between fork and bind refuses every probe, and a refusal is the
	// one conclusive verdict below — so a sweep landing in that window would
	// SIGTERM a server that is starting exactly as it should.
	//
	// Asked after the pid, not before: the grace is for a process that exists
	// and has not bound yet, and a start that crashed instead is not that. Held
	// off until the timestamp expired, such a record would answer every ensure
	// for its worktree with a dead port for the rest of the window.
	if withinStartGrace(r) {
		return true
	}
	alive, conclusive := endpointProbe(r.Port)
	if alive {
		return true
	}
	// One question left, and identity answers it whichever way the probe went:
	// may this process be signalled, and — where the probe proved nothing — is
	// the record worth another sweep at all. Keeping an unproven record on the
	// probe alone would be a one-way door, since nothing ever revisits a record
	// kept as alive: a port some unrelated listener took over, after a reboot
	// recycled the pid too, would leave this worktree pointed at it until the
	// map is edited by hand.
	ours := isOurGopls(r.PID, r.Port)
	if !conclusive {
		return ours // merely busy or answering oddly, or a stale number
	}
	if ours {
		_ = syscall.Kill(r.PID, syscall.SIGTERM)
	}
	return false
}

// isOurGopls reports whether pid is still the gopls this tool started for port.
//
// The map records nothing but a number and survives reboots, after which every
// pid in it belongs to whoever the kernel handed it to next. Matching the name
// alone would not be enough: the process most likely to be called gopls is the
// one the user's editor started, and killing that is the very mistake this
// guards against. The listen address we spawn with appears in no other gopls
// command line, so it identifies ours exactly.
//
// ponytail: one fork per record, and a sweep asks for every record that did not
// answer — 8 of them cost ~18ms against ~1.3ms when they are all alive
// (BenchmarkSweepProbes), inside the map's flock. `ps -p` takes a list, so the
// whole sweep is one fork if that ever matters; it costs cleanRecords a batching
// pass before the fan-out, and it only runs when a gopls has actually died.
func isOurGopls(pid, port int) bool {
	out, err := exec.Command("ps", "-ww", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	return err == nil && strings.Contains(string(out), goplsBinary) && strings.Contains(string(out), mcpAddress(port))
}

func portUnavailable(port int) bool {
	listener, err := net.Listen("tcp", mcpAddress(port))
	if err != nil {
		return true
	}
	_ = listener.Close()
	return false
}

// withRecords runs body under the map's flock, over the records whose server is
// still alive, and writes back what it returns.
//
// The write belongs here rather than to each caller because probing is what
// reaps those servers: a caller that returned without writing would leave the
// file claiming ports nothing holds. One write, so the caller that adds a
// record does not fsync twice under the same lock — a cost every other
// process's ensure queues behind.
func (m *manager) withRecords(body func([]record) ([]record, error)) ([]record, error) {
	return m.withMap(func(stored []record) ([]record, error) { return body(cleanRecords(stored, m.alive)) })
}

// withMap runs body under the map's flock, over what the file holds, and writes
// back what it returns. The sweep is withRecords' addition, not this one's:
// forget goes through here to drop one record without probing every other
// worktree under the lock.
//
// Skipped entirely when body hands back what it was given, which is the steady
// state: the write is a temp file, an fsync and a rename inside a lock every
// process on the machine shares — some sixty times the rest of this function
// (BenchmarkWithRecords). The intact flag is the other half of
// the question: the lines readMap refused are gone from stored already, so
// equal records do not mean an equal file, and only readMap can say whether the
// repair is still owed.
//
// body must not mutate the slice it is handed. That slice is the comparison
// basis, so a body editing it in place — slices.DeleteFunc does — would compare
// its own result against itself, find no change, and leave the map saying what
// it said before.
func (m *manager) withMap(body func([]record) ([]record, error)) ([]record, error) {
	var updated []record
	err := withFileLock(m.mapPath, func() error {
		stored, intact, err := readMap(m.mapPath)
		if err != nil {
			return err
		}
		if updated, err = body(stored); err != nil {
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

// ensure answers with the port worktree's gopls listens on, starting one if the
// map has none.
//
// The readiness wait runs with the map lock released. Under the lock it made
// every other gopls-mcp-manager process — and so every other worktree's next
// tool call — queue behind one cold start for up to readyTimeout, which is
// precisely the case worktree isolation produces. The record written before the
// lock is dropped is what makes that safe: it reserves the port, and startGrace
// keeps a concurrent sweep from reaping a server that has not bound yet.
func (m *manager) ensure(worktree string) (int, error) {
	claimed, started, err := m.claimPort(worktree)
	if err != nil {
		return 0, err
	}
	// P6. A record inside its start grace was vouched for by L7 without being
	// probed at all, so its port can still refuse: the gopls it names was forked
	// by somebody who has not seen it bind yet. Waited for here rather than under
	// the flock, so the wait costs only this caller (P4).
	//
	// Our own start is waited for on the fact rather than the clock: the grace
	// would almost always cover it too, but "almost" would make the cleanup
	// below depend on how long the sweep's probes took, and a slow one would
	// silently skip both the wait and the SIGTERM that follows it.
	if started == nil && !withinStartGrace(claimed) {
		return claimed.Port, nil
	}
	if err := m.ready(claimed.Port); err != nil {
		// Wrapped here rather than in awaitReady, which knows only a port: the
		// worktree is in hand here, and the success path still does not hash it
		// for a message nobody reads.
		err = fmt.Errorf("%w; see %s", err, m.logPath(worktree))
		if started == nil {
			// Somebody else's start, and its failure belongs to them: that
			// process signals and forgets. This only reports it.
			return 0, err
		}
		// Signalled through the process we hold rather than by pid: identity is
		// not in doubt here, and the isOurGopls check could only refuse.
		_ = started.Signal(syscall.SIGTERM)
		// Escalated, because forgetting the record makes this pid unfindable: no
		// sweep reaches a process the map no longer names, so a gopls sitting in
		// its SIGTERM — wedged in filesystem I/O is enough — would run unowned
		// until reboot, holding its port and its 1-2GB. SIGKILL is the one signal
		// it cannot ignore; startGopls' reaper collects the corpse either way.
		//
		// ponytail: fired off the clock rather than waited for, so this does not
		// park the lane for exitGrace on a path that has already spent
		// readyTimeout. The window it leaves is this process exiting first, which
		// costs exactly what today costs. Wait for it — startGopls would have to
		// hand its reaper's channel back — if a stranded gopls is ever observed.
		time.AfterFunc(exitGrace, func() { _ = started.Kill() })
		// The record now names a port with nothing on it, and its own grace
		// would spare it from the next sweep — so it is dropped here rather
		// than left for one.
		return 0, errors.Join(err, m.forget(claimed))
	}
	return claimed.Port, nil
}

// claimPort reserves worktree's record under the map lock, spawning a gopls
// when the map has none. A process comes back only when this call is what
// started it, which is how ensure tells its own readiness wait from the one it
// may still owe another process's start.
func (m *manager) claimPort(worktree string) (record, *os.Process, error) {
	var claimed record
	var started *os.Process
	_, err := m.withRecords(func(records []record) ([]record, error) {
		for _, r := range records {
			if r.Worktree == worktree {
				claimed = r
				return records, nil
			}
		}
		port, err := allocatePort(worktree, records, portUnavailable)
		if err != nil {
			return nil, err
		}
		started, err = m.startGopls(worktree, port)
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
			_ = started.Signal(syscall.SIGTERM)
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
func (m *manager) forget(started record) error {
	// Cloned because DeleteFunc edits in place, and what it is handed is what
	// withMap compares against to decide whether to write at all.
	_, err := m.withMap(func(records []record) ([]record, error) {
		return slices.DeleteFunc(slices.Clone(records), func(r record) bool { return r == started }), nil
	})
	return err
}

// list prints the surviving records. Printed after the lock is dropped, so a
// client that stopped reading cannot hold it, and so that the sweep's result is
// already on disk whatever stdout does.
func (m *manager) list(w io.Writer) error {
	records, err := m.withRecords(func(records []record) ([]record, error) { return records, nil })
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

func (m *manager) logPath(worktree string) string {
	hash := sha256.Sum256([]byte(worktree))
	return filepath.Join(filepath.Dir(m.mapPath), "gopls-mcp-logs", fmt.Sprintf("%x.log", hash[:8]))
}

// startGopls spawns a gopls for worktree and returns as soon as the child
// exists. Whether it ever serves its endpoint is awaitReady's question, and the
// caller's to ask outside the map lock — see ensure.
func (m *manager) startGopls(worktree string, port int) (*os.Process, error) {
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
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start gopls: %w", err)
	}
	// Reap the child rather than releasing it. Setsid does not reparent, so an
	// unwaited gopls stays a zombie child of this process for the whole session
	// — and kill(pid, 0) succeeds against a zombie, which would make the PID
	// half of recordAlive report a dead server as alive.
	go func() { _ = cmd.Wait() }()
	return cmd.Process, nil
}

// awaitReady polls until the gopls just spawned on port serves its endpoint.
// The caller signals the process it holds when this gives up: a bare kill(pid)
// could land on whatever the kernel handed that number to next.
func awaitReady(port int) error {
	deadline := time.Now().Add(readyTimeout)
	// Backed off rather than polled flat: a gopls that binds 15ms after the fork
	// is not noticed for the rest of the tick, and this wait is on the lane's
	// goroutine with the send budget running. Doubling to a 100ms ceiling costs
	// a handful of extra probes on a slow start and nothing on a failed one.
	for wait := 10 * time.Millisecond; time.Now().Before(deadline); wait = min(2*wait, 100*time.Millisecond) {
		if alive, _ := endpointProbe(port); alive {
			return nil
		}
		time.Sleep(wait)
	}
	return fmt.Errorf("gopls did not become ready on %s", mcpAddress(port))
}
