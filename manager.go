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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	firstPort = 61100
	lastPort  = 65100
	portCount = lastPort - firstPort + 1
)

type record struct {
	Worktree string
	Port     int
	PID      int
}

type manager struct {
	mapPath string
	alive   func(record) bool // recordAlive; replaced by tests, and called concurrently — see cleanRecords
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
	}, nil
}

func basePort(worktree string) int {
	sum := sha256.Sum256([]byte(worktree))
	return firstPort + int(binary.BigEndian.Uint64(sum[:8])%uint64(portCount))
}

func mcpAddress(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
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

func readMap(path string) ([]record, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	// A line we cannot parse names a gopls we cannot manage anyway, so it is
	// dropped rather than failed on: every entry point reads this file, so one
	// bad line used to leave no way to inspect or repair it from the tool. The
	// next write rewrites the file without it.
	//
	// The fields are checked, not just decoded, because this file is meant to be
	// editable by hand and every one of them is an argument to kill(2) or to a
	// probe that decides on a kill: pid 0 signals this process's own group, a
	// negative pid signals every process the user owns, and a port outside the
	// range we allocate from can only refuse a probe and condemn a live server.
	var records []record
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
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
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return records, nil
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
	req, _ := http.NewRequest(http.MethodGet, "http://"+mcpAddress(port), nil)
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
func isOurGopls(pid, port int) bool {
	out, err := exec.Command("ps", "-ww", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	return err == nil && strings.Contains(string(out), "gopls") && strings.Contains(string(out), mcpAddress(port))
}

func portUnavailable(port int) bool {
	listener, err := net.Listen("tcp", mcpAddress(port))
	if err != nil {
		return true
	}
	_ = listener.Close()
	return false
}

func (m *manager) readLiveRecords() ([]record, error) {
	records, err := readMap(m.mapPath)
	if err != nil {
		return nil, err
	}
	return cleanRecords(records, m.alive), nil
}

func (m *manager) ensure(worktree string) (int, error) {
	var selected int
	err := withFileLock(m.mapPath, func() error {
		records, err := m.readLiveRecords()
		if err != nil {
			return err
		}
		for _, r := range records {
			if r.Worktree == worktree {
				selected = r.Port
				return writeMap(m.mapPath, records)
			}
		}

		port, err := allocatePort(worktree, records, portUnavailable)
		if err != nil {
			return err
		}
		proc, err := m.startGopls(worktree, port)
		if err != nil {
			return err
		}
		records = append(records, record{Worktree: worktree, Port: port, PID: proc.Pid})
		if err := writeMap(m.mapPath, records); err != nil {
			// Unrecorded, so nothing would ever find it again. Signalled through
			// the process we hold rather than by pid: identity is not in doubt
			// here, and the isOurGopls check could only refuse.
			_ = proc.Signal(syscall.SIGTERM)
			return err
		}
		selected = port
		return nil
	})
	return selected, err
}

func (m *manager) list(w io.Writer) error {
	return withFileLock(m.mapPath, func() error {
		records, err := m.readLiveRecords()
		if err != nil {
			return err
		}
		if err := writeMap(m.mapPath, records); err != nil {
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
	})
}

func (m *manager) startGopls(worktree string, port int) (*os.Process, error) {
	logDir := filepath.Join(filepath.Dir(m.mapPath), "gopls-mcp-logs")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(worktree))
	logPath := filepath.Join(logDir, fmt.Sprintf("%x.log", hash[:8]))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() { _ = logFile.Close() }()

	// exec.Command stashes a failed PATH lookup in cmd.Err and Start returns it,
	// so a missing gopls arrives here as "start gopls: ... not found in $PATH".
	cmd := exec.Command("gopls", "mcp", "-listen", mcpAddress(port))
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

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if alive, _ := endpointProbe(port); alive {
			return cmd.Process, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Signalled through cmd.Process, which knows whether the reaper above has
	// already collected the child; a bare kill(pid) here could land on whatever
	// the kernel handed that number to next.
	_ = cmd.Process.Signal(syscall.SIGTERM)
	return nil, fmt.Errorf("gopls did not become ready on 127.0.0.1:%d; see %s", port, logPath)
}
