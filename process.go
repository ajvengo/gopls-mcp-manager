package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var probeClient = func() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	return &http.Client{Transport: t, Timeout: 500 * time.Millisecond,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}()

func endpointProbe(ctx context.Context, port int) (alive, conclusive bool) {
	// The url is ours and fixed, so NewRequest cannot reject it.
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, mcpURL(port), nil)
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

func matchesGopls(command string, port int) bool {
	fields := strings.Fields(command)
	if len(fields) < 4 {
		return false
	}
	// ps can include an interpreter prefix for a gopls wrapper. The manager's
	// actual executable spelling and complete argument tail must still match;
	// a port prefix or a diagnostic argument mentioning gopls is not identity.
	args := fields[len(fields)-4:]
	return filepath.Base(args[0]) == goplsBinary && args[1] == "mcp" && args[2] == "-listen" && args[3] == mcpAddress(port)
}

const goplsBinary = "gopls"

const (
	readyTimeout = 10 * time.Second
	exitGrace    = 2 * time.Second
)

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func (m *manager) logPath(worktree string) string {
	hash := sha256.Sum256([]byte(worktree))
	return filepath.Join(filepath.Dir(m.mapPath), "gopls-mcp-logs", fmt.Sprintf("%x.log", hash[:8]))
}

func awaitReady(ctx context.Context, port, pid int) error {
	if pid <= 0 {
		return errors.New("cannot verify readiness without an owned child PID")
	}
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	// Backed off rather than polled flat: a gopls that binds 15ms after the fork
	// is not noticed for the rest of the tick, and this wait is on the lane's
	// goroutine with the send budget running. Doubling to a 100ms ceiling costs
	// a handful of extra probes on a slow start and nothing on a failed one.
	for wait := 10 * time.Millisecond; ctx.Err() == nil; wait = min(2*wait, 100*time.Millisecond) {
		if alive, _ := endpointProbe(ctx, port); alive {
			owned, err := ownsMCPListener(ctx, port, pid)
			if err != nil {
				return fmt.Errorf("verify gopls listener on %s: %w", mcpAddress(port), err)
			}
			if owned {
				return nil
			}
			return fmt.Errorf("gopls child %d does not own the MCP listener on %s", pid, mcpAddress(port))
		}
		if err := waitContext(ctx, wait); err != nil {
			break
		}
	}
	return fmt.Errorf("gopls did not become ready on %s: %w", mcpAddress(port), ctx.Err())
}

func requireListenerCensus() error {
	if _, err := exec.LookPath("lsof"); err != nil {
		return fmt.Errorf("lsof is required to verify the gopls listener: %w", err)
	}
	return nil
}

func ownsMCPListener(ctx context.Context, port, pid int) (bool, error) {
	identity := strconv.Itoa(pid)
	output, err := boundedCommandOutput(ctx, 4096, "lsof", "-n", "-P", "-w", "-a",
		"-p", identity, "-iTCP@"+mcpAddress(port), "-sTCP:LISTEN", "-Fpf")
	if err != nil {
		var exit *exec.ExitError
		if ctx.Err() == nil && !errors.Is(err, errCommandOutputLimit) && errors.As(err, &exit) &&
			exit.ExitCode() == 1 && len(output) == 0 && len(exit.Stderr) == 0 {
			return false, nil
		}
		return false, err
	}
	counts, err := parseOpenFiles(string(output))
	if err != nil {
		return false, err
	}
	if len(counts) != 1 || counts[pid] == 0 {
		return false, errors.New("unexpected listener census")
	}
	return true, nil
}

const (
	maxCommandStderrBytes = 64 << 10
	// A descendant may retain a pipe after the direct child exits. Close the
	// manager's pipe readers instead of retaining their copying goroutines.
	subprocessWaitDelay = 250 * time.Millisecond
)

var errCommandOutputLimit = errors.New("subprocess output limit exceeded")

// Run joins the sole os/exec writer before fields are read. Allocate the cap
// once to avoid retaining both an old buffer and an over-budget replacement.
type commandOutputBuffer struct {
	data     []byte
	limit    int
	cancel   context.CancelFunc
	overflow bool
}

func (b *commandOutputBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-len(b.data) {
		b.overflow = true
		b.cancel()
		return 0, errCommandOutputLimit
	}
	if len(p) > 0 && b.data == nil {
		b.data = make([]byte, 0, b.limit)
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

// boundedCommandOutput owns and reaps the direct child. Each stream has a hard
// capture-capacity ceiling; os/exec's fixed pipe-copy buffers are additional.
// Caller cancellation and overflow kill the direct child; WaitDelay bounds
// pipe retention by descendants, but does not claim ownership of grandchildren.
//
// An ordinary ExitError retains stdout and bounded stderr, including lsof's
// useful exit-1 output. Overflow never wraps an ExitError and returns nil output
// so that an exit-status exception cannot accidentally accept truncated data.
func boundedCommandOutput(ctx context.Context, maxStdoutBytes int, name string, args ...string) ([]byte, error) {
	return boundedCommandOutputEnv(ctx, nil, maxStdoutBytes, name, args...)
}

// nil env inherits; a non-nil env replaces it without entering argv or diagnostics.
func boundedCommandOutputEnv(ctx context.Context, env []string, maxStdoutBytes int, name string, args ...string) ([]byte, error) {
	if maxStdoutBytes <= 0 {
		return nil, errors.New("subprocess stdout limit must be positive")
	}
	commandCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stdout := &commandOutputBuffer{limit: maxStdoutBytes, cancel: cancel}
	stderr := &commandOutputBuffer{limit: maxCommandStderrBytes, cancel: cancel}
	cmd := exec.CommandContext(commandCtx, name, args...)
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.WaitDelay = subprocessWaitDelay
	err := cmd.Run()
	switch {
	case stdout.overflow:
		return nil, fmt.Errorf("%w: stdout exceeds %d bytes", errCommandOutputLimit, maxStdoutBytes)
	case stderr.overflow:
		return nil, fmt.Errorf("%w: stderr exceeds %d bytes", errCommandOutputLimit, maxCommandStderrBytes)
	case err == nil:
		return stdout.data, nil
	case ctx.Err() != nil:
		return nil, errors.Join(ctx.Err(), err)
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		exit.Stderr = stderr.data
		return stdout.data, err
	}
	return nil, err
}

const openFilesTimeout = 5 * time.Second

func countOpenFiles(ctx context.Context, records []record) (map[int]int, error) {
	ctx, cancel := context.WithTimeout(ctx, openFilesTimeout)
	defer cancel()
	list := make([]string, len(records))
	for i, r := range records {
		list[i] = strconv.Itoa(r.PID)
	}
	out, err := boundedCommandOutput(ctx, 4<<20, "lsof", "-n", "-P", "-w", "-F", "f", "-p", strings.Join(list, ","))
	if ctx.Err() != nil || errors.Is(err, errCommandOutputLimit) {
		return nil, err
	}
	// lsof exits 1 when a listed pid has gone, and still lists the others.
	var exit *exec.ExitError
	if err != nil && (!errors.As(err, &exit) || exit.ExitCode() != 1) {
		return nil, err
	}
	return parseOpenFiles(string(out))
}

func parseOpenFiles(out string) (map[int]int, error) {
	counts := make(map[int]int)
	pid := 0
	for line := range strings.SplitSeq(out, "\n") {
		switch {
		case line == "":
			continue
		case strings.HasPrefix(line, "p"):
			var err error
			pid, err = strconv.Atoi(line[1:])
			if err != nil || pid <= 0 {
				return nil, errors.New("invalid lsof process identity")
			}
		case strings.HasPrefix(line, "f") && len(line) > 1 && pid > 0:
			if line == "ferr" || line == "fNOFD" {
				return nil, errors.New("incomplete lsof file snapshot")
			}
			counts[pid]++
		default:
			return nil, errors.New("invalid lsof file snapshot")
		}
	}
	return counts, nil
}
