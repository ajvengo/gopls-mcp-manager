package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"debug/buildinfo"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Keep the lease descriptor reachable until OS process exit, including main's
// final diagnostic/exit path. Closing it when runGuardian returns would expose
// an inert reservation while a still-resident guardian consumes resources.
var guardianLifetimeLease *os.File

func runGuardian(args []string) error {
	if len(args) > 0 && args[0] == "child" {
		return guardianExecChild(args[1:])
	}
	if len(args) != 0 {
		return errors.New("internal-guardian accepts no arguments")
	}
	if syscall.Getpgrp() != os.Getpid() {
		return errors.New("guardian must anchor its own process group")
	}
	control := os.NewFile(3, "guardian-control")
	status := os.NewFile(4, "guardian-status")
	lease := os.NewFile(5, "guardian-lifetime-lease")
	if control == nil || status == nil || lease == nil {
		return errors.New("guardian pipes are unavailable")
	}
	guardianLifetimeLease = lease
	defer func() { runtime.KeepAlive(guardianLifetimeLease) }()
	defer func() { _ = control.Close(); _ = status.Close() }()
	syscall.CloseOnExec(3)
	syscall.CloseOnExec(4)
	syscall.CloseOnExec(5)
	reader := bufio.NewReaderSize(control, guardianFrameBytes)
	line, err := reader.ReadSlice('\n')
	if err != nil {
		return fmt.Errorf("guardian config: %w", err)
	}
	var config guardianConfig
	if err := json.Unmarshal(line, &config); err != nil {
		return err
	}
	if err := guardianValidateConfig(config); err != nil {
		return err
	}
	info, err := lease.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("guardian lifetime lease is unavailable")
	}
	expected, err := os.Lstat(filepath.Join(filepath.Dir(config.StatePath), "leases", fmt.Sprintf("port-%d.lock", config.Port)))
	if err != nil || !os.SameFile(info, expected) {
		return errors.New("guardian inherited an unexpected lifetime lease")
	}
	if err := syscall.Flock(int(lease.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}
	return guardianServe(config, reader, status)
}

func guardianSend(writer io.Writer, event guardianEvent) {
	data, err := json.Marshal(event)
	if err == nil && len(data)+1 <= guardianFrameBytes {
		_, _ = writer.Write(append(data, '\n'))
	}
}

type guardianWaitResult struct {
	confirmed bool
	err       error
}

// This is used only before child.Start succeeds, when removing a reservation is
// proof of an unstarted child rather than a guess from a helper's exit status.
func guardianUnstartedFailure(status io.Writer, path string, cause error) error {
	removeErr := guardianRemoveRecord(path)
	err := errors.Join(cause, removeErr)
	guardianSend(status, guardianEvent{Kind: "startup-error", Error: err.Error()})
	if removeErr == nil {
		guardianSend(status, guardianEvent{Kind: "confirmed"})
	}
	return err
}

func guardianServe(config guardianConfig, control io.Reader, status io.Writer) error {
	// Catch our own group TERM before the gopls can exist. The guardian itself
	// keeps the PGID allocated throughout every group signal and exit check.
	terms := make(chan os.Signal, 1)
	signal.Notify(terms, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(terms)
	commands := make(chan byte, 1)
	ownerGone := make(chan struct{})
	serveDone := make(chan struct{})
	defer close(serveDone)
	go func() {
		defer close(ownerGone)
		var command [1]byte
		for {
			if _, err := io.ReadFull(control, command[:]); err != nil {
				return
			}
			select {
			case commands <- command[0]:
			case <-serveDone:
				return
			}
		}
	}()
	record := guardianRecord{Version: guardianProtocol, Token: config.Token,
		GuardianPID: os.Getpid(), Worktree: config.Worktree, Port: config.Port,
		CreatedAt: time.Now().UnixMilli(), Scope: "process-group; telemetry suppressed", MaxOpenFiles: config.MaxOpenFiles}
	if err := guardianWriteRecord(config.StatePath, record); err != nil {
		return guardianUnstartedFailure(status, config.StatePath, err)
	}
	select {
	case <-ownerGone:
		return guardianRemoveRecord(config.StatePath)
	default:
	}
	if err := guardianSetLimit(guardianMaxOpenFiles, config.MaxOpenFiles); err != nil {
		return guardianUnstartedFailure(status, config.StatePath, err)
	}
	executable, err := os.Executable()
	if err != nil {
		return guardianUnstartedFailure(status, config.StatePath, err)
	}
	childArgs := append(append(append([]string{}, config.ExecArgs...), strconv.Itoa(config.MaxOpenFiles), config.Command), config.Args...)
	child := exec.Command(executable, childArgs...)
	child.Dir = config.Worktree
	child.Env, err = guardianTelemetryEnvironment(filepath.Dir(config.StatePath))
	if err != nil {
		return guardianUnstartedFailure(status, config.StatePath, err)
	}
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	if err := child.Start(); err != nil {
		return guardianUnstartedFailure(status, config.StatePath, err)
	}
	waited := make(chan guardianWaitResult, 1)
	go func() {
		err := child.Wait()
		var exit *exec.ExitError
		waited <- guardianWaitResult{confirmed: err == nil || errors.As(err, &exit), err: err}
	}()
	record.ChildPID = child.Process.Pid
	durableErr := guardianWriteRecord(config.StatePath, record)
	if durableErr == nil {
		guardianSend(status, guardianEvent{Kind: "started", PID: child.Process.Pid})
	} else {
		guardianSend(status, guardianEvent{Kind: "startup-error", PID: child.Process.Pid, Error: durableErr.Error()})
	}
	stopping := false
	childWaited := false
	escaped := false
	var initialObservationErr error
	uncertaintyReported := false
	var stoppedAt time.Time
	beginStop := func() error {
		if !stopping {
			stopping, stoppedAt = true, time.Now()
			rows, err := guardianSnapshot()
			if err == nil {
				escaped = guardianEscaped(rows, child.Process.Pid, os.Getpid())
			} else {
				// Once the child exits, escaped descendants can be reparented.
				// A later empty group cannot reconstruct this missing ancestry.
				initialObservationErr = err
			}
		}
		// pid 0 means our current group, whose leader is this live guardian.
		return syscall.Kill(0, syscall.SIGTERM)
	}
	if durableErr != nil {
		_ = beginStop()
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	ownerDone := ownerGone
	for {
		select {
		case <-ownerDone:
			ownerDone = nil
			record.OwnerGone = true
			_ = guardianWriteRecord(config.StatePath, record)
			_ = beginStop()
		case command := <-commands:
			var err error
			switch command {
			case 'T':
				err = beginStop()
			case 'K':
				// Escalation is only for an explicitly failed owned start.
				// Kill this still-owned direct child, never an unanchored group.
				_ = beginStop()
				err = child.Process.Kill()
				if errors.Is(err, os.ErrProcessDone) {
					err = nil
				}
			default:
				err = errors.New("invalid guardian control command")
			}
			event := guardianEvent{Kind: "ack"}
			if err != nil {
				event.Error = err.Error()
			}
			guardianSend(status, event)
		case <-terms:
			if !stopping {
				_ = beginStop()
			}
		case result := <-waited:
			childWaited = result.confirmed
			if !result.confirmed {
				record.Uncertain = fmt.Sprintf("child wait not confirmed: %v", result.err)
			}
			_ = beginStop()
		case <-ticker.C:
			if !stopping {
				continue
			}
			if !uncertaintyReported && time.Since(stoppedAt) >= guardianExitGrace {
				uncertaintyReported = true
				record.Uncertain = "process group exit not confirmed within TERM grace"
				_ = guardianWriteRecord(config.StatePath, record)
				guardianSend(status, guardianEvent{Kind: "uncertain", Error: record.Uncertain})
			}
			if !childWaited {
				continue
			}
			rows, err := guardianSnapshot()
			if err != nil || guardianGroupMembers(rows, os.Getpid()) != 0 {
				continue
			}
			if initialObservationErr != nil {
				record.Uncertain = fmt.Sprintf("initial descendant observation failed; exit not proven: %v", initialObservationErr)
			} else if escaped {
				record.Uncertain = "a descendant escaped the anchored process group; exit not proven"
			}
			if initialObservationErr != nil || escaped {
				_ = guardianWriteRecord(config.StatePath, record)
				guardianSend(status, guardianEvent{Kind: "uncertain", Error: record.Uncertain})
				return errors.New(record.Uncertain)
			}
			if err := guardianRemoveRecord(config.StatePath); err != nil {
				guardianSend(status, guardianEvent{Kind: "uncertain", Error: err.Error()})
				return err
			}
			guardianSend(status, guardianEvent{Kind: "confirmed"})
			return nil
		}
	}
}

// The child changes only its own limits, then execs gopls without another PID.
// The resident guardian keeps its separate 32-descriptor soft ceiling.
func guardianExecChild(args []string) error {
	if len(args) < 2 {
		return errors.New("invalid guardian child invocation")
	}
	quota, err := strconv.Atoi(args[0])
	if err != nil || quota < 256 || quota > 1<<20 {
		return errors.New("invalid guardian child descriptor quota")
	}
	if err := guardianSetLimit(quota, quota); err != nil {
		return err
	}
	telemetryDir := os.Getenv("TEST_TELEMETRY_DIR")
	if !filepath.IsAbs(telemetryDir) {
		return errors.New("guardian child private telemetry environment is missing")
	}
	return syscall.Exec(args[1], append([]string{args[1]}, args[2:]...), guardianEnvironment(os.Environ(), telemetryDir))
}

func guardianSetLimit(soft, hard int) error {
	var inherited syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &inherited); err != nil {
		return err
	}
	if uint64(hard) > inherited.Max {
		return fmt.Errorf("guardian descriptor quota %d exceeds inherited hard limit %d", hard, inherited.Max)
	}
	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Cur: uint64(soft), Max: uint64(hard)})
}

func guardianRemoveRecord(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

type guardianRow struct{ pid, parent, group int }

// A process snapshot is bounded by bytes and time. It establishes group
// membership only; it cannot discover a descendant that already escaped and
// reparented before the snapshot. That limitation is material on Darwin.
func guardianSnapshot() ([]guardianRow, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "ps", "-axo", "pid=,ppid=,pgid=")
	// Keep the observer outside the watched group, otherwise it counts itself.
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = subprocessWaitDelay
	var output guardianOutput
	command.Stdout = &output
	if err := command.Run(); err != nil {
		return nil, err
	}
	rows, err := guardianParseSnapshot(string(output.bytes))
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if row.pid == os.Getpid() && row.group == syscall.Getpgrp() {
			return rows, nil
		}
	}
	return nil, errors.New("guardian process snapshot omits observer identity")
}

type guardianOutput struct{ bytes []byte }

func (b *guardianOutput) Write(value []byte) (int, error) {
	if len(value) > (2<<20)-len(b.bytes) {
		return 0, errors.New("guardian process snapshot exceeds 2 MiB")
	}
	b.bytes = append(b.bytes, value...)
	return len(value), nil
}

func guardianParseSnapshot(output string) ([]guardianRow, error) {
	if len(output) > 2<<20 {
		return nil, errors.New("guardian process snapshot exceeds 2 MiB")
	}
	var rows []guardianRow
	for line := range strings.SplitSeq(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 3 {
			return nil, errors.New("invalid guardian process snapshot")
		}
		values := [3]int{}
		for i, field := range fields {
			n, err := strconv.Atoi(field)
			if err != nil || n < 0 {
				return nil, errors.New("invalid guardian process identity")
			}
			values[i] = n
		}
		if values[0] == 0 {
			continue
		}
		rows = append(rows, guardianRow{values[0], values[1], values[2]})
	}
	return rows, nil
}

func guardianGroupMembers(rows []guardianRow, group int) int {
	count := 0
	for _, row := range rows {
		if row.group == group && row.pid != group {
			count++
		}
	}
	return count
}

func guardianEscaped(rows []guardianRow, child, group int) bool {
	children := make(map[int][]guardianRow, len(rows))
	for _, row := range rows {
		children[row.parent] = append(children[row.parent], row)
	}
	descendants := map[int]bool{child: true}
	queue := []int{child}
	for next := 0; next < len(queue); next++ {
		for _, row := range children[queue[next]] {
			if !descendants[row.pid] {
				descendants[row.pid] = true
				queue = append(queue, row.pid)
			}
		}
	}
	for _, row := range rows {
		if descendants[row.pid] && row.group != group {
			return true
		}
	}
	return false
}

// guardianRecovery never signals a record's PID. A valid both-zero reservation
// can be removed after its lifetime lease proves no pre-start actor remains.
// Positive-PID uncertainty still needs the guardian's own child/group proof or
// explicit operator reconciliation, even after the lease has been released.
func guardianRecovery(ctx context.Context, stateDir string) error {
	ctx, cancel := context.WithTimeout(ctx, guardianExitGrace)
	defer cancel()
	for {
		count, err := guardianManifestCount(stateDir)
		if err != nil {
			return err
		}
		leases, err := guardianLeaseCount(stateDir)
		if err != nil {
			return err
		}
		if count == 0 && leases == 0 {
			return nil
		}
		if err := waitContext(ctx, 50*time.Millisecond); err != nil {
			return fmt.Errorf("%d guardian manifests and %d live lifetime leases remain; exit is unconfirmed: %w", count, leases, err)
		}
	}
}

func guardianManifestCount(stateDir string) (int, error) {
	dir, err := os.Open(stateDir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = dir.Close() }()
	count, scanned := 0, 0
	for {
		entries, err := dir.ReadDir(128)
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		for _, entry := range entries {
			scanned++
			if scanned > 2*portCount {
				return 0, errors.New("guardian state directory entry limit exceeded")
			}
			if !strings.HasPrefix(entry.Name(), "guardian-") || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			path := filepath.Join(stateDir, entry.Name())
			record, err := guardianReadRecord(path)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return 0, err
			}
			if record.GuardianPID == 0 && record.ChildPID == 0 {
				released, err := guardianReconcileUnstarted(path, record.Port)
				if err != nil {
					return 0, err
				}
				if released {
					continue
				}
			}
			count++
		}
		if errors.Is(err, io.EOF) {
			return count, nil
		}
	}
}

// Leases use stable port names, so retained lock inodes are bounded by the
// allocation range. Never unlink a lock: flock identity belongs to its inode.
// The owner acquires before manifest creation and passes the SAME open-file
// description to the guardian. Close parent copies; never LOCK_UN after fork.
func guardianAcquireLease(stateDir string, port int) (*os.File, error) {
	if port < firstPort || port > lastPort {
		return nil, errors.New("invalid guardian lifetime lease port")
	}
	dir := filepath.Join(stateDir, "leases")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, fmt.Sprintf("port-%d.lock", port))
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, errors.New("guardian lifetime lease must be a regular file")
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

// A manifest can disappear before the resident guardian's final OS exit. Its
// inherited lease still charges capacity. Historical unlocked lock inodes are
// safe to retain; the port namespace bounds the scan and files permanently.
func guardianLeaseCount(stateDir string) (int, error) {
	dir, err := os.Open(filepath.Join(stateDir, "leases"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = dir.Close() }()
	count, scanned := 0, 0
	for {
		entries, err := dir.ReadDir(128)
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		for _, entry := range entries {
			scanned++
			if scanned > portCount {
				return 0, errors.New("guardian lifetime lease entry limit exceeded")
			}
			name := entry.Name()
			port, parseErr := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "port-"), ".lock"))
			if parseErr != nil || port < firstPort || port > lastPort || name != fmt.Sprintf("port-%d.lock", port) {
				return 0, errors.New("unexpected guardian lifetime lease name")
			}
			lease, lockErr := guardianAcquireLease(stateDir, port)
			if errors.Is(lockErr, syscall.EWOULDBLOCK) || errors.Is(lockErr, syscall.EAGAIN) {
				count++
				continue
			}
			if lockErr != nil {
				return 0, lockErr
			}
			if err := lease.Close(); err != nil {
				return 0, err
			}
		}
		if errors.Is(err, io.EOF) {
			return count, nil
		}
	}
}

func guardianReadRecord(path string) (guardianRecord, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return guardianRecord{}, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > guardianFrameBytes {
		return guardianRecord{}, errors.New("guardian manifest must be a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, guardianFrameBytes+1))
	if err != nil {
		return guardianRecord{}, err
	}
	if len(data) > guardianFrameBytes {
		return guardianRecord{}, errors.New("guardian manifest exceeds frame limit")
	}
	var record guardianRecord
	if err := json.Unmarshal(data, &record, json.RejectUnknownMembers(true)); err != nil {
		return guardianRecord{}, err
	}
	if record.Version != guardianProtocol || record.Port < firstPort || record.Port > lastPort ||
		record.GuardianPID < 0 || record.ChildPID < 0 || !filepath.IsAbs(record.Worktree) ||
		record.MaxOpenFiles < 256 || record.MaxOpenFiles > 1<<20 || len(record.Token) != 26 ||
		filepath.Base(path) != "guardian-"+record.Token+".json" {
		return guardianRecord{}, errors.New("invalid or unsupported guardian manifest; reservation retained")
	}
	for _, c := range record.Token {
		if !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567", c) {
			return guardianRecord{}, errors.New("invalid guardian manifest token")
		}
	}
	return record, nil
}

// Absence under the exclusive lifetime lease proves no predecessor can still
// start. Existing positive-PID records are ALWAYS uncertain, regardless of PID
// probes, lease availability or age. For a both-zero record, re-read while
// holding the lease before removing it. No process is signalled here.
func guardianReconcileUnstarted(path string, port int) (bool, error) {
	lease, err := guardianAcquireLease(filepath.Dir(path), port)
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = lease.Close() }()
	record, err := guardianReadRecord(path)
	if errors.Is(err, os.ErrNotExist) {
		// Also persist an earlier removal whose directory sync may have failed.
		if err := guardianRemoveRecord(path); err != nil {
			return false, err
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if record.Port != port || record.GuardianPID != 0 || record.ChildPID != 0 {
		return false, nil
	}
	if err := guardianRemoveRecord(path); err != nil {
		return false, err
	}
	return true, nil
}

const (
	guardianProtocol       = 2
	guardianFrameBytes     = 64 << 10
	guardianCommandTimeout = 2 * time.Second
	guardianExitGrace      = 10 * time.Second
	guardianMaxOpenFiles   = 32
	// One resident guardian plus at most one concurrent ps observer, each
	// bounded by the inherited 32-descriptor soft limit.
	guardianFDReservation    = 2 * guardianMaxOpenFiles
	guardianTelemetryVersion = "v0.0.0-20260625142307-59b4966ccb57"
	guardianTelemetrySum     = "h1:nwGZBCt+FnXUrGsj5vjzAsEmkcaFvd82BbOjECiFYZc="
)

// Process is metadata only; signals must use Signal. ConfirmedDone requires
// guardian/child reap and anchored-group exit, not arbitrary escaped descendants.
// EOF is uncertainty, not exit proof; both channels may close after recovery.
type guardianChild struct {
	Process       *os.Process
	GuardianPID   int
	StatePath     string
	ConfirmedDone <-chan struct{}
	UncertainDone <-chan struct{}
	Signal        func(os.Signal) error
	CloseOwner    func() error
}

// guardianStartError means a reservation may survive this failed startup. The
// owner MUST retain its generation/FD capacity until Child confirms cleanup or
// Released obtains the lifetime lease and proves the manifest is absent or an
// unstarted both-zero reservation. A partial Child may have metadata PID zero.
type guardianStartError struct {
	StatePath   string
	GuardianPID int
	Child       *guardianChild
	Err         error
	port        int
}

func (e *guardianStartError) Error() string {
	return fmt.Sprintf("guardian startup reservation retained at %s: %v", e.StatePath, e.Err)
}

func (e *guardianStartError) Unwrap() error { return e.Err }

func (p *guardianChild) stopFailedStart() error {
	select {
	case <-p.ConfirmedDone:
		return nil
	default:
	}
	_ = p.Signal(syscall.SIGTERM)
	timer := time.NewTimer(exitGrace)
	defer timer.Stop()
	select {
	case <-p.ConfirmedDone:
		return nil
	case <-timer.C:
	}
	if err := p.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("kill failed-start gopls through guardian: %w", err)
	}
	timer.Reset(exitGrace)
	select {
	case <-p.ConfirmedDone:
		return nil
	case <-timer.C:
		return errors.New("failed-start guardian has not confirmed cleanup after SIGKILL")
	}
}

func (e *guardianStartError) Released() (bool, error) {
	if e.Child != nil {
		select {
		case <-e.Child.ConfirmedDone:
			return true, nil
		default:
		}
	}
	return guardianReconcileUnstarted(e.StatePath, e.port)
}

type guardianConfig struct {
	Version      int
	Worktree     string
	Port         int
	StatePath    string
	Token        string
	Command      string
	Args         []string
	ExecArgs     []string
	MaxOpenFiles int
}

// guardianRecord is durable before the actual child starts. Recovery must count
// these files as reservations, including records whose ChildPID is still zero.
// An unexpected guardian death leaves the record; no PID-only signal is safe.
type guardianRecord struct {
	Version      int
	Token        string
	GuardianPID  int
	ChildPID     int
	Worktree     string
	Port         int
	CreatedAt    int64
	Scope        string
	MaxOpenFiles int
	OwnerGone    bool   `json:",omitzero"`
	Uncertain    string `json:",omitzero"`
}

type guardianEvent struct {
	Kind  string
	PID   int    `json:",omitzero"`
	Error string `json:",omitzero"`
}

// The supervisor owns the bounded, concurrency-safe sink inherited by both processes.
func startGuardian(ctx context.Context, worktree string, port int, stateDir string, output io.Writer, maxOpenFiles int) (*guardianChild, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	binary, err := exec.LookPath(goplsBinary)
	if err != nil {
		return nil, err
	}
	if err := guardianCheckTelemetry(binary); err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	config := guardianConfig{Version: guardianProtocol, Worktree: worktree, Port: port,
		Token: rand.Text(), Command: binary, Args: []string{"mcp", "-listen", mcpAddress(port)},
		ExecArgs: []string{"internal-guardian", "child"}, MaxOpenFiles: maxOpenFiles}
	config.StatePath = filepath.Join(stateDir, "guardian-"+config.Token+".json")
	return startGuardianCommand(ctx, config, output, exec.Command(executable, "internal-guardian"))
}

// The internal telemetry switch is a tested dependency contract, not a public
// gopls option. Refuse unknown/replaced implementations rather than leave a
// daemonized telemetry child outside the guardian's process group.
func guardianCheckTelemetry(binary string) error {
	info, err := os.Stat(binary)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 256<<20 {
		return fmt.Errorf("guardian requires a regular gopls executable no larger than 256 MiB")
	}
	build, err := buildinfo.ReadFile(binary)
	if err != nil {
		return fmt.Errorf("guardian cannot verify gopls telemetry contract: %w", err)
	}
	if build.Path != "golang.org/x/tools/gopls" {
		return fmt.Errorf("guardian requires a Go gopls binary with the pinned telemetry contract")
	}
	for _, dep := range build.Deps {
		if dep.Path == "golang.org/x/telemetry" && dep.Version == guardianTelemetryVersion &&
			dep.Sum == guardianTelemetrySum && dep.Replace == nil {
			return nil
		}
	}
	return fmt.Errorf("unsupported gopls telemetry dependency: require unreplaced golang.org/x/telemetry %s; managed telemetry must not fork an unowned session", guardianTelemetryVersion)
}

func guardianEnvironment(env []string, telemetryDir string) []string {
	out := make([]string, 0, len(env)+1)
	for _, value := range env {
		if !strings.HasPrefix(value, "GO_TELEMETRY_CHILD=") && !strings.HasPrefix(value, "GO_TELEMETRY_CHILD_UPLOAD=") && !strings.HasPrefix(value, "TEST_TELEMETRY_DIR=") {
			out = append(out, value)
		}
	}
	return append(out, "GO_TELEMETRY_CHILD=2", "TEST_TELEMETRY_DIR="+telemetryDir)
}

// The Go toolchain's TEST_TELEMETRY_DIR is a second internal compatibility
// contract. The owner must validate the selected per-worktree Go reports this
// exact private directory and mode off before allowing execution. No HOME,
// credentials, module cache, GOTOOLCHAIN or user-global mode is changed.
func guardianTelemetryEnvironment(stateDir string) ([]string, error) {
	if !filepath.IsAbs(stateDir) {
		return nil, errors.New("guardian telemetry state directory must be absolute")
	}
	dir := filepath.Join(stateDir, "telemetry-off")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "mode")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		_, writeErr := file.WriteString("off\n")
		syncErr := file.Sync()
		closeErr := file.Close()
		if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	read, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = read.Close() }()
	info, err := read.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 128 {
		return nil, errors.New("private telemetry mode must be a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(read, 129))
	if err != nil || string(data) != "off\n" {
		return nil, errors.New("private telemetry mode is not off")
	}
	return guardianEnvironment(os.Environ(), dir), nil
}

type guardianController struct {
	control       *os.File
	status        *os.File
	mu            sync.Mutex
	closeOnce     sync.Once
	uncertainOnce sync.Once
	confirmedOnce sync.Once
	completionMu  sync.Mutex
	proof         bool
	reaped        bool
	confirmed     chan struct{}
	uncertain     chan struct{}
	acks          chan guardianEvent
}

func startGuardianCommand(ctx context.Context, config guardianConfig, output io.Writer, command *exec.Cmd) (*guardianChild, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if output == nil {
		return nil, errors.New("guardian output sink is required")
	}
	if err := guardianValidateConfig(config); err != nil {
		return nil, err
	}
	if _, err := guardianTelemetryEnvironment(filepath.Dir(config.StatePath)); err != nil {
		return nil, err
	}
	lease, err := guardianAcquireLease(filepath.Dir(config.StatePath), config.Port)
	if err != nil {
		return nil, err
	}
	// Close only: the guardian inherits the same locked open-file description.
	// An explicit LOCK_UN here would also unlock the guardian's live lease.
	defer func() { _ = lease.Close() }()
	retained := func(err error, child *guardianChild, pid int) (*guardianChild, error) {
		return child, &guardianStartError{StatePath: config.StatePath, GuardianPID: pid,
			Child: child, Err: err, port: config.Port}
	}
	unstarted := func(err error) (*guardianChild, error) {
		if removeErr := guardianRemoveRecord(config.StatePath); removeErr != nil {
			return retained(errors.Join(err, removeErr), nil, 0)
		}
		return nil, err
	}
	// Reserve before the helper can exist. Recovery then cannot overlook a
	// helper between fork and its first durable child identity update.
	initial := guardianRecord{Version: guardianProtocol, Token: config.Token,
		Worktree: config.Worktree, Port: config.Port, CreatedAt: time.Now().UnixMilli(),
		Scope: "process-group; telemetry suppressed", MaxOpenFiles: config.MaxOpenFiles}
	if err := guardianWriteRecord(config.StatePath, initial); err != nil {
		return unstarted(err)
	}
	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		return unstarted(err)
	}
	defer func() { _ = controlRead.Close() }()
	statusRead, statusWrite, err := os.Pipe()
	if err != nil {
		_ = controlWrite.Close()
		return unstarted(err)
	}
	defer func() { _ = statusWrite.Close() }()
	command.ExtraFiles = []*os.File{controlRead, statusWrite, lease}
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	command.Stdout, command.Stderr = output, output
	// Pipe-copy workers must not delay reaping a crashed guardian indefinitely.
	command.WaitDelay = time.Second
	if err := ctx.Err(); err != nil {
		_ = controlWrite.Close()
		_ = statusRead.Close()
		return unstarted(err)
	}
	if err := command.Start(); err != nil {
		_ = controlWrite.Close()
		_ = statusRead.Close()
		return unstarted(err)
	}
	controller := &guardianController{control: controlWrite, status: statusRead,
		confirmed: make(chan struct{}), uncertain: make(chan struct{}), acks: make(chan guardianEvent, 1)}
	child := &guardianChild{Process: &os.Process{}, GuardianPID: command.Process.Pid,
		StatePath: config.StatePath, ConfirmedDone: controller.confirmed, UncertainDone: controller.uncertain,
		Signal: controller.Signal, CloseOwner: controller.CloseOwner}
	started := make(chan guardianEvent, 1)
	go controller.read(started)
	go func() {
		_ = command.Wait()
		if command.ProcessState != nil {
			controller.observeGuardianExit()
		} else {
			controller.failUncertain()
		}
		_ = controller.CloseOwner()
	}()
	data, _ := json.Marshal(config)
	_ = controlWrite.SetWriteDeadline(time.Now().Add(guardianCommandTimeout))
	if _, err := controlWrite.Write(append(data, '\n')); err != nil {
		_ = controller.CloseOwner()
		return retained(err, child, command.Process.Pid)
	}
	select {
	case event := <-started:
		if event.PID > 0 {
			child.Process = &os.Process{Pid: event.PID}
		}
		if event.Kind != "started" {
			_ = controller.CloseOwner()
			return retained(fmt.Errorf("guardian startup: %s", event.Error), child, command.Process.Pid)
		}
		if event.PID <= 0 {
			_ = controller.CloseOwner()
			return retained(errors.New("guardian reported an invalid child PID"), child, command.Process.Pid)
		}
		return child, nil
	case <-controller.uncertain:
		_ = controller.CloseOwner()
		return retained(errors.New("guardian exited before confirming child startup; manifest retained"), child, command.Process.Pid)
	case <-ctx.Done():
		_ = controller.CloseOwner()
		return retained(ctx.Err(), child, command.Process.Pid)
	}
}

func (c *guardianController) CloseOwner() error {
	var err error
	c.closeOnce.Do(func() { err = c.control.Close() })
	return err
}

func (c *guardianController) failUncertain() {
	c.completionMu.Lock()
	proof := c.proof
	c.completionMu.Unlock()
	if proof {
		return
	}
	c.uncertainOnce.Do(func() { close(c.uncertain) })
}

func (c *guardianController) observeCleanupProof() {
	c.completionMu.Lock()
	defer c.completionMu.Unlock()
	c.proof = true
	c.confirmIfCompleteLocked()
}

func (c *guardianController) observeGuardianExit() {
	c.completionMu.Lock()
	defer c.completionMu.Unlock()
	c.reaped = true
	c.confirmIfCompleteLocked()
}

func (c *guardianController) confirmIfCompleteLocked() {
	if c.proof && c.reaped {
		c.confirmedOnce.Do(func() { close(c.confirmed) })
	}
}

func (c *guardianController) read(started chan<- guardianEvent) {
	defer c.failUncertain()
	defer func() { _ = c.status.Close() }()
	defer func() { _ = c.CloseOwner() }()
	scanner := bufio.NewScanner(c.status)
	scanner.Buffer(make([]byte, 4096), guardianFrameBytes)
	first := true
	for scanner.Scan() {
		var event guardianEvent
		if json.Unmarshal(scanner.Bytes(), &event) != nil {
			return
		}
		if first {
			first = false
			select {
			case started <- event:
			default:
				return
			}
			if event.Kind != "started" && event.Kind != "startup-error" {
				return
			}
			continue
		}
		switch event.Kind {
		case "ack":
			select {
			case c.acks <- event:
			default:
				return
			}
		case "confirmed":
			c.observeCleanupProof()
			_ = c.CloseOwner()
			return
		case "uncertain":
			c.failUncertain()
		default:
			return
		}
	}
	if scanner.Err() != nil {
		c.failUncertain()
	}
}

func (c *guardianController) Signal(signal os.Signal) error {
	var command byte
	switch signal {
	case syscall.SIGTERM:
		command = 'T'
	case syscall.SIGKILL:
		command = 'K'
	default:
		return fmt.Errorf("guardian does not support signal %v", signal)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.confirmed:
		return os.ErrProcessDone
	case <-c.uncertain:
		return errors.New("guardian process ownership is uncertain")
	default:
	}
	_ = c.control.SetWriteDeadline(time.Now().Add(guardianCommandTimeout))
	if _, err := c.control.Write([]byte{command}); err != nil {
		c.failUncertain()
		return err
	}
	timer := time.NewTimer(guardianCommandTimeout)
	defer timer.Stop()
	select {
	case event := <-c.acks:
		if event.Error != "" {
			return errors.New(event.Error)
		}
		return nil
	case <-c.confirmed:
		return nil
	case <-c.uncertain:
		return errors.New("guardian failed while signalling; manifest retained")
	case <-timer.C:
		c.failUncertain()
		_ = c.CloseOwner()
		return errors.New("guardian signal acknowledgement timed out; manifest retained")
	}
}

func guardianValidateConfig(config guardianConfig) error {
	if config.Version != guardianProtocol || !filepath.IsAbs(config.Worktree) ||
		!filepath.IsAbs(config.StatePath) || config.Command == "" || len(config.Args) > 64 ||
		len(config.ExecArgs) == 0 || len(config.ExecArgs) > 16 ||
		config.MaxOpenFiles < 256 || config.MaxOpenFiles > 1<<20 ||
		len(config.Token) != 26 || config.Port < firstPort || config.Port > lastPort {
		return errors.New("invalid guardian configuration")
	}
	for _, c := range config.Token {
		if !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567", c) {
			return errors.New("invalid guardian token")
		}
	}
	data, err := json.Marshal(config)
	if err != nil || len(data)+1 > guardianFrameBytes {
		return errors.New("guardian configuration exceeds frame limit")
	}
	return nil
}

func guardianWriteRecord(path string, record guardianRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(data) > guardianFrameBytes {
		return errors.New("guardian record exceeds frame limit")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".guardian-*")
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}
