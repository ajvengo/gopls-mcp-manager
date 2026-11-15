package main

import (
	"bufio"
	"bytes"
	"context"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const restartCheckpointLimit = 8 << 20

// This descriptor must survive all Go defers and finalizers: only process exit
// proves that the old owner's complete OS resource footprint has disappeared.
var restartProcessLifetime *os.File

func restartFDReserve(m *manager) (int, error) {
	base, err := supervisorFDAllowance(m.limits)
	if err != nil {
		return 0, err
	}
	reserve := base + m.limits.OwnerSessions + 8
	if base+reserve > m.limits.OpenFiles {
		return 0, fmt.Errorf("managed FD budget cannot hold both owners during hot restart")
	}
	return reserve, nil
}

func (s *supervisor) awaitPreviousOwner(reserve int) <-chan error {
	result := make(chan error, 1)
	file := s.m.restart.inherited.lifetime
	go func() {
		defer func() { _ = file.Close() }()
		stop := context.AfterFunc(s.ctx, func() { _ = file.Close() })
		defer stop()
		var data [1]byte
		_, err := file.Read(data[:])
		if !errors.Is(err, io.EOF) {
			if err == nil {
				err = errors.New("unexpected data on the process-lifetime pipe")
			}
			result <- fmt.Errorf("previous owner exit is unconfirmed: %w", err)
			return
		}
		s.mu.Lock()
		s.fdReserved -= reserve
		s.mu.Unlock()
		s.restart.mu.Lock()
		s.restart.previousOwner = false
		s.restart.mu.Unlock()
		result <- nil
	}()
	return result
}

type restartOwner struct {
	lock        *os.File
	transferred uint32
	inherited   *restartInheritance
}

func (o *restartOwner) isTransferred() bool { return atomic.LoadUint32(&o.transferred) != 0 }
func (o *restartOwner) markTransferred()    { atomic.StoreUint32(&o.transferred, 1) }
func (o *restartOwner) clearTransferred()   { atomic.StoreUint32(&o.transferred, 0) }

type restartState struct {
	mu            sync.Mutex
	active        bool
	closed        bool // refuses new frontend attachments and /ensure
	admissions    int
	failure       string
	sessions      map[net.Conn]*restartSession
	listener      net.Listener
	slots         chan struct{}
	committed     chan struct{}
	blockHTTP     bool
	previousOwner bool
	abort         context.CancelCauseFunc // stops the owner with a non-nil serve error
}

type restartSession struct {
	peer   *restartPeer
	router *router
	home   string
}

type restartSessionSnapshot struct {
	Home       string
	Sticky     string
	Initialize []byte
	Pause      uint64
	HTTP       bool
}

type restartCheckpoint struct {
	Protocol  string
	Config    string
	Sessions  []restartSessionSnapshot
	Worktrees []string
	Deadline  time.Time
}

type restartInheritance struct {
	checkpoint restartCheckpoint
	ready      *os.File
	commit     *os.File
	activated  bool
	lock       *os.File
	listener   *os.File
	files      []*os.File
	lifetime   *os.File
}

type restartRequest struct {
	Hot     bool
	Binary  string
	Timeout int64
}

type restartReceipt struct {
	Boot  string
	PID   int
	Error string `json:",omitempty"`
}

type restartStatus struct {
	Active bool
	Error  string `json:",omitempty"`
}

func restartCommand(args []string, reload bool, output io.Writer) error {
	flags := flag.NewFlagSet("restart", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	hot := flags.Bool("hot", reload, "replace the owner using fork/exec")
	binary := flags.String("binary", "", "replacement manager binary (requires --hot)")
	timeout := flags.Duration("timeout", 5*time.Minute, "maximum drain and startup wait")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *timeout <= 0 || *timeout > time.Hour || *binary != "" && !*hot || reload && !*hot {
		return fmt.Errorf("usage: restart [--hot] [--binary path] [--timeout duration] | reload [--binary path] [--timeout duration]; timeout must be positive and at most 1h")
	}
	if *hot {
		var err error
		if *binary == "" {
			*binary, err = os.Executable()
		} else {
			*binary, err = filepath.Abs(*binary)
		}
		if err != nil {
			return err
		}
	}
	m, err := newManager()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	body, err := json.Marshal(restartRequest{Hot: *hot, Binary: *binary, Timeout: int64(*timeout)})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://127.0.0.1/restart", bytes.NewReader(body))
	if err != nil {
		return err
	}
	if err := ownerHeaders(req, m, ""); err != nil {
		return err
	}
	client := supervisorHTTPClient(m)
	defer client.CloseIdleConnections()
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("restart refused (HTTP %d): %s", response.StatusCode, data)
	}
	var receipt restartReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		return err
	}
	if *hot {
		if err := restartWait(ctx, func() (bool, error) {
			var out bytes.Buffer
			if err := querySupervisor(ctx, m, "/status", "", &out); err != nil {
				// The old owner closes connections it accepted while handing
				// over the socket; only the outer deadline bounds those.
				if _, ok := errors.AsType[*url.Error](err); ok {
					return false, nil
				}
				return false, err
			}
			var status struct {
				Boot    string
				Restart restartStatus
			}
			if err := json.Unmarshal(out.Bytes(), &status); err != nil {
				return false, err
			}
			if status.Boot != receipt.Boot || status.Restart.Active {
				return false, nil
			}
			if status.Restart.Error != "" {
				return false, errors.New(status.Restart.Error)
			}
			return true, nil
		}); err != nil {
			return fmt.Errorf("replacement owner %d: %w", receipt.PID, err)
		}
	}
	return json.MarshalWrite(output, receipt)
}

func restartWait(ctx context.Context, ready func() (bool, error)) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, err := ready()
		if err != nil || ok {
			return err
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *supervisor) restartStatus() restartStatus {
	s.restart.mu.Lock()
	defer s.restart.mu.Unlock()
	return restartStatus{Active: s.restart.active, Error: s.restart.failure}
}

func (s *supervisor) restartHTTPBlocked() bool {
	s.restart.mu.Lock()
	defer s.restart.mu.Unlock()
	return s.restart.blockHTTP
}

func (s *supervisor) restartQuiescent() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, operations := range s.operationMaps() {
		for _, operation := range operations {
			if !operation.abandoned.IsZero() {
				return false, fmt.Errorf("unconfirmed abandoned work prevents restart")
			}
		}
	}
	if s.operationUsageLocked() != 0 || len(s.frontends.http) != 0 {
		return false, nil
	}
	s.frontends.mu.Lock()
	defer s.frontends.mu.Unlock()
	for _, entry := range s.frontends.backends {
		select {
		case <-entry.ready:
		default:
			return false, nil
		}
		if entry.active != 0 || entry.retiring {
			return false, nil
		}
	}
	return true, nil
}

func (s *supervisor) frontendAdmission() (func(), bool) {
	s.restart.mu.Lock()
	defer s.restart.mu.Unlock()
	if s.restart.closed {
		return nil, false
	}
	s.restart.admissions++
	return func() { s.restart.mu.Lock(); s.restart.admissions--; s.restart.mu.Unlock() }, true
}

func (s *supervisor) registerRestartSession(session *restartSession) func() {
	s.restart.mu.Lock()
	if s.restart.sessions == nil {
		s.restart.sessions = make(map[net.Conn]*restartSession)
	}
	s.restart.sessions[session.peer.conn] = session
	s.restart.mu.Unlock()
	return func() { s.restart.mu.Lock(); delete(s.restart.sessions, session.peer.conn); s.restart.mu.Unlock() }
}

func (s *supervisor) runRestartSession(session *restartSession) {
	peer := session.peer
	peer.start()
	defer func() { _ = peer.Close() }()
	if session.router == nil {
		for {
			message, err := peer.Read(s.ctx)
			if err != nil {
				return
			}
			if barrier := peer.barrier(message); barrier != nil {
				close(barrier)
			} else {
				return
			}
		}
	}
	_ = serveRouter(session.router, peer)
}

func (s *supervisor) handleRestart(w http.ResponseWriter, req *http.Request) {
	var request restartRequest
	data, err := io.ReadAll(io.LimitReader(req.Body, 4097))
	if err != nil || len(data) > 4096 || json.Unmarshal(data, &request) != nil || request.Timeout <= 0 || request.Timeout > int64(time.Hour) || request.Hot && !filepath.IsAbs(request.Binary) {
		http.Error(w, "invalid restart request", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), time.Duration(request.Timeout))
	defer cancel()
	receipt, err := s.restartAll(ctx, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if request.Hot {
		defer func() {
			s.restart.mu.Lock()
			close(s.restart.committed)
			s.restart.mu.Unlock()
			_ = s.restart.listener.Close()
		}()
	}
	data, err = json.Marshal(receipt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// Flush must publish a complete response before the old server closes.
	// A chunked response still needs its final chunk after this handler returns.
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data)
	_ = http.NewResponseController(w).Flush()
}

func (s *supervisor) restartAll(ctx context.Context, request restartRequest) (restartReceipt, error) {
	s.restart.mu.Lock()
	if s.restart.active {
		s.restart.mu.Unlock()
		return restartReceipt{}, fmt.Errorf("a restart is already in progress")
	}
	if s.restart.previousOwner {
		s.restart.mu.Unlock()
		return restartReceipt{}, fmt.Errorf("previous owner exit is still unconfirmed")
	}
	s.restart.active, s.restart.failure = true, ""
	s.restart.mu.Unlock()
	var sessions []*restartSession
	defer func() {
		if s.m.restart.isTransferred() {
			return
		}
		s.restart.mu.Lock()
		s.restart.blockHTTP = false
		s.restart.mu.Unlock()
		resumeCtx, cancel := context.WithTimeout(s.ctx, sendBudget)
		defer cancel()
		for _, session := range sessions {
			// A failed resume closes its own session.
			_ = session.peer.resume(resumeCtx)
		}
		s.restart.mu.Lock()
		s.restart.active, s.restart.closed = false, false
		s.restart.mu.Unlock()
	}()
	if request.Hot {
		if _, err := restartFDReserve(s.m); err != nil {
			return restartReceipt{}, err
		}
		if s.m.restart.lock == nil {
			return restartReceipt{}, fmt.Errorf("owner has no transferable singleton lock")
		}
		fingerprint, err := supervisorConfig(s.m)
		if err != nil {
			return restartReceipt{}, err
		}
		output, err := boundedCommandOutput(ctx, 4096, request.Binary, "internal-restart-check")
		if err != nil {
			return restartReceipt{}, fmt.Errorf("validate replacement: %w", err)
		}
		if string(output) != supervisorProtocol+"\n"+fingerprint+"\n" {
			return restartReceipt{}, fmt.Errorf("replacement protocol/configuration is incompatible")
		}
	}
	// Keep normal ingress, new attachments and cancellation live while admitted
	// calls drain; close admission only once the owner is already quiet.
	if err := restartWait(ctx, s.restartQuiescent); err != nil {
		return restartReceipt{}, err
	}
	s.restart.mu.Lock()
	s.restart.closed = true
	s.restart.mu.Unlock()
	if err := restartWait(ctx, func() (bool, error) {
		s.restart.mu.Lock()
		defer s.restart.mu.Unlock()
		return s.restart.admissions == 0, nil
	}); err != nil {
		return restartReceipt{}, err
	}
	if err := restartWait(ctx, s.restartQuiescent); err != nil {
		return restartReceipt{}, err
	}
	s.restart.mu.Lock()
	for _, session := range s.restart.sessions {
		sessions = append(sessions, session)
	}
	s.restart.mu.Unlock()
	quiet := func() error {
		ready, err := s.restartQuiescent()
		if err == nil && !ready {
			err = fmt.Errorf("new work arrived while pausing; retry restart when clients are quiet")
		}
		return err
	}
	for _, session := range sessions {
		if _, err := session.peer.pause(ctx, quiet); err != nil {
			return restartReceipt{}, fmt.Errorf("pause frontend: %w", err)
		}
	}
	s.restart.mu.Lock()
	s.restart.blockHTTP = true
	s.restart.mu.Unlock()
	if err := quiet(); err != nil {
		return restartReceipt{}, err
	}
	// HTTP has no public session state. Retire its idle internal routers before
	// stopping generations, so no retained cache can race the checkpoint.
	s.frontends.mu.Lock()
	backends := make(map[string]*supervisorHTTPBackend)
	for _, entry := range s.frontends.backends {
		if entry.active != 0 || entry.retiring {
			s.frontends.mu.Unlock()
			return restartReceipt{}, fmt.Errorf("HTTP backend is still active or retiring")
		}
	}
	for home, entry := range s.frontends.backends {
		entry.retiring = true
		backends[home] = entry
	}
	s.frontends.mu.Unlock()
	for home, entry := range backends {
		s.closeHTTPBackend(home, entry)
	}
	s.mu.Lock()
	generations := make([]*ownedGeneration, 0, len(s.generations))
	for _, generation := range s.generations {
		generations = append(generations, generation)
	}
	s.mu.Unlock()
	worktrees := make([]string, 0, len(generations))
	for _, generation := range generations {
		if !s.drain(generation, "operator restart", false) {
			return restartReceipt{}, fmt.Errorf("generation %s cannot be safely drained", generation.worktree)
		}
		worktrees = append(worktrees, generation.worktree)
	}
	if err := restartWait(ctx, func() (bool, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, generation := range s.generations {
			if generation.err != nil {
				return false, generation.err
			}
		}
		return len(s.generations) == 0, nil
	}); err != nil {
		return restartReceipt{}, err
	}
	for _, session := range sessions {
		if session.router != nil {
			if err := restartWait(ctx, func() (bool, error) { return session.router.upstreamReaders.Load() == 0, nil }); err != nil {
				return restartReceipt{}, err
			}
		}
	}
	if err := restartWait(ctx, func() (bool, error) {
		retained := 0
		for _, session := range sessions {
			if session.router != nil && session.router.initializeReservation != nil {
				retained += maxOwnerInitializeBytes
			}
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.retainedBytes == retained && s.operationUsageLocked() == 0, nil
	}); err != nil {
		return restartReceipt{}, err
	}
	if !request.Hot {
		for _, worktree := range worktrees {
			if _, err := s.ensureRecord(ctx, worktree); err != nil {
				return restartReceipt{}, err
			}
		}
		return restartReceipt{Boot: s.boot, PID: os.Getpid()}, nil
	}
	return s.forkReplacement(ctx, request.Binary, sessions, worktrees)
}

func restartConnFile(conn net.Conn) (*os.File, error) {
	if limited, ok := conn.(*httpLimitedConn); ok {
		return restartConnFile(limited.Conn)
	}
	if conn, ok := conn.(interface{ File() (*os.File, error) }); ok {
		return conn.File()
	}
	return nil, fmt.Errorf("connection does not support descriptor inheritance")
}

func (m *manager) restartListener(path string) (net.Listener, error) {
	if m.restart.inherited == nil {
		if err := prepareSupervisorRuntime(path); err != nil {
			return nil, err
		}
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			return nil, err
		}
		listener.SetUnlinkOnClose(false)
		return listener, nil
	}
	file := m.restart.inherited.listener
	listener, err := net.FileListener(file)
	_ = file.Close()
	if err != nil {
		return nil, err
	}
	if listener.Addr().Network() != "unix" || listener.Addr().String() != path {
		_ = listener.Close()
		return nil, fmt.Errorf("invalid inherited listener")
	}
	return listener, nil
}

func (m *manager) closeRestartListener(listener net.Listener, path string) {
	_ = listener.Close()
	if !m.restart.isTransferred() && (m.restart.inherited == nil || m.restart.inherited.activated) {
		_ = os.Remove(path)
	}
}

func (s *supervisor) forkReplacement(ctx context.Context, binary string, sessions []*restartSession, worktrees []string) (restartReceipt, error) {
	fingerprint, err := supervisorConfig(s.m)
	if err != nil {
		return restartReceipt{}, err
	}
	listener, ok := s.restart.listener.(*net.UnixListener)
	if !ok {
		return restartReceipt{}, fmt.Errorf("restart requires a Unix listener")
	}
	deadline, bounded := ctx.Deadline()
	if !bounded {
		return restartReceipt{}, fmt.Errorf("hot restart requires a deadline")
	}
	checkpoint := restartCheckpoint{Protocol: supervisorProtocol, Config: fingerprint, Worktrees: worktrees, Deadline: deadline}
	listenerFile, err := listener.File()
	if err != nil {
		return restartReceipt{}, err
	}
	defer func() { _ = listenerFile.Close() }()
	var files []*os.File
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()
	budget := 4096
	for _, worktree := range worktrees {
		budget += 6*len(worktree) + 8
		if budget > restartCheckpointLimit {
			return restartReceipt{}, fmt.Errorf("worktree checkpoint exceeds byte limit")
		}
	}
	for _, session := range sessions {
		snapshot := restartSessionSnapshot{Home: session.home, HTTP: session.router == nil}
		snapshot.Pause = session.peer.pausedState().id
		if session.router != nil {
			snapshot.Sticky = session.router.sticky
			if initialize := session.router.initialize.Load(); initialize != nil {
				snapshot.Initialize, err = jsonrpc.EncodeMessage(initialize)
				if err != nil {
					return restartReceipt{}, err
				}
			}
		}
		budget += 6*(len(snapshot.Home)+len(snapshot.Sticky)) + 2*len(snapshot.Initialize) + 512
		if budget > restartCheckpointLimit {
			return restartReceipt{}, fmt.Errorf("session checkpoint exceeds byte limit")
		}
		file, err := restartConnFile(session.peer.conn)
		if err != nil {
			return restartReceipt{}, err
		}
		files = append(files, file)
		checkpoint.Sessions = append(checkpoint.Sessions, snapshot)
	}
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return restartReceipt{}, fmt.Errorf("encode checkpoint: %w", err)
	}
	if len(data) > restartCheckpointLimit {
		return restartReceipt{}, fmt.Errorf("checkpoint exceeds byte limit")
	}
	state, err := os.CreateTemp("", "gopls-mcp-handoff-*")
	if err != nil {
		return restartReceipt{}, err
	}
	defer func() { _ = state.Close() }()
	if err := os.Remove(state.Name()); err != nil {
		return restartReceipt{}, err
	}
	if _, err := state.Write(data); err != nil {
		return restartReceipt{}, err
	}
	if _, err := state.Seek(0, io.SeekStart); err != nil {
		return restartReceipt{}, err
	}
	readyRead, readyWrite, err := os.Pipe()
	if err != nil {
		return restartReceipt{}, err
	}
	defer func() { _ = readyRead.Close(); _ = readyWrite.Close() }()
	commitRead, commitWrite, err := os.Pipe()
	if err != nil {
		return restartReceipt{}, err
	}
	defer func() { _ = commitRead.Close(); _ = commitWrite.Close() }()
	lifetimeRead, lifetimeWrite, err := os.Pipe()
	if err != nil {
		return restartReceipt{}, err
	}
	defer func() {
		_ = lifetimeRead.Close()
		if !s.m.restart.isTransferred() {
			_ = lifetimeWrite.Close()
		}
	}()
	command := exec.Command(binary, "internal-restart-child")
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	// Startup panics precede the successor's own log sinks.
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	command.ExtraFiles = append([]*os.File{state, readyWrite, commitRead, s.m.restart.lock, listenerFile, lifetimeRead}, files...)
	if err := command.Start(); err != nil {
		return restartReceipt{}, err
	}
	_ = readyWrite.Close()
	_ = commitRead.Close()
	committed := false
	defer func() {
		if !committed {
			s.m.restart.clearTransferred()
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	stop := context.AfterFunc(ctx, func() { _ = readyRead.Close() })
	defer stop()
	line, err := bufio.NewReader(io.LimitReader(readyRead, 4096)).ReadBytes('\n')
	if err != nil {
		return restartReceipt{}, fmt.Errorf("replacement failed before readiness: %w", err)
	}
	var receipt restartReceipt
	if err := json.Unmarshal(line, &receipt); err != nil {
		return restartReceipt{}, err
	}
	if receipt.Error != "" {
		return restartReceipt{}, errors.New(receipt.Error)
	}
	if receipt.PID != command.Process.Pid || len(receipt.Boot) != 32 {
		return restartReceipt{}, fmt.Errorf("invalid replacement readiness")
	}
	s.m.restart.markTransferred()
	if err := ctx.Err(); err != nil {
		return restartReceipt{}, err
	}
	if _, err := commitWrite.Write([]byte{1}); err != nil {
		return restartReceipt{}, err
	}
	committed = true
	restartProcessLifetime = lifetimeWrite
	s.restart.mu.Lock()
	s.restart.committed = make(chan struct{})
	s.restart.mu.Unlock()
	go func() { _ = command.Wait() }()
	return receipt, nil
}

func runRestartCheck(output io.Writer) error {
	m, err := newManager()
	if err != nil {
		return err
	}
	fingerprint, err := supervisorConfig(m)
	if err != nil {
		return err
	}
	if _, err := restartFDReserve(m); err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, "%s\n%s\n", supervisorProtocol, fingerprint)
	return err
}

func runRestartChild() (err error) {
	ready := os.NewFile(4, "restart-ready")
	defer func() {
		if err != nil {
			data, _ := json.Marshal(restartReceipt{Error: err.Error()})
			_, _ = ready.Write(append(data, '\n'))
		}
		_ = ready.Close()
	}()
	state := os.NewFile(3, "restart-state")
	data, err := io.ReadAll(io.LimitReader(state, restartCheckpointLimit+1))
	_ = state.Close()
	if err != nil || len(data) > restartCheckpointLimit {
		return fmt.Errorf("invalid inherited checkpoint")
	}
	var checkpoint restartCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return err
	}
	m, err := newManager()
	if err != nil {
		return err
	}
	fingerprint, err := supervisorConfig(m)
	if err != nil {
		return err
	}
	if checkpoint.Protocol != supervisorProtocol || checkpoint.Config != fingerprint || len(checkpoint.Sessions) > m.limits.OwnerSessions || len(checkpoint.Sessions) > m.limits.HTTPConnections || len(checkpoint.Worktrees) > m.limits.SharedServers {
		return fmt.Errorf("incompatible inherited checkpoint")
	}
	// Inherited descriptors arrive without FD_CLOEXEC. A gopls or guardian
	// holding the singleton lock would outlive this owner and block recovery.
	for fd := 3; fd < 9+len(checkpoint.Sessions); fd++ {
		syscall.CloseOnExec(fd)
	}
	lifetime, err := pollableFile(8, "restart-lifetime")
	if err != nil {
		return err
	}
	m.restart.inherited = &restartInheritance{checkpoint: checkpoint, ready: ready, commit: os.NewFile(5, "restart-commit"), lock: os.NewFile(6, "restart-lock"), listener: os.NewFile(7, "restart-listener"), lifetime: lifetime}
	for index := range checkpoint.Sessions {
		m.restart.inherited.files = append(m.restart.inherited.files, os.NewFile(uintptr(9+index), "restart-session"))
	}
	defer func() {
		for _, file := range m.restart.inherited.files {
			_ = file.Close()
		}
		_ = m.restart.inherited.lock.Close()
		_ = m.restart.inherited.listener.Close()
	}()
	defer func() { _ = m.restart.inherited.commit.Close(); _ = m.restart.inherited.lifetime.Close() }()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runSupervisor(ctx, m)
}

// Inherited descriptors arrive blocking, and Close cannot interrupt a pending
// Read on a blocking file. The previous-owner wait must stay cancellable.
func pollableFile(fd int, name string) (*os.File, error) {
	if err := syscall.SetNonblock(fd, true); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (s *supervisor) restoreRestartSessions() error {
	inherit := s.m.restart.inherited
	if inherit == nil {
		return nil
	}
	if inherit.checkpoint.Deadline.IsZero() || time.Until(inherit.checkpoint.Deadline) <= 0 || time.Until(inherit.checkpoint.Deadline) > time.Hour {
		return fmt.Errorf("invalid inherited restart deadline")
	}
	if len(inherit.files) != len(inherit.checkpoint.Sessions) {
		return fmt.Errorf("inherited descriptor count mismatch")
	}
	reserve, err := restartFDReserve(s.m)
	if err != nil {
		return err
	}
	if inherit.lifetime == nil {
		return fmt.Errorf("missing previous-owner lifetime pipe")
	}
	s.mu.Lock()
	s.fdReserved += reserve
	s.mu.Unlock()
	s.restart.mu.Lock()
	s.restart.previousOwner = true
	s.restart.mu.Unlock()
	var sessions []*restartSession
	for index, snapshot := range inherit.checkpoint.Sessions {
		if !filepath.IsAbs(snapshot.Home) || len(snapshot.Home) > 4096 || len(snapshot.Sticky) > 4096 || snapshot.Pause == 0 {
			return fmt.Errorf("invalid inherited session")
		}
		file := inherit.files[index]
		conn, err := net.FileConn(file)
		_ = file.Close()
		if err != nil {
			return err
		}
		s.restart.slots <- struct{}{}
		conn = &httpLimitedConn{Conn: conn, slots: s.restart.slots}
		peer := newRestartPeer(s.ctx, conn, bufio.NewReader(conn), s.m.limits.MessageBytes, false)
		peer.sequence = snapshot.Pause
		peer.paused = &restartPauseState{id: snapshot.Pause, resume: make(chan struct{})}
		session := &restartSession{peer: peer, home: snapshot.Home}
		if !snapshot.HTTP {
			r := newRouter(s.ctx, s, snapshot.Home)
			r.sticky = snapshot.Sticky
			if len(snapshot.Initialize) > 0 {
				message, err := jsonrpc.DecodeMessage(snapshot.Initialize)
				if err != nil {
					return err
				}
				initialize, ok := message.(*jsonrpc.Request)
				if !ok || initialize.Method != "initialize" || len(initialize.Params)+len(initialize.Method) > maxOwnerInitializeBytes || !validOwnerID(initialize.ID) {
					return fmt.Errorf("invalid inherited initialize")
				}
				r.initializeReservation, err = s.reserveMessageBytes(maxOwnerInitializeBytes)
				if err != nil {
					return err
				}
				r.initialize.Store(initialize)
			}
			session.router = r
		}
		s.frontends.sessions[conn] = struct{}{}
		s.registerRestartSession(session)
		sessions = append(sessions, session)
	}
	inherit.checkpoint.Sessions, inherit.files = nil, nil
	receipt, err := json.Marshal(restartReceipt{Boot: s.boot, PID: os.Getpid()})
	if err != nil {
		return err
	}
	if _, err := inherit.ready.Write(append(receipt, '\n')); err != nil {
		return err
	}
	_ = inherit.ready.Close()
	var commit [1]byte
	if _, err := io.ReadFull(inherit.commit, commit[:]); err != nil || commit[0] != 1 {
		return fmt.Errorf("handoff was not committed")
	}
	_ = inherit.commit.Close()
	inherit.activated = true
	s.restart.mu.Lock()
	s.restart.active, s.restart.closed, s.restart.blockHTTP = true, true, true
	s.restart.mu.Unlock()
	for _, session := range sessions {
		go s.runRestoredSession(session)
	}
	previous := s.awaitPreviousOwner(reserve)
	go func() {
		ctx, cancel := context.WithDeadline(s.ctx, inherit.checkpoint.Deadline)
		defer cancel()
		var err error
		select {
		case err = <-previous:
		case <-ctx.Done():
			err = ctx.Err()
		}
		if err != nil {
			// Without exit proof the old owner may still hold its processes and
			// descriptors. Fail the handoff: frontends see EOF and reconnect.
			err = fmt.Errorf("hot restart handoff failed: %w", err)
			s.restart.mu.Lock()
			s.restart.failure = err.Error()
			s.restart.mu.Unlock()
			s.restart.abort(err)
			return
		}
		for _, worktree := range inherit.checkpoint.Worktrees {
			if _, err = s.ensureRecord(ctx, worktree); err != nil {
				break
			}
		}
		inherit.checkpoint.Worktrees = nil
		s.restart.mu.Lock()
		s.restart.blockHTTP = false
		s.restart.mu.Unlock()
		resumeCtx, resumeCancel := context.WithTimeout(s.ctx, sendBudget)
		defer resumeCancel()
		for _, session := range sessions {
			if resumeErr := session.peer.resume(resumeCtx); resumeErr != nil {
				err = errors.Join(err, resumeErr)
			}
		}
		s.restart.mu.Lock()
		if err != nil {
			s.restart.failure = err.Error()
		}
		s.restart.active, s.restart.closed = false, false
		s.restart.mu.Unlock()
	}()
	return nil
}

func (s *supervisor) runRestoredSession(session *restartSession) {
	defer func() {
		s.restart.mu.Lock()
		delete(s.restart.sessions, session.peer.conn)
		s.restart.mu.Unlock()
		s.frontends.mu.Lock()
		delete(s.frontends.sessions, session.peer.conn)
		s.frontends.mu.Unlock()
	}()
	s.runRestartSession(session)
}

func (r *router) startUpstreamReader(conn mcp.Connection, worktree string) {
	r.upstreamReaders.Add(1)
	go func() { defer r.upstreamReaders.Add(-1); r.readFromUpstream(conn, worktree) }()
}

func restartBridge(ctx context.Context, m *manager, home string, input io.Reader, output io.Writer) error {
	conn, reader, err := openSupervisorAttachment(ctx, m, home, "/bridge")
	if err != nil {
		return err
	}
	peer := newRestartPeer(ctx, conn, reader, m.limits.MessageBytes, true)
	peer.start()
	defer func() { _ = peer.Close() }()
	readCloser, ok := input.(io.ReadCloser)
	if !ok {
		readCloser = io.NopCloser(input)
	}
	stdio := transport.NewStdio(readCloser, output, m.limits.MessageBytes)
	defer func() { _ = stdio.Close() }()
	go func() {
		for {
			message, err := stdio.Read(ctx)
			if err == nil {
				err = peer.Write(ctx, message)
			}
			if err != nil {
				_ = peer.Close()
				return
			}
		}
	}()
	for {
		message, err := peer.Read(ctx)
		if err != nil {
			// The peer context derives from ctx: cancellation may surface as EOF.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := stdio.Write(ctx, message); err != nil {
			return err
		}
	}
}
