package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func guardianStateDir(m *manager) string {
	return filepath.Join(filepath.Dir(m.mapPath), "gopls-mcp-guardians")
}

func prepareSupervisorRuntime(path string) error {
	dir := filepath.Dir(path)
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	identity, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode().Perm() != 0o700 || !ok || identity.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("supervisor runtime directory must be a private owned directory with mode0700")
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("supervisor endpoint path is not a socket")
		}
		return os.Remove(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func withSupervisorSingleton(ctx context.Context, m *manager, run func() error) error {
	if err := os.MkdirAll(filepath.Dir(m.mapPath), 0o700); err != nil {
		return err
	}
	var f *os.File
	var err error
	if m.restart.inherited != nil {
		f = m.restart.inherited.lock
		actual, statErr := f.Stat()
		expected, pathErr := os.Stat(m.mapPath + ".owner.lock")
		if statErr != nil || pathErr != nil || !os.SameFile(actual, expected) {
			_ = f.Close()
			return fmt.Errorf("invalid inherited singleton lock")
		}
	} else {
		f, err = os.OpenFile(m.mapPath+".owner.lock", os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
	}
	defer func() { _ = f.Close() }()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("another supervisor already owns this registry: %w", err)
	}
	m.restart.lock = f
	// A transferred or inherited open file description is shared with the other
	// owner, so only close releases it; LOCK_UN would unlock that owner too.
	// Otherwise unlock explicitly: a forked, not yet exec'd descendant shares
	// the description and would keep the lock past close.
	defer func() {
		if !m.restart.isTransferred() && m.restart.inherited == nil {
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	return run()
}

func runSupervisor(ctx context.Context, m *manager) error {
	return withSupervisorSingleton(ctx, m, func() error {
		if _, err := supervisorConfig(m); err != nil {
			return err
		}
		if err := guardianRecovery(ctx, guardianStateDir(m)); err != nil {
			return err
		}
		if err := validateOwnerRegistry(ctx, m); err != nil {
			return err
		}
		if err := enforceSupervisorFDLimit(m.limits); err != nil {
			return err
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		logs, err := newOwnerLog(ctx, filepath.Dir(m.logPath("")), m.limits.SharedServers+1, int64(m.limits.OwnerLogBytes))
		if err != nil {
			return err
		}
		defer logs.Close()
		diagnostics, err := logs.Sink(filepath.Join(filepath.Dir(m.logPath("")), "manager.log"))
		if err != nil {
			return err
		}
		m.ownerLogs, m.diagnostics = logs, diagnostics
		go func() {
			select {
			case <-ctx.Done():
			case failure := <-logs.Failures():
				if failure != nil {
					cancel()
				}
			}
		}()
		path, err := supervisorSocket(m)
		if err != nil {
			return err
		}
		listener, err := m.restartListener(path)
		if err != nil {
			return err
		}
		defer m.closeRestartListener(listener, path)
		if err := os.Chmod(path, 0o600); err != nil {
			return err
		}

		err = serveSupervisor(ctx, m, listener)
		if failure := logs.Snapshot().Failure; failure != "" {
			return errors.Join(err, fmt.Errorf("owner log failed; all delivery fenced: %s", failure))
		}
		return err
	})
}

func (m *manager) validateManagedToolchain(ctx context.Context, worktree string) error {
	env, err := guardianTelemetryEnvironment(guardianStateDir(m))
	if err != nil {
		return err
	}
	out, err := boundedCommandOutputEnv(ctx, env, 64<<10, "go", "-C", worktree, "env", "GOTELEMETRY", "GOTELEMETRYDIR")
	if err != nil {
		return fmt.Errorf("validate managed Go telemetry contract: %w", err)
	}
	want := "off\n" + filepath.Join(guardianStateDir(m), "telemetry-off") + "\n"
	if string(out) != want {
		return fmt.Errorf("selected Go toolchain does not honor managed private telemetry-off contract")
	}
	return nil
}

func (m *manager) startOwnedGuardian(ctx context.Context, worktree string, port, quota int) (*guardianChild, error) {
	if err := m.validateManagedToolchain(ctx, worktree); err != nil {
		return nil, err
	}
	sink, err := m.ownerLogs.Sink(m.logPath(worktree))
	if err != nil {
		return nil, err
	}
	child, err := startGuardian(ctx, worktree, port, guardianStateDir(m), sink, quota)
	var retained *guardianStartError
	if child == nil && !errors.As(err, &retained) {
		m.ownerLogs.Release(m.logPath(worktree))
	}
	return child, err
}

func querySupervisor(ctx context.Context, m *manager, endpoint, home string, output io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, routingBudget+sendBudget)
	defer cancel()
	client := supervisorHTTPClient(m)
	defer client.CloseIdleConnections()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1"+endpoint, nil)
	if err := ownerHeaders(req, m, home); err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("supervisor refused (HTTP%d): %s", resp.StatusCode, strings.TrimSpace(string(message)))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRegistryBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxRegistryBytes {
		return fmt.Errorf("supervisor response exceeds byte limit")
	}
	_, err = output.Write(body)
	return err
}

const supervisorProtocol = "2"

const supervisorVersionHeader = "Gopls-Manager-Owner-Version"

const supervisorConfigHeader = "Gopls-Manager-Owner-Config"

const supervisorHomeHeader = "Gopls-Manager-Home"

type supervisorHTTPBackend struct {
	backend  *httpBackend
	handler  http.Handler
	ready    chan struct{}
	err      error
	active   int
	lastUsed time.Time
	retiring bool // remains charged until actual backend teardown completes
}

type supervisorFrontends struct {
	mu       sync.Mutex
	sessions map[net.Conn]struct{}
	backends map[string]*supervisorHTTPBackend
	http     chan struct{}
}

func (s *supervisor) initializeFrontends() {
	s.frontends = &supervisorFrontends{sessions: make(map[net.Conn]struct{}), backends: make(map[string]*supervisorHTTPBackend),
		http: make(chan struct{}, min(s.m.limits.Session, s.m.limits.HTTPBytes/s.m.limits.MessageBytes))}
}

func supervisorConfig(m *manager) (string, error) {
	// JSONv2 deliberately has no default time.Duration encoding. Encode every
	// duration as integer nanoseconds while retaining the complete Limits struct,
	// so new policy fields cannot silently fall outside the compatibility check.
	durations := json.MarshalFunc(func(value time.Duration) ([]byte, error) {
		return []byte(strconv.FormatInt(int64(value), 10)), nil
	})
	data, err := json.Marshal(m.limits, json.Deterministic(true), json.WithMarshalers(durations))
	if err != nil {
		return "", fmt.Errorf("encode supervisor configuration: %w", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func supervisorSocket(m *manager) (string, error) {
	parent, err := filepath.EvalSymlinks(filepath.Dir(m.mapPath))
	if err != nil {
		return "", err
	}
	identity := sha256.Sum256([]byte(filepath.Join(parent, filepath.Base(m.mapPath))))
	dir := fmt.Sprintf("/tmp/gopls-mcp-owner-%d-%x", os.Getuid(), identity[:8])
	return filepath.Join(dir, "owner.sock"), nil
}

func supervisorHTTPClient(m *manager) *http.Client {
	return &http.Client{Transport: &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			path, err := supervisorSocket(m)
			if err != nil {
				return nil, err
			}
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		},
		MaxConnsPerHost: m.limits.HTTPConnections, MaxIdleConns: m.limits.HTTPConnections,
		MaxIdleConnsPerHost: m.limits.HTTPConnections, IdleConnTimeout: time.Minute,
		// JSONResponse sends headers only after tool execution. Its execution
		// budget belongs to the caller, not a transport-wide header timeout.
		MaxResponseHeaderBytes: 16 << 10,
	}}
}

func ownerHeaders(req *http.Request, m *manager, home string) error {
	fingerprint, err := supervisorConfig(m)
	if err != nil {
		return err
	}
	req.Header.Set(supervisorVersionHeader, supervisorProtocol)
	req.Header.Set(supervisorConfigHeader, fingerprint)
	if home != "" {
		req.Header.Set(supervisorHomeHeader, home)
	}
	return nil
}

func supervisorPing(ctx context.Context, m *manager) error {
	client := supervisorHTTPClient(m)
	defer client.CloseIdleConnections()
	query, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(query, http.MethodGet, "http://127.0.0.1/ping", nil)
	if err := ownerHeaders(req, m, ""); err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("supervisor protocol/configuration rejected (HTTP %d); restart it with matching configuration", resp.StatusCode)
	}
	return nil
}

func supervisorAbsent(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED)
}

func ensureSupervisor(ctx context.Context, m *manager) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout+2*supervisorTermGrace)
	defer cancel()
	if err := os.MkdirAll(filepath.Dir(m.mapPath), 0o700); err != nil {
		return err
	}
	if err := supervisorPing(ctx, m); err == nil {
		return nil
	} else if !supervisorAbsent(err) {
		return err
	}
	return withFileLock(ctx, m.mapPath+".owner-start", func() error {
		if err := supervisorPing(ctx, m); err == nil {
			return nil
		} else if !supervisorAbsent(err) {
			return err
		}
		if err := validateFrontendRegistry(m); err != nil {
			return err
		}
		if _, err := exec.LookPath(goplsBinary); err != nil {
			return fmt.Errorf("find gopls before starting supervisor: %w", err)
		}
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		// The owner does not inherit frontend pipes or terminal lifetime. It
		// opens its bounded diagnostic/log sink itself after startup.
		null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		defer func() { _ = null.Close() }()
		cmd := exec.Command(executable, "supervisor")
		cmd.Stdin, cmd.Stdout, cmd.Stderr = null, null, null
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start persistent supervisor: %w", err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		startup, cancel := context.WithTimeout(ctx, readyTimeout+supervisorTermGrace)
		defer cancel()
		for {
			if err := supervisorPing(startup, m); err == nil {
				return nil
			} else if !supervisorAbsent(err) {
				return err
			}
			select {
			case err := <-done:
				return fmt.Errorf("persistent supervisor exited before readiness: %v; run supervisor in the foreground for the startup error", err)
			default:
			}
			if err := waitContext(startup, 20*time.Millisecond); err != nil {
				return fmt.Errorf("wait for persistent supervisor: %w", err)
			}
		}
	})
}

// A frontend has no recovery authority. In particular, a dead gopls PID is
// insufficient proof that its guardian and descendants have exited.
func validateFrontendRegistry(m *manager) error {
	records, _, err := readMap(m.mapPath)
	if err != nil {
		return err
	}
	for _, r := range records {
		if r.OwnerBoot == "" {
			return fmt.Errorf("cannot adopt live or uncertain legacy gopls pid %d; explicit offline cleanup is required", r.PID)
		}
	}
	return nil
}

func validateOwnerRegistry(ctx context.Context, m *manager) error {
	records, _, err := readMap(m.mapPath)
	if err != nil {
		return err
	}
	for _, r := range records {
		if r.OwnerBoot == "" {
			return fmt.Errorf("legacy process reservation requires explicit offline cleanup")
		}
		if !errors.Is(syscall.Kill(r.PID, 0), syscall.ESRCH) {
			return fmt.Errorf("cannot adopt live or uncertain gopls pid %d from a previous/legacy owner; stop legacy managers and wait for guardian cleanup before restarting", r.PID)
		}
	}
	_, err = m.withMap(ctx, func(current []record) ([]record, error) {
		for _, r := range current {
			if r.OwnerBoot == "" || !errors.Is(syscall.Kill(r.PID, 0), syscall.ESRCH) {
				return nil, fmt.Errorf("registry changed during supervisor startup; ownership is uncertain")
			}
		}
		return nil, nil
	})
	return err
}

func serveSupervisor(ctx context.Context, m *manager, listener net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := newSupervisor(ctx, m)
	s.initializeFrontends()
	s.bindListener(listener)
	if err := s.restoreRestartSessions(); err != nil {
		return err
	}
	return s.serve()
}

// Restored sessions consume connection slots before serve starts accepting.
func (s *supervisor) bindListener(listener net.Listener) {
	s.restart.listener = listener
	s.restart.slots = make(chan struct{}, s.m.limits.HTTPConnections)
}

func (s *supervisor) serve() error {
	ctx, m := s.ctx, s.m
	defer s.shutdown()
	server := &http.Server{Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10,
		ErrorLog:    log.New(m.diagnosticWriter(), "supervisor HTTP: ", 0),
		BaseContext: func(net.Listener) context.Context { return ctx }}
	defer func() { _ = server.Close(); s.closeFrontends() }()
	stop := context.AfterFunc(ctx, func() { _ = server.Close(); s.closeFrontends() })
	defer stop()
	go s.maintenance()
	err := server.Serve(&httpConnectionListener{Listener: s.restart.listener, slots: s.restart.slots})
	s.restart.mu.Lock()
	committed := s.restart.committed
	s.restart.mu.Unlock()
	// Only a committed hot restart closes the listener under a running server.
	if errors.Is(err, http.ErrServerClosed) || committed != nil && errors.Is(err, net.ErrClosed) {
		if committed != nil {
			<-committed
		}
		if cause := context.Cause(ctx); !errors.Is(cause, context.Canceled) {
			return cause
		}
		return nil
	}
	return err
}

func (s *supervisor) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w = &httpReplyWriter{ResponseWriter: w}
		fingerprint, err := supervisorConfig(s.m)
		if err != nil {
			http.Error(w, "supervisor configuration cannot be encoded", http.StatusInternalServerError)
			return
		}
		if req.Header.Get(supervisorVersionHeader) != supervisorProtocol || req.Header.Get(supervisorConfigHeader) != fingerprint {
			http.Error(w, "unsupported supervisor protocol or configuration", http.StatusConflict)
			return
		}
		if req.Method == http.MethodGet && req.URL.Path == "/ping" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if req.Method == http.MethodGet && req.URL.Path == "/status" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.MarshalWrite(w, s.snapshot())
			return
		}
		if req.Method == http.MethodGet && req.URL.Path == "/list" {
			if err := s.list(req.Context(), w); err != nil {
				return
			}
			return
		}
		if req.Method == http.MethodPost && req.URL.Path == "/restart" {
			s.handleRestart(w, req)
			return
		}
		if req.URL.Path == "/ensure" {
			done, ok := s.frontendAdmission()
			if !ok {
				http.Error(w, "owner is restarting", http.StatusServiceUnavailable)
				return
			}
			defer done()
		}
		home := req.Header.Get(supervisorHomeHeader)
		if !filepath.IsAbs(home) || len(home) > 4096 {
			http.Error(w, "absolute home worktree required", http.StatusBadRequest)
			return
		}
		switch {
		case req.Method == http.MethodConnect && (req.URL.Path == "/bridge" || req.URL.Path == "/http-frontend"):
			s.attachFrontend(w, req, home)
		case req.URL.Path == "/mcp":
			select {
			case s.frontends.http <- struct{}{}:
				defer func() { <-s.frontends.http }()
			default:
				http.Error(w, "global supervisor HTTP body capacity reached", http.StatusServiceUnavailable)
				return
			}
			if s.restartHTTPBlocked() {
				http.Error(w, "owner is restarting", http.StatusServiceUnavailable)
				return
			}
			s.http(w, req, home)
		case req.Method == http.MethodGet && req.URL.Path == "/ensure":
			if err := s.validateHome(req.Context(), home); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			r, err := s.ensureRecord(req.Context(), home)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			_, _ = fmt.Fprintln(w, r.Port)
		default:
			http.NotFound(w, req)
		}
	})
}

func (s *supervisor) validateHome(ctx context.Context, home string) error {
	query, cancel := context.WithTimeout(ctx, routingBudget)
	defer cancel()
	type result struct {
		path string
		err  error
	}
	resolved, err := ownerFilesystemCall(query, s, func() result {
		path, err := worktreePath(query, home)
		return result{path, err}
	})
	if err != nil {
		return err
	}
	if resolved.err != nil {
		return resolved.err
	}
	if resolved.path != home {
		return fmt.Errorf("home must name its physical worktree root")
	}
	return nil
}

func (s *supervisor) attachFrontend(w http.ResponseWriter, req *http.Request, home string) {
	done, ok := s.frontendAdmission()
	if !ok {
		http.Error(w, "owner is restarting", http.StatusServiceUnavailable)
		return
	}
	release := sync.OnceFunc(done)
	defer release()
	s.frontends.mu.Lock()
	full := len(s.frontends.sessions)+len(s.frontends.backends) >= s.m.limits.OwnerSessions
	if full {
		s.frontends.mu.Unlock()
		http.Error(w, "global supervisor session capacity reached", http.StatusServiceUnavailable)
		return
	}
	// Reserve before initialization; replace the placeholder only after hijack.
	reservation := &sessionReservationConn{}
	s.frontends.sessions[reservation] = struct{}{}
	s.frontends.mu.Unlock()
	defer func() {
		s.frontends.mu.Lock()
		delete(s.frontends.sessions, reservation)
		s.frontends.mu.Unlock()
	}()
	if err := s.validateHome(req.Context(), home); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := s.ensureRecord(req.Context(), home); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	conn, buffered, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Time{})
	if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := buffered.Flush(); err != nil {
		return
	}
	s.frontends.mu.Lock()
	delete(s.frontends.sessions, reservation)
	s.frontends.sessions[conn] = struct{}{}
	s.frontends.mu.Unlock()
	defer func() {
		s.frontends.mu.Lock()
		delete(s.frontends.sessions, conn)
		s.frontends.mu.Unlock()
	}()
	session := &restartSession{peer: newRestartPeer(s.ctx, conn, buffered.Reader, s.m.limits.MessageBytes, false), home: home}
	if req.URL.Path != "/http-frontend" {
		session.router = newRouter(s.ctx, s, home)
	}
	unregister := s.registerRestartSession(session)
	defer unregister()
	release()
	s.runRestartSession(session)
}

// Unique comparable identity for an admission in progress, without allocating
// an OS descriptor. Never used for I/O.
type sessionReservationConn struct{ net.Conn }

func (*sessionReservationConn) Close() error { return nil }

func (s *supervisor) http(w http.ResponseWriter, req *http.Request, home string) {
	s.frontends.mu.Lock()
	entry := s.frontends.backends[home]
	if entry != nil && entry.retiring {
		s.frontends.mu.Unlock()
		http.Error(w, "HTTP backend is retiring", http.StatusServiceUnavailable)
		return
	}
	if entry != nil && entry.failed() {
		retire := entry.active == 0
		if retire {
			entry.retiring = true
		}
		s.frontends.mu.Unlock()
		if retire {
			go s.closeHTTPBackend(home, entry)
		}
		http.Error(w, "HTTP backend failed; a new request is required after retirement", http.StatusServiceUnavailable)
		return
	}
	if entry == nil {
		if len(s.frontends.sessions)+len(s.frontends.backends) >= s.m.limits.OwnerSessions {
			s.frontends.mu.Unlock()
			http.Error(w, "global supervisor session capacity reached", http.StatusServiceUnavailable)
			return
		}
		entry = &supervisorHTTPBackend{ready: make(chan struct{}), lastUsed: s.now()}
		s.frontends.backends[home] = entry
		go s.initializeHTTPBackend(home, entry)
	}
	entry.active++
	s.frontends.mu.Unlock()
	defer func() {
		s.frontends.mu.Lock()
		entry.active--
		failed := entry.failed()
		if !failed {
			entry.lastUsed = s.now()
		}
		retire := failed && entry.active == 0 && !entry.retiring
		if retire {
			entry.retiring = true
		}
		s.frontends.mu.Unlock()
		if retire {
			go s.closeHTTPBackend(home, entry)
		}
	}()
	select {
	case <-entry.ready:
		if entry.err != nil {
			http.Error(w, entry.err.Error(), http.StatusServiceUnavailable)
			return
		}
		if entry.failed() {
			http.Error(w, "HTTP backend failed before delivery", http.StatusServiceUnavailable)
			return
		}
		entry.handler.ServeHTTP(w, req)
	case <-req.Context().Done():
		return
	case <-s.ctx.Done():
		return
	}
}

// Backend publication is synchronized by ready. A canceled router cannot be
// revived by refreshing the entry's idle timestamp with another failed request.
func (entry *supervisorHTTPBackend) failed() bool {
	select {
	case <-entry.ready:
		return entry.err != nil || entry.backend != nil && entry.backend.r.ctx != nil && entry.backend.r.ctx.Err() != nil
	default:
		return false
	}
}

func (s *supervisor) initializeHTTPBackend(home string, entry *supervisorHTTPBackend) {
	if err := s.validateHome(s.ctx, home); err != nil {
		entry.err = err
		close(entry.ready)
		return
	}
	b := newHTTPBackend(s.ctx, s, home)
	b.start()
	instructions, err := b.initialize(s.ctx)
	if err == nil {
		entry.backend, entry.handler = b, newHTTPHandler(b, instructions)
	} else {
		entry.err = err
		b.close()
	}
	close(entry.ready)
}

func (s *supervisor) retireHTTPBackends() {
	s.frontends.mu.Lock()
	retired := make(map[string]*supervisorHTTPBackend)
	for home, entry := range s.frontends.backends {
		select {
		case <-entry.ready:
		default:
			continue
		}
		if !entry.retiring && entry.active == 0 && (entry.failed() || s.now().Sub(entry.lastUsed) >= s.m.limits.Idle) {
			entry.retiring = true
			retired[home] = entry
		}
	}
	s.frontends.mu.Unlock()
	for home, entry := range retired {
		s.closeHTTPBackend(home, entry)
	}
}

// Only the caller that marked retiring under frontends.mu owns close. Keep
// the entry visible and charged while routers/workers/metrics are still live.
func (s *supervisor) closeHTTPBackend(home string, entry *supervisorHTTPBackend) {
	if entry.backend != nil {
		entry.backend.close()
	}
	s.frontends.mu.Lock()
	if s.frontends.backends[home] == entry {
		delete(s.frontends.backends, home)
	}
	s.frontends.mu.Unlock()
}

func (s *supervisor) closeFrontends() {
	s.frontends.mu.Lock()
	connections := make([]net.Conn, 0, len(s.frontends.sessions))
	for conn := range s.frontends.sessions {
		connections = append(connections, conn)
	}
	backends := make(map[string]*supervisorHTTPBackend)
	for home, entry := range s.frontends.backends {
		select {
		case <-entry.ready:
			if !entry.retiring {
				entry.retiring = true
				backends[home] = entry
			}
		default:
		}
	}
	s.frontends.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
	for home, entry := range backends {
		s.closeHTTPBackend(home, entry)
	}
}

func (s *supervisor) maintenance() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.maintenanceSweep(s.ctx); err != nil {
				_, _ = fmt.Fprintf(s.m.diagnosticWriter(), "supervisor maintenance: %v\n", err)
			}
			s.retireHTTPBackends()
		}
	}
}

type supervisorGenerationSnapshot struct {
	Worktree, Generation, State, TerminationReason, Error string
	PID, GuardianPID, Attachments, Operations             int
	LastCompletion, IdleDeadline                          time.Time
	ConfirmedExit                                         bool
}

type supervisorSnapshot struct {
	Boot                         string
	PID                          int
	Queued, Operations, Sessions int
	Generations                  []supervisorGenerationSnapshot
	FDReserved                   int
	RetainedMessageBytes         int
	Logs                         *ownerLogSnapshot
	Restart                      restartStatus
}

func (s *supervisor) snapshot() supervisorSnapshot {
	s.mu.Lock()
	result := supervisorSnapshot{Boot: s.boot, PID: os.Getpid(), Queued: len(s.queued), Operations: len(s.operations) + len(s.controls), FDReserved: s.fdReserved, RetainedMessageBytes: s.retainedBytes, Restart: s.restartStatus()}
	for _, g := range s.generations {
		item := supervisorGenerationSnapshot{Worktree: g.worktree, Generation: g.id, State: string(g.state),
			TerminationReason: g.reason, PID: g.record.PID, GuardianPID: g.record.GuardianPID,
			Attachments: len(g.connections), LastCompletion: g.completed, IdleDeadline: g.completed.Add(s.m.limits.Idle), ConfirmedExit: g.state == generationExited}
		if g.err != nil {
			item.Error = g.err.Error()
		}
		for _, op := range s.operations {
			if op.generation == g {
				item.Operations++
			}
		}
		for _, op := range s.controls {
			if op.generation == g {
				item.Operations++
			}
		}
		result.Generations = append(result.Generations, item)
	}
	s.mu.Unlock()
	if s.frontends != nil {
		s.frontends.mu.Lock()
		result.Sessions = len(s.frontends.sessions) + len(s.frontends.backends)
		s.frontends.mu.Unlock()
	}
	if s.m.ownerLogs != nil {
		logs := s.m.ownerLogs.Snapshot()
		result.Logs = &logs
	}
	return result
}

func openSupervisorAttachment(ctx context.Context, m *manager, home, endpoint string) (net.Conn, *bufio.Reader, error) {
	req := &http.Request{Method: http.MethodConnect, URL: &url.URL{Path: endpoint}, Host: "127.0.0.1", Header: make(http.Header)}
	if err := ownerHeaders(req, m, home); err != nil {
		return nil, nil, err
	}
	path, err := supervisorSocket(m)
	if err != nil {
		return nil, nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	_ = conn.SetDeadline(time.Now().Add(routingBudget + sendBudget))
	if err := req.Write(conn); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = conn.Close()
		return nil, nil, fmt.Errorf("supervisor attachment refused (HTTP %d): %s", resp.StatusCode, message)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, reader, nil
}

func proxyHTTPViaSupervisor(ctx context.Context, m *manager, home, address string, output io.Writer) error {
	fingerprint, err := supervisorConfig(m)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	control, reader, err := openSupervisorAttachment(ctx, m, home, "/http-frontend")
	if err != nil {
		return err
	}
	peer := newRestartPeer(ctx, control, reader, m.limits.MessageBytes, true)
	gate := &restartHTTPGate{}
	peer.onPause, peer.onResume = gate.pause, gate.resume
	client := supervisorHTTPClient(m)
	defer client.CloseIdleConnections()
	peer.onDrained = client.CloseIdleConnections
	peer.start()
	defer func() { _ = peer.Close() }()
	go func() { _, _ = peer.Read(ctx); cancel() }()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	if !listener.Addr().(*net.TCPAddr).IP.IsLoopback() {
		return fmt.Errorf("HTTP MCP must listen on a loopback address")
	}
	proxy := &httputil.ReverseProxy{Transport: client.Transport, Rewrite: func(proxy *httputil.ProxyRequest) {
		req := proxy.Out
		req.URL.Scheme, req.URL.Host, req.URL.Path = "http", "127.0.0.1", "/mcp"
		req.Host = "127.0.0.1"
		req.Header.Set(supervisorVersionHeader, supervisorProtocol)
		req.Header.Set(supervisorConfigHeader, fingerprint)
		req.Header.Set(supervisorHomeHeader, home)
	}}
	handler := protectLoopbackHTTP(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if err := gate.enter(req.Context()); err != nil {
			return
		}
		defer gate.leave()
		proxy.ServeHTTP(w, req)
	}))
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return ctx }}
	stop := context.AfterFunc(ctx, func() { _ = server.Close() })
	defer stop()
	if _, err := fmt.Fprintf(output, "MCP endpoint: http://%s/mcp\n", listener.Addr()); err != nil {
		return err
	}
	err = server.Serve(&httpConnectionListener{Listener: listener, slots: make(chan struct{}, m.limits.HTTPConnections)})
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
