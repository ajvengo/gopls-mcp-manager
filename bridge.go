package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// bridge multiplexes one stdio MCP client across per-worktree gopls servers.
//
// A gopls instance only knows the worktree it was started in: asked about a file
// outside it, it answers "no package metadata" rather than routing anywhere. One
// upstream therefore cannot serve a client whose files live in a linked worktree
// — which is exactly what an agent working under worktree isolation asks for.
//
// So every tools/call carrying a file path is routed to the gopls of THAT path's
// worktree, started on demand. The client sees one server; each worktree keeps
// its own index, which is the isolation the worktree was created for.
//
// Calls with no path argument (go_workspace, go_search, go_package_api) carry no
// routable evidence. They follow the most recent path-bearing call, falling back
// to the worktree the bridge was started in.
func bridge(ctx context.Context, m *manager, home string) error {
	ctx, cancel := context.WithCancel(ctx)

	stdio, err := (&mcp.StdioTransport{}).Connect(ctx)
	if err != nil {
		cancel()
		return err
	}

	r := newRouter(ctx, m, home)

	// Only the reader is waited for, because r.conns is its state and
	// closeUpstreams must not race it. The writer is left where it stands: the
	// stdio transport's Close closes stdin and no-ops on stdout, so a Write
	// already inside a full stdout pipe never unblocks, and waiting for it would
	// hang the exit on a client that stopped reading us.
	readerDone := make(chan struct{})
	go r.writeToClient(stdio)
	go func() {
		defer close(readerDone)
		r.readFromClient(stdio)
	}()

	err = <-r.errs
	cancel()
	_ = stdio.Close()
	<-readerDone
	r.closeUpstreams()
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, mcp.ErrConnectionClosed) {
		return nil
	}
	return err
}

// Router state is owned by the readFromClient goroutine, except for
// awaitingUpstream under mu, which the per-upstream reader goroutines also
// touch.
type router struct {
	ctx       context.Context
	m         *manager
	home      string
	conns     map[string]mcp.Connection
	worktrees map[string]string // containing directory -> worktree, see worktreeOf
	dial      func(string) (mcp.Connection, error)

	// How long one client message gets to handshake an upstream, its retry
	// included; see handshake.
	handshakeTimeout time.Duration

	mu sync.Mutex
	// Client requests we forwarded and whose answer an upstream still owes, so
	// that a dying gopls fails its callers instead of leaving them hanging.
	awaitingUpstream map[jsonrpc.ID]owed

	out  chan jsonrpc.Message
	errs chan error

	initialize *jsonrpc.Request // replayed to every upstream opened later
	sticky     string           // worktree of the last path-bearing call
	initSeq    int
}

// owed is an upstream that still owes an answer, and the worktree it serves.
//
// Ownership is matched on the connection, never on the worktree: send()
// replaces a dead connection with a live one under the same worktree, and
// matching by name would let the dead one's reader fail calls the reconnect had
// already placed successfully. The worktree rides along because a cancellation
// has to name one (R7) and owe() is the only place that knows it.
type owed struct {
	conn     mcp.Connection
	worktree string
}

func newRouter(ctx context.Context, m *manager, home string) *router {
	r := &router{
		ctx:              ctx,
		m:                m,
		home:             home,
		conns:            make(map[string]mcp.Connection),
		worktrees:        make(map[string]string),
		awaitingUpstream: make(map[jsonrpc.ID]owed),
		out:              make(chan jsonrpc.Message, 64),
		errs:             make(chan error, 4),
		// Generous: ensure has already waited for the endpoint to serve a
		// request, but §8 keeps a server that listens and answers nothing.
		handshakeTimeout: 30 * time.Second,
	}
	r.dial = r.dialGopls
	return r
}

func (r *router) writeToClient(stdio mcp.Connection) {
	for {
		select {
		case <-r.ctx.Done():
			return
		case msg := <-r.out:
			if err := stdio.Write(r.ctx, msg); err != nil {
				r.errs <- err
				return
			}
		}
	}
}

func (r *router) readFromClient(stdio mcp.Connection) {
	for {
		msg, err := stdio.Read(r.ctx)
		if err != nil {
			r.errs <- err
			return
		}
		req, ok := msg.(*jsonrpc.Request)
		if !ok {
			// A response answers a question some upstream asked, and we ask the
			// client none: roots/list is answered here and everything else is
			// refused. So this replies to nothing, and handing it to an upstream
			// would only make one dial and handshake to be told so.
			continue
		}
		initial := req.Method == "initialize"
		if initial {
			req.Params = withRootsCapability(req.Params)
			r.initialize = req
		}
		r.send(r.target(req), msg, req.ID, initial)
	}
}

// withRootsCapability makes every upstream ask this bridge for its roots. Some
// MCP clients omit the optional capability; without it gopls installs no file
// watcher and newly-created Go files remain invisible until its daemon restarts.
// Parameters we cannot rewrite — invalid ones, or ones that will not marshal
// back — are forwarded untouched and left for gopls to reject.
func withRootsCapability(params json.RawMessage) json.RawMessage {
	var initialize map[string]json.RawMessage
	if err := json.Unmarshal(params, &initialize); err != nil || initialize == nil {
		return params
	}

	var capabilities map[string]json.RawMessage
	if raw := initialize["capabilities"]; !absentJSON(raw) {
		if err := json.Unmarshal(raw, &capabilities); err != nil || capabilities == nil {
			return params
		}
	}
	if capabilities == nil {
		capabilities = make(map[string]json.RawMessage)
	}
	if absentJSON(capabilities["roots"]) {
		capabilities["roots"] = json.RawMessage(`{}`)
	}

	rawCapabilities, err := json.Marshal(capabilities)
	if err != nil {
		return params
	}
	initialize["capabilities"] = rawCapabilities

	rewritten, err := json.Marshal(initialize)
	if err != nil {
		return params
	}
	return rewritten
}

// absentJSON reports whether a client left this value out, spelled either way:
// the key missing entirely, or present and null.
func absentJSON(raw json.RawMessage) bool {
	return len(raw) == 0 || strings.TrimSpace(string(raw)) == "null"
}

// target picks the worktree that should answer req.
func (r *router) target(req *jsonrpc.Request) string {
	switch req.Method {
	case "tools/call":
		if worktree := r.toolCallWorktree(req.Params); worktree != "" {
			r.sticky = worktree
			return worktree
		}
		if r.sticky != "" {
			return r.sticky
		}
	case "notifications/cancelled":
		if worktree := r.cancelTarget(req.Params); worktree != "" {
			return worktree
		}
	}
	return r.home
}

// cancelTarget reports the worktree whose upstream owes the request this
// notification cancels — see R7. An id nobody owes reports nothing, and the
// notification goes home like every other one.
func (r *router) cancelTarget(params json.RawMessage) string {
	var cancelled mcp.CancelledParams
	if err := json.Unmarshal(params, &cancelled); err != nil {
		return ""
	}
	// Ids arrive off the wire as the same nil/float64/string that json.Unmarshal
	// produces here, and both sides go through MakeID, so one rebuilt from the
	// notification compares equal to the one the owed list was keyed by.
	id, err := jsonrpc.MakeID(cancelled.RequestID)
	if err != nil {
		return ""
	}
	r.mu.Lock()
	worktree := r.awaitingUpstream[id].worktree
	r.mu.Unlock()
	// Only a worktree we still hold a connection to. send() dials what it has
	// none for, and dialling runs ensure — an flock wait, a gopls spawn and a
	// full handshake, all on the goroutine reading the client — to hand a brand
	// new process a cancellation for a call it never received. An owed id whose
	// connection is already gone therefore reports nothing, like an unowed one:
	// the reader that lost it fails the call anyway (F2).
	if _, connected := r.conns[worktree]; !connected {
		return ""
	}
	return worktree
}

func (r *router) send(worktree string, msg jsonrpc.Message, id jsonrpc.ID, initial bool) {
	// One budget for both attempts, because the retry redials: a per-attempt
	// deadline would let a wedged upstream stall the client twice over. An
	// absolute deadline rather than a context, so that the common path — an
	// upstream already connected, nothing to hand it to — allocates no timer.
	deadline := time.Now().Add(r.handshakeTimeout)
	var err error
	for range 2 {
		var conn mcp.Connection
		conn, err = r.upstream(deadline, worktree, initial)
		if err == nil {
			// Recorded before the write, not after: a local gopls can answer
			// before Write returns, and a registration landing after its
			// response was already deleted would outlive the call and produce
			// a second, contradictory reply to the same id later on.
			r.owe(id, owed{conn: conn, worktree: worktree})
			if err = conn.Write(r.ctx, msg); err == nil {
				// Cached only once the connection has taken a write: a dial
				// handing back an already-dead upstream then fails above, on
				// this goroutine, and the retry is reached. A reader started
				// before the write would race it for the id instead, leaving
				// scheduling order to decide whether the client saw a
				// transparent retry or an error.
				r.cache(worktree, conn)
				return
			}
			// A failed write and conn's own reader seeing the upstream die are
			// the same event racing, and whoever claims the id owns the reply.
			claimed := r.claim(id)
			delete(r.conns, worktree)
			_ = conn.Close()
			if !claimed {
				// Its reader answered this call already. Retrying would reply
				// again to an id the client has closed — a reply it ignores,
				// so the retry would look fine while the client kept the error.
				return
			}
		}
	}
	if !id.IsValid() {
		return // a notification has nowhere to report a failure to
	}
	r.fail(id, "gopls for %s: %v", worktree, err)
}

// owe records that an upstream owes an answer to id. Ids the client did not ask
// an answer for are not tracked.
func (r *router) owe(id jsonrpc.ID, upstream owed) {
	if !id.IsValid() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.awaitingUpstream[id] = upstream
}

// claim takes id off the owed list, reporting whether this caller is the one
// that got it. Exactly one party may answer a call, and taking the id off the
// list is what confers that right — see also failInFlight, which claims in
// bulk. A notification is nobody's to answer, so claiming its absent id
// succeeds; a request id already off the list has been answered by somebody
// else, and claiming it fails.
func (r *router) claim(id jsonrpc.ID) bool {
	if !id.IsValid() {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, won := r.awaitingUpstream[id]
	delete(r.awaitingUpstream, id)
	return won
}

// fail answers id with an internal error: the only way the bridge can report a
// problem for a call the client is still waiting on.
func (r *router) fail(id jsonrpc.ID, format string, args ...any) {
	r.forward(&jsonrpc.Response{ID: id, Error: &jsonrpc.Error{
		Code:    jsonrpc.CodeInternalError,
		Message: fmt.Sprintf(format, args...),
	}})
}

// forward hands a message to the client, giving up once the session is over so
// that a stopped writer cannot strand its sender.
func (r *router) forward(msg jsonrpc.Message) {
	select {
	case r.out <- msg:
	case <-r.ctx.Done():
	}
}

// cache records conn as worktree's upstream and starts the goroutine that reads
// it. Both happen here and nowhere else, so that an entry in r.conns is never a
// connection nobody reads — one would swallow that upstream's answers, and its
// death, for the rest of the session. Idempotent: the common path re-offers a
// connection that is already cached.
func (r *router) cache(worktree string, conn mcp.Connection) {
	if r.conns[worktree] == conn {
		return
	}
	r.conns[worktree] = conn
	go r.readFromUpstream(conn, worktree)
}

// upstream returns the connection for worktree, dialling and handshaking one if
// there is none. deadline bounds the handshake, and is shared with the caller's
// other attempt: see send. Dialling is not covered — see §10. A connection it
// dialled is not cached here — see cache.
func (r *router) upstream(deadline time.Time, worktree string, initial bool) (mcp.Connection, error) {
	if conn, ok := r.conns[worktree]; ok {
		return conn, nil
	}
	// The first attempt can burn the whole budget in the handshake. Dialling
	// again would then spend an unbounded flock wait, and possibly a process
	// spawn, on a handshake guaranteed to expire on its first write.
	if !time.Now().Before(deadline) {
		return nil, context.DeadlineExceeded
	}
	conn, err := r.dial(worktree)
	if err != nil {
		return nil, err
	}
	// Only the first home connection receives the client's initialize directly.
	// Every later connection, including a restarted home daemon, is initialized
	// behind the client's back because the client handshakes only once.
	if !initial {
		ctx, cancel := context.WithDeadline(r.ctx, deadline)
		defer cancel()
		if err := r.handshake(ctx, conn, worktree); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func (r *router) dialGopls(worktree string) (mcp.Connection, error) {
	port, err := r.m.ensure(worktree)
	if err != nil {
		return nil, err
	}
	conn, err := (&mcp.SSEClientTransport{Endpoint: "http://" + mcpAddress(port)}).Connect(r.ctx)
	if err != nil {
		return nil, fmt.Errorf("connect to shared gopls MCP: %w", err)
	}
	return conn, nil
}

// handshake replays the client's initialize under a private id and swallows the
// reply, so the client sees exactly one initialize result — the home server's.
// ctx bounds it: this runs on the readFromClient goroutine, so an upstream that
// answers nothing would otherwise block every later client message too. The
// connection itself keeps r.ctx, because the SSE transport reads its stream
// under the context it was dialled with and a deadline there would cut the
// upstream loose the moment it expired.
func (r *router) handshake(ctx context.Context, conn mcp.Connection, worktree string) error {
	if r.initialize == nil {
		return errors.New("tool call arrived before the client sent initialize")
	}
	r.initSeq++
	id, err := jsonrpc.MakeID(fmt.Sprintf("gopls-mcp-manager-init-%d", r.initSeq))
	if err != nil {
		return err
	}
	if err := conn.Write(ctx, &jsonrpc.Request{ID: id, Method: r.initialize.Method, Params: r.initialize.Params}); err != nil {
		return err
	}
	for {
		msg, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if resp, ok := msg.(*jsonrpc.Response); ok && resp.ID == id {
			if resp.Error != nil {
				return resp.Error
			}
			break
		}
		// gopls asks for its roots the moment it has seen the capability in
		// initialize — which is inside this window, before the reader that
		// normally fields the question is even running.
		if r.answeredUpstream(ctx, conn, worktree, msg) {
			continue
		}
		r.forward(msg) // anything else it volunteered is the client's
	}
	return conn.Write(ctx, &jsonrpc.Request{Method: "notifications/initialized"})
}

func (r *router) readFromUpstream(conn mcp.Connection, worktree string) {
	for {
		msg, err := conn.Read(r.ctx)
		if err != nil {
			// An upstream dying is not the stdio client's problem. Its cached
			// connection fails the next write, which redials and retries once.
			r.failInFlight(conn, worktree, err)
			return
		}
		if r.answeredUpstream(r.ctx, conn, worktree, msg) {
			continue
		}
		if resp, ok := msg.(*jsonrpc.Response); ok && !r.claim(resp.ID) {
			// send() got to this id first — its write failed after the upstream
			// had already taken the message — and answered it. Forwarding now
			// would be a second reply to a call the client has closed.
			continue
		}
		r.forward(msg)
	}
}

// failInFlight answers every request still owed by conn with an error. The
// calls in flight when a gopls dies get no retry — nothing else will ever
// produce their ids — so left alone they strand a client that has no timeout.
func (r *router) failInFlight(conn mcp.Connection, worktree string, cause error) {
	r.mu.Lock()
	var stranded []jsonrpc.ID
	for id, owner := range r.awaitingUpstream {
		if owner.conn == conn {
			stranded = append(stranded, id)
			delete(r.awaitingUpstream, id)
		}
	}
	r.mu.Unlock()

	for _, id := range stranded {
		r.fail(id, "gopls for %s went away mid-call: %v", worktree, cause)
	}
}

// answeredUpstream handles a server-initiated request here rather than passing
// it to the client, and reports whether it did.
//
// Every read loop that can see one has to call this: a roots/list slipping
// through to the client comes back naming the tree the session opened in, which
// is the whole failure this bridge exists to prevent. Anything else is refused
// outright — the client would answer it addressed to nobody in particular, and
// an upstream holding an unanswerable question waits forever, so a definite
// error is the kinder reply. gopls asks for nothing else today.
// ctx is the caller's own budget for talking to conn: the handshake answers
// roots inside its deadline (S2 — gopls asks the moment it sees the capability,
// so this is the normal path, not an exotic one), while a running reader has
// only the session to bound it.
func (r *router) answeredUpstream(ctx context.Context, conn mcp.Connection, worktree string, msg jsonrpc.Message) bool {
	req, ok := msg.(*jsonrpc.Request)
	if !ok || !req.ID.IsValid() {
		return false
	}
	if req.Method == "roots/list" {
		r.answerRoots(ctx, conn, worktree, req.ID)
		return true
	}
	_ = conn.Write(ctx, &jsonrpc.Response{ID: req.ID, Error: &jsonrpc.Error{
		Code:    jsonrpc.CodeMethodNotFound,
		Message: fmt.Sprintf("gopls-mcp-manager does not relay %q to the client", req.Method),
	}})
	return true
}

// answerRoots tells an upstream that its one workspace root is the worktree it
// serves, instead of forwarding the question to the client.
//
// This is what makes a gopls notice a file created after it started. Its headless
// MCP mode already runs a full in-process LSP session and an fsnotify watcher
// that feeds it didChangeWatchedFiles — but it only watches the roots the client
// reports, and only asks for them at all once it has seen the roots capability in
// initialize. Left to the client, every upstream would hear the same answer: the
// single tree the session was opened in. Every other worktree would then watch
// nothing and answer "no package metadata" for a file that is right there on disk,
// which is indistinguishable from a genuinely broken tree.
func (r *router) answerRoots(ctx context.Context, conn mcp.Connection, worktree string, id jsonrpc.ID) {
	// Two strings in a fixed shape: marshaling them cannot fail, and there would
	// be nobody to tell anyway — the id belongs to the upstream, so an error
	// reply sent to the client would answer a question it never asked.
	result, _ := json.Marshal(mcp.ListRootsResult{Roots: []*mcp.Root{{
		URI:  (&url.URL{Scheme: "file", Path: worktree}).String(),
		Name: filepath.Base(worktree),
	}}})
	// A write failure needs no recovery, and must not touch r.conns: that map
	// belongs to the readFromClient goroutine. An upstream that never hears its
	// roots just keeps the file set it started with — today's behaviour — and the
	// next call for it redials anyway.
	_ = conn.Write(ctx, &jsonrpc.Response{ID: id, Result: result})
}

func (r *router) closeUpstreams() {
	for _, conn := range r.conns {
		_ = conn.Close()
	}
}

// toolCallWorktree reports the worktree owning the first path argument of a
// tools/call, or "" when the call names no path we can resolve.
func (r *router) toolCallWorktree(params json.RawMessage) string {
	var call struct {
		Arguments struct {
			File  string   `json:"file"`
			Dir   string   `json:"dir"`
			Files []string `json:"files"`
		} `json:"arguments"`
	}
	if err := json.Unmarshal(params, &call); err != nil {
		return ""
	}
	paths := append([]string{call.Arguments.File, call.Arguments.Dir}, call.Arguments.Files...)
	for _, path := range paths {
		// An absent argument is no evidence, and a relative one is worse: it
		// would resolve against this process's own cwd — the home worktree —
		// and beat the sticky routing that was right, for every worktree but
		// home. gopls's schemas ask for absolute paths.
		if !filepath.IsAbs(path) {
			continue
		}
		if worktree := r.worktreeOf(path); worktree != "" {
			return worktree
		}
	}
	return ""
}

// worktreeOf resolves one path argument, memoizing the answer under the
// directory holding it rather than the path itself: resolution shells out to
// git at ~13ms a call, every path in one directory has the same answer, and a
// session names many files under the same handful of directories. Only
// successes are cached, so a path that becomes resolvable later still gets its
// own lookup; a worktree removed mid-session keeps answering until the routed
// gopls reports the path itself.
func (r *router) worktreeOf(path string) string {
	// The key only; worktreePath gets the path itself and reduces it the same
	// way, from the same argument. Handing it the reduced form instead would
	// reduce twice, and a second pass over a directory that does not exist
	// climbs to its parent — resolving a path against a tree it never named.
	//
	// ponytail: the key costs a stat(2) even when it hits, since containingDir
	// is what tells a file from a directory. One syscall against the ~13ms git
	// call it saves; carry a second map from path to key if a hot loop ever
	// makes it show.
	dir := containingDir(path)
	if worktree, ok := r.worktrees[dir]; ok {
		return worktree
	}
	worktree, err := worktreePath(path)
	if err != nil {
		return ""
	}
	r.worktrees[dir] = worktree
	return worktree
}

// containingDir is input itself when it names a directory, and its parent
// otherwise — including when it cannot be stat'd at all, since a path we were
// handed and cannot see is a file far more often than a directory.
//
// Cleaned either way, because this is a memo key: a dir argument spelled with a
// trailing separator, or with a "." in it, names the same directory as one
// without and must not fork git a second time for the answer already stored.
// filepath.Dir cleans on its own, so only the directory branch needs saying.
func containingDir(input string) string {
	if info, err := os.Stat(input); err == nil && info.IsDir() {
		return filepath.Clean(input)
	}
	return filepath.Dir(input)
}

// worktreePath resolves a file or directory to the root of the worktree holding
// it. A path that does not exist, or is in no repository, resolves to an error:
// callers routing a tool call treat that as "no evidence, keep the current
// server", which then reports its own error for the path.
func worktreePath(input string) (string, error) {
	// Kept here even though worktreeOf computes the same directory for its memo
	// key, and not hoisted into the callers: every argument this is reached with
	// is shaped like a file, and a caller that forgot to reduce one would get
	// "Not a directory" from git — which reads as "no evidence" and misroutes to
	// home rather than reporting anything.
	input = containingDir(input)
	// Bounded, because this runs on the goroutine that reads the client: a git
	// that never returns — a stalled network filesystem is enough — would wedge
	// the session with neither side timing out, which is the failure H5 bounds
	// for the handshake. Generous, since rev-parse reads no tree: only a hang
	// should ever reach it.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// git -C resolves its own cwd physically, so the toplevel it prints already
	// has the symlinks taken out of it — and a relative input resolves against
	// this process's cwd, which is what the "." default means.
	out, err := exec.CommandContext(ctx, "git", "-C", input, "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse --show-toplevel: %w", err)
	}
	return filepath.EvalSymlinks(strings.TrimSpace(string(out)))
}
