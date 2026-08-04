package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type fakeConn struct {
	reads  chan jsonrpc.Message
	writes chan jsonrpc.Message

	// onWrite replaces the default write, and is the one seam these tests need
	// from a connection: every upstream they stub differs from a plain one in
	// what writing to it does — it fails, it answers out of the write itself, it
	// dies first — and in nothing else. nil is the default, which queues the
	// message on writes. A hook that wants the default for the messages it does
	// not care about calls queue.
	onWrite func(ctx context.Context, msg jsonrpc.Message) error
}

func (c *fakeConn) Read(ctx context.Context) (jsonrpc.Message, error) {
	select {
	case msg, ok := <-c.reads:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case <-ctx.Done():
		// A real Connection honours its context, and a test for the handshake
		// deadline would hang against one that did not.
		return nil, ctx.Err()
	}
}

func (c *fakeConn) Write(ctx context.Context, msg jsonrpc.Message) error {
	if c.onWrite != nil {
		return c.onWrite(ctx, msg)
	}
	return c.queue(ctx, msg)
}

// queue is the default write: hand the message over and let a test read it back.
func (c *fakeConn) queue(ctx context.Context, msg jsonrpc.Message) error {
	select {
	case c.writes <- msg:
		return nil
	case <-ctx.Done():
		// A real Connection honours its context, and the retried handshake in
		// TestAWedgedUpstreamFailsItsCallWithinOneBudget hangs against one
		// that does not: by then the shared deadline has already expired.
		return ctx.Err()
	}
}

// failingConn fails every write, which is what a lane still holding a
// connection to a gopls that has gone away runs into.
func failingConn() *fakeConn {
	return &fakeConn{onWrite: func(context.Context, jsonrpc.Message) error { return io.EOF }}
}

func (c *fakeConn) Close() error      { return nil }
func (c *fakeConn) SessionID() string { return "" }

// newFakeConn is the shape most of these tests want: reads handed over one at a
// time, and room for a single write so that a send need not be raced to be
// taken.
func newFakeConn() *fakeConn {
	return &fakeConn{reads: make(chan jsonrpc.Message), writes: make(chan jsonrpc.Message, 1)}
}

// deadConn is an upstream that has already gone away: msgs are everything it
// will ever hand over, and its reader sees EOF straight after them.
func deadConn(msgs ...jsonrpc.Message) *fakeConn {
	reads := make(chan jsonrpc.Message, len(msgs))
	for _, msg := range msgs {
		reads <- msg
	}
	close(reads)
	return &fakeConn{reads: reads}
}

// newHandshakingConn answers the private handshake out of its own write, so
// that a test wanting a lane on a connected upstream needs no goroutine
// scripting one. Everything the handshake does not swallow is queued as on a
// plain fakeConn, which is what lets a test still await the call it placed.
//
// The buffered read is what keeps that answer from blocking on a handshake that
// is not reading yet — it writes before it reads. The writes are buffered for
// the same reason, deep enough for the whole tail of one handshaked call: the
// initialized notification, then the call itself.
func newHandshakingConn() *fakeConn {
	c := &fakeConn{
		reads:  make(chan jsonrpc.Message, 1),
		writes: make(chan jsonrpc.Message, 2),
	}
	c.onWrite = func(ctx context.Context, msg jsonrpc.Message) error {
		if req, ok := msg.(*jsonrpc.Request); ok && req.Method == "initialize" {
			c.reads <- &jsonrpc.Response{ID: req.ID, Result: json.RawMessage(`{}`)}
			return nil
		}
		return c.queue(ctx, msg)
	}
	return c
}

// newTestRouter is a router over the test's own context, with no manager: every
// test below either stubs r.dial or never reaches one. The home worktree is a
// path no test resolves, so routing decisions stay the ones the test makes.
//
// testing.TB rather than *testing.T so the benchmarks build their router the
// same way; they pass a real directory as home, which is the only thing they
// need that differs.
func newTestRouter(tb testing.TB, home string) *router {
	tb.Helper()
	return newRouter(tb.Context(), nil, home)
}

// testHome is the fallback worktree for the tests that do not care what it is.
const testHome = "/tmp/home"

// testWorktree is the destination every test that needs one routes to: a path
// no test resolves either, so naming it is what picks the lane, not what is on
// disk. Distinct from newTestRouter's home, which is the fallback.
const testWorktree = "/tmp/wt"

// clientInitializeParams stands in for the real client's initialize params. It
// carries the roots capability on purpose: that capability is what makes gopls
// ask roots/list at all, so a replay that dropped the params would leave every
// secondary upstream never asking, and S1-S3 unreachable in production.
const clientInitializeParams = `{"capabilities":{"roots":{"listChanged":true}}}`

// handshakeReady gives the router the client initialize every private handshake
// replays. Without one, upstream() refuses to open any connection but the first.
func handshakeReady(r *router) {
	r.initialize.Store(&jsonrpc.Request{Method: "initialize", Params: json.RawMessage(clientInitializeParams)})
}

// sendCall puts a tools/call on l under a fresh id and hands the id back, which
// is what every caller then asserts on.
func sendCall(t *testing.T, l *lane, name string) jsonrpc.ID {
	t.Helper()
	id := mustID(t, name)
	l.send(&jsonrpc.Request{ID: id, Method: "tools/call"})
	return id
}

// connectedLane registers worktree's lane already holding conn, which is the
// state a successful dial leaves it in.
//
// No goroutine runs it: these tests call send themselves, so that a call and
// the assertions about it stay on one goroutine. A test that routes through
// readFromClient instead starts one with go l.run().
func connectedLane(r *router, worktree string, conn mcp.Connection) *lane {
	l := newLane(r, worktree)
	l.conn = conn
	r.lanes[worktree] = l
	return l
}

// parkedDial makes every dial hang until the test is over, which is what an
// flock held by another process's cold start looks like from here.
//
// Parked on the test's own context rather than the dial's: dialBounded cancels
// that one at sendBudget, which would unpark the dial mid-bubble.
func parkedDial(t *testing.T, r *router) {
	t.Helper()
	r.dial = func(context.Context, string) (mcp.Connection, error) {
		<-t.Context().Done()
		return nil, io.EOF
	}
}

// fixedDial hands every lane the same connection, for the tests whose subject
// is what the bridge does with an upstream rather than how it came by one.
func fixedDial(r *router, conn mcp.Connection) {
	r.dial = func(context.Context, string) (mcp.Connection, error) { return conn, nil }
}

// forbidDial fails the test if a lane reaches for a connection at all, naming
// what that would mean. For the tests whose whole property is that it does not.
func forbidDial(t *testing.T, r *router, whatWouldBeWrong string) {
	t.Helper()
	r.dial = func(context.Context, string) (mcp.Connection, error) {
		t.Error(whatWouldBeWrong)
		return nil, io.EOF
	}
}

// startClient runs the reader over a channel the test sends its client messages
// on, and shuts the reader and every lane it opened down with the test. The
// order matters and is the reason this is shared: closing the reads is what ends
// readFromClient, and only then may closeLanes touch r.lanes, which that
// goroutine owns.
func startClient(t *testing.T, r *router, depth int) chan<- jsonrpc.Message {
	t.Helper()
	reads := make(chan jsonrpc.Message, depth)
	go r.readFromClient(&fakeConn{reads: reads})
	t.Cleanup(func() {
		close(reads)
		r.closeLanes()
	})
	return reads
}

// mustWriteFile puts content on disk, for the tests and benchmarks that need a
// real file for git or EvalSymlinks to walk.
func mustWriteFile(tb testing.TB, path, content string) {
	tb.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		tb.Fatal(err)
	}
}

// bubble runs fn under synctest's clock, which only moves once every goroutine
// in it has blocked. Two things these tests want follow from that: a wait for
// something that never comes costs no real time at all, and "nothing reached
// the client" can be asserted once the bridge has run itself out, rather than
// merely at the instant of looking.
//
// A test stays outside when it does something this clock cannot account for — a
// child process, a real listener or HTTP server, or a real timeout it means to
// measure, which between them cover every test in manager_test.go that is not
// here. So does a test that waits on nothing, which would gain only a closure;
// and t.Run is forbidden inside a bubble.
func bubble(t *testing.T, fn func(t *testing.T)) {
	t.Helper()
	t.Parallel()
	synctest.Test(t, fn)
}

// testWait bounds every wait in this file, all of which are inside a bubble. A
// bridge that never makes the progress a test is waiting for is a bug, and an
// unbounded channel operation leaves synctest to report it — as a bare deadlock
// panic naming no expectation. Getting there first is the whole job.
//
// It costs nothing on a passing run: this timer can only come due once every
// goroutine is already blocked, which is the hang itself.
const testWait = 10 * time.Second

// recvWithin takes the next value from ch, reporting a timeout rather than
// hanging. It answers with an error instead of failing t, so that it is usable
// from a background goroutine: see inBackground.
func recvWithin[T any](ch <-chan T, wanted string) (T, error) {
	select {
	case v := <-ch:
		return v, nil
	case <-time.After(testWait):
		var zero T
		return zero, fmt.Errorf("timed out after %s waiting for %s", testWait, wanted)
	}
}

// recvRequest insists the next message is a request naming method, which is the
// shape every wait on a connection's writes has.
func recvRequest(ch <-chan jsonrpc.Message, method string) (*jsonrpc.Request, error) {
	msg, err := recvWithin(ch, method)
	if err != nil {
		return nil, err
	}
	req, ok := msg.(*jsonrpc.Request)
	if !ok || req.Method != method {
		return nil, fmt.Errorf("upstream got %#v, want a %s request", msg, method)
	}
	return req, nil
}

// mustRecv fails the test rather than returning the timeout. Call it only from
// the test's own goroutine — see inBackground for why.
func mustRecv[T any](t *testing.T, ch <-chan T, wanted string) T {
	t.Helper()
	v, err := recvWithin(ch, wanted)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// inBackground runs fn on its own goroutine and hands back its verdict, which
// the test is expected to wait for and assert on itself.
//
// fn must not touch *testing.T. Fatalf ends the goroutine that calls it, which
// from here is fn's own: the test would run on, unaware, and reach a verdict of
// its own — reporting a bridge that answered nothing as a pass. (Outliving the
// test, the other hazard, a bubble rules out on its own: synctest waits for
// every goroutine in it.) The buffer is load-bearing rather than tidy — after a
// test that gave up on fn at testWait, an unbuffered send would park fn forever
// and deadlock the bubble.
func inBackground(fn func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	return done
}

// upstreamPair returns the two ends of one connection: upstream is what the
// router holds, gopls is what a real server would be answering on.
func upstreamPair(t *testing.T) (upstream, gopls mcp.Connection) {
	t.Helper()
	upstreamT, goplsT := mcp.NewInMemoryTransports()
	upstream, err := upstreamT.Connect(t.Context())
	if err != nil {
		t.Fatalf("connect upstream: %v", err)
	}
	gopls, err = goplsT.Connect(t.Context())
	if err != nil {
		t.Fatalf("connect gopls: %v", err)
	}
	// Closed, not merely abandoned: each of these runs a decode pump of its own
	// that no context cancels, and a bubble does not end while one is still
	// parked on its half of the pipe.
	t.Cleanup(func() {
		_ = upstream.Close()
		_ = gopls.Close()
	})
	return upstream, gopls
}

// wantClientQuiet fails if anything reached the client, naming what should not
// have. Most of these tests exist to prove the bridge kept something to itself.
//
// The wait is what makes it an assertion rather than a coincidence: without it
// this says only that nothing had arrived yet at the instant it looked. Call
// from inside a bubble — the wait panics anywhere else.
func wantClientQuiet(t *testing.T, r *router, whatWouldBeWrong string) {
	t.Helper()
	synctest.Wait()
	select {
	case msg := <-r.out:
		t.Fatalf("%s: %v", whatWouldBeWrong, msg)
	default:
	}
}

// wantClientError is its twin: the bridge must answer a call nothing else will
// ever answer, or a client without a timeout hangs on it forever. Same rule —
// call it from inside a bubble.
func wantClientError(t *testing.T, r *router, id jsonrpc.ID, whatWouldBeWrong string) {
	t.Helper()
	synctest.Wait()
	select {
	case msg := <-r.out:
		resp, ok := msg.(*jsonrpc.Response)
		if !ok {
			t.Fatalf("client got %T, want an error *jsonrpc.Response", msg)
		}
		if resp.ID != id {
			t.Errorf("failed id = %v, want the call %v", resp.ID, id)
		}
		if resp.Error == nil {
			t.Errorf("call %v was answered without an error", id)
		}
	default:
		t.Fatal(whatWouldBeWrong)
	}
}

// wantResponse insists msg is the answer to id, which is the shape of every
// reply these tests read back off a connection.
func wantResponse(t *testing.T, msg jsonrpc.Message, id jsonrpc.ID) *jsonrpc.Response {
	t.Helper()
	resp, ok := msg.(*jsonrpc.Response)
	if !ok || resp.ID != id {
		t.Fatalf("upstream got %#v, want a response to %v", msg, id)
	}
	return resp
}

// wantWireError insists resp carries a JSON-RPC error under code, and hands it
// back so callers can go on to assert on the message itself.
func wantWireError(t *testing.T, resp *jsonrpc.Response, code int64) *jsonrpc.Error {
	t.Helper()
	var wire *jsonrpc.Error
	if !errors.As(resp.Error, &wire) || wire.Code != code {
		t.Fatalf("error = %v, want one with code %d", resp.Error, code)
	}
	return wire
}

// wantOneRoot reads a roots/list answer, which names exactly one root — the
// tree the upstream that asked belongs to, and never any other. Callers assert
// on the root itself; that there is only the one is the shared part.
func wantOneRoot(t *testing.T, resp *jsonrpc.Response) *mcp.Root {
	t.Helper()
	var got mcp.ListRootsResult
	if err := json.Unmarshal(resp.Result, &got); err != nil {
		t.Fatalf("unmarshal roots answer %s: %v", resp.Result, err)
	}
	if len(got.Roots) != 1 {
		t.Fatalf("got %d roots, want exactly the upstream's own: %#v", len(got.Roots), got.Roots)
	}
	return got.Roots[0]
}

// mustID takes any of the nil/float64/string an id arrives off the wire as, so
// that a numeric id in a test is built the same way as a name — and by the same
// call the benchmarks use.
func mustID(tb testing.TB, raw any) jsonrpc.ID {
	tb.Helper()
	id, err := jsonrpc.MakeID(raw)
	if err != nil {
		tb.Fatalf("MakeID(%v): %v", raw, err)
	}
	return id
}

func TestWithRootsCapability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "missing capabilities", in: `{}`, want: `{"capabilities":{"roots":{}}}`},
		{name: "null capabilities", in: `{"capabilities":null}`, want: `{"capabilities":{"roots":{}}}`},
		{name: "preserves peers", in: `{"capabilities":{"sampling":{}}}`, want: `{"capabilities":{"roots":{},"sampling":{}}}`},
		{name: "preserves roots", in: `{"capabilities":{"roots":{"listChanged":true}}}`, want: `{"capabilities":{"roots":{"listChanged":true}}}`},
		{name: "replaces null roots", in: `{"capabilities":{"roots":null}}`, want: `{"capabilities":{"roots":{}}}`},
		{name: "invalid parameters", in: `[`, want: `[`},
		{name: "invalid capabilities", in: `{"capabilities":[]}`, want: `{"capabilities":[]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := string(withRootsCapability(json.RawMessage(test.in))); got != test.want {
				t.Errorf("withRootsCapability(%s) = %s, want %s", test.in, got, test.want)
			}
		})
	}
}

// Refusing on the reader is what keeps the client's initialize from reaching a
// connection that has already had one: let through, it would wake the home lane
// into spending the private handshake on that connection, and gopls's
// already-initialized error would come back as the client's initialize result,
// ending the session before it began.
//
// Asserted on the dial rather than that outcome, because the outcome needs the
// lane to lose a race it usually wins: no dial at all holds whoever wins.
//
// A notification is dropped rather than refused, having no id to answer, and
// the lane still must not wake. Not a cancellation: that is refused a dial
// anyway (R7), so the row would pass with the refusal deleted.
func TestAMessageBeforeInitializeNeverWakesALane(t *testing.T) {
	t.Parallel()
	id := mustID(t, "call-before-initialize")
	tests := []struct {
		name string
		msg  *jsonrpc.Request
		want func(t *testing.T, r *router)
	}{
		{"request is refused", &jsonrpc.Request{ID: id, Method: "tools/call"}, func(t *testing.T, r *router) {
			wantClientError(t, r, id, "a tools/call before initialize was left unanswered")
		}},
		{"notification is dropped", &jsonrpc.Request{Method: "notifications/initialized"}, func(t *testing.T, r *router) {
			wantClientQuiet(t, r, "a notification sent before initialize was answered")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				r := newTestRouter(t, testHome)
				forbidDial(t, r, "a lane opened a connection for a message that arrived before initialize")
				clientReads := startClient(t, r, 1)

				clientReads <- test.msg

				test.want(t, r)
				synctest.Wait()
			})
		})
	}
}

func TestRootlessInitializeIsForwardedWithWorkingRoots(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		upstreamReads := make(chan jsonrpc.Message, 1)
		upstream := &fakeConn{reads: upstreamReads, writes: make(chan jsonrpc.Message, 2)}
		// Dialled rather than handed over pre-connected, because that is the only
		// way home gets a connection while the client's initialize is what opens
		// it: a lane already holding one refuses that message under H2a. laneFor
		// runs the lane and cache starts its reader, so neither is started here.
		fixedDial(r, upstream)
		// Registered before startClient so it runs after it: cleanups are LIFO,
		// and the upstream's reader must not be cut loose before the lanes it
		// belongs to have been closed.
		t.Cleanup(func() { close(upstreamReads) })
		clientReads := startClient(t, r, 1)

		clientReads <- &jsonrpc.Request{
			ID:     mustID(t, "initialize-rootless"),
			Method: "initialize",
			Params: json.RawMessage(`{"capabilities":{"sampling":{}}}`),
		}
		forwarded := mustRecv(t, upstream.writes, "the forwarded initialize").(*jsonrpc.Request)
		if got, want := string(forwarded.Params), `{"capabilities":{"roots":{},"sampling":{}}}`; got != want {
			t.Fatalf("forwarded initialize params = %s, want %s", got, want)
		}

		rootsID := mustID(t, "roots-after-initialize")
		upstreamReads <- &jsonrpc.Request{ID: rootsID, Method: "roots/list"}
		resp := wantResponse(t, mustRecv(t, upstream.writes, "the local roots/list answer"), rootsID)
		if root := wantOneRoot(t, resp); root.URI != "file:///tmp/home" {
			t.Fatalf("root uri = %q, want the home tree", root.URI)
		}
	})
}

// A cold worktree used to be dialled on the goroutine that reads the client, so
// every other worktree's calls queued behind its flock wait, gopls spawn and
// handshake — up to the whole handshake budget. Each worktree has a lane of its
// own now, and a call to one that is already connected must land while a cold
// one is still stuck in its dial.
func TestAColdWorktreeDoesNotHoldUpAnother(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		home := r.home
		warm := newFakeConn()

		handshakeReady(r)
		parkedDial(t, r)
		// Routing by path would have to resolve one through git, which a bubble
		// cannot account for. A sticky worktree sends a path-less call to the same
		// place, which is all this needs.
		r.sticky = "/tmp/cold"
		go connectedLane(r, home, warm).run()
		// LIFO, so this runs after the lanes are closed — see the note above.
		t.Cleanup(func() { close(warm.reads) })
		clientReads := startClient(t, r, 2)

		clientReads <- &jsonrpc.Request{
			ID:     mustID(t, "call-into-the-cold"),
			Method: "tools/call",
			Params: json.RawMessage(`{"arguments":{"query":"Foo"}}`),
		}
		clientReads <- &jsonrpc.Request{ID: mustID(t, "list"), Method: "tools/list"}

		if _, err := recvRequest(warm.writes, "tools/list"); err != nil {
			t.Fatalf("%v — want the tools/list that followed the cold call, while the cold worktree is still dialling", err)
		}
	})
}

// No bubble: readFromUpstream runs to completion on this goroutine, so there is
// nothing for a clock to wait on.
func TestHomeUpstreamEOFDoesNotCloseClient(t *testing.T) {
	t.Parallel()

	r := newTestRouter(t, testHome)
	r.readFromUpstream(deadConn(), r.home)

	select {
	case err := <-r.errs:
		t.Fatalf("home upstream EOF closed the client: %v", err)
	default:
	}
}

func TestSendReconnectsAndInitializesRestartedHome(t *testing.T) {
	bubble(t, func(t *testing.T) {
		fresh := &fakeConn{
			// Buffered, so that handing over the handshake's answer below cannot
			// block on the router picking it up.
			reads:  make(chan jsonrpc.Message, 1),
			writes: make(chan jsonrpc.Message),
		}
		r := newTestRouter(t, testHome)
		l := connectedLane(r, r.home, failingConn())
		handshakeReady(r)
		fixedDial(r, fresh)

		done := inBackground(func() error {
			initialize, err := recvRequest(fresh.writes, "initialize")
			if err != nil {
				return err
			}
			// The id is the private one and the params are the client's own:
			// asserting only that some id is valid would leave both the fixed
			// id H2 relies on and the replayed capabilities ungated.
			if initialize.ID != initID {
				return fmt.Errorf("reconnect opened with id %v, want the private %v", initialize.ID, initID)
			}
			if string(initialize.Params) != clientInitializeParams {
				return fmt.Errorf("replayed initialize params = %s, want the client's %s", initialize.Params, clientInitializeParams)
			}
			fresh.reads <- &jsonrpc.Response{ID: initialize.ID, Result: json.RawMessage(`{}`)}
			if _, err := recvRequest(fresh.writes, "notifications/initialized"); err != nil {
				return err
			}
			_, err = recvRequest(fresh.writes, "tools/call")
			return err
		})
		// The upstream outlives the assertions below: the call it just accepted is
		// still in flight, and closing it here would — correctly — fail that call.
		t.Cleanup(func() { close(fresh.reads) })

		sendCall(t, l, "call-7")
		if err := mustRecv(t, done, "the reconnect assertions to finish"); err != nil {
			t.Fatal(err)
		}

		wantClientQuiet(t, r, "successful reconnect returned an error to the client")
	})
}

func TestSendRetriesInitialInitializeWithoutPrivateHandshake(t *testing.T) {
	bubble(t, func(t *testing.T) {
		fresh := newFakeConn()
		dials := 0
		r := newTestRouter(t, testHome)
		home := r.home
		r.dial = func(context.Context, string) (mcp.Connection, error) {
			dials++
			if dials == 1 {
				return yieldingConn(), nil
			}
			return fresh, nil
		}
		id := mustID(t, "client-initialize")
		initialize := &jsonrpc.Request{ID: id, Method: "initialize", Params: json.RawMessage(`{}`)}
		r.initialize.Store(initialize)

		newLane(r, home).send(initialize)
		// The retry is the whole point: with a reader racing the write for the id,
		// send() finds the call already answered and this write never comes.
		got := mustRecv(t, fresh.writes, "the retried initialize on a second connection").(*jsonrpc.Request)
		if got.ID != id || got.Method != "initialize" {
			t.Fatalf("retry wrote %#v, want original initialize id %v", got, id)
		}
		if dials != 2 {
			t.Fatalf("dial count = %d, want one retry", dials)
		}
		// Asserted while the retry is still owed, and only then killed: closing the
		// connection under a call in flight fails that call on purpose, so the other
		// order races the reader for the verdict and answers a different question.
		wantClientQuiet(t, r, "successful initialize retry returned an error to the client")
		close(fresh.reads)
	})
}

// The reader refuses the disorder that produces this — H1a — so reaching it
// takes a lane driven directly. That is the point: were the guard only on the
// reader, a lane whose connection was opened by anything else would hand gopls
// the duplicate initialize and lose the session.
func TestTheClientInitializeIsRefusedByAnAlreadyHandshakenUpstream(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		// Cached means handshaken, which is the state a successful dial leaves.
		held := newFakeConn()
		l := connectedLane(r, r.home, held)

		id := mustID(t, "client-initialize")
		l.send(&jsonrpc.Request{ID: id, Method: "initialize", Params: json.RawMessage(`{}`)})

		wantClientError(t, r, id, "a second initialize was left unanswered")
		select {
		case msg := <-held.writes:
			t.Fatalf("upstream got a second initialize %#v, want nothing written to it", msg)
		default:
		}
	})
}

// yieldingConn is an upstream that is already gone, and whose write is certain
// to lose the race it is in: it lets every other goroutine in the bubble block
// first, so a reader started before the write — the regression
// TestSendRetriesInitialInitializeWithoutPrivateHandshake pins — always claims
// the id. Left to a real race, that reader wins about once in ten thousand runs.
func yieldingConn() *fakeConn {
	c := deadConn()
	c.onWrite = func(context.Context, jsonrpc.Message) error {
		synctest.Wait()
		return io.EOF
	}
	return c
}

// A wedged upstream must not be waited on forever, whichever half of opening it
// hangs. Both run on the lane's goroutine, so either would stall every later
// call to that worktree with neither side timing out — and §8 keeps exactly
// such a server alive, so ensure hands its port back.
//
// The rows are the two halves: an upstream that accepts the connection and then
// answers nothing is what H5 bounds; one whose SSE connect never returns a first
// event never reaches H5 at all. The budget is one for both attempts either way,
// which is what the elapsed time and the dial count together say.
func TestAWedgedUpstreamFailsItsCallWithinOneBudget(t *testing.T) {
	tests := []struct {
		name string
		dial func(context.Context) (mcp.Connection, error)
	}{
		{"an upstream that never answers", func(context.Context) (mcp.Connection, error) {
			return newFakeConn(), nil
		}},
		{"a connect that never completes", func(ctx context.Context) (mcp.Connection, error) {
			<-ctx.Done() // what Connect does while it waits for the endpoint event
			return nil, ctx.Err()
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				r := newTestRouter(t, testHome)
				handshakeReady(r)
				dials := 0
				r.dial = func(ctx context.Context, _ string) (mcp.Connection, error) {
					dials++
					return test.dial(ctx)
				}

				start := time.Now()
				id := sendCall(t, newLane(r, testWorktree), "call-into-the-void")

				wantClientError(t, r, id, "a wedged upstream blocked the lane instead of failing the call")
				// The shipped budget, not a shortened one — under this clock it costs
				// the same nothing — and spent exactly once. The elapsed time is what
				// says the two attempts shared one deadline rather than getting a full
				// one each.
				if elapsed := time.Since(start); elapsed != sendBudget {
					t.Errorf("gave up after %s, want the whole %s budget spent once", elapsed, sendBudget)
				}
				// The budget is spent, so the retry must not dial again: that costs an
				// unbounded flock wait to reach a handshake that expires on its first
				// write.
				if dials != 1 {
					t.Errorf("dial count = %d, want no retry once the deadline has passed", dials)
				}
			})
		})
	}
}

// The dial context must outlive the budget — a connection reads its SSE stream
// under it for the whole of its life, so a plain deadline context would cut a
// healthy upstream loose when the budget expired, the failure H5 describes
// caused by the fix for it — and must not outlive the connection, or a wedged
// upstream redialling on every call leaks one live child context per call for
// as long as the client stays. Both halves are asserted on the one connection,
// in the order they happen.
//
// The first is measured while the connection is still in use, which is the only
// place it means anything: on one the lane had already closed, it would hold
// with the context leaked too.
func TestADialContextLivesExactlyAsLongAsItsConnection(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		var dialled context.Context
		r.dial = func(ctx context.Context, _ string) (mcp.Connection, error) {
			dialled = ctx
			return newHandshakingConn(), nil
		}
		l := newLane(r, testWorktree)
		sendCall(t, l, "call-that-outlives-its-budget")

		// Long past the deadline the dial was given, and the connection is still
		// the lane's: a context carrying that deadline would be done by now.
		time.Sleep(2 * sendBudget)
		if err := dialled.Err(); err != nil {
			t.Errorf("the dialled context is %v after the budget expired, want a context still good for the stream", err)
		}

		// The close every site that gives a connection up performs, and that
		// lane.run performs on its way out.
		if err := l.conn.Close(); err != nil {
			t.Fatal(err)
		}
		if dialled.Err() == nil {
			t.Error("the dialled context is still live after its connection was closed, want it released with the connection")
		}
	})
}

// The reader has already answered this id, so the retry must not run: the
// client keeps the first reply it gets, so a redial that succeeded would look
// fine here while the client held the error.
func TestSendLeavesACallItsDyingUpstreamAlreadyFailed(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		// The upstream loses the race a real one loses when it dies mid-write:
		// its reader sees the death and fails the in-flight call before the
		// write reports it.
		conn := &fakeConn{reads: make(chan jsonrpc.Message)}
		conn.onWrite = func(context.Context, jsonrpc.Message) error {
			close(conn.reads)
			r.readFromUpstream(conn, testWorktree)
			return io.EOF
		}
		l := connectedLane(r, testWorktree, conn)
		forbidDial(t, r, "send retried a call its upstream had already failed")

		id := sendCall(t, l, "raced-call")

		wantClientError(t, r, id, "the dying upstream's reader left the call unanswered")
		wantClientQuiet(t, r, "one call was answered twice")
	})
}

// A gopls that dies mid-call must fail the calls it still owes. Nothing else
// will ever produce their ids, so a client without a timeout would hang.
//
// failInFlight takes its ids off the awaited list in bulk rather than through
// refuse, and dropping that delete costs nothing the first assertion can see.
// The second is what catches it: an entry per dead call for the rest of the
// session, and the next cancellation routed to a lane that stopped waiting.
func TestUpstreamDeathFailsItsInFlightCalls(t *testing.T) {
	bubble(t, func(t *testing.T) {
		conn := newFakeConn()
		r := newTestRouter(t, testHome)
		// Already connected: no dial or handshake to satisfy.
		l := connectedLane(r, testWorktree, conn)

		id := sendCall(t, l, "call-in-flight")
		mustRecv(t, conn.writes, "the call to reach the upstream, so an answer is owed")

		close(conn.reads) // the gopls dies before answering
		r.readFromUpstream(conn, testWorktree)

		wantClientError(t, r, id, "upstream died and the in-flight call was left unanswered")

		// Home, because nobody owes this id any more. Named against the lane it
		// was sent to: still owed, the cancellation would go there instead.
		got, _ := r.target(&jsonrpc.Request{
			Method: "notifications/cancelled",
			Params: json.RawMessage(`{"requestId":"call-in-flight"}`),
		})
		if got != testHome {
			t.Fatalf("cancelling a call its dead upstream already failed went to %q, want home %q", got, testHome)
		}
	})
}

// The claim rule is symmetric: a reader that loses the race drops the answer it
// read. send() claims an id when its write fails, and the upstream may have
// taken the message anyway — a POST that times out after the server accepted it
// is enough — so its answer arrives for a call the client has already closed.
func TestUpstreamAnswerToAnAlreadyFailedCallIsDropped(t *testing.T) {
	bubble(t, func(t *testing.T) {
		id := mustID(t, "already-failed")
		r := newTestRouter(t, testHome)
		// Nothing owes this id: send() claimed it and answered the client already.
		r.readFromUpstream(deadConn(&jsonrpc.Response{ID: id, Result: json.RawMessage(`{}`)}), testWorktree)

		wantClientQuiet(t, r, "a call the bridge had already answered was answered a second time")
	})
}

// A reconnect leaves two connections under one worktree, and the replaced one's
// reader is still running.
func TestStaleUpstreamDeathSparesTheReconnectedCall(t *testing.T) {
	bubble(t, func(t *testing.T) {
		live := newFakeConn()
		defer close(live.reads)
		r := newTestRouter(t, testHome)
		l := connectedLane(r, testWorktree, live)

		sendCall(t, l, "call-after-reconnect")
		mustRecv(t, live.writes, "the live connection to take the call it now owes")

		r.readFromUpstream(deadConn(), testWorktree)

		wantClientQuiet(t, r, "the replaced connection failed a call owned by the live one")
	})
}

// Forwarding would return the client's reply addressed to nobody in particular,
// and leave the upstream stalled on an answer that never comes, with no timeout
// of its own.
func TestUnroutableUpstreamRequestIsRefusedNotForwarded(t *testing.T) {
	bubble(t, func(t *testing.T) {
		id := mustID(t, "sample-1")
		reads := make(chan jsonrpc.Message, 1)
		reads <- &jsonrpc.Request{ID: id, Method: "sampling/createMessage"}
		conn := &fakeConn{reads: reads, writes: make(chan jsonrpc.Message, 1)}

		r := newTestRouter(t, testHome)
		go r.readFromUpstream(conn, testWorktree)

		resp := wantResponse(t, mustRecv(t, conn.writes, "the refusal sent back to the upstream"), id)
		_ = wantWireError(t, resp, jsonrpc.CodeMethodNotFound)
		close(reads)
		wantClientQuiet(t, r, "the client was asked something it cannot be given an answer for")
	})
}

// A call answered that quickly must leave nothing owed. Recorded after the
// write, the entry outlives the answer, and the next upstream death turns it
// into a second, contradictory reply to an id the client has already closed.
func TestCallAnsweredBeforeItsWriteReturnsLeavesNothingOwed(t *testing.T) {
	bubble(t, func(t *testing.T) {
		id := mustID(t, "answered-during-write")
		r := newTestRouter(t, testHome)
		// The upstream answers before its write returns, the way a gopls on the
		// same machine can, and does not return until the router has finished
		// accounting for that answer. Touching t here is allowed only because
		// this hook runs on the goroutine that called send — the one goroutine
		// where failing the test is safe; see inBackground.
		conn := &fakeConn{reads: make(chan jsonrpc.Message, 1)}
		conn.onWrite = func(context.Context, jsonrpc.Message) error {
			conn.reads <- &jsonrpc.Response{ID: id, Result: json.RawMessage(`{}`)}
			// The reader has now forgotten the call, before send() records it.
			mustRecv(t, r.out, "the answer the reader forwarded during this write")
			return nil
		}
		l := connectedLane(r, testWorktree, conn)

		// Not inBackground: this goroutine has no verdict to report, only an end,
		// and routing it through an error channel would leave a branch that cannot
		// fire and a linter asking why it is unchecked.
		done := make(chan struct{})
		go func() {
			defer close(done)
			r.readFromUpstream(conn, testWorktree)
		}()

		// Not sendCall: instantConn was built around this exact id, and minting a
		// second one from the same string would only hide that.
		l.send(&jsonrpc.Request{ID: id, Method: "tools/call"})
		close(conn.reads) // the upstream dies once the call is long since answered
		mustRecv(t, done, "the upstream reader to finish")

		wantClientQuiet(t, r, "an answered call was answered a second time")
	})
}

// gopls asks for its roots as soon as it has seen the capability in initialize,
// which lands inside the private handshake — before the reader that normally
// fields the question is running. One escaping to the client there comes back
// naming the tree the session opened in, and this upstream watches the wrong
// files for the rest of the session.
func TestRootsAskedDuringHandshakeIsAnsweredLocally(t *testing.T) {
	bubble(t, func(t *testing.T) {
		ctx := t.Context()
		upstream, gopls := upstreamPair(t)

		r := newTestRouter(t, testHome)
		handshakeReady(r)
		fixedDial(r, upstream)

		rootsID := mustID(t, "roots-mid-handshake")
		done := inBackground(func() error {
			msg, err := gopls.Read(ctx)
			if err != nil {
				return fmt.Errorf("read initialize: %w", err)
			}
			initialize, ok := msg.(*jsonrpc.Request)
			if !ok || initialize.Method != "initialize" {
				return fmt.Errorf("handshake opened with %#v, want initialize", msg)
			}
			// Asked before the handshake is answered, exactly as gopls does.
			if err := gopls.Write(ctx, &jsonrpc.Request{ID: rootsID, Method: "roots/list"}); err != nil {
				return fmt.Errorf("write roots/list: %w", err)
			}
			if msg, err = gopls.Read(ctx); err != nil {
				return fmt.Errorf("read roots answer: %w", err)
			}
			if resp, ok := msg.(*jsonrpc.Response); !ok || resp.ID != rootsID {
				return fmt.Errorf("mid-handshake roots/list got %#v, want an answer to %v", msg, rootsID)
			}
			if err := gopls.Write(ctx, &jsonrpc.Response{ID: initialize.ID, Result: json.RawMessage(`{}`)}); err != nil {
				return fmt.Errorf("answer initialize: %w", err)
			}
			if _, err := gopls.Read(ctx); err != nil { // notifications/initialized
				return fmt.Errorf("read initialized notification: %w", err)
			}
			return nil
		})

		if _, err := newLane(r, testWorktree).upstream(t.Context(), false); err != nil {
			t.Fatalf("handshake: %v", err)
		}
		if err := mustRecv(t, done, "the fake gopls to finish the handshake"); err != nil {
			t.Fatal(err)
		}

		wantClientQuiet(t, r, "roots/list from inside the handshake reached the client")
	})
}

// An upstream asking for roots must be told about its own worktree, and the
// client must never see the question: forwarding it would hand every gopls the
// tree the session opened in, leaving the others watching nothing.
func TestUpstreamRootsAnsweredWithItsOwnWorktree(t *testing.T) {
	bubble(t, func(t *testing.T) {
		const worktree = "/tmp/a worktree/wt" // the space exercises URI escaping
		ctx := t.Context()
		upstream, gopls := upstreamPair(t)

		id := mustID(t, "roots-1")
		r := newTestRouter(t, testHome)
		go r.readFromUpstream(upstream, worktree)

		if err := gopls.Write(ctx, &jsonrpc.Request{ID: id, Method: "roots/list"}); err != nil {
			t.Fatalf("write roots/list: %v", err)
		}
		msg, err := gopls.Read(ctx)
		if err != nil {
			t.Fatalf("read reply: %v", err)
		}
		root := wantOneRoot(t, wantResponse(t, msg, id))
		if want := "file:///tmp/a%20worktree/wt"; root.URI != want {
			t.Errorf("root uri = %q, want %q", root.URI, want)
		}
		if want := "wt"; root.Name != want {
			t.Errorf("root name = %q, want %q", root.Name, want)
		}

		wantClientQuiet(t, r, "roots/list was forwarded to the client")
	})
}

// worktreePath reduces a file to its directory itself, because every argument
// it is reached with is shaped like a file and a caller that forgot would get
// "Not a directory" from git — an error the router reads as "no evidence" and
// answers by misrouting to home. It must reduce exactly once, though: a path
// under a directory that does not exist yet must not climb to the grandparent
// and resolve against a tree the caller never named.
//
// One repository pair for every row: newLinkedWorktree forks git three times,
// and none of these rows can see another's paths.
func TestWorktreePathResolvesEachPathToItsOwnWorktree(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)

	file := filepath.Join(root, "main.go")
	target := filepath.Join(linked, "target.go")
	for path, content := range map[string]string{file: "package main\n", target: "package linked\n"} {
		mustWriteFile(t, path, content)
	}
	rootNested, linkedNested := filepath.Join(root, "nested"), filepath.Join(linked, "nested")
	for _, dir := range []string{rootNested, linkedNested} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// A link pointing into the other worktree: the reduction to a directory is
	// lexical, so without resolving first the link's own parent is what git
	// would be asked about, and here the two disagree.
	link := filepath.Join(root, "link.go")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		path string
		want string // "" wants an error instead
	}{
		{"a file in the root", file, root},
		{"a directory in the root", rootNested, root},
		{"the same directory in the linked worktree", linkedNested, linked},
		{"a symlink to a file in the linked worktree", link, linked},
		// R2: a file that does not exist yet still resolves through its parent,
		// and is not made unresolvable by the symlink pass — EvalSymlinks
		// refuses a path whose every component does not exist.
		{"a file not created yet", filepath.Join(root, "not-created-yet.go"), root},
		{"a file under a missing directory", filepath.Join(root, "absent", "x.go"), ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := worktreePath(t.Context(), test.path)
			if test.want == "" {
				if err == nil {
					t.Fatalf("worktreePath(%q) = %q, want no answer", test.path, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("worktreePath(%q): %v", test.path, err)
			}
			if got != test.want {
				t.Fatalf("worktreePath(%q) = %q, want %q", test.path, got, test.want)
			}
		})
	}
}

func TestToolCallRoutesToTheWorktreeOwningItsPath(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	file := filepath.Join(linked, "main.go")
	mustWriteFile(t, file, "package main\n")
	rootFile := filepath.Join(root, "main.go")
	mustWriteFile(t, rootFile, "package main\n")

	tests := []struct {
		name      string
		arguments string
		want      []string
	}{
		// The routing that matters: a file in the linked worktree must not be
		// answered by the gopls of the checkout the bridge was started in.
		{name: "file argument", arguments: `{"file":` + strconv.Quote(file) + `}`, want: []string{linked}},
		{name: "files argument", arguments: `{"files":[` + strconv.Quote(file) + `]}`, want: []string{linked}},
		{name: "dir argument", arguments: `{"dir":` + strconv.Quote(linked) + `}`, want: []string{linked}},
		// One worktree named twice is still one destination: the call is
		// answerable, and reporting a repeat would refuse it for no reason.
		{name: "one worktree named twice", arguments: `{"files":[` + strconv.Quote(file) + `,` + strconv.Quote(filepath.Join(linked, "other.go")) + `]}`, want: []string{linked}},
		// Both are reported so that route can refuse the call. Answering from
		// the first would say nothing at all about the file in the second, and
		// the client cannot see the omission.
		{name: "paths in two worktrees", arguments: `{"files":[` + strconv.Quote(file) + `,` + strconv.Quote(rootFile) + `]}`, want: []string{linked, root}},
		// No path, or an unresolvable one: the caller keeps its current server.
		{name: "no path argument", arguments: `{"query":"Foo"}`},
		{name: "unresolvable path", arguments: `{"file":"/nonexistent/x.go"}`},
		// A relative path would resolve against the bridge's own cwd and then
		// stick in the memo, misrouting every later call naming it.
		{name: "relative path", arguments: `{"file":"internal/foo.go"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := newTestRouter(t, testHome)
			got := r.toolCallWorktrees(json.RawMessage(`{"arguments":` + test.arguments + `}`))
			if !slices.Equal(got, test.want) {
				t.Fatalf("toolCallWorktrees(%s) = %q, want %q", test.arguments, got, test.want)
			}
		})
	}
}

// A tools/call spanning two worktrees is refused rather than answered from one
// of them: no single gopls knows both trees, and a result covering only the
// files it happened to route by is an omission the client cannot see.
func TestToolCallSpanningTwoWorktreesIsRefused(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)
	for _, dir := range []string{root, linked} {
		mustWriteFile(t, filepath.Join(dir, "main.go"), "package main\n")
	}

	r := newTestRouter(t, testHome)
	handshakeReady(r)
	id := mustID(t, "spanning-call")
	arguments := `{"files":[` + strconv.Quote(filepath.Join(linked, "main.go")) + `,` + strconv.Quote(filepath.Join(root, "main.go")) + `]}`
	r.route(&jsonrpc.Request{ID: id, Method: "tools/call", Params: json.RawMessage(`{"arguments":` + arguments + `}`)})

	// Refused on the reader's own goroutine, so the response is already queued
	// and no lane was ever asked for.
	select {
	case msg := <-r.out:
		wire := wantWireError(t, wantResponse(t, msg, id), jsonrpc.CodeInvalidParams)
		if !strings.Contains(wire.Message, root) || !strings.Contains(wire.Message, linked) {
			t.Fatalf("error %q names neither worktree, want both", wire.Message)
		}
	default:
		t.Fatal("spanning call was routed instead of refused")
	}
	if len(r.lanes) != 0 {
		t.Fatalf("refused call opened %d lane(s), want none", len(r.lanes))
	}
}

// What worktreeOf answers, and what it costs to answer it: each row queries a
// list of paths in order and then states how many directory entries the memo
// should hold, which is exactly the number of git forks the session paid for.
//
// One repository pair for every row, because none of them writes: newLinkedWorktree
// forks git three times, and a pair per row would pay that over again to
// re-create the same two trees. Each row still gets a router of its own — the
// memo counts are the assertion, so they must not carry across.
func TestWorktreeOfResolvesAndMemoizes(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)

	tests := []struct {
		name string
		// paths are queried in order; repeats are deliberate, and want holds the
		// answer expected for each in the same order.
		paths    []string
		want     []string
		wantMemo int // directory-memo entries afterwards
	}{
		{
			// Every path in a directory has the same worktree, and resolving one
			// costs a ~13ms fork. A memo keyed by the path would pay that again
			// for every file the session names in a directory already resolved.
			// Neither file exists, which is also the answer to "what does it key
			// on for a file gopls is about to create": the directory holding it.
			// The third is that same directory as a dir argument, spelled the way
			// a client may well send one — a key of its own would fork git again
			// for an answer already in the memo.
			name: "one directory, three spellings, one fork",
			paths: []string{
				filepath.Join(linked, "a.go"),
				filepath.Join(linked, "b.go"),
				linked + string(filepath.Separator),
			},
			want:     []string{linked, linked, linked},
			wantMemo: 1,
		},
		{
			// The verbatim-path memo in front of the directory one answers without
			// walking the argument at all, so it is the one that could hand a file
			// the worktree of a file it was merely asked about near. Two trees of
			// one repository is where that would show: they share a git directory
			// and differ only in the path. Twice each and interleaved, so that the
			// second pass — served from the memo — has to answer what the first did.
			name: "two worktrees stay apart across a memo hit",
			paths: []string{
				filepath.Join(root, "a.go"), filepath.Join(linked, "a.go"),
				filepath.Join(root, "a.go"), filepath.Join(linked, "a.go"),
			},
			want: []string{root, linked, root, linked},
			// One per worktree, and not one per query: the repeats were memo hits.
			wantMemo: 2,
		},
		{
			// A path is reduced to a directory exactly once, so a file in a
			// subdirectory that does not exist yet resolves to nothing rather than
			// climbing to the grandparent — which would answer with the enclosing
			// repository, evidence the call never offered and R2 says is not
			// evidence at all. Nothing is memoized, because nothing resolved.
			name:     "a missing directory is no evidence",
			paths:    []string{filepath.Join(root, "absent", "x.go")},
			want:     []string{""},
			wantMemo: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := newTestRouter(t, testHome)
			for i, path := range test.paths {
				if got := r.worktreeOf(path); got != test.want[i] {
					t.Fatalf("worktreeOf(%q) = %q, want %q", path, got, test.want[i])
				}
			}
			if len(r.worktrees) != test.wantMemo {
				t.Fatalf("memo holds %d directory entries, want %d: %v", len(r.worktrees), test.wantMemo, r.worktrees)
			}
		})
	}
}

// Both halves go through target(), because the sticky worktree is written
// there and nowhere else: seeded by hand, this test would pass with that write
// deleted, and in production every path-less follow-up — go_workspace,
// go_search — would go home instead of following the call before it.
func TestTargetFallsBackToTheLastPathBearingCall(t *testing.T) {
	t.Parallel()
	_, linked := newLinkedWorktree(t)
	r := newTestRouter(t, testHome)

	bearing := json.RawMessage(`{"arguments":{"file":"` + filepath.Join(linked, "x.go") + `"}}`)
	if got, _ := r.target(&jsonrpc.Request{Method: "tools/call", Params: bearing}); got != linked {
		t.Fatalf("path-bearing tools/call went to %q, want %q", got, linked)
	}

	params := json.RawMessage(`{"arguments":{"query":"Foo"}}`)
	if got, _ := r.target(&jsonrpc.Request{Method: "tools/call", Params: params}); got != linked {
		t.Fatalf("path-less tools/call went to %q, want the worktree the call before it named", got)
	}
	if got, _ := r.target(&jsonrpc.Request{Method: "tools/list"}); got != r.home {
		t.Fatalf("non-call request went to %q, want home", got)
	}
}

// A cancellation can arrive while its call is still queued at a lane that has
// not dialled yet — the cold-start window the lanes exist to keep off the
// reader is both the slowest and the likeliest moment for a client to give up.
// The destination is therefore bound where both messages pass, in the client's
// own order, and not where the lane finally writes the call: bound there, this
// cancellation would find nothing owed and go home, naming an id home never
// issued while the right gopls kept working.
func TestCancellationFindsACallItsLaneHasNotSentYet(t *testing.T) {
	bubble(t, func(t *testing.T) {
		const cold = "/tmp/cold"
		r := newTestRouter(t, testHome)
		r.sticky = cold  // routing by path would have to resolve one through git
		parkedDial(t, r) // where a cold worktree spends its time
		t.Cleanup(r.closeLanes)

		// Routed and picked up, but nothing is written yet, so the lane has no
		// connection to name and has not owed this id.
		r.route(&jsonrpc.Request{
			ID:     mustID(t, "slow-call"),
			Method: "tools/call",
			Params: json.RawMessage(`{"arguments":{"query":"Foo"}}`),
		})
		synctest.Wait()

		got, _ := r.target(&jsonrpc.Request{
			Method: "notifications/cancelled",
			Params: json.RawMessage(`{"requestId":"slow-call"}`),
		})
		if got != cold {
			t.Fatalf("cancellation of a queued call went to %q, want %q", got, cold)
		}
	})
}

// A lane with no upstream drops a cancellation rather than dialling one:
// bringing a gopls up costs an flock wait, a spawn and a whole handshake, to
// tell a brand new process about a call it never received. Nothing is stranded
// — the call died with the connection, and that connection's reader failed it.
func TestCancellationForADisconnectedUpstreamIsDroppedNotDialled(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		forbidDial(t, r, "a cancellation dialled the worktree that owed its call")

		cancel := &jsonrpc.Request{
			Method: "notifications/cancelled",
			Params: json.RawMessage(`{"requestId":"call-3"}`),
		}
		// A lane that never dialled, and one whose connection dies under this
		// very write — the retry starts from the same "no upstream to tell", so
		// the refusal has to be asked on every attempt rather than on the way in.
		newLane(r, "/tmp/gone").send(cancel)
		connectedLane(r, "/tmp/dying", failingConn()).send(cancel)

		wantClientQuiet(t, r, "a cancellation produced a message for the client")
	})
}

// A cancellation follows the id, not the default route: sent home it names an
// id home never issued, so the gopls actually running the call never hears it.
func TestCancellationFollowsTheRequestToItsUpstream(t *testing.T) {
	t.Parallel()
	r := newTestRouter(t, testHome)

	r.track(mustID(t, "call-1"), nil, "/repo/linked")
	// A numeric id reaches MakeID as the float64 json.Unmarshal produces, from
	// the notification here and from DecodeMessage when the call was keyed: the
	// two must land on the same ID or every real client's cancellation misroutes.
	r.track(mustID(t, float64(7)), nil, "/repo/numbered")
	// A worktree with no lane at all: routing follows the id regardless, since
	// whether that worktree has an upstream left to tell is the lane's
	// question, not this one — see
	// TestCancellationForADisconnectedUpstreamIsDroppedNotDialled. Which is
	// also why no case here registers a lane or a connection.
	r.track(mustID(t, "call-3"), nil, "/repo/gone")

	tests := []struct {
		name      string
		requestID string
		want      string
	}{
		{"owed id", `"call-1"`, "/repo/linked"},
		{"owed numeric id", `7`, "/repo/numbered"},
		{"disconnected upstream", `"call-3"`, "/repo/gone"},
		// Nobody owes these: an id already answered, and one no client would send.
		{"unowed id", `"call-2"`, r.home},
		{"unusable id", `{"bad":true}`, r.home},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, _ := r.target(&jsonrpc.Request{
				Method: "notifications/cancelled",
				Params: json.RawMessage(`{"requestId":` + test.requestID + `}`),
			})
			if got != test.want {
				t.Fatalf("cancellation of %s went to %q, want %q", test.requestID, got, test.want)
			}
		})
	}
}

// A retry places the call on a fresh connection, and its cancellation route has
// to come back with it. claim() ends the failed attempt by dropping the id's
// record whole, so a track() that restored only the ownership would leave the
// client's cancellation routed home — naming an id home never issued, while the
// gopls actually running the call never hears it.
func TestCancellationFollowsACallOntoItsRetryConnection(t *testing.T) {
	bubble(t, func(t *testing.T) {
		fresh := newHandshakingConn()
		r := newTestRouter(t, testHome)
		handshakeReady(r)
		// Never routed, only sent: the route under test is the one track() puts
		// back, not the one route() recorded on the way in.
		l := connectedLane(r, testWorktree, failingConn())
		fixedDial(r, fresh)

		id := sendCall(t, l, "call-retried")
		// The retry is only as good as the connection it landed on, so the call
		// is read off fresh before the route is asked about.
		if _, err := recvRequest(fresh.writes, "notifications/initialized"); err != nil {
			t.Fatal(err)
		}
		if _, err := recvRequest(fresh.writes, "tools/call"); err != nil {
			t.Fatal(err)
		}

		// Encoded from the type the router decodes into, rather than spelled by
		// hand: a test that splices the id into JSON itself also assumes it is a
		// string, and would keep passing if the field were renamed under it.
		params, err := json.Marshal(mcp.CancelledParams{RequestID: id.Raw()})
		if err != nil {
			t.Fatal(err)
		}
		got, _ := r.target(&jsonrpc.Request{Method: "notifications/cancelled", Params: params})
		if got != testWorktree {
			t.Errorf("cancellation of a retried call went to %q, want %q", got, testWorktree)
		}
	})
}

// brokenConn is a client transport whose read fails with something other than
// the end of the stream — the one case serve reports rather than swallowing.
// Everything else about it is a plain fakeConn.
type brokenConn struct {
	*fakeConn
	err error
}

func (c *brokenConn) Read(context.Context) (jsonrpc.Message, error) { return nil, c.err }

// A client closing its end is how every session ends, so reporting that as an
// error would make a clean exit look like a crash to whatever ran this tool —
// while a transport that broke for some other reason is the one thing the exit
// status has to carry.
func TestServeSwallowsTheEndOfTheClientAndReportsAnythingElse(t *testing.T) {
	t.Parallel()
	broken := errors.New("stdin fell over")
	for _, tc := range []struct {
		name string
		conn mcp.Connection
		want error
	}{
		{name: "client closed", conn: deadConn(), want: nil},
		{name: "transport broke", conn: &brokenConn{fakeConn: deadConn(), err: broken}, want: broken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Serve returning at all is half of what this asserts: it waits for its
			// reader and closes every lane, so a shutdown that ordered those wrongly
			// would hang here rather than answer. The context is the test's own and
			// is never cancelled — serve derives what it cancels, so a session that
			// leaned on the caller for that would hang here too.
			if err := serve(t.Context(), nil, testHome, tc.conn); !errors.Is(err, tc.want) {
				t.Fatalf("serve() = %v, want %v", err, tc.want)
			}
		})
	}
}

// writeToClient is the only path from an answer to the client, and the only
// thing it decides is what to do when that path fails. Reporting once and
// stopping is what lets serve exit; spinning on a transport that is gone would
// burn a core for as long as the client stayed dead.
func TestWriteToClientReportsAFailedWriteOnce(t *testing.T) {
	t.Parallel()
	r := newTestRouter(t, testHome)
	writes := 0
	conn := &fakeConn{onWrite: func(context.Context, jsonrpc.Message) error {
		writes++
		return io.ErrClosedPipe
	}}
	// Two queued, so that a writer which kept going after the failure would take
	// the second one and be caught doing it.
	r.out <- &jsonrpc.Response{ID: mustID(t, float64(1)), Result: json.RawMessage(`{}`)}
	r.out <- &jsonrpc.Response{ID: mustID(t, float64(2)), Result: json.RawMessage(`{}`)}

	r.writeToClient(conn) // returns, or this test times out

	if err := <-r.errs; !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writeToClient() reported %v, want the write's own error", err)
	}
	if writes != 1 {
		t.Fatalf("writeToClient() attempted %d writes after the first one failed, want it to stop at 1", writes)
	}
}

// The session ending is the other way out, and it is the ordinary one: serve
// cancels the context before closing the transport, so a writer still holding a
// message must drop it rather than report a failure nobody is left to read.
func TestWriteToClientStopsWithTheSessionWithoutReportingIt(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	r := newRouter(ctx, nil, testHome)
	cancel()

	r.writeToClient(&fakeConn{writes: make(chan jsonrpc.Message, 1)})

	select {
	case err := <-r.errs:
		t.Fatalf("writeToClient() reported %v for a session that ended normally", err)
	default:
	}
}

// newLinkedWorktree returns a fresh repository and a linked worktree of it.
func newLinkedWorktree(t *testing.T) (root, linked string) {
	t.Helper()
	root = filepath.Join(t.TempDir(), "main")
	linked = filepath.Join(t.TempDir(), "linked")
	runGit(t, "init", root)
	runGit(t, "-C", root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-m", "initial")
	runGit(t, "-C", root, "worktree", "add", "--detach", linked)
	return mustEvalSymlinks(t, root), mustEvalSymlinks(t, linked)
}

func mustEvalSymlinks(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func runGit(t *testing.T, args ...string) {
	t.Helper()
	if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}
