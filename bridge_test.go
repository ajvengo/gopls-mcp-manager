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
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type fakeConn struct {
	reads  chan jsonrpc.Message
	writes chan jsonrpc.Message

	// writeErr fails every write.
	writeErr error
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
	if c.writeErr != nil {
		return c.writeErr
	}
	select {
	case c.writes <- msg:
		return nil
	case <-ctx.Done():
		// A real Connection honours its context, and the retried handshake in
		// TestHandshakeGivesUpOnAnUpstreamThatNeverAnswers hangs against one
		// that does not: by then the shared deadline has already expired.
		return ctx.Err()
	}
}

func (c *fakeConn) Close() error      { return nil }
func (c *fakeConn) SessionID() string { return "" }

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

func mustID(t *testing.T, raw string) jsonrpc.ID {
	t.Helper()
	id, err := jsonrpc.MakeID(raw)
	if err != nil {
		t.Fatalf("MakeID(%q): %v", raw, err)
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

func TestRootlessInitializeIsForwardedWithWorkingRoots(t *testing.T) {
	bubble(t, func(t *testing.T) {
		const home = "/tmp/home"
		clientReads := make(chan jsonrpc.Message, 1)
		upstreamReads := make(chan jsonrpc.Message, 1)
		upstream := &fakeConn{reads: upstreamReads, writes: make(chan jsonrpc.Message, 2)}
		r := newRouter(t.Context(), nil, home)
		r.conns[home] = upstream
		go r.readFromClient(&fakeConn{reads: clientReads})
		go r.readFromUpstream(upstream, home)
		t.Cleanup(func() {
			close(clientReads)
			close(upstreamReads)
		})

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

// No bubble: readFromUpstream runs to completion on this goroutine, so there is
// nothing for a clock to wait on.
func TestHomeUpstreamEOFDoesNotCloseClient(t *testing.T) {
	t.Parallel()

	reads := make(chan jsonrpc.Message)
	close(reads)
	r := newRouter(t.Context(), nil, "/tmp/home")
	r.readFromUpstream(&fakeConn{reads: reads}, r.home)

	select {
	case err := <-r.errs:
		t.Fatalf("home upstream EOF closed the client: %v", err)
	default:
	}
}

func TestSendReconnectsAndInitializesRestartedHome(t *testing.T) {
	bubble(t, func(t *testing.T) {
		home := "/tmp/home"
		fresh := &fakeConn{
			// Buffered, so that handing over the handshake's answer below cannot
			// block on the router picking it up.
			reads:  make(chan jsonrpc.Message, 1),
			writes: make(chan jsonrpc.Message),
		}
		r := newRouter(t.Context(), nil, home)
		r.conns[home] = &fakeConn{writeErr: io.EOF}
		r.initialize = &jsonrpc.Request{Method: "initialize", Params: json.RawMessage(`{}`)}
		r.dial = func(string) (mcp.Connection, error) { return fresh, nil }

		done := inBackground(func() error {
			initialize, err := recvRequest(fresh.writes, "initialize")
			if err != nil {
				return err
			}
			if !initialize.ID.IsValid() {
				return fmt.Errorf("reconnect opened with %#v, want a private initialize id", initialize)
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

		id := mustID(t, "call-7")
		r.send(home, &jsonrpc.Request{ID: id, Method: "tools/call"}, id, false)
		if err := mustRecv(t, done, "the reconnect assertions to finish"); err != nil {
			t.Fatal(err)
		}

		wantClientQuiet(t, r, "successful reconnect returned an error to the client")
	})
}

func TestSendRetriesInitialInitializeWithoutPrivateHandshake(t *testing.T) {
	bubble(t, func(t *testing.T) {
		home := "/tmp/home"
		fresh := &fakeConn{
			reads:  make(chan jsonrpc.Message),
			writes: make(chan jsonrpc.Message, 1),
		}
		failedReads := make(chan jsonrpc.Message)
		close(failedReads)
		dials := 0
		r := newRouter(t.Context(), nil, home)
		r.dial = func(string) (mcp.Connection, error) {
			dials++
			if dials == 1 {
				return &yieldingConn{fakeConn{reads: failedReads}}, nil
			}
			return fresh, nil
		}
		id := mustID(t, "client-initialize")
		initialize := &jsonrpc.Request{ID: id, Method: "initialize", Params: json.RawMessage(`{}`)}
		r.initialize = initialize

		r.send(home, initialize, id, true)
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

// yieldingConn is an upstream that is already gone, and whose write is certain
// to lose the race it is in: it lets every other goroutine in the bubble block
// first, so a reader started before the write — the regression
// TestSendRetriesInitialInitializeWithoutPrivateHandshake pins — always claims
// the id. Left to a real race, that reader wins about once in ten thousand runs.
type yieldingConn struct{ fakeConn }

func (c *yieldingConn) Write(context.Context, jsonrpc.Message) error {
	synctest.Wait()
	return io.EOF
}

// The handshake runs on the goroutine that reads the client, so an upstream
// that accepts the connection and never answers must not be waited on forever:
// it would stall every later client message too, and neither side times out.
// recordAlive keeps exactly such a server alive, so ensure hands one back.
func TestHandshakeGivesUpOnAnUpstreamThatNeverAnswers(t *testing.T) {
	bubble(t, func(t *testing.T) {
		const worktree = "/tmp/wt"
		wedged := &fakeConn{reads: make(chan jsonrpc.Message), writes: make(chan jsonrpc.Message, 1)}
		r := newRouter(t.Context(), nil, "/tmp/home")
		r.initialize = &jsonrpc.Request{Method: "initialize", Params: json.RawMessage(`{}`)}
		dials := 0
		r.dial = func(string) (mcp.Connection, error) {
			dials++
			return wedged, nil
		}

		id := mustID(t, "call-into-the-void")
		start := time.Now()
		r.send(worktree, &jsonrpc.Request{ID: id, Method: "tools/call"}, id, false)

		wantClientError(t, r, id, "a wedged upstream blocked the handshake instead of failing the call")
		// The shipped budget, not a shortened one — under this clock it costs the
		// same nothing — and spent exactly once. The elapsed time is what says the
		// two attempts shared one deadline rather than getting a full one each.
		if elapsed := time.Since(start); elapsed != r.handshakeTimeout {
			t.Errorf("gave up after %s, want the whole %s budget spent once", elapsed, r.handshakeTimeout)
		}
		// The budget is spent, so the retry must not dial again: that costs an
		// unbounded flock wait to reach a handshake that expires on its first write.
		if dials != 1 {
			t.Errorf("dial count = %d, want no retry once the deadline has passed", dials)
		}
	})
}

// dyingConn loses the race a real upstream loses when it dies mid-write: its
// reader sees the death and fails the in-flight call before Write reports it.
type dyingConn struct {
	fakeConn
	r        *router
	worktree string
}

func (c *dyingConn) Write(context.Context, jsonrpc.Message) error {
	close(c.reads)
	c.r.readFromUpstream(c, c.worktree)
	return io.EOF
}

// The reader has already answered this id, so the retry must not run: the
// client keeps the first reply it gets, so a redial that succeeded would look
// fine here while the client held the error.
func TestSendLeavesACallItsDyingUpstreamAlreadyFailed(t *testing.T) {
	bubble(t, func(t *testing.T) {
		const worktree = "/tmp/wt"
		r := newRouter(t.Context(), nil, "/tmp/home")
		conn := &dyingConn{fakeConn: fakeConn{reads: make(chan jsonrpc.Message)}, r: r, worktree: worktree}
		r.conns[worktree] = conn
		r.dial = func(string) (mcp.Connection, error) {
			t.Error("send retried a call its upstream had already failed")
			return nil, io.EOF
		}

		id := mustID(t, "raced-call")
		r.send(worktree, &jsonrpc.Request{ID: id, Method: "tools/call"}, id, false)

		wantClientError(t, r, id, "the dying upstream's reader left the call unanswered")
		wantClientQuiet(t, r, "one call was answered twice")
	})
}

// A gopls that dies mid-call must fail the calls it still owes. Nothing else
// will ever produce their ids, so a client without a timeout would hang.
func TestUpstreamDeathFailsItsInFlightCalls(t *testing.T) {
	bubble(t, func(t *testing.T) {
		const worktree = "/tmp/wt"
		reads := make(chan jsonrpc.Message)
		conn := &fakeConn{reads: reads, writes: make(chan jsonrpc.Message, 1)}
		r := newRouter(t.Context(), nil, "/tmp/home")
		r.conns[worktree] = conn // already connected: no dial or handshake to satisfy

		id := mustID(t, "call-in-flight")
		r.send(worktree, &jsonrpc.Request{ID: id, Method: "tools/call"}, id, false)
		mustRecv(t, conn.writes, "the call to reach the upstream, so an answer is owed")

		close(reads) // the gopls dies before answering
		r.readFromUpstream(conn, worktree)

		wantClientError(t, r, id, "upstream died and the in-flight call was left unanswered")
	})
}

// The claim rule is symmetric: a reader that loses the race drops the answer it
// read. send() claims an id when its write fails, and the upstream may have
// taken the message anyway — a POST that times out after the server accepted it
// is enough — so its answer arrives for a call the client has already closed.
func TestUpstreamAnswerToAnAlreadyFailedCallIsDropped(t *testing.T) {
	bubble(t, func(t *testing.T) {
		const worktree = "/tmp/wt"
		id := mustID(t, "already-failed")
		reads := make(chan jsonrpc.Message, 1)
		// Nothing owes this id: send() claimed it and answered the client already.
		reads <- &jsonrpc.Response{ID: id, Result: json.RawMessage(`{}`)}
		close(reads)

		r := newRouter(t.Context(), nil, "/tmp/home")
		r.readFromUpstream(&fakeConn{reads: reads}, worktree)

		wantClientQuiet(t, r, "a call the bridge had already answered was answered a second time")
	})
}

// A reconnect gives a worktree a new connection under the same name, so the
// replaced one's reader must not fail the calls the live one already accepted.
func TestStaleUpstreamDeathSparesTheReconnectedCall(t *testing.T) {
	bubble(t, func(t *testing.T) {
		const worktree = "/tmp/wt"
		live := &fakeConn{reads: make(chan jsonrpc.Message), writes: make(chan jsonrpc.Message, 1)}
		defer close(live.reads)
		r := newRouter(t.Context(), nil, "/tmp/home")
		r.conns[worktree] = live

		id := mustID(t, "call-after-reconnect")
		r.send(worktree, &jsonrpc.Request{ID: id, Method: "tools/call"}, id, false)
		mustRecv(t, live.writes, "the live connection to take the call it now owes")

		replaced := make(chan jsonrpc.Message)
		close(replaced)
		r.readFromUpstream(&fakeConn{reads: replaced}, worktree)

		wantClientQuiet(t, r, "the replaced connection failed a call owned by the live one")
	})
}

// A question we cannot route an answer back to is refused at once. Forwarding
// it would return the client's reply addressed to nobody in particular, and an
// upstream waiting on an answer that never comes stalls with no timeout.
func TestUnroutableUpstreamRequestIsRefusedNotForwarded(t *testing.T) {
	bubble(t, func(t *testing.T) {
		id := mustID(t, "sample-1")
		reads := make(chan jsonrpc.Message, 1)
		reads <- &jsonrpc.Request{ID: id, Method: "sampling/createMessage"}
		conn := &fakeConn{reads: reads, writes: make(chan jsonrpc.Message, 1)}

		r := newRouter(t.Context(), nil, "/tmp/home")
		go r.readFromUpstream(conn, "/tmp/wt")

		resp := wantResponse(t, mustRecv(t, conn.writes, "the refusal sent back to the upstream"), id)
		var wire *jsonrpc.Error
		if !errors.As(resp.Error, &wire) || wire.Code != jsonrpc.CodeMethodNotFound {
			t.Errorf("refusal was %#v, want a method-not-found error", resp.Error)
		}
		close(reads)
		wantClientQuiet(t, r, "the client was asked something it cannot be given an answer for")
	})
}

// instantConn answers every write before the write returns, the way a gopls on
// the same machine can, and does not return until the router has finished
// accounting for that answer.
//
// It holds a *testing.T, which inBackground forbids its own callers, because
// its Write only ever runs on the goroutine that called send — the one
// goroutine where failing the test is allowed.
type instantConn struct {
	fakeConn
	t  *testing.T
	r  *router
	id jsonrpc.ID
}

func (c *instantConn) Write(context.Context, jsonrpc.Message) error {
	c.reads <- &jsonrpc.Response{ID: c.id, Result: json.RawMessage(`{}`)}
	// The reader has now forgotten the call, before send() records it.
	mustRecv(c.t, c.r.out, "the answer the reader forwarded during this write")
	return nil
}

// A call answered that quickly must leave nothing owed. Recorded after the
// write, the entry outlives the answer, and the next upstream death turns it
// into a second, contradictory reply to an id the client has already closed.
func TestCallAnsweredBeforeItsWriteReturnsLeavesNothingOwed(t *testing.T) {
	bubble(t, func(t *testing.T) {
		const worktree = "/tmp/wt"
		id := mustID(t, "answered-during-write")
		r := newRouter(t.Context(), nil, "/tmp/home")
		conn := &instantConn{fakeConn: fakeConn{reads: make(chan jsonrpc.Message, 1)}, t: t, r: r, id: id}
		r.conns[worktree] = conn

		// Not inBackground: this goroutine has no verdict to report, only an end,
		// and routing it through an error channel would leave a branch that cannot
		// fire and a linter asking why it is unchecked.
		done := make(chan struct{})
		go func() {
			defer close(done)
			r.readFromUpstream(conn, worktree)
		}()

		r.send(worktree, &jsonrpc.Request{ID: id, Method: "tools/call"}, id, false)
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
		const worktree = "/tmp/wt"
		ctx := t.Context()
		upstream, gopls := upstreamPair(t)

		r := newRouter(ctx, nil, "/tmp/home")
		r.initialize = &jsonrpc.Request{Method: "initialize", Params: json.RawMessage(`{}`)}
		r.dial = func(string) (mcp.Connection, error) { return upstream, nil }

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

		if _, err := r.upstream(time.Now().Add(time.Minute), worktree, false); err != nil {
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
		r := newRouter(ctx, nil, "/tmp/home")
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
func TestWorktreePathReducesToADirectoryExactlyOnce(t *testing.T) {
	t.Parallel()
	root, _ := newLinkedWorktree(t)
	file := filepath.Join(root, "main.go")
	if err := os.WriteFile(file, []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := worktreePath(file)
	if err != nil {
		t.Fatalf("worktreePath(%q): %v", file, err)
	}
	if got != root {
		t.Errorf("worktreePath(%q) = %q, want %q", file, got, root)
	}

	absent := filepath.Join(root, "absent", "x.go")
	if got, err := worktreePath(absent); err == nil {
		t.Errorf("worktreePath(%q) = %q, want no answer for a path under a missing directory", absent, got)
	}
}

func TestWorktreePathSeparatesLinkedWorktreesAndSharesNestedDirectories(t *testing.T) {
	t.Parallel()
	root, linked := newLinkedWorktree(t)

	rootNested := filepath.Join(root, "nested")
	linkedNested := filepath.Join(linked, "nested")
	for _, dir := range []string{rootNested, linkedNested} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	rootPath, err := worktreePath(rootNested)
	if err != nil {
		t.Fatal(err)
	}
	linkedPath, err := worktreePath(linkedNested)
	if err != nil {
		t.Fatal(err)
	}
	if rootPath != root {
		t.Fatalf("worktreePath(%q) = %q, want %q", rootNested, rootPath, root)
	}
	if linkedPath != linked {
		t.Fatalf("worktreePath(%q) = %q, want %q", linkedNested, linkedPath, linked)
	}
	if rootPath == linkedPath {
		t.Fatalf("linked worktrees share identity %q", rootPath)
	}
}

func TestToolCallRoutesToTheWorktreeOwningItsPath(t *testing.T) {
	t.Parallel()
	_, linked := newLinkedWorktree(t)
	file := filepath.Join(linked, "main.go")
	if err := os.WriteFile(file, []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		arguments string
		want      string
	}{
		// The routing that matters: a file in the linked worktree must not be
		// answered by the gopls of the checkout the bridge was started in.
		{"file argument", `{"file":` + strconv.Quote(file) + `}`, linked},
		{"files argument", `{"files":[` + strconv.Quote(file) + `]}`, linked},
		{"dir argument", `{"dir":` + strconv.Quote(linked) + `}`, linked},
		// No path, or an unresolvable one: the caller keeps its current server.
		{"no path argument", `{"query":"Foo"}`, ""},
		{"unresolvable path", `{"file":"/nonexistent/x.go"}`, ""},
		// A relative path would resolve against the bridge's own cwd and then
		// stick in the memo, misrouting every later call naming it.
		{"relative path", `{"file":"internal/foo.go"}`, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := newRouter(t.Context(), nil, "")
			got := r.toolCallWorktree(json.RawMessage(`{"arguments":` + test.arguments + `}`))
			if got != test.want {
				t.Fatalf("toolCallWorktree(%s) = %q, want %q", test.arguments, got, test.want)
			}
		})
	}
}

// Every path in a directory has the same worktree, and resolving one costs a
// ~13ms fork. A memo keyed by the path would pay that again for every file the
// session names in a directory it has already resolved.
func TestWorktreeOfMemoizesByDirectory(t *testing.T) {
	t.Parallel()
	_, linked := newLinkedWorktree(t)
	r := newRouter(t.Context(), nil, "")

	// Neither file exists, which is also the answer to "what does it key on for
	// a file gopls is about to create": the directory holding it. The third is
	// that same directory as a dir argument, spelled the way a client may well
	// send one — a key of its own would fork git again for an answer already in
	// the memo.
	for _, path := range []string{
		filepath.Join(linked, "a.go"),
		filepath.Join(linked, "b.go"),
		linked + string(filepath.Separator),
	} {
		if got := r.worktreeOf(path); got != linked {
			t.Fatalf("worktreeOf(%q) = %q, want %q", path, got, linked)
		}
	}
	if len(r.worktrees) != 1 {
		t.Fatalf("memo holds %d entries for one directory: %v", len(r.worktrees), r.worktrees)
	}
}

// worktreeOf must hand worktreePath the path itself, not the directory it keyed
// the memo by. Reduced a second time, a path under a directory that does not
// exist yet climbs to the grandparent — so a file in an uncreated subdirectory
// would resolve against the enclosing repository, which is evidence the call
// never offered and R2 says is not evidence at all.
func TestWorktreeOfDoesNotResolveThroughAMissingDirectory(t *testing.T) {
	t.Parallel()
	root, _ := newLinkedWorktree(t)
	r := newRouter(t.Context(), nil, "")

	absent := filepath.Join(root, "absent", "x.go")
	if got := r.worktreeOf(absent); got != "" {
		t.Fatalf("worktreeOf(%q) = %q, want no evidence", absent, got)
	}
}

func TestTargetFallsBackToTheLastPathBearingCall(t *testing.T) {
	t.Parallel()
	r := newRouter(t.Context(), nil, "/repo/home")
	r.sticky = "/repo/linked"

	params := json.RawMessage(`{"arguments":{"query":"Foo"}}`)
	if got := r.target(&jsonrpc.Request{Method: "tools/call", Params: params}); got != "/repo/linked" {
		t.Fatalf("path-less tools/call went to %q, want the sticky worktree", got)
	}
	if got := r.target(&jsonrpc.Request{Method: "tools/list"}); got != "/repo/home" {
		t.Fatalf("non-call request went to %q, want home", got)
	}
}

// A cancellation follows the id, not the default route: sent home it names an
// id home never issued, so the gopls actually running the call never hears it.
func TestCancellationFollowsTheRequestToItsUpstream(t *testing.T) {
	t.Parallel()
	r := newRouter(t.Context(), nil, "/repo/home")

	r.conns["/repo/linked"] = &fakeConn{}
	r.owe(mustID(t, "call-1"), owed{conn: r.conns["/repo/linked"], worktree: "/repo/linked"})
	// A numeric id reaches MakeID as the float64 json.Unmarshal produces, from
	// the notification here and from DecodeMessage when the call was keyed: the
	// two must land on the same ID or every real client's cancellation misroutes.
	numeric, err := jsonrpc.MakeID(float64(7))
	if err != nil {
		t.Fatal(err)
	}
	r.conns["/repo/numbered"] = &fakeConn{}
	r.owe(numeric, owed{conn: r.conns["/repo/numbered"], worktree: "/repo/numbered"})
	// Owed, but by a connection send() has already dropped. Dialling a
	// replacement would spawn a gopls to hear a cancellation for a call it never
	// received, so this goes home like an unowed id.
	r.owe(mustID(t, "call-3"), owed{conn: &fakeConn{}, worktree: "/repo/gone"})

	tests := []struct {
		name      string
		requestID string
		want      string
	}{
		{"owed id", `"call-1"`, "/repo/linked"},
		{"owed numeric id", `7`, "/repo/numbered"},
		// Nobody connected owes these: an id already answered, one no client
		// would send, and one whose upstream is gone.
		{"unowed id", `"call-2"`, "/repo/home"},
		{"unusable id", `{"bad":true}`, "/repo/home"},
		{"disconnected upstream", `"call-3"`, "/repo/home"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := r.target(&jsonrpc.Request{
				Method: "notifications/cancelled",
				Params: json.RawMessage(`{"requestId":` + test.requestID + `}`),
			})
			if got != test.want {
				t.Fatalf("cancellation of %s went to %q, want %q", test.requestID, got, test.want)
			}
		})
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
	var err error
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	linked, err = filepath.EvalSymlinks(linked)
	if err != nil {
		t.Fatal(err)
	}
	return root, linked
}

func runGit(t *testing.T, args ...string) {
	t.Helper()
	if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}
