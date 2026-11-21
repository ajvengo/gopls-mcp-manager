package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// A test or fuzz target, as declared in Go and as cited in the docs.
	declaredPattern = regexp.MustCompile(`func ((?:Test|Fuzz)[A-Za-z0-9_]*)\(`)
	citedPattern    = regexp.MustCompile("`((?:Test|Fuzz)[A-Za-z0-9_]*)`")
	// A table row's name, and the citation naming one or more of them.
	rowPattern      = regexp.MustCompile(`name:\s*"([^"]*)"`)
	citedRowPattern = regexp.MustCompile(`\(cases? ("[^)]*")\)`)
	quotedPattern   = regexp.MustCompile(`"([^"]*)"`)
	spacePattern    = regexp.MustCompile(`\s+`)
)

// SPEC.md and README.md name the test behind each invariant, which is worth
// something only while the names still resolve. Eight had already drifted when
// this check was written: folding four recordAlive tests and three worktreeOf
// tests into tables renamed them, and prose cannot fail to compile.
//
// Case names are checked too, since a table's rows are where the invariants
// ended up. Whitespace is collapsed first, because a citation — and a quoted
// case name inside it — may be wrapped across lines.
func TestDocsNameTestsThatExist(t *testing.T) {
	t.Parallel()

	declared := make(map[string]bool)
	rows := make(map[string]bool)
	testFiles, err := filepath.Glob("*_test.go")
	require.NoError(t, err)
	internalFiles, err := filepath.Glob("internal/*/*_test.go")
	require.NoError(t, err)
	testFiles = append(testFiles, internalFiles...)
	for _, path := range testFiles {
		source := mustReadString(t, path)
		for _, m := range declaredPattern.FindAllStringSubmatch(source, -1) {
			declared[m[1]] = true
		}
		for _, m := range rowPattern.FindAllStringSubmatch(source, -1) {
			rows[m[1]] = true
		}
	}

	// Both sides are counted, because a pattern that matched nothing would let
	// this pass on anything — which is the failure it exists to prevent.
	var citedTests, citedRows int
	for _, doc := range []string{"SPEC.md", "README.md"} {
		prose := spacePattern.ReplaceAllString(mustReadString(t, doc), " ")
		for _, m := range citedPattern.FindAllStringSubmatch(prose, -1) {
			citedTests++
			assert.Truef(t, declared[m[1]], "%s names %s, which no test declares", doc, m[1])
		}
		for _, citation := range citedRowPattern.FindAllStringSubmatch(prose, -1) {
			for _, m := range quotedPattern.FindAllStringSubmatch(citation[1], -1) {
				citedRows++
				assert.Truef(t, rows[m[1]], "%s names case %q, which no table row declares", doc, m[1])
			}
		}
	}
	switch {
	case len(declared) == 0:
		require.FailNow(t, "no tests found, so this check would pass on anything")
	case citedTests == 0:
		require.FailNow(t, "no test citation found in the docs, so this check would pass on anything")
	case citedRows == 0:
		require.FailNow(t, "no case citation found in the docs, so the case check would pass on anything")
	}
}

func mustReadString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

func (c *fakeConn) Close() error { return nil }

// Tests use the production owner lifecycle with fake OS boundaries and raw dials.
func newTestRouter(tb testing.TB, home string) *router {
	tb.Helper()
	return newRouter(tb.Context(), routerTestOwner(tb, nil), home)
}

func routerTestOwner(tb testing.TB, m *manager) *supervisor {
	tb.Helper()
	if m == nil {
		m = &manager{limits: config.Default()}
		m.limits.OwnerRetainedBytes = 1 << 30
	}
	if m.watch == nil {
		m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	}
	ctx, cancel := context.WithCancel(tb.Context())
	s := newSupervisor(ctx, m)
	tb.Cleanup(func() { s.shutdown(); cancel() })
	s.validateToolchain = func(context.Context, string) error { return nil }
	s.portBusy = func(int) bool { return false }
	s.estimateQuota = func(context.Context, watchPlan, int) watchFDQuota { return watchFDQuota{quota: 512} }
	s.start = func(context.Context, string, int, int) (*guardianChild, error) {
		done := make(chan struct{})
		var once sync.Once
		stop := func(os.Signal) error { once.Do(func() { close(done) }); return nil }
		return &guardianChild{Process: &os.Process{Pid: 123}, ConfirmedDone: done, Signal: stop, CloseOwner: func() error { return stop(syscall.SIGTERM) }}, nil
	}
	s.ready = func(context.Context, int, int) error { return nil }
	useMemoryRegistry(s)
	return s
}

func useMemoryRegistry(s *supervisor) {
	var mu sync.Mutex
	var records []record
	s.updateRecords = func(_ context.Context, update func([]record) ([]record, error)) ([]record, error) {
		mu.Lock()
		defer mu.Unlock()
		next, err := update(slices.Clone(records))
		if err == nil {
			records = next
		}
		return slices.Clone(records), err
	}
}

func testUpstream(r *router, worktree string, raw mcp.Connection, ctx context.Context, cancel context.CancelFunc) *ownedUpstream {
	rec, err := r.owner.ensureRecord(r.owner.ctx, worktree)
	if err != nil {
		panic(err)
	}
	conn, err := r.owner.connection(raw, rec, ctx, cancel)
	if err != nil {
		panic(err)
	}
	return conn.(*ownedUpstream)
}

func testConnection(r *router, raw mcp.Connection, worktree string) mcp.Connection {
	if raw == nil {
		return nil
	}
	if _, ok := raw.(*ownedUpstream); ok {
		return raw
	}
	r.owner.mu.Lock()
	for _, g := range r.owner.generations {
		for c := range g.connections {
			if c.Connection == raw {
				r.owner.mu.Unlock()
				return c
			}
		}
	}
	r.owner.mu.Unlock()
	c := testUpstream(r, worktree, raw, r.ctx, func() {})
	if id := serverOf(raw); id.known() {
		r.owner.mu.Lock()
		c.g.record.PID = id.pid
		c.g.record.StartedAt = id.started
		r.owner.mu.Unlock()
	}
	return c
}

// Tests drive send synchronously so assertions do not race a lane worker.
func connectedLane(r *router, worktree string, conn mcp.Connection) *lane {
	conn = testConnection(r, conn, worktree)
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
	r.dial = func(context.Context, string, int) (mcp.Connection, error) {
		<-t.Context().Done()
		return nil, io.EOF
	}
}

// fixedDial hands every lane the same connection, for the tests whose subject
// is what the bridge does with an upstream rather than how it came by one.
func fixedDial(r *router, conn mcp.Connection) {
	r.dial = func(context.Context, string, int) (mcp.Connection, error) { return conn, nil }
}

// forbidDial fails the test if a lane reaches for a connection at all, naming
// what that would mean. For the tests whose whole property is that it does not.
func forbidDial(t *testing.T, r *router, whatWouldBeWrong string) {
	t.Helper()
	r.dial = func(context.Context, string, int) (mcp.Connection, error) {
		assert.Fail(t, whatWouldBeWrong)
		return nil, io.EOF
	}
}

// wantClientError is its twin: the bridge must answer a call nothing else will
// ever answer, or a client without a timeout hangs on it forever. Same rule —
// call it from inside a bubble.
// Returns the response so a caller that cares which error it was can hand it to
// wantWireError; most only care that the call was answered at all.
func wantClientError(t *testing.T, r *router, id jsonrpc.ID, whatWouldBeWrong string) *jsonrpc.Response {
	t.Helper()
	synctest.Wait()
	select {
	case msg := <-r.out:
		releaseMessage(msg)
		resp, ok := msg.(*jsonrpc.Response)
		require.Truef(t, ok, "client got %T, want an error *jsonrpc.Response", msg)
		assert.Equalf(t, id, resp.ID, "failed id = %v, want the call %v", resp.ID, id)
		assert.NotNilf(t, resp.Error, "call %v was answered without an error", id)
		return resp
	default:
		require.FailNow(t, whatWouldBeWrong)
		return nil
	}
}

// The argument grammar decides between three commands, two of which fork a
// gopls, and it is the last point at which a mistyped command line is cheap.
func TestRunRefusesCommandLinesItHasNoMeaningFor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "unknown command", args: []string{"gopls-mcp-manager", "unknown"}},
		{name: "list takes no worktree", args: []string{"gopls-mcp-manager", "list", "/repo"}},
		{name: "bridge takes at most one", args: []string{"gopls-mcp-manager", "bridge", "/repo", "/other"}},
		{name: "ensure takes at most one", args: []string{"gopls-mcp-manager", "ensure", "/repo", "/other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer

			err := run(tc.args, &out)
			require.Errorf(t, err, "run(%q) accepted a command line it has no meaning for", tc.args)
			// Refused on the grammar rather than on something further in: every
			// other error here would mean it had already reached a manager, a git
			// call or a gopls on the strength of a command line it cannot read.
			require.Truef(t, strings.HasPrefix(err.Error(), "usage: "), "run(%q) = %v, want the usage error", tc.args, err)
			require.Zerof(t, out.Len(), "run(%q) wrote %q before refusing", tc.args, out.String())
		})
	}
}

// list is the one command that completes without a gopls or a git call, so it
// is what says run reaches a manager at all rather than stopping at the grammar
// above.
//
// Not parallel: HOME is process-wide, and newManager is the reason this test
// sets it.
func TestRunListReportsThroughTheWriterItWasGiven(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var out bytes.Buffer

	err := run([]string{"gopls-mcp-manager", "list"}, &out)
	require.NoError(t, err)

	// The header alone, because the map under a fresh home holds nothing: what
	// this asserts is that the command ran and reported to the writer it was
	// handed, not what a sweep of somebody's real map would have found.
	const wantOutput = "PORT\tPID\tWORKTREE\n"
	require.Equalf(t, wantOutput, out.String(), "run(list) wrote %q, want %q", out.String(), wantOutput)
}

// HOME is process-wide, so this legacy-adoption regression must remain serial.
func TestRunRefusesLegacySharedProcesses(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	repo := filepath.Join(t.TempDir(), "repo")
	runGit(t, "init", repo)

	worktree, err := worktreePath(t.Context(), repo)
	require.NoError(t, err)
	listener, port := listenInAllocationRange(t, worktree)
	serveEndpoint(t, listener, 0)

	// The path from newManager rather than composed here, since run will look for
	// the file wherever that function says — a third spelling of it would seed a
	// map nothing reads and fail the tests below for the wrong reason.
	m, err := newManager()
	require.NoError(t, err)
	mustWriteMap(t, m.mapPath, []record{{Worktree: worktree, Port: port, PID: os.Getpid()}})
	var out bytes.Buffer
	err = run([]string{"gopls-mcp-manager", "ensure", repo}, &out)
	require.ErrorContains(t, err, "cannot adopt live or uncertain legacy gopls")
	require.Empty(t, out.String())
	require.Equal(t, []record{{Worktree: worktree, Port: port, PID: os.Getpid()}}, mustReadMap(t, m.mapPath), "legacy reservation changed during refused adoption")
}

// The port is the whole output of ensure, and it is what a caller wiring an
// editor at this tool reads: printed on its own line, to the writer run was
// given rather than to whatever stdout happened to be.
//
// Not parallel: HOME is process-wide.
func TestRunEnsurePrintsThePortOfAGoplsAlreadyServing(t *testing.T) {
	repo, port := supervisorRunFixture(t)
	var out bytes.Buffer

	err := run([]string{"gopls-mcp-manager", "ensure", repo}, &out)
	require.NoError(t, err)

	want := fmt.Sprintf("%d\n", port)
	require.Equalf(t, want, out.String(), "run(ensure) wrote %q, want %q", out.String(), want)
}

// A client that says nothing and goes away is how every session ends. bridge
// has to come back from it, and come back without an error: run's error is the
// process's exit status, so a clean shutdown reported as a failure is what a
// supervisor would restart on.
//
// Not parallel: HOME and os.Stdin are both process-wide.
func TestRunBridgeReturnsCleanlyWhenTheClientGoesAway(t *testing.T) {
	repo, _ := supervisorRunFixture(t)
	// A stdin already at end of file, which is a client that connected and
	// closed. The real one is this process's, so it is swapped for the call and
	// put back after — the transport reads os.Stdin when it connects.
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)
	stdin := os.Stdin
	os.Stdin = reader
	t.Cleanup(func() { os.Stdin = stdin; _ = reader.Close() })

	err = run([]string{"gopls-mcp-manager", "bridge", repo}, io.Discard)
	require.NoErrorf(t, err, "run(bridge) = %v for a client that closed its end, want a clean return", err)
}

// Every command past the grammar reaches for something outside this process —
// a home directory to put the map in, a git worktree to serve, a gopls to serve
// it with — and any of the three can be missing on a machine where the tool
// itself is installed perfectly. All three are startup errors on purpose: run's
// return is the exit status, so a bridge that came up without one of them would
// instead fail its first tool call, halfway into a session, where the client
// reads it as the tool being broken rather than the install.
//
// Not parallel: HOME and PATH are both process-wide.
func TestRunReportsWhatItCouldNotFind(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T) []string
	}{
		{
			name: "no home directory to keep the map in",
			setup: func(t *testing.T) []string {
				t.Setenv("HOME", "")
				return []string{"gopls-mcp-manager", "list"}
			},
		},
		{
			name: "no worktree at the path given",
			setup: func(t *testing.T) []string {
				t.Setenv("HOME", t.TempDir())
				return []string{"gopls-mcp-manager", "ensure", filepath.Join(t.TempDir(), "not-a-repo")}
			},
		},
		{
			name: "no gopls to start",
			setup: func(t *testing.T) []string {
				t.Setenv("HOME", t.TempDir())
				repo := filepath.Join(t.TempDir(), "repo")
				runGit(t, "init", repo)
				// A PATH holding git and nothing else: the worktree still resolves,
				// which is what puts the failure at the start rather than before it.
				bin := t.TempDir()
				git, err := exec.LookPath("git")
				require.NoError(t, err)
				err = os.Symlink(git, filepath.Join(bin, "git"))
				require.NoError(t, err)
				t.Setenv("PATH", bin)
				return []string{"gopls-mcp-manager", "ensure", repo}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := tc.setup(t)
			var out bytes.Buffer

			err := run(args, &out)
			require.Errorf(t, err, "run(%q) succeeded, want the missing piece reported", args[1:])
			// Not the usage error: what is missing is on the machine, not in the
			// command line, and answering with the grammar would send whoever ran
			// this to re-read arguments that were right all along.
			require.Falsef(t, strings.HasPrefix(err.Error(), "usage: "), "run(%q) = %v, want an error about what is missing", args[1:], err)
			require.Zerof(t, out.Len(), "run(%q) wrote %q before failing", args[1:], out.String())
		})
	}
}

// Where the map lives is the whole of newManager's decision, and it is a
// decision every process on the machine has to reach the same way: the file is
// the shared claim, so a manager that composed a different path would allocate
// ports nobody else knows are taken.
//
// Not parallel: HOME is process-wide.
func TestNewManagerPutsItsMapUnderTheHomeDirectory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	m, err := newManager()
	require.NoError(t, err)

	want := filepath.Join(home, ".local", "share", "gopls-ports.map")
	require.Equalf(t, want, m.mapPath, "mapPath = %q, want %q", m.mapPath, want)
	// Both hooks are what production runs with; a nil one would be a panic on
	// the first sweep or the first cold start rather than a test failure.

}

// With no home there is nowhere to put the map, and that is a startup error
// rather than something to guess at: a manager falling back to a relative path
// would give every process a private map and let them all allocate the same
// port.
//
// Not parallel: HOME is process-wide.
func TestNewManagerFailsWithNoHomeDirectory(t *testing.T) {
	t.Setenv("HOME", "")

	m, err := newManager()
	require.Errorf(t, err, "newManager() = %#v with no home directory, want an error", m)
}

// track seeds or updates an outstanding request for tests of delivered traffic.
func (r *router) track(id jsonrpc.ID, conn mcp.Connection, worktree string) {
	conn = testConnection(r, conn, worktree)
	if !id.IsValid() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	owner := r.awaitingUpstream[id]
	if owner.worktree != worktree {
		if owner.worktree != "" {
			r.perWorktree[owner.worktree]--
		}
		r.perWorktree[worktree]++
	}
	if owner.state == nil {
		owner.state = &callState{}
	}
	owner.conn, owner.worktree = conn, worktree
	r.awaitingUpstream[id] = owner
}

// route and target are the two ingress steps run back to back, which is what a
// test wants to say and what no production path says: the stdio reader admits
// on one goroutine and routes on another, so that a cancellation is not queued
// behind the call it cancels. They live here so the routing file describes only
// what the binary runs.

// route admits req without waiting for lane capacity. Its budget starts here,
// so waiting in the queue does not buy another delivery budget.
func (r *router) route(req *jsonrpc.Request) {
	if call, ok := r.admitted(req); ok {
		r.routePrepared(call)
	}
}

// target picks the worktree that should answer req, or reports why no single
// one can — see toolCallWorktrees.
func (r *router) target(req *jsonrpc.Request) (string, error) {
	return r.targetContext(r.ctx, req)
}

type identifiedConn struct {
	mcp.Connection
	id serverID
}

func (c *identifiedConn) server() serverID { return c.id }
