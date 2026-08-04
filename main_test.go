package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The argument grammar decides between three commands, two of which fork a
// gopls, and it is the last point at which a mistyped command line is cheap.
func TestRunRefusesCommandLinesItHasNoMeaningFor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "unknown command", args: []string{"gopls-mcp-manager", "restart"}},
		{name: "list takes no worktree", args: []string{"gopls-mcp-manager", "list", "/repo"}},
		{name: "bridge takes at most one", args: []string{"gopls-mcp-manager", "bridge", "/repo", "/other"}},
		{name: "ensure takes at most one", args: []string{"gopls-mcp-manager", "ensure", "/repo", "/other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer

			err := run(tc.args, &out)
			if err == nil {
				t.Fatalf("run(%q) accepted a command line it has no meaning for", tc.args)
			}
			// Refused on the grammar rather than on something further in: every
			// other error here would mean it had already reached a manager, a git
			// call or a gopls on the strength of a command line it cannot read.
			if !strings.HasPrefix(err.Error(), "usage: ") {
				t.Fatalf("run(%q) = %v, want the usage error", tc.args, err)
			}
			if out.Len() != 0 {
				t.Fatalf("run(%q) wrote %q before refusing", tc.args, out.String())
			}
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

	if err := run([]string{"gopls-mcp-manager", "list"}, &out); err != nil {
		t.Fatal(err)
	}

	// The header alone, because the map under a fresh home holds nothing: what
	// this asserts is that the command ran and reported to the writer it was
	// handed, not what a sweep of somebody's real map would have found.
	const wantOutput = "PORT\tPID\tWORKTREE\n"
	if out.String() != wantOutput {
		t.Fatalf("run(list) wrote %q, want %q", out.String(), wantOutput)
	}
}

// runnableWorktree sets up everything the two commands past the grammar need
// before they can answer: a home for the map file, a git worktree to resolve,
// and a record naming a gopls already serving that worktree. The record is what
// keeps this off a real gopls — ensure answers with a port it finds alive
// without starting anything, which is also the ordinary case in a session.
//
// Callers cannot be parallel: HOME is process-wide.
func runnableWorktree(t *testing.T) (repo string, port int) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	repo = filepath.Join(t.TempDir(), "repo")
	runGit(t, "init", repo)

	worktree, err := worktreePath(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	// Answering from the moment it binds, so the probe reaches its verdict
	// outright instead of waiting out a silent server and then forking a ps to
	// ask whose process it is.
	listener, port := listenInAllocationRange(t, worktree)
	serveEndpoint(t, listener, 0)

	// The path from newManager rather than composed here, since run will look for
	// the file wherever that function says — a third spelling of it would seed a
	// map nothing reads and fail the tests below for the wrong reason.
	m, err := newManager()
	if err != nil {
		t.Fatal(err)
	}
	// StartedAt zero puts the record outside any start grace, which is what makes
	// ensure probe it and answer rather than wait for a start of its own.
	mustWriteMap(t, m.mapPath, []record{{Worktree: worktree, Port: port, PID: os.Getpid()}})
	return repo, port
}

// The port is the whole output of ensure, and it is what a caller wiring an
// editor at this tool reads: printed on its own line, to the writer run was
// given rather than to whatever stdout happened to be.
//
// Not parallel: HOME is process-wide.
func TestRunEnsurePrintsThePortOfAGoplsAlreadyServing(t *testing.T) {
	repo, port := runnableWorktree(t)
	var out bytes.Buffer

	if err := run([]string{"gopls-mcp-manager", "ensure", repo}, &out); err != nil {
		t.Fatal(err)
	}

	want := fmt.Sprintf("%d\n", port)
	if out.String() != want {
		t.Fatalf("run(ensure) wrote %q, want %q", out.String(), want)
	}
}

// A client that says nothing and goes away is how every session ends. bridge
// has to come back from it, and come back without an error: run's error is the
// process's exit status, so a clean shutdown reported as a failure is what a
// supervisor would restart on.
//
// Not parallel: HOME and os.Stdin are both process-wide.
func TestRunBridgeReturnsCleanlyWhenTheClientGoesAway(t *testing.T) {
	repo, _ := runnableWorktree(t)
	// A stdin already at end of file, which is a client that connected and
	// closed. The real one is this process's, so it is swapped for the call and
	// put back after — the transport reads os.Stdin when it connects.
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	stdin := os.Stdin
	os.Stdin = reader
	t.Cleanup(func() { os.Stdin = stdin; _ = reader.Close() })

	if err := run([]string{"gopls-mcp-manager", "bridge", repo}, io.Discard); err != nil {
		t.Fatalf("run(bridge) = %v for a client that closed its end, want a clean return", err)
	}
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
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(git, filepath.Join(bin, "git")); err != nil {
					t.Fatal(err)
				}
				t.Setenv("PATH", bin)
				return []string{"gopls-mcp-manager", "ensure", repo}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := tc.setup(t)
			var out bytes.Buffer

			err := run(args, &out)
			if err == nil {
				t.Fatalf("run(%q) succeeded, want the missing piece reported", args[1:])
			}
			// Not the usage error: what is missing is on the machine, not in the
			// command line, and answering with the grammar would send whoever ran
			// this to re-read arguments that were right all along.
			if strings.HasPrefix(err.Error(), "usage: ") {
				t.Fatalf("run(%q) = %v, want an error about what is missing", args[1:], err)
			}
			if out.Len() != 0 {
				t.Fatalf("run(%q) wrote %q before failing", args[1:], out.String())
			}
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
	if err != nil {
		t.Fatal(err)
	}

	want := filepath.Join(home, ".local", "share", "gopls-ports.map")
	if m.mapPath != want {
		t.Fatalf("mapPath = %q, want %q", m.mapPath, want)
	}
	// Both hooks are what production runs with; a nil one would be a panic on
	// the first sweep or the first cold start rather than a test failure.
	if m.alive == nil || m.ready == nil {
		t.Fatal("newManager() left a manager the first sweep or start would panic on")
	}
}

// With no home there is nowhere to put the map, and that is a startup error
// rather than something to guess at: a manager falling back to a relative path
// would give every process a private map and let them all allocate the same
// port.
//
// Not parallel: HOME is process-wide.
func TestNewManagerFailsWithNoHomeDirectory(t *testing.T) {
	t.Setenv("HOME", "")

	if m, err := newManager(); err == nil {
		t.Fatalf("newManager() = %#v with no home directory, want an error", m)
	}
}
