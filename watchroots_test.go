package main

import (
	"context"
	"encoding/json/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustTree creates every path under root: a trailing slash makes a directory,
// anything else a file holding its contents.
func mustTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		full := filepath.Join(root, filepath.FromSlash(name))
		if name[len(name)-1] == '/' {
			require.NoError(t, os.MkdirAll(full, 0o700))
			continue
		}
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
		mustWriteFile(t, full, content)
	}
}

func relRoots(t *testing.T, worktree string, plan watchPlan) []string {
	t.Helper()
	rels := make([]string, len(plan.roots))
	for i, root := range plan.roots {
		rel, err := filepath.Rel(worktree, root)
		require.NoError(t, err)
		rels[i] = filepath.ToSlash(rel)
	}
	return rels
}

func TestPathIncludeFuncMatchesGopls(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		rules []string
		path  string
		want  bool
	}{
		{nil, "docs", true},
		{[]string{"-docs"}, "docs", false},
		{[]string{"-docs"}, "docs/a/b", false},
		{[]string{"-docs"}, "docsite", true},
		{[]string{"-docs"}, "web/docs", true},
		{[]string{"-**/node_modules"}, "node_modules", false},
		{[]string{"-**/node_modules"}, "web/app/node_modules/x", false},
		{[]string{"-docs", "+docs/go"}, "docs/go/pkg", true},
		{[]string{"-docs", "+docs/go"}, "docs/other", false},
		{[]string{"+docs/go", "-docs"}, "docs/go", false}, // last match wins
		{[]string{"-"}, "anything", false},
	} {
		assert.Equalf(t, tc.want, pathIncludeFunc(tc.rules)(tc.path), "rules %q on %q", tc.rules, tc.path)
	}
}

func TestModuleIgnoreFiltersReadGoModAndGoWork(t *testing.T) {
	t.Parallel()
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{
		"go.work":        "go 1.27\n\nuse (\n\t.\n\t./sub\n\t../outside\n)\n",
		"go.mod":         "module example.com/root\n\ngo 1.27\n\nignore (\n\t./docs/\n\tnode_modules\n)\n",
		"sub/go.mod":     "module example.com/sub\n\ngo 1.27\n\nignore (\n\t./gen\n\tmocks\n)\n",
		"broken/go.mod":  "not a go.mod",
		"../outside/x.g": "",
	})
	assert.Equal(t, []string{"-docs", "-**/node_modules", "-sub/gen", "-sub/mocks", "-sub/**/mocks"}, moduleIgnoreFilters(worktree))
	assert.Nil(t, moduleIgnoreFilters(t.TempDir()), "no go.mod, no filters")
}

func TestPlanWatchWithoutExclusionsIsTheWorktree(t *testing.T) {
	t.Parallel()
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{"go.mod": "module m\n", "docs/": "", "pkg/a.go": "package pkg\n"})
	assert.Equal(t, watchPlan{roots: []string{worktree}}, planWatch(worktree, nil), "no rules")
	assert.Equal(t, watchPlan{roots: []string{worktree}}, planWatch(worktree, []string{"-nothing-here"}), "rules that exclude nothing")
}

func TestPlanWatchCoversTheWorktreeMinusExcludedTrees(t *testing.T) {
	t.Parallel()
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{
		"go.mod":                    "module m\n\ngo 1.27\n\nignore ./docs\nignore node_modules\n",
		"main.go":                   "package main\n",
		"README.md":                 "",
		"cmd/tool/main.go":          "package main\n",
		"internal/a/a.go":           "package a\n",
		"docs/guide/index.md":       "",
		"scripts/run.py":            "",
		"web/src/app.go":            "package app\n",
		"web/node_modules/pkg/x.js": "",
		".git/objects/":             "",
		"_tools/":                   "",
		"testdata/":                 "",
	})
	plan := planWatch(worktree, []string{"-scripts"})
	assert.Equal(t, []string{"cmd", "internal", "web/src"}, relRoots(t, worktree, plan))
	assert.Equal(t, map[string]partialEntries{
		worktree:                       {names: []string{"cmd/", "go.mod", "internal/", "main.go", "web/"}, files: true},
		filepath.Join(worktree, "web"): {names: []string{"src/"}, files: true},
	}, plan.partial)
}

// TestGitDirectoryIsNeverWatched pins that a worktree's .git directory is no
// root and no relevant entry, even when a + rule names it: gopls' watcher skips
// it, and its churn on every commit must not condemn a server either.
func TestGitDirectoryIsNeverWatched(t *testing.T) {
	t.Parallel()
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{".git/objects/": "", "docs/": "", "pkg/a.go": "package pkg\n"})
	plan := planWatch(worktree, []string{"-docs", "+.git"})
	assert.Equal(t, []string{"pkg"}, relRoots(t, worktree, plan))
	assert.Equal(t, []string{"pkg/"}, plan.partial[worktree].names)
}

func TestPlanWatchReincludesBelowAnExcludedTree(t *testing.T) {
	t.Parallel()
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{"docs/notes.md": "", "docs/go/x.go": "package x\n", "docs/deep/more/y.go": "package y\n", "pkg/": ""})
	plan := planWatch(worktree, []string{"-docs", "+docs/go", "+docs/deep/more"})
	assert.Equal(t, []string{"docs/deep/more", "docs/go", "pkg"}, relRoots(t, worktree, plan))
	assert.Equal(t, partialEntries{names: []string{"deep/", "go/"}}, plan.partial[filepath.Join(worktree, "docs")],
		"an excluded directory records its subdirectories only")
	assert.Equal(t, partialEntries{names: []string{"more/"}}, plan.partial[filepath.Join(worktree, "docs", "deep")])
}

// TestPlanWatchDescendsOnlyWhereAPlusRuleCanReach pins that an excluded tree is
// walked, and its subdirectories recorded, only when a + rule could include
// something in it: a big node_modules costs nothing next to a +docs/go rule.
func TestPlanWatchDescendsOnlyWhereAPlusRuleCanReach(t *testing.T) {
	t.Parallel()
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{
		"web/node_modules/a/b/c/": "", "web/node_modules/d/": "", "docs/go/x.go": "package x\n", "docs/other/deep/": "", "pkg/": "",
	})
	keys := func(rules ...string) []string {
		var rels []string
		for dir := range planWatch(worktree, rules).partial {
			rel, err := filepath.Rel(worktree, dir)
			require.NoError(t, err)
			rels = append(rels, filepath.ToSlash(rel))
		}
		slices.Sort(rels)
		return rels
	}
	assert.Equal(t, []string{".", "docs", "web"}, keys("-**/node_modules", "-docs", "+docs/go"),
		"nothing under node_modules, nor docs/other, is walked")
	assert.Equal(t, []string{".", "docs", "docs/go", "docs/other", "docs/other/deep", "web", "web/node_modules", "web/node_modules/a", "web/node_modules/a/b", "web/node_modules/a/b/c", "web/node_modules/d"},
		keys("-**/node_modules", "-docs", "+**/x"), "a ** segment may match anywhere: the full walk")
	assert.Equal(t, []string{".", "docs"}, keys("-", "+docs/go/"),
		"a bare - with later + rules walks only toward them")
}

// TestWatchStaleJudgesExcludedSubdirectoriesByPlusRules pins that a new
// directory under an excluded tree condemns a server only when a + rule can
// reach it.
func TestWatchStaleJudgesExcludedSubdirectoriesByPlusRules(t *testing.T) {
	t.Parallel()
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{"go.mod": "module m\n", "docs/notes.md": "", "pkg/": ""})
	m := &manager{watch: &watchPlans{plans: make(map[string]watchSnapshot)}, limits: config.Limits{DirectoryFilters: []string{"-docs", "+docs/go"}}}
	past := time.Now().Add(-2 * time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(worktree, "go.mod"), past, past))
	r := record{Worktree: worktree, StartedAt: time.Now().Add(-time.Minute).Unix()}
	plan := planWatch(worktree, m.limits.DirectoryFilters)
	assert.Equal(t, []string{"pkg"}, relRoots(t, worktree, plan))
	m.rememberWatch(worktree, serverID{}, plan)

	require.NoError(t, os.Mkdir(filepath.Join(worktree, "docs", "x"), 0o700))
	require.False(t, watchStale(m, r), "docs/x is unreachable for +docs/go")
	require.NoError(t, os.Mkdir(filepath.Join(worktree, "docs", "go"), 0o700))
	require.True(t, watchStale(m, r), "docs/go is re-included")
}

func TestWatchStaleNoticesWhatNoRootCovers(t *testing.T) {
	t.Parallel()
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{"go.mod": "module m\n", "main.go": "package main\n", "docs/": "", "pkg/": ""})
	m := &manager{watch: &watchPlans{plans: make(map[string]watchSnapshot)}, limits: config.Limits{DirectoryFilters: []string{"-docs"}}}
	past := time.Now().Add(-2 * time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(worktree, "go.mod"), past, past))
	r := record{Worktree: worktree, StartedAt: time.Now().Add(-time.Minute).Unix()}
	remember := func() { m.rememberWatch(worktree, serverID{}, planWatch(worktree, m.limits.DirectoryFilters)) }

	remember()
	require.False(t, watchStale(m, r), "nothing changed")
	mustWriteFile(t, filepath.Join(worktree, "README.md"), "")
	mustWriteFile(t, filepath.Join(worktree, "main.go"), "package main // edited\n")
	require.NoError(t, os.Mkdir(filepath.Join(worktree, "docs", "more"), 0o700))
	require.False(t, watchStale(m, r), "a non-Go file, an edit gopls checks itself and an excluded directory are not news")

	mustWriteFile(t, filepath.Join(worktree, "new.go"), "package main\n")
	require.True(t, watchStale(m, r), "a Go file no root covers was added")
	require.False(t, watchStale(m, r), "the stale verdict drops the snapshot: one change condemns one server")

	remember()
	require.NoError(t, os.Mkdir(filepath.Join(worktree, "cmd"), 0o700))
	require.True(t, watchStale(m, r), "a new top-level directory")

	remember()
	future := time.Now().Add(time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(worktree, "go.mod"), future, future))
	require.True(t, watchStale(m, r), "go.mod changed after the server started")

	remember()
	require.False(t, watchStale(m, record{Worktree: worktree, StartedAt: time.Now().Add(time.Hour).Unix()}),
		"a snapshot older than the server describes a predecessor")
	require.False(t, watchStale(m, record{Worktree: worktree}), "legacy records skip the mtime check")
	require.False(t, watchStale(&manager{}, r), "a manager without plans condemns nothing")
}

func TestRememberWatchForgetsAPlanWithNothingUncovered(t *testing.T) {
	t.Parallel()
	m := &manager{watch: &watchPlans{plans: map[string]watchSnapshot{"/w": {}}}}
	m.rememberWatch("/w", serverID{}, watchPlan{roots: []string{"/w"}})
	assert.Empty(t, m.watch.plans)
	var none *manager
	none.rememberWatch("/w", serverID{}, watchPlan{}) // nil-safe
}

func TestMaintenanceRestartsAServerThatMissedAnUncoveredChange(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	m.limits.DirectoryFilters = []string{"-docs"}
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{"go.mod": "module m\n", "docs/": "", "pkg/": ""})
	past := time.Now().Add(-2 * time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(worktree, "go.mod"), past, past))
	process := startFakeGopls(t, firstPort)
	exited := make(chan struct{})
	go func() { _ = process.Wait(); close(exited) }()
	stale := record{Worktree: worktree, PID: process.Process.Pid, Port: firstPort, StartedAt: time.Now().Add(-time.Minute).Unix()}
	mustWriteMap(t, m.mapPath, []record{stale})
	m.alive = func(context.Context, record) probeVerdict { return probeLive }
	m.rememberWatch(worktree, serverID{}, planWatch(worktree, m.limits.DirectoryFilters))

	_, err := m.sweepMaintenance(t.Context())
	require.NoError(t, err)
	wantRunning(t, process, "an unchanged worktree was restarted")

	require.NoError(t, os.Mkdir(filepath.Join(worktree, "cmd"), 0o700))
	m.pending = newPendingCalls()
	call := operationKey{id: mustID(t, "busy")}
	m.beginPending(call, worktree)
	_, err = m.sweepMaintenance(t.Context())
	require.NoError(t, err)
	wantRunning(t, process, "a server with a fresh call was restarted")

	m.endPending(call)
	_, err = m.sweepMaintenance(t.Context())
	require.NoError(t, err)
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the server that missed a new directory was not terminated once its worktree went idle")
	}
	stale.Terminating = true
	wantRecords(t, m.mapPath, "the restart did not persist termination", stale)
}

func TestUpstreamRootsCoverTheWorktreeMinusFilters(t *testing.T) {
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{"docs/": "", "pkg/": "", "cmd/": ""})
	bubble(t, func(t *testing.T) {
		ctx := t.Context()
		upstream, gopls := upstreamPair(t)
		r := newTestRouter(t, testHome)
		r.limits.DirectoryFilters = []string{"-docs"}
		go r.readFromUpstream(upstream, worktree)

		id := mustID(t, "roots-filtered")
		require.NoError(t, gopls.Write(ctx, &jsonrpc.Request{ID: id, Method: "roots/list"}))
		msg, err := gopls.Read(ctx)
		require.NoError(t, err)
		var got mcp.ListRootsResult //nolint:staticcheck // gopls still asks for roots.
		require.NoError(t, json.Unmarshal(wantResponse(t, msg, id).Result, &got))
		var uris []string
		for _, root := range got.Roots {
			uris = append(uris, root.URI)
		}
		assert.Equal(t, []string{"file://" + filepath.Join(worktree, "cmd"), "file://" + filepath.Join(worktree, "pkg")}, uris)
	})
}

// watchStale is watchChanged plus what the sweep does with a condemned server:
// the verdict drops the snapshot, so one change condemns one server.
func watchStale(m *manager, r record) bool {
	snapshot, stale := m.watchChanged(r)
	if stale {
		m.forgetWatch(r.Worktree, snapshot)
	}
	return stale
}

// partialWorktree is a worktree whose docs directory is excluded, so that the
// worktree itself is a partial directory.
func partialWorktree(t *testing.T) (*manager, string) {
	t.Helper()
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{"go.mod": "module m\n", "main.go": "package main\n", "docs/": "", "pkg/": ""})
	past := time.Now().Add(-2 * time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(worktree, "go.mod"), past, past))
	m := &manager{watch: &watchPlans{plans: make(map[string]watchSnapshot)}, limits: config.Limits{DirectoryFilters: []string{"-docs"}}}
	return m, worktree
}

func serverRecord(worktree string, pid int, started time.Time) record {
	return record{Worktree: worktree, PID: pid, StartedAt: started.Unix(), StartedAtMilli: started.UnixMilli()}
}

// A later roots answer comes from another session reaching the same gopls, whose
// watcher still holds only what the first answer told it. The tree that answer
// describes already holds whatever was added since, so it must not replace the
// snapshot the server's watcher is judged by.
func TestRememberWatchKeepsTheFirstSnapshotOfAServer(t *testing.T) {
	t.Parallel()
	m, worktree := partialWorktree(t)
	started := time.Now().Add(-time.Minute)
	first := serverRecord(worktree, 100, started)
	remember := func(r record) { m.rememberWatch(worktree, r.server(), planWatch(worktree, m.limits.DirectoryFilters)) }

	remember(first)
	mustWriteFile(t, filepath.Join(worktree, "new.go"), "package main\n")
	remember(first) // the second session's answer already sees new.go
	rewritten := first
	rewritten.StartedAtMilli = 0 // an older manager rewrote the map
	require.True(t, watchStale(m, rewritten), "the change between the two answers is still the server's to be condemned for, whoever rewrote its record")

	// A restart reports for itself: its snapshot replaces, and the predecessor's
	// verdict cannot be read off it.
	remember(first)
	second := serverRecord(worktree, 101, started.Add(time.Second))
	remember(second)
	mustWriteFile(t, filepath.Join(worktree, "newer.go"), "package main\n")
	require.False(t, watchStale(m, first), "a snapshot from another server describes no watcher of this one")
	require.True(t, watchStale(m, second), "the restarted server's own snapshot")
}

func TestWatchChangedIgnoresASnapshotFromAnotherServer(t *testing.T) {
	t.Parallel()
	m, worktree := partialWorktree(t)
	started := time.Now().Add(-time.Minute)
	mine := serverRecord(worktree, 100, started)
	m.rememberWatch(worktree, serverRecord(worktree, 100, started.Add(time.Second)).server(), planWatch(worktree, m.limits.DirectoryFilters))
	mustWriteFile(t, filepath.Join(worktree, "new.go"), "package main\n")
	require.False(t, watchStale(m, mine), "same pid, another start: a different server")

	// Unknown on either side falls back to comparing the clock.
	m.rememberWatch(worktree, serverID{}, planWatch(worktree, m.limits.DirectoryFilters))
	mustWriteFile(t, filepath.Join(worktree, "newer.go"), "package main\n")
	require.True(t, watchStale(m, record{Worktree: worktree, PID: 100, StartedAt: started.Unix()}), "an unidentified snapshot taken since the start")
}

// A server told to watch the whole worktree never unwatches it, so a later
// answer with partial directories cannot make it miss anything: it must store
// nothing that could condemn it.
func TestRememberWatchPinsAServerThatHadNothingUncovered(t *testing.T) {
	t.Parallel()
	m, worktree := partialWorktree(t)
	started := time.Now().Add(-time.Minute)
	r := serverRecord(worktree, 100, started)

	m.rememberWatch(worktree, r.server(), watchPlan{roots: []string{worktree}})
	assert.Contains(t, m.watch.plans, worktree, "the fact that nothing is uncovered is what later answers keep")
	m.rememberWatch(worktree, r.server(), planWatch(worktree, m.limits.DirectoryFilters))
	mustWriteFile(t, filepath.Join(worktree, "new.go"), "package main\n")
	require.False(t, watchStale(m, r))
	assert.Empty(t, m.watch.plans[worktree].partial)

	// An unidentified server still stores nothing for a plan with no partials.
	m.rememberWatch(worktree, serverID{}, watchPlan{roots: []string{worktree}})
	assert.NotContains(t, m.watch.plans, worktree)
}

func TestServerOfReadsTheIdentityThroughBoundedConn(t *testing.T) {
	t.Parallel()
	id := serverID{pid: 7, started: 1_700_000_000}
	conn := &boundedConn{Connection: &identifiedConn{Connection: loopbackConn(), id: id}, release: func() {}}
	assert.Equal(t, id, serverOf(conn))
	assert.False(t, serverOf(loopbackConn()).known(), "a stub dial carries no identity")
	assert.False(t, (&boundedConn{Connection: loopbackConn(), release: func() {}}).server().known())
}

func TestStartedPrecisely(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		r    record
		want time.Time
		ok   bool
	}{
		{name: "no start", r: record{}},
		{name: "milliseconds", r: record{StartedAt: 1000, StartedAtMilli: 1_000_900}, want: time.UnixMilli(1_000_900), ok: true},
		{name: "seconds only", r: record{StartedAt: 1000}, want: time.Unix(1000, 0), ok: true},
		{name: "inconsistent milliseconds", r: record{StartedAt: 1000, StartedAtMilli: 5_000_900}, want: time.Unix(1000, 0), ok: true},
		{name: "milliseconds without seconds", r: record{StartedAtMilli: 1_000_900}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := tc.r.startedPrecisely()
			assert.Equal(t, tc.ok, ok)
			assert.True(t, tc.want.Equal(got), "got %v, want %v", got, tc.want)
		})
	}
}

// go.mod tidied at 12:00:00.6 and a server started at 12:00:00.9 is not a
// server that missed the edit, although whole seconds would say so.
func TestModuleFileMtimeIsComparedWithThePreciseStart(t *testing.T) {
	t.Parallel()
	whole := time.Now().Add(-time.Minute).Truncate(time.Second)
	started := whole.Add(900 * time.Millisecond)
	for _, tc := range []struct {
		name  string
		mtime time.Time
		r     record
		want  bool
	}{
		{name: "300ms before", mtime: started.Add(-300 * time.Millisecond), r: serverRecord("", 1, started), want: false},
		{name: "300ms after", mtime: started.Add(300 * time.Millisecond), r: serverRecord("", 1, started), want: true},
		{name: "no milliseconds falls back to seconds", mtime: started.Add(-300 * time.Millisecond), r: record{PID: 1, StartedAt: started.Unix()}, want: true},
		{name: "inconsistent milliseconds falls back to seconds", mtime: started.Add(-300 * time.Millisecond), r: record{PID: 1, StartedAt: started.Unix(), StartedAtMilli: started.UnixMilli() + 5000}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, worktree := partialWorktree(t)
			require.NoError(t, os.Chtimes(filepath.Join(worktree, "go.mod"), tc.mtime, tc.mtime))
			m.rememberWatch(worktree, serverID{}, planWatch(worktree, m.limits.DirectoryFilters))
			tc.r.Worktree = worktree
			_, stale := m.watchChanged(tc.r)
			assert.Equal(t, tc.want, stale)
		})
	}
}

func TestMapKeepsTheMillisecondStart(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	want := record{Worktree: "/repo/a", Port: 62001, PID: 11, StartedAt: 1000, StartedAtMilli: 1_000_900}
	mustWriteMap(t, m.mapPath, []record{want})
	got, intact, err := readMap(m.mapPath)
	require.NoError(t, err)
	require.True(t, intact)
	assert.Equal(t, []record{want}, got)

	// A line an older manager wrote has no such member and still parses.
	appendToMap(t, m.mapPath, `{"Worktree":"/repo/old","Port":62002,"PID":12,"StartedAt":2000}`+"\n")
	got, intact, err = readMap(m.mapPath)
	require.NoError(t, err)
	require.True(t, intact)
	require.Len(t, got, 2)
	assert.Zero(t, got[1].StartedAtMilli)
	assert.EqualValues(t, 2000, got[1].StartedAt)
}
