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
	m.rememberWatch(worktree, plan)

	require.NoError(t, os.Mkdir(filepath.Join(worktree, "docs", "x"), 0o700))
	require.False(t, m.watchStale(r), "docs/x is unreachable for +docs/go")
	require.NoError(t, os.Mkdir(filepath.Join(worktree, "docs", "go"), 0o700))
	require.True(t, m.watchStale(r), "docs/go is re-included")
}

func TestWatchStaleNoticesWhatNoRootCovers(t *testing.T) {
	t.Parallel()
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{"go.mod": "module m\n", "main.go": "package main\n", "docs/": "", "pkg/": ""})
	m := &manager{watch: &watchPlans{plans: make(map[string]watchSnapshot)}, limits: config.Limits{DirectoryFilters: []string{"-docs"}}}
	past := time.Now().Add(-2 * time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(worktree, "go.mod"), past, past))
	r := record{Worktree: worktree, StartedAt: time.Now().Add(-time.Minute).Unix()}
	remember := func() { m.rememberWatch(worktree, planWatch(worktree, m.limits.DirectoryFilters)) }

	remember()
	require.False(t, m.watchStale(r), "nothing changed")
	mustWriteFile(t, filepath.Join(worktree, "README.md"), "")
	mustWriteFile(t, filepath.Join(worktree, "main.go"), "package main // edited\n")
	require.NoError(t, os.Mkdir(filepath.Join(worktree, "docs", "more"), 0o700))
	require.False(t, m.watchStale(r), "a non-Go file, an edit gopls checks itself and an excluded directory are not news")

	mustWriteFile(t, filepath.Join(worktree, "new.go"), "package main\n")
	require.True(t, m.watchStale(r), "a Go file no root covers was added")
	require.False(t, m.watchStale(r), "the stale verdict drops the snapshot: one change condemns one server")

	remember()
	require.NoError(t, os.Mkdir(filepath.Join(worktree, "cmd"), 0o700))
	require.True(t, m.watchStale(r), "a new top-level directory")

	remember()
	future := time.Now().Add(time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(worktree, "go.mod"), future, future))
	require.True(t, m.watchStale(r), "go.mod changed after the server started")

	remember()
	require.False(t, m.watchStale(record{Worktree: worktree, StartedAt: time.Now().Add(time.Hour).Unix()}),
		"a snapshot older than the server describes a predecessor")
	require.False(t, m.watchStale(record{Worktree: worktree}), "legacy records skip the mtime check")
	require.False(t, (&manager{}).watchStale(r), "a manager without plans condemns nothing")
}

func TestRememberWatchForgetsAPlanWithNothingUncovered(t *testing.T) {
	t.Parallel()
	m := &manager{watch: &watchPlans{plans: map[string]watchSnapshot{"/w": {}}}}
	m.rememberWatch("/w", watchPlan{roots: []string{"/w"}})
	assert.Empty(t, m.watch.plans)
	var none *manager
	none.rememberWatch("/w", watchPlan{}) // nil-safe
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
	m.rememberWatch(worktree, planWatch(worktree, m.limits.DirectoryFilters))

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
