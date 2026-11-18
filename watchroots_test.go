package main

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"
	"github.com/ajvengo/gopls-mcp-manager/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpiredRootsPlanCannotRememberSnapshot(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		ctx, cancel := context.WithCancel(t.Context())
		r.plan = func(string, []string) watchPlan {
			cancel()
			return watchPlan{partial: map[string]partialEntries{testHome: {}}}
		}
		conn := &identifiedConn{Connection: newFakeConn(), id: serverID{pid: 1, started: 1}}
		err := r.answerRoots(ctx, testConnection(r, conn, testHome), testHome, mustID(t, "roots"))
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, r.owner.m.watch.plans, "expired planning published an obsolete server snapshot")
	})
}

func TestLateRootsPlanCannotReplaceNewerGeneration(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		m := r.owner.m
		entered, release := make(chan struct{}), make(chan struct{})
		r.plan = func(string, []string) watchPlan {
			close(entered)
			<-release
			return watchPlan{roots: []string{testHome}}
		}
		old, current := serverID{pid: 1, started: 1}, serverID{pid: 2, started: 2}
		conn := &identifiedConn{Connection: newFakeConn(), id: old}
		done := make(chan error, 1)
		id := mustID(t, "roots")
		go func() { done <- r.answerRoots(t.Context(), testConnection(r, conn, testHome), testHome, id) }()
		<-entered
		time.Sleep(time.Nanosecond)
		m.rememberWatch(testHome, current, watchPlan{started: time.Now(), roots: []string{testHome}})
		close(release)
		require.NoError(t, <-done)
		require.Equal(t, current, m.watch.plans[testHome].server)
	})
}

// Simulate messages already decoded when Close cancels the connection, followed
// by EOF. Reader cleanup will park forwarding a failure to the full output.
type bufferedRootsConn struct {
	mcp.Connection
	messages []jsonrpc.Message
}

func TestClosedConnectionCannotStartObsoleteRootsPlan(t *testing.T) {
	bubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r := newRouter(ctx, routerTestOwner(t, nil), testHome)
		var planned atomic.Int64
		r.plan = func(string, []string) watchPlan { planned.Add(1); return watchPlan{} }
		for range cap(r.out) {
			r.out <- &jsonrpc.Request{Method: "notification"}
		}
		connCtx, closeConn := context.WithCancel(ctx)
		conn := testUpstream(r, r.home, &bufferedRootsConn{
			Connection: newFakeConn(),
			messages:   []jsonrpc.Message{&jsonrpc.Request{ID: mustID(t, "roots"), Method: "roots/list"}},
		}, connCtx, closeConn)
		r.track(mustID(t, "unanswered"), conn, testHome)
		require.NoError(t, conn.Close())
		done := make(chan struct{})
		go func() { defer close(done); r.readFromUpstream(testConnection(r, conn, testHome), testHome) }()
		synctest.Wait()
		require.Zero(t, planned.Load(), "queued asks must observe Close before reader cleanup finishes")
		cancel()
		<-done
	})
}

func FuzzDirectoryFilterPrecedence(f *testing.F) {
	f.Add("-foo,+foo/**/bar", "foo/bar")
	f.Add("-foo,+foo/**/bar", "foo/one/two/bar/child")
	f.Add("-foo,+foo/**/bar", "foo/barrier")
	f.Add("-,+cmd,-cmd/generated,+cmd/generated/public", "cmd/generated/public/nested")
	f.Add("-**/vendor,+internal/**/vendor", "internal/a/vendor")
	f.Add("-**, +cmd", "cmd/internal")
	f.Add("-docs,+docs/go", "docs/go")
	f.Add("-**/node_modules", "a/node_modules")
	f.Fuzz(func(t *testing.T, raw, rel string) {
		if len(raw) > 4096 || len(rel) > 1024 {
			t.Skip()
		}
		rules, err := config.ParseDirectoryFilters(raw)
		if err != nil {
			return
		}
		filter, err := newDirFilter(rules)
		if err != nil {
			require.ErrorIs(t, err, errWatchLimit)
			return
		}
		_ = filter.worthListing(rel)
		require.Equal(t, directoryFilterReference(rules, rel), filter.include(rel), "segment matcher and regexp disagree")
		for _, last := range []string{"+", "-"} {
			include, err := pathIncludeFunc(append(rules, last))
			if err != nil {
				require.ErrorIs(t, err, errWatchLimit)
				continue
			}
			require.Equal(t, last == "+", include(rel), "last global rule must win")
		}
	})
}

func TestDirectoryFilterCompilationRejectsUnusableRules(t *testing.T) {
	t.Parallel()
	for _, rule := range []string{"", "+\x80", "+" + strings.Repeat("**/", 2000)} {
		require.NotPanics(t, func() {
			filter, err := newDirFilter([]string{"-blocked", rule})
			require.NoError(t, err)
			require.False(t, filter.include("blocked"))
		})
	}
	_, err := config.ParseDirectoryFilters("+" + strings.Repeat("a", config.MaxDirectoryFilterBytes))
	require.Error(t, err)
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
		go r.readFromUpstream(testConnection(r, upstream, worktree), worktree)

		err := gopls.Write(ctx, &jsonrpc.Request{ID: id, Method: "roots/list"})
		require.NoErrorf(t, err, "write roots/list: %v", err)
		msg, err := gopls.Read(ctx)
		require.NoErrorf(t, err, "read reply: %v", err)
		root := wantOneRoot(t, wantResponse(t, msg, id))
		wantURI := "file:///tmp/a%20worktree/wt"
		assert.Equalf(t, wantURI, root.URI, "root uri = %q, want %q", root.URI, wantURI)
		wantName := "wt"
		assert.Equalf(t, wantName, root.Name, "root name = %q, want %q", root.Name, wantName)

		wantClientQuiet(t, r, "roots/list was forwarded to the client")
	})
}

func TestSlowRootsPlanDoesNotBlockResponses(t *testing.T) {
	bubble(t, func(t *testing.T) {
		ctx := t.Context()
		upstream, gopls := upstreamPair(t)
		r := newTestRouter(t, testHome)
		entered, release := make(chan struct{}), make(chan struct{})
		r.plan = func(worktree string, _ []string) watchPlan {
			close(entered)
			<-release
			return watchPlan{roots: []string{worktree}}
		}
		call := mustID(t, "call")
		r.track(call, upstream, testWorktree)
		go r.readFromUpstream(testConnection(r, upstream, testWorktree), testWorktree)

		rootsID := mustID(t, "roots")
		require.NoError(t, gopls.Write(ctx, &jsonrpc.Request{ID: rootsID, Method: "roots/list"}))
		mustRecv(t, entered, "roots plan")
		require.NoError(t, gopls.Write(ctx, &jsonrpc.Response{ID: call, Result: jsontext.Value(`{}`)}))
		resp, ok := mustRecv(t, r.out, "response behind a pending roots plan").(*jsonrpc.Response)
		require.True(t, ok)
		assert.Equal(t, call, resp.ID)

		close(release)
		msg, err := gopls.Read(ctx)
		require.NoError(t, err)
		_ = wantResponse(t, msg, rootsID)
	})
}

func TestDirectoryFilterDoubleStarMatchesZeroOrMoreSegments(t *testing.T) {
	t.Parallel()
	rules := []string{"-foo", "+foo/**/bar"}
	include, err := pathIncludeFunc(rules)
	require.NoError(t, err)
	for _, tc := range []struct {
		rel  string
		want bool
	}{
		{rel: "foo/bar", want: true},
		{rel: "foo/one/bar", want: true},
		{rel: "foo/one/two/bar", want: true},
		{rel: "foo/bar/child", want: true},
		{rel: "foo/one/bar/child", want: true},
		{rel: "foo/barrier"},
		{rel: "foo/one/barrier"},
		{rel: "foo/one"},
	} {
		require.Equalf(t, tc.want, include(tc.rel), "rules %q on %q", rules, tc.rel)
		require.Equal(t, tc.want, directoryFilterReference(rules, tc.rel))
	}
	leading, err := pathIncludeFunc([]string{"-**/bar"})
	require.NoError(t, err)
	require.False(t, leading("bar"))
	require.False(t, leading("one/two/bar/child"))
	require.True(t, leading("one/two/barrier"))
}

func TestPlanWatchReincludesDirectAndNestedDoubleStarChildren(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustTree(t, root, map[string]string{
		"foo/bar/pkg/": "", "foo/one/bar/": "", "foo/one/two/bar/": "", "foo/barrier/": "",
	})
	plan := planWatch(root, []string{"-foo", "+foo/**/bar"})
	require.NoError(t, plan.err)
	require.Equal(t, []string{"foo/bar", "foo/one/bar", "foo/one/two/bar"}, relRoots(t, root, plan))
}

func TestRootsDirectoryReplacementCannotBlock(t *testing.T) {
	const marker = "gopls-manager-roots-replacement-fixture"
	if index := slices.Index(os.Args, marker); index >= 0 {
		require.Less(t, index+1, len(os.Args))
		root := os.Args[index+1]
		child := filepath.Join(root, "child")
		budget := &watchBudget{remaining: defaultWatchLimits()}
		w := planner{budget: budget, plan: watchPlan{partial: make(map[string]partialEntries)},
			filter: dirFilter{
				include: func(rel string) bool {
					if rel == "child" {
						require.NoError(t, os.Remove(child))
						require.NoError(t, syscall.Mkfifo(child, 0o600))
					}
					return true
				},
				mayInclude: func(string) bool { return false },
			}}
		_, err := w.walk(root, "", 0)
		require.Error(t, err, "a replaced directory must fail the complete plan")
		return
	}
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "child"), 0o700))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRootsDirectoryReplacementCannotBlock$", "--", marker, root)
	err := child.Run() // fixture diagnostics go to /dev/null, never unbounded capture.
	require.NoError(t, ctx.Err(), "a directory replaced by FIFO wedged the roots worker")
	require.NoError(t, err)
}

func rootsReplyFixture(t *testing.T) (*supervisor, *ownedGeneration, *router) {
	t.Helper()
	s := stateSupervisor(t)
	s.m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	g := readyTestGeneration(t, s, testHome)
	g.record.StartedAt = 1
	g.fdQuota = childFDHeadroom
	return s, g, newRouter(t.Context(), s, testHome)
}

func TestRootsFirstViewSurvivesAcknowledgementOrder(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"not delivered", transport.ErrMessageTooLarge},
		{"uncertain", errors.New("reply applied; acknowledgement lost")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s, g, r := rootsReplyFixture(t)
				old := map[string]partialEntries{testHome: {names: []string{"old.go"}, files: true}}
				next := map[string]partialEntries{testHome: {names: []string{"old.go", "new.go"}, files: true}}
				plans := 0
				r.plan = func(string, []string) watchPlan {
					plans++
					if plans == 1 {
						return watchPlan{partial: old}
					}
					return watchPlan{partial: next}
				}
				entered, release := make(chan struct{}), make(chan struct{})
				releaseReply := sync.OnceFunc(func() { close(release) })
				defer releaseReply()
				first := newFakeConn()
				first.onWrite = func(context.Context, jsonrpc.Message) error { close(entered); <-release; return tc.err }
				one, err := s.connection(first, g.record, context.Background(), func() {})
				require.NoError(t, err)
				two := readyTestConnection(t, s, g)
				idOne, idTwo := mustID(t, "one"), mustID(t, "two")
				doneOne, doneTwo := make(chan error, 1), make(chan error, 1)
				go func() { doneOne <- r.answerRoots(t.Context(), testConnection(r, one, testHome), testHome, idOne) }()
				<-entered
				time.Sleep(time.Nanosecond)
				go func() { doneTwo <- r.answerRoots(t.Context(), testConnection(r, two, testHome), testHome, idTwo) }()
				synctest.Wait()
				releaseReply()
				if tc.err == nil {
					require.NoError(t, <-doneOne)
				} else {
					require.ErrorIs(t, <-doneOne, tc.err)
				}
				require.NoError(t, <-doneTwo)
				want := old
				if errors.Is(tc.err, transport.ErrMessageTooLarge) {
					want = next
				}
				snapshot := s.m.watch.plans[testHome]
				require.Equal(t, want, snapshot.partial)
				require.Equal(t, tc.err != nil && !errors.Is(tc.err, transport.ErrMessageTooLarge), snapshot.uncertain)
			})
		})
	}
}

func TestWaitingRootsReplyCanCancelWithoutReleasingAnotherReply(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, g, r := rootsReplyFixture(t)
		r.plan = func(string, []string) watchPlan { return watchPlan{} }
		entered, release := make(chan struct{}), make(chan struct{})
		releaseReply := sync.OnceFunc(func() { close(release) })
		defer releaseReply()
		first := newFakeConn()
		first.onWrite = func(context.Context, jsonrpc.Message) error { close(entered); <-release; return nil }
		one, err := s.connection(first, g.record, context.Background(), func() {})
		require.NoError(t, err)
		two := readyTestConnection(t, s, g)
		idOne, idTwo := mustID(t, "one"), mustID(t, "two")
		doneOne, doneTwo := make(chan error, 1), make(chan error, 1)
		go func() { doneOne <- r.answerRoots(t.Context(), testConnection(r, one, testHome), testHome, idOne) }()
		<-entered
		ctx, cancel := context.WithCancel(t.Context())
		go func() { doneTwo <- r.answerRoots(ctx, testConnection(r, two, testHome), testHome, idTwo) }()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-doneTwo, context.Canceled)
		require.Len(t, g.rootReplies, 1)
		releaseReply()
		require.NoError(t, <-doneOne)
		require.Empty(t, g.rootReplies)
	})
}

func TestFailedRootsPlanIsNeverForwarded(t *testing.T) {
	t.Parallel()
	r := newTestRouter(t, testHome)
	want := errors.New("roots planning resource budget exhausted")
	r.plan = func(string, []string) watchPlan { return watchPlan{err: want} }
	conn := newFakeConn()

	err := r.answerRoots(t.Context(), testConnection(r, conn, testHome), testHome, mustID(t, "bounded-roots"))
	require.ErrorIs(t, err, want)
	require.Empty(t, conn.writes, "an incomplete roots plan reached gopls as a complete answer")
	require.Empty(t, r.owner.m.watch.plans, "failed planning published a freshness snapshot")
}

func TestRootsSnapshotTracksDeliveryCertainty(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"size rejection", transport.ErrMessageTooLarge},
		{"uncertain write", errors.New("lost acknowledgement")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s, g, r := rootsReplyFixture(t)
				old := map[string]partialEntries{testHome: {names: []string{"old.go"}, files: true}}
				next := map[string]partialEntries{testHome: {names: []string{"old.go", "new.go"}, files: true}}
				if tc.err != nil {
					s.m.rememberWatch(testHome, g.record.server(), watchPlan{partial: old})
				}
				before := s.m.watch.plans[testHome]
				r.plan = func(string, []string) watchPlan { return watchPlan{partial: next} }
				base := newFakeConn()
				base.onWrite = func(context.Context, jsonrpc.Message) error { return tc.err }
				conn, err := s.connection(base, g.record, context.Background(), func() {})
				require.NoError(t, err)
				err = r.answerRoots(t.Context(), testConnection(r, conn, testHome), testHome, mustID(t, "roots"))
				if tc.err != nil {
					require.ErrorIs(t, err, tc.err)
					if !errors.Is(tc.err, transport.ErrMessageTooLarge) {
						before.uncertain = true
					}
					require.Equal(t, before, s.m.watch.plans[testHome])
				} else {
					require.NoError(t, err)
					require.Equal(t, next, s.m.watch.plans[testHome].partial)
				}
			})
		})
	}
}

func TestInvalidRootNameCannotPublishWatcherSnapshot(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.plan = func(string, []string) watchPlan { return watchPlan{roots: []string{"/root/\xff"}} }
		conn := newFakeConn()
		err := r.answerRoots(t.Context(), testConnection(r, conn, testHome), testHome, mustID(t, "roots"))
		require.Error(t, err)
		require.Empty(t, conn.writes)
		require.Empty(t, r.owner.m.watch.plans)
	})
}

func TestRootsSnapshotCannotCrossGenerationReplacement(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, g, r := rootsReplyFixture(t)
		s.fdReserved += g.fdQuota + guardianFDAllowance
		r.plan = func(string, []string) watchPlan {
			return watchPlan{partial: map[string]partialEntries{testHome: {names: []string{"old.go"}, files: true}}}
		}
		replacement := map[string]partialEntries{testHome: {names: []string{"replacement.go"}, files: true}}
		base := newFakeConn()
		base.onWrite = func(context.Context, jsonrpc.Message) error {
			s.confirmExited(g)
			next := readyTestGeneration(t, s, testHome)
			next.record.StartedAt = 2
			s.m.rememberWatch(testHome, next.record.server(), watchPlan{partial: replacement})
			return nil
		}
		conn, err := s.connection(base, g.record, context.Background(), func() {})
		require.NoError(t, err)
		err = r.answerRoots(t.Context(), testConnection(r, conn, testHome), testHome, mustID(t, "roots"))
		require.ErrorContains(t, err, "retired generation")
		require.Equal(t, replacement, s.m.watch.plans[testHome].partial)
	})
}

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
	require.NoError(t, plan.err)
	rels := make([]string, len(plan.roots))
	for i, root := range plan.roots {
		rel, err := filepath.Rel(worktree, root)
		require.NoError(t, err)
		rels[i] = filepath.ToSlash(rel)
	}
	return rels
}

// These are documented directory-filter contract examples. The independent
// segment-DP reference also checks generated cases in FuzzDirectoryFilterPrecedence.
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
		include, err := pathIncludeFunc(tc.rules)
		require.NoError(t, err)
		assert.Equalf(t, tc.want, include(tc.path), "rules %q on %q", tc.rules, tc.path)
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
	filters, err := (&watchBudget{remaining: defaultWatchLimits()}).moduleIgnoreFilters(worktree)
	require.NoError(t, err)
	assert.Equal(t, []string{"-docs", "-**/node_modules", "-sub/gen", "-sub/mocks", "-sub/**/mocks"}, filters)
	filters, err = (&watchBudget{remaining: defaultWatchLimits()}).moduleIgnoreFilters(t.TempDir())
	require.NoError(t, err)
	assert.Nil(t, filters, "no go.mod, no filters")
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
	require.False(t, watchStale(t, m, r), "docs/x is unreachable for +docs/go")
	require.NoError(t, os.Mkdir(filepath.Join(worktree, "docs", "go"), 0o700))
	require.True(t, watchStale(t, m, r), "docs/go is re-included")
}

func TestWatchStaleNoticesWhatNoRootCovers(t *testing.T) {
	t.Parallel()
	m, worktree := partialWorktree(t)
	r := record{Worktree: worktree, StartedAt: time.Now().Add(-time.Minute).Unix()}
	remember := func() { m.rememberWatch(worktree, serverID{}, planWatch(worktree, m.limits.DirectoryFilters)) }

	remember()
	require.False(t, watchStale(t, m, r), "nothing changed")
	mustWriteFile(t, filepath.Join(worktree, "README.md"), "")
	mustWriteFile(t, filepath.Join(worktree, "main.go"), "package main // edited\n")
	require.NoError(t, os.Mkdir(filepath.Join(worktree, "docs", "more"), 0o700))
	require.False(t, watchStale(t, m, r), "a non-Go file, an edit gopls checks itself and an excluded directory are not news")

	mustWriteFile(t, filepath.Join(worktree, "new.go"), "package main\n")
	require.True(t, watchStale(t, m, r), "a Go file no root covers was added")
	require.False(t, watchStale(t, m, r), "the stale verdict drops the snapshot: one change condemns one server")

	remember()
	require.NoError(t, os.Mkdir(filepath.Join(worktree, "cmd"), 0o700))
	require.True(t, watchStale(t, m, r), "a new top-level directory")

	remember()
	future := time.Now().Add(time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(worktree, "go.mod"), future, future))
	require.True(t, watchStale(t, m, r), "go.mod changed after the server started")

	remember()
	require.False(t, watchStale(t, m, record{Worktree: worktree, StartedAt: time.Now().Add(time.Hour).Unix()}),
		"a snapshot older than the server describes a predecessor")
	require.False(t, watchStale(t, m, record{Worktree: worktree}), "legacy records skip the mtime check")
	require.False(t, watchStale(t, &manager{}, r), "a manager without plans condemns nothing")
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
	s := testSupervisor(t)
	m := s.m
	m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	m.limits.DirectoryFilters = []string{"-docs"}
	root := t.TempDir()
	mustTree(t, root, map[string]string{"go.mod": "module m\n", "docs/": "", "pkg/": ""})
	past := time.Now().Add(-2 * time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(root, "go.mod"), past, past))
	g := readyTestGeneration(t, s, root)
	g.record.StartedAt = time.Now().Add(-time.Minute).Unix()
	require.NoError(t, writeMap(m.mapPath, []record{g.record}))
	m.rememberWatch(root, g.record.server(), planWatch(root, m.limits.DirectoryFilters))
	done := make(chan struct{})
	var once sync.Once
	g.child = &guardianChild{ConfirmedDone: done, Signal: func(os.Signal) error { once.Do(func() { close(done) }); return nil }}
	_, err := s.maintenanceSweep(t.Context())
	require.NoError(t, err)
	require.Equal(t, generationReady, supervisorGenerationState(s, g), "an unchanged worktree was restarted")
	require.NoError(t, os.Mkdir(filepath.Join(root, "cmd"), 0o700))
	call := &callState{}
	require.NoError(t, s.admit(call))
	require.NoError(t, s.bind(call, root))
	_, err = s.maintenanceSweep(t.Context())
	require.NoError(t, err)
	require.Equal(t, generationReady, supervisorGenerationState(s, g), "a server with queued work was restarted")
	uncertain, cancel := context.WithCancel(t.Context())
	cancel()
	_, _ = s.maintenanceSweep(uncertain)
	require.Equal(t, generationReady, supervisorGenerationState(s, g))
	require.Contains(t, m.watch.plans, root, "uncertain observation used the change up")
	s.releaseQueued(call)
	_, err = s.maintenanceSweep(t.Context())
	require.NoError(t, err)
	select {
	case <-g.drainDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stale worktree did not retire")
	}
	require.Equal(t, generationTerminating, supervisorGenerationState(s, g))
	require.Contains(t, m.watch.plans, root, "TERM alone discarded watcher state")
	stored := g.record
	stored.Terminating = true
	wantRecords(t, m.mapPath, "retirement did not persist termination", stored)
	s.confirmExited(g)
	require.Empty(t, s.snapshot().Generations)
	require.Empty(t, m.watch.plans)
}

func TestUpstreamRootsCoverTheWorktreeMinusFilters(t *testing.T) {
	worktree := t.TempDir()
	mustTree(t, worktree, map[string]string{"docs/": "", "pkg/": "", "cmd/": ""})
	bubble(t, func(t *testing.T) {
		ctx := t.Context()
		upstream, gopls := upstreamPair(t)
		r := newTestRouter(t, testHome)
		r.limits.DirectoryFilters = []string{"-docs"}
		go r.readFromUpstream(testConnection(r, upstream, worktree), worktree)

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
func watchStale(t *testing.T, m *manager, r record) bool {
	t.Helper()
	snapshot, stale, err := m.watchChanged(r)
	require.NoError(t, err)
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
	require.True(t, watchStale(t, m, rewritten), "the change between the two answers is still the server's to be condemned for, whoever rewrote its record")

	// A restart reports for itself: its snapshot replaces, and the predecessor's
	// verdict cannot be read off it.
	remember(first)
	second := serverRecord(worktree, 101, started.Add(time.Second))
	remember(second)
	mustWriteFile(t, filepath.Join(worktree, "newer.go"), "package main\n")
	require.False(t, watchStale(t, m, first), "a snapshot from another server describes no watcher of this one")
	require.True(t, watchStale(t, m, second), "the restarted server's own snapshot")
}

func TestWatchChangedIgnoresASnapshotFromAnotherServer(t *testing.T) {
	t.Parallel()
	m, worktree := partialWorktree(t)
	started := time.Now().Add(-time.Minute)
	mine := serverRecord(worktree, 100, started)
	m.rememberWatch(worktree, serverRecord(worktree, 100, started.Add(time.Second)).server(), planWatch(worktree, m.limits.DirectoryFilters))
	mustWriteFile(t, filepath.Join(worktree, "new.go"), "package main\n")
	require.False(t, watchStale(t, m, mine), "same pid, another start: a different server")

	// Unknown on either side falls back to comparing the clock.
	m.rememberWatch(worktree, serverID{}, planWatch(worktree, m.limits.DirectoryFilters))
	mustWriteFile(t, filepath.Join(worktree, "newer.go"), "package main\n")
	require.True(t, watchStale(t, m, record{Worktree: worktree, PID: 100, StartedAt: started.Unix()}), "an unidentified snapshot taken since the start")
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
	require.False(t, watchStale(t, m, r))
	assert.Empty(t, m.watch.plans[worktree].partial)

	// An unidentified server still stores nothing for a plan with no partials.
	m.rememberWatch(worktree, serverID{}, watchPlan{roots: []string{worktree}})
	assert.NotContains(t, m.watch.plans, worktree)
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
			_, stale, err := m.watchChanged(tc.r)
			require.NoError(t, err)
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

// forgetWatch drops snapshot, unless a newer one has replaced it meanwhile.
func (m *manager) forgetWatch(worktree string, snapshot watchSnapshot) {
	m.watch.mu.Lock()
	defer m.watch.mu.Unlock()
	if current, ok := m.watch.plans[worktree]; ok && current.taken.Equal(snapshot.taken) {
		delete(m.watch.plans, worktree)
	}
}

func TestRootsPlanningHonoursBudget(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		entered, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		r.plan = func(string, []string) watchPlan {
			close(entered)
			<-release
			return watchPlan{}
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		done := make(chan error, 1)
		id := mustID(t, "roots")
		go func() { done <- r.answerRoots(ctx, testConnection(r, newFakeConn(), testHome), testHome, id) }()
		<-entered
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		default:
			require.FailNow(t, "roots planning outlived its caller's budget")
		}
	})
}

func TestRootsWorkersStayBoundedAcrossTimeouts(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		release := make(chan struct{})
		defer close(release)
		var calls atomic.Int64
		r.plan = func(string, []string) watchPlan {
			calls.Add(1)
			<-release
			return watchPlan{}
		}
		id := mustID(t, "roots")
		for range 20 {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			err := r.answerRoots(ctx, testConnection(r, newFakeConn(), testHome), testHome, id)
			cancel()
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
		require.Equal(t, int64(1), calls.Load(), "timeouts must not create replacement filesystem workers")
	})
}

// Actual module reads, directory scans and registry fsync are outside synctest.
// Process completion is an explicit synthetic child proof; no PID is signalled.
func TestSupervisorExpandedRootsRefuseBeforePublicationAndPreserveRetirement(t *testing.T) {
	t.Parallel()
	s := testSupervisor(t)
	s.m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	home := t.TempDir()
	module := filepath.Join(home, "go.mod")
	mustTree(t, home, map[string]string{
		"go.mod":      "module example.com/quota\n\ngo 1.27\n\nignore ./excluded\n",
		"src/main.go": "package src\n", "excluded/": "",
	})
	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(module, old, old))
	for i := range 600 {
		require.NoError(t, os.WriteFile(filepath.Join(home, "excluded", fmt.Sprintf("entry-%d", i)), nil, 0o600))
	}
	g := readyTestGeneration(t, s, home)
	g.record.StartedAt = old.Add(time.Minute).Unix()
	require.NoError(t, writeMap(s.m.mapPath, []record{g.record}))
	baseline := s.snapshot().FDReserved
	require.NoError(t, s.reserveGenerationFDs(t.Context(), g))
	reserved, quota := s.snapshot().FDReserved, g.fdQuota
	r := newRouter(t.Context(), s, home)
	underlying := newFakeConn()
	conn, err := s.connection(underlying, g.record, context.Background(), func() {})
	require.NoError(t, err)
	require.NoError(t, r.answerRoots(t.Context(), testConnection(r, conn, home), home, mustID(t, "original")))
	<-underlying.writes
	s.m.watch.mu.Lock()
	original := s.m.watch.plans[home]
	s.m.watch.mu.Unlock()
	state := &callState{}
	require.NoError(t, s.admit(state))
	require.NoError(t, s.bind(state, home))
	key := operationKey{conn, mustID(t, "active")}
	require.NoError(t, s.begin(state, key, home, nil, 0, 0))
	require.NoError(t, os.WriteFile(module, []byte("module example.com/quota\n\ngo 1.27\n"), 0o600))
	err = r.answerRoots(t.Context(), testConnection(r, conn, home), home, mustID(t, "expanded"))
	require.ErrorContains(t, err, "new generation is required")
	require.Empty(t, underlying.writes, "expanded roots reached the child")
	require.Equal(t, quota, g.fdQuota)
	require.Equal(t, reserved, s.snapshot().FDReserved)
	s.m.watch.mu.Lock()
	require.Equal(t, original, s.m.watch.plans[home], "refusal changed the established watcher snapshot")
	s.m.watch.mu.Unlock()
	require.Contains(t, s.snapshot().Generations[0].Error, "reserved")
	_, err = s.maintenanceSweep(t.Context())
	require.NoError(t, err)
	require.Equal(t, generationReady, supervisorGenerationState(s, g), "roots refusal bypassed active-work protection")
	require.NoError(t, conn.Close())
	_, err = s.maintenanceSweep(t.Context())
	require.NoError(t, err)
	require.Equal(t, generationReady, supervisorGenerationState(s, g), "disconnect skipped abandoned-work grace")
	done := make(chan struct{})
	var signals atomic.Int32
	g.child = &guardianChild{Process: &os.Process{Pid: 123}, ConfirmedDone: done, Signal: func(os.Signal) error { signals.Add(1); close(done); return nil }}
	s.mu.Lock()
	op := s.operations[remoteKeyOf(key)]
	op.abandoned = time.Now().Add(-s.m.limits.Idle)
	s.operations[remoteKeyOf(key)] = op
	s.mu.Unlock()
	_, err = s.maintenanceSweep(t.Context())
	require.NoError(t, err)
	select {
	case <-g.drainDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "controlled retirement did not finish")
	}
	require.Equal(t, int32(1), signals.Load())
	require.Equal(t, reserved, s.snapshot().FDReserved, "TERM alone refunded capacity")
	s.confirmExited(g) // The fixture's child done channel now proves completion.
	require.Empty(t, s.snapshot().Generations)
	require.Zero(t, s.snapshot().Operations)
	require.Equal(t, baseline, s.snapshot().FDReserved)
	require.Empty(t, s.m.watch.plans)
}

func TestSupervisorSameAndSmallerRootsFitImmutableQuota(t *testing.T) {
	t.Parallel()
	s := testSupervisor(t)
	s.m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	home := t.TempDir()
	mustTree(t, home, map[string]string{"go.mod": "module example.com/smaller\n\ngo 1.27\n", "src/main.go": "package src\n", "excluded/a.go": "package excluded\n"})
	g := readyTestGeneration(t, s, home)
	require.NoError(t, s.reserveGenerationFDs(t.Context(), g))
	quota, reserved := g.fdQuota, s.snapshot().FDReserved
	r := newRouter(t.Context(), s, home)
	underlying := newFakeConn()
	conn, err := s.connection(underlying, g.record, context.Background(), func() {})
	require.NoError(t, err)
	for _, contents := range []string{"module example.com/smaller\n\ngo 1.27\n", "module example.com/smaller\n\ngo 1.27\n\nignore ./excluded\n"} {
		require.NoError(t, os.WriteFile(filepath.Join(home, "go.mod"), []byte(contents), 0o600))
		require.NoError(t, r.answerRoots(t.Context(), testConnection(r, conn, home), home, mustID(t, "fits")))
		require.IsType(t, &jsonrpc.Response{}, <-underlying.writes)
		require.Equal(t, quota, g.fdQuota)
		require.Equal(t, reserved, s.snapshot().FDReserved)
	}
}

func TestUncertainRootsStayStaleThroughLaterSuccessfulReplies(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, g, r := rootsReplyFixture(t)
		r.plan = func(string, []string) watchPlan { return watchPlan{} }
		first := newFakeConn()
		first.onWrite = func(context.Context, jsonrpc.Message) error { return errors.New("lost acknowledgement") }
		one, err := s.connection(first, g.record, context.Background(), func() {})
		require.NoError(t, err)
		require.Error(t, r.answerRoots(t.Context(), testConnection(r, one, testHome), testHome, mustID(t, "one")))
		two := readyTestConnection(t, s, g)
		require.NoError(t, r.answerRoots(t.Context(), testConnection(r, two, testHome), testHome, mustID(t, "two")))
		_, stale, err := s.m.watchChanged(g.record)
		require.NoError(t, err)
		require.True(t, stale)
	})
}

func TestWatchPlanResourceBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		tree  map[string]string
		rules []string
		limit func(string, *watchLimits, int)
		exact int
	}{
		{name: "entries", tree: map[string]string{"a/": "", "b/": "", "blocked/": ""}, rules: []string{"-blocked"}, exact: 3,
			limit: func(_ string, l *watchLimits, n int) { l.entries = n }},
		{name: "depth", tree: map[string]string{"a/b/c/": ""}, rules: []string{"-absent"}, exact: 3,
			limit: func(_ string, l *watchLimits, n int) { l.depth = n }},
		{name: "roots", tree: map[string]string{"a/": "", "b/": "", "blocked/": ""}, rules: []string{"-blocked"}, exact: 2,
			limit: func(_ string, l *watchLimits, n int) { l.roots = n }},
		{name: "partials", tree: map[string]string{"a/blocked/": "", "b/blocked/": ""}, rules: []string{"-**/blocked"}, exact: 3,
			limit: func(_ string, l *watchLimits, n int) { l.partials = n }},
		{name: "filter count", rules: []string{"-absent", "+present"}, exact: 2,
			limit: func(_ string, l *watchLimits, n int) { l.filters = n }},
		{name: "filter bytes", rules: []string{"-a", "+b"}, exact: 4,
			limit: func(_ string, l *watchLimits, n int) { l.filterBytes = n }},
		{name: "name bytes", exact: 0,
			limit: func(root string, l *watchLimits, n int) {
				l.nameBytes = 2*len(root) + len(filepath.Join(root, "go.work")) + len(filepath.Join(root, "go.mod")) + n
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			mustTree(t, root, tc.tree)
			limits := defaultWatchLimits()
			tc.limit(root, &limits, tc.exact)
			require.NoError(t, planWatchWithLimits(root, tc.rules, limits).err, "exact boundary")
			tc.limit(root, &limits, tc.exact-1)
			plan := planWatchWithLimits(root, tc.rules, limits)
			require.ErrorIs(t, plan.err, errWatchLimit)
			require.Nil(t, plan.roots, "a rejected plan must not expose partial roots")
			require.Nil(t, plan.partial, "a rejected plan must not expose a partial snapshot")
		})
	}
}

func TestWatchDirectoryBudgetBatchesBeforeRetaining(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for i := range watchDirectoryBatch + 1 {
		mustWriteFile(t, filepath.Join(root, fmt.Sprintf("file-%03d.go", i)), "")
	}
	limits := defaultWatchLimits()
	limits.entries = watchDirectoryBatch
	budget := watchBudget{remaining: limits}
	entries, err := budget.readDir(root, "", 0)
	require.ErrorIs(t, err, errWatchLimit, "the first entry of a second batch must exceed the shared limit")
	require.Nil(t, entries)
	require.NoError(t, os.Remove(filepath.Join(root, fmt.Sprintf("file-%03d.go", watchDirectoryBatch))))
	budget = watchBudget{remaining: limits}
	entries, err = budget.readDir(root, "", 0)
	require.NoError(t, err)
	require.Len(t, entries, watchDirectoryBatch)
	require.Zero(t, budget.remaining.entries)
}

func TestModuleMetadataAggregateBudgets(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := map[string]string{
		"go.work":  "go 1.27\nuse (\n.\n./a\n./b\n)\n",
		"go.mod":   "module example.com/root\nignore ./generated\n",
		"a/go.mod": "module example.com/a\nignore ./generated\n",
		"b/go.mod": "module example.com/b\nignore ./generated\n",
	}
	mustTree(t, root, files)
	var totalBytes int
	for _, data := range files {
		totalBytes += len(data)
	}
	for _, tc := range []struct {
		name  string
		limit func(*watchLimits, int)
		exact int
	}{
		{name: "file count", exact: len(files), limit: func(l *watchLimits, n int) { l.metadataFiles = n }},
		{name: "aggregate bytes", exact: totalBytes, limit: func(l *watchLimits, n int) { l.metadataBytes = n }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			limits := defaultWatchLimits()
			tc.limit(&limits, tc.exact)
			budget := watchBudget{remaining: limits}
			filters, err := budget.moduleIgnoreFilters(root)
			require.NoError(t, err)
			require.Equal(t, []string{"-generated", "-a/generated", "-b/generated"}, filters)
			tc.limit(&limits, tc.exact-1)
			budget = watchBudget{remaining: limits}
			filters, err = budget.moduleIgnoreFilters(root)
			require.ErrorIs(t, err, errWatchLimit)
			require.Nil(t, filters, "aggregate overflow cannot return a partial rule set")
		})
	}
}

func TestDirectoryFilterAggregateLimitsBeforeCompilation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		count int
		rule  string
	}{
		{name: "count", count: maxWatchFilters + 1, rule: "-x"},
		{name: "bytes", count: maxWatchFilterBytes/4096 + 1, rule: "-" + strings.Repeat("x", 4095)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rules := make([]string, tc.count)
			for i := range rules {
				rules[i] = tc.rule
			}
			include, err := pathIncludeFunc(rules)
			require.ErrorIs(t, err, errWatchLimit)
			require.Nil(t, include)
			_, err = newDirFilter(rules)
			require.ErrorIs(t, err, errWatchLimit)
		})
	}
}

func TestWatchCheckSharesBudgetAcrossPartialDirectories(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustTree(t, root, map[string]string{"a/blocked/": "", "b/blocked/": ""})
	m := newTestManager(t)
	m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	m.limits.DirectoryFilters = []string{"-**/blocked"}
	plan := planWatch(root, m.limits.DirectoryFilters)
	require.NoError(t, plan.err)
	r := record{Worktree: root}
	m.rememberWatch(root, serverID{}, plan)
	limits := defaultWatchLimits()
	limits.entries = 4 // root's two entries plus one in each partial child.
	_, changed, err := m.watchChangedWithLimits(r, limits)
	require.NoError(t, err)
	require.False(t, changed)
	limits.entries--
	_, changed, err = m.watchChangedWithLimits(r, limits)
	require.ErrorIs(t, err, errWatchLimit)
	require.False(t, changed, "overflow is uncertainty, not proof of stale content")
	require.Equal(t, plan.partial, m.watch.plans[root].partial, "an uncertain scan must retain its snapshot")
}

func TestWatchPlanningAndCheckingReportReadErrors(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustTree(t, root, map[string]string{"blocked/": "", "pkg/": ""})
	plan := planWatch(root, []string{"-blocked"})
	require.NoError(t, plan.err)
	m := newTestManager(t)
	m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	m.limits.DirectoryFilters = []string{"-blocked"}
	m.rememberWatch(root, serverID{}, plan)
	require.NoError(t, os.RemoveAll(root))
	failed := planWatch(root, []string{"-blocked"})
	require.ErrorIs(t, failed.err, os.ErrNotExist)
	require.Nil(t, failed.roots)
	_, changed, err := m.watchChanged(record{Worktree: root})
	require.ErrorIs(t, err, os.ErrNotExist)
	require.False(t, changed)
	require.Contains(t, m.watch.plans, root)
}

func TestMaintenanceSparesUncertainWatchMetadata(t *testing.T) {
	t.Parallel()
	s := testSupervisor(t)
	m := s.m
	m.watch = &watchPlans{plans: make(map[string]watchSnapshot)}
	m.limits.DirectoryFilters = []string{"-blocked"}
	root := t.TempDir()
	mustTree(t, root, map[string]string{"go.mod": "module example.com/root\n", "blocked/": "", "pkg/": ""})
	g := readyTestGeneration(t, s, root)
	g.record.StartedAt = time.Now().Add(-time.Minute).Unix()
	require.NoError(t, writeMap(m.mapPath, []record{g.record}))
	plan := planWatch(root, m.limits.DirectoryFilters)
	require.NoError(t, plan.err)
	m.rememberWatch(root, g.record.server(), plan)
	mustWriteFile(t, filepath.Join(root, "go.mod"), "//"+strings.Repeat("x", maxModuleFileBytes))
	_, err := s.maintenanceSweep(t.Context())
	require.NoError(t, err)
	wantRecords(t, m.mapPath, "uncertain watcher metadata must not signal/drop a server", g.record)
	require.Equal(t, plan.partial, m.watch.plans[root].partial)
	require.Equal(t, generationReady, supervisorGenerationState(s, g))
}

func TestUpstreamIdentity(t *testing.T) {
	t.Parallel()
	id := serverID{pid: 7, started: 1_700_000_000}
	conn := &ownedUpstream{Connection: loopbackConn(), g: &ownedGeneration{record: record{PID: id.pid, StartedAt: id.started}}}
	assert.Equal(t, id, serverOf(conn))
	assert.False(t, serverOf(loopbackConn()).known(), "a stub dial carries no identity")
	assert.False(t, (&ownedUpstream{Connection: loopbackConn(), g: &ownedGeneration{}}).server().known())
}
