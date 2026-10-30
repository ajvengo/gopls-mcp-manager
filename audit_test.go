package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestShutdownReleasesPendingMirror(t *testing.T) {
	t.Parallel()
	m := &manager{pending: newPendingCalls()}
	r := newRouter(t.Context(), m, testHome)
	conn := newFakeConn()
	id := mustID(t, "lost")
	state, err := r.admit(id, func() {})
	require.NoError(t, err)
	require.NoError(t, r.beginOperation(id, state, conn, testHome, func() {}, nil))
	r.failInFlight(conn, testHome, io.EOF)
	require.Len(t, m.pending.ops, 1, "uncertain work remains until router shutdown")
	r.clearRequests()
	require.Empty(t, m.pending.ops, "shutdown must release the manager's connection references")
}

func TestFullErrorQueueDoesNotBlockIngressExit(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		for range cap(r.errs) {
			r.errs <- io.ErrClosedPipe
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			r.readIngress(deadConn(), make(chan delivery))
		}()
		synctest.Wait()
		select {
		case <-done:
		default:
			require.Error(t, <-r.errs) // release the old implementation before failing
			require.FailNow(t, "ingress exit blocked reporting a redundant session error")
		}
	})
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
		go func() { done <- r.answerRoots(ctx, newFakeConn(), testHome, id) }()
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

func TestExpiredRootsPlanCannotRememberSnapshot(t *testing.T) {
	bubble(t, func(t *testing.T) {
		m := &manager{watch: &watchPlans{plans: make(map[string]watchSnapshot)}}
		r := newRouter(t.Context(), m, testHome)
		ctx, cancel := context.WithCancel(t.Context())
		r.plan = func(string, []string) watchPlan {
			cancel()
			return watchPlan{partial: map[string]partialEntries{testHome: {}}}
		}
		conn := &identifiedConn{Connection: newFakeConn(), id: serverID{pid: 1, started: 1}}
		err := r.answerRoots(ctx, conn, testHome, mustID(t, "roots"))
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, m.watch.plans, "expired planning published an obsolete server snapshot")
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
			err := r.answerRoots(ctx, newFakeConn(), testHome, id)
			cancel()
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
		require.Equal(t, int64(1), calls.Load(), "timeouts must not create replacement filesystem workers")
	})
}

func TestLateRootsPlanCannotReplaceNewerGeneration(t *testing.T) {
	bubble(t, func(t *testing.T) {
		m := &manager{watch: &watchPlans{plans: make(map[string]watchSnapshot)}}
		r := newRouter(t.Context(), m, testHome)
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
		go func() { done <- r.answerRoots(t.Context(), conn, testHome, id) }()
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

func (c *bufferedRootsConn) Read(context.Context) (jsonrpc.Message, error) {
	if len(c.messages) == 0 {
		return nil, io.EOF
	}
	msg := c.messages[0]
	c.messages = c.messages[1:]
	return msg, nil
}

func TestClosedConnectionCannotStartObsoleteRootsPlan(t *testing.T) {
	bubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r := newRouter(ctx, nil, testHome)
		var planned atomic.Int64
		r.plan = func(string, []string) watchPlan { planned.Add(1); return watchPlan{} }
		for range cap(r.out) {
			r.out <- &jsonrpc.Request{Method: "notification"}
		}
		connCtx, closeConn := context.WithCancel(ctx)
		conn := &boundedConn{Connection: &bufferedRootsConn{
			Connection: newFakeConn(),
			messages:   []jsonrpc.Message{&jsonrpc.Request{ID: mustID(t, "roots"), Method: "roots/list"}},
		}, release: closeConn, ctx: connCtx}
		r.track(mustID(t, "unanswered"), conn, testHome)
		require.NoError(t, conn.Close())
		done := make(chan struct{})
		go func() { defer close(done); r.readFromUpstream(conn, testHome) }()
		synctest.Wait()
		require.Zero(t, planned.Load(), "queued asks must observe Close before reader cleanup finishes")
		cancel()
		<-done
	})
}

func TestModuleMetadataReadsAreBounded(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "go.mod")
	mustWriteFile(t, path, "module example.com/a\nignore ./generated\n//"+strings.Repeat("x", maxModuleFileBytes))
	require.Empty(t, moduleIgnoreFilters(root), "oversize metadata should contribute no filters")
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Symlink("/dev/zero", path))
	_, err := readModuleFile(path)
	require.Error(t, err, "device input must be rejected before reading")
	require.NoError(t, os.Remove(path))
	target := filepath.Join(root, "regular.mod")
	mustWriteFile(t, target, "module example.com/a\nignore ./generated\n")
	require.NoError(t, os.Symlink(target, path))
	require.Equal(t, []string{"-generated"}, moduleIgnoreFilters(root), "regular-file symlinks remain supported")
}

func FuzzDirectoryFilterPrecedence(f *testing.F) {
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
		_ = newDirFilter(rules).worthListing(rel)
		require.True(t, pathIncludeFunc(append(rules, "+"))(rel), "last global include must win")
		require.False(t, pathIncludeFunc(append(rules, "-"))(rel), "last global exclude must win")
	})
}

func TestModuleIgnoreFiltersRejectInvalidUTF8(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "go.mod"), "module example.com/a\nignore \"\\x80\"\n")
	require.NotPanics(t, func() { _ = planWatch(root, nil) })
}

func TestDirectoryFilterCompilationRejectsUnusableRules(t *testing.T) {
	t.Parallel()
	for _, rule := range []string{"", "+\x80", "+" + strings.Repeat("**/", 2000)} {
		require.NotPanics(t, func() {
			filter := newDirFilter([]string{"-blocked", rule})
			require.False(t, filter.include("blocked"))
		})
	}
	_, err := config.ParseDirectoryFilters("+" + strings.Repeat("a", config.MaxDirectoryFilterBytes))
	require.Error(t, err)
}
