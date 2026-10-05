package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

func TestOverBudgetEvictsLeastFrequentlyUsed(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	old := now.Add(-time.Hour).Unix()
	busy := record{Worktree: "/busy", PID: 1, StartedAt: old, Uses: 10, UsedAt: now.Unix()}
	idle := record{Worktree: "/idle", PID: 2, StartedAt: old, Uses: 1, UsedAt: now.Unix()}
	// Ten uses six half-lives ago are worth less than one use now.
	faded := record{Worktree: "/faded", PID: 3, StartedAt: old, Uses: 10, UsedAt: now.Add(-6 * useHalfLife).Unix()}
	fresh := record{Worktree: "/fresh", PID: 4, StartedAt: now.Unix()}
	leaving := record{Worktree: "/leaving", PID: 5, StartedAt: old, Terminating: true}
	duplicate := record{Worktree: "/duplicate", PID: 1, StartedAt: old, Uses: 10, UsedAt: now.Unix()}
	for _, tc := range []struct {
		name      string
		records   []record
		files     map[int]int
		busy      map[string]bool
		budget    int
		victims   []record
		remaining int
	}{
		{name: "within budget", records: []record{busy, idle},
			files: map[int]int{1: 10, 2: 10}, budget: 20, remaining: 20},
		{name: "least frequent first", records: []record{busy, idle, faded},
			files: map[int]int{1: 10, 2: 10, 3: 10}, budget: 15, victims: []record{faded, idle}, remaining: 10},
		{name: "new and terminating spared but counted", records: []record{busy, fresh, leaving},
			files: map[int]int{1: 10, 4: 10, 5: 10}, budget: 15, victims: []record{busy}, remaining: 20},
		{name: "unmeasured not chosen", records: []record{idle, busy},
			files: map[int]int{1: 30}, budget: 15, victims: []record{busy}, remaining: 0},
		{name: "busy worktree spared", records: []record{busy, idle},
			files: map[int]int{1: 10, 2: 10}, busy: map[string]bool{"/idle": true}, budget: 15,
			victims: []record{busy}, remaining: 10},
		{name: "shared pid counted once", records: []record{busy, duplicate},
			files: map[int]int{1: 10}, budget: 10, remaining: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			victims, remaining := overBudget(tc.records, tc.files, tc.busy, tc.budget, now)
			if !slices.Equal(victims, tc.victims) || remaining != tc.remaining {
				t.Fatalf("overBudget = %+v, %d; want %+v, %d", victims, remaining, tc.victims, tc.remaining)
			}
		})
	}
}

func TestFlushUsesAgesAndAddsCounts(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.uses = &useCounts{counts: make(map[string]int)}
	hourAgo := time.Now().Add(-useHalfLife).Unix()
	used := record{Worktree: "/used", PID: 1, Port: firstPort, Uses: 8, UsedAt: hourAgo}
	leaving := record{Worktree: "/leaving", PID: 2, Port: firstPort + 1, Terminating: true}
	unused := record{Worktree: "/unused", PID: 3, Port: firstPort + 2, Uses: 8, UsedAt: hourAgo}
	mustWriteMap(t, m.mapPath, []record{used, leaving, unused})
	for _, worktree := range []string{"/used", "/used", "/leaving", "/gone"} {
		m.used(worktree)
	}
	got, err := m.withMap(t.Context(), m.flushUses)
	if err != nil {
		t.Fatal(err)
	}
	// Eight uses one half-life ago age to four, plus the two just counted.
	if math.Abs(got[0].Uses-6) > 0.01 || time.Since(time.Unix(got[0].UsedAt, 0)) > time.Minute {
		t.Fatalf("used record = %+v, want about 6 uses as of now", got[0])
	}
	if got[1] != leaving || got[2] != unused {
		t.Fatalf("records without counts changed: %+v", got[1:])
	}
	if len(m.uses.counts) != 0 {
		t.Fatalf("counts survived the flush: %v", m.uses.counts)
	}
	wantRecords(t, m.mapPath, "flushed uses were not written", got...)
	var nothing *manager
	nothing.used("/nil-safe")
}

// The budget's only kill path is the sweep's: identity is checked and
// Terminating persisted before SIGTERM, as for any other condemned record.
func TestMaintenanceEvictsLeastFrequentlyUsedOverBudget(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.limits.OpenFiles = 30
	busyProcess, idleProcess := startFakeGopls(t, firstPort), startFakeGopls(t, firstPort+1)
	exited := make(chan struct{})
	go func() { _ = idleProcess.Wait(); close(exited) }()
	now := time.Now().Unix()
	busy := record{Worktree: t.TempDir(), PID: busyProcess.Process.Pid, Port: firstPort, Uses: 5, UsedAt: now}
	idle := record{Worktree: t.TempDir(), PID: idleProcess.Process.Pid, Port: firstPort + 1, Uses: 1, UsedAt: now}
	mustWriteMap(t, m.mapPath, []record{busy, idle})
	m.alive = func(context.Context, record) probeVerdict { return probeLive }
	m.openFiles = func(_ context.Context, measured []record) (map[int]int, error) {
		if !slices.Equal(measured, []record{busy, idle}) {
			t.Errorf("measured %+v, want both records", measured)
		}
		return map[int]int{busy.PID: 20, idle.PID: 20}, nil
	}
	if _, err := m.sweepMaintenance(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("the least frequently used server was not terminated")
	}
	wantRunning(t, busyProcess, "the budget evicted the busier server")
	idle.Terminating = true
	wantRecords(t, m.mapPath, "eviction did not persist termination", busy, idle)
}

func TestMaintenanceSweepsWhenOpenFilesCannotBeCounted(t *testing.T) {
	t.Parallel()
	for _, err := range []error{exec.ErrNotFound, errors.New("lsof failed")} {
		m := newTestManager(t)
		mustWriteMap(t, m.mapPath, []record{{Worktree: "/removed", PID: 12345, Port: firstPort}})
		m.alive = func(context.Context, record) probeVerdict { return probeGone }
		m.openFiles = func(context.Context, []record) (map[int]int, error) { return nil, err }
		if records, sweepErr := m.sweepMaintenance(t.Context()); sweepErr != nil || len(records) != 0 {
			t.Fatalf("%v: sweep = %+v, %v; want the dead record reaped", err, records, sweepErr)
		}
	}
}

func TestDeliveredToolCallsCountUses(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := newTestRouter(t, testHome)
		r.m = &manager{uses: &useCounts{counts: make(map[string]int)}, pending: newPendingCalls()}
		conn := newFakeConn()
		l := connectedLane(r, testWorktree, conn)
		sendCall(t, l, "counted")
		mustRecv(t, conn.writes, "the tools/call")
		id := mustID(t, "uncounted")
		r.track(id, nil, testWorktree)
		l.send(t.Context(), &jsonrpc.Request{ID: id, Method: "tools/list"}, nil)
		mustRecv(t, conn.writes, "the tools/list")
		if got := r.m.uses.counts; !maps.Equal(got, map[string]int{testWorktree: 1}) {
			t.Fatalf("uses = %v, want one tools/call for %s", got, testWorktree)
		}
		if got := r.m.pending.newest(); len(got) != 1 || got[testWorktree].IsZero() {
			t.Fatalf("pending = %v, want both deliveries on %s", got, testWorktree)
		}
		r.finish(id, conn, nil)
		r.finish(mustID(t, "counted"), conn, nil)
		if got := r.m.pending.newest(); len(got) != 0 {
			t.Fatalf("answered operations still pending: %v", got)
		}
	})
}

func TestParseOpenFiles(t *testing.T) {
	t.Parallel()
	got := parseOpenFiles("p10\nfcwd\nftxt\nf0\np11\nf3\nfmem\nnoise\np\nf9\n")
	if want := map[int]int{10: 3, 11: 2}; !maps.Equal(got, want) {
		t.Fatalf("parseOpenFiles = %v, want %v", got, want)
	}
}

func TestCountOpenFilesToleratesMissingPID(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("lsof"); err != nil {
		t.Skip("lsof not installed")
	}
	got, err := countOpenFiles(t.Context(), []record{{PID: os.Getpid()}, {PID: 2147483647}})
	if err != nil || got[os.Getpid()] == 0 {
		t.Fatalf("countOpenFiles = %v, %v; want this process counted", got, err)
	}
}

// Another manager's fresh pending call must stop this one evicting, including
// one arriving after the victims were picked but before the verdict.
func TestMaintenanceSparesServersWithFreshPendingCalls(t *testing.T) {
	t.Parallel()
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprint(late), func(t *testing.T) {
			t.Parallel()
			m := newTestManager(t)
			m.limits.OpenFiles = 30
			process := startFakeGopls(t, firstPort)
			worktree := t.TempDir()
			r := record{Worktree: worktree, PID: process.Process.Pid, Port: firstPort}
			mustWriteMap(t, m.mapPath, []record{r})
			other := m.pendingPath(process.Process.Pid)
			busy := func() {
				if err := writePending(other, map[string]time.Time{worktree: time.Now()}); err != nil {
					t.Error(err)
				}
			}
			if !late {
				busy()
			}
			m.alive = func(context.Context, record) probeVerdict {
				if late {
					busy()
				}
				return probeLive
			}
			m.openFiles = func(context.Context, []record) (map[int]int, error) {
				return map[int]int{r.PID: 40}, nil
			}
			if _, err := m.sweepMaintenance(t.Context()); err != nil {
				t.Fatal(err)
			}
			wantRecords(t, m.mapPath, "a server with a fresh pending call was evicted", r)
			wantRunning(t, process, "a server with a fresh pending call was signalled")
		})
	}
}

func TestBusyWorktreesAgesEntriesAndDropsDeadManagers(t *testing.T) {
	t.Parallel()
	m := newTestManager(t)
	m.pending = newPendingCalls()
	m.beginPending(operationKey{id: mustID(t, "own")}, "/own")
	now := time.Now()
	m.pending.update(func(ops map[operationKey]pendingCall) {
		ops[operationKey{id: mustID(t, "own-stale")}] = pendingCall{"/own-stale", now.Add(-pendingFresh)}
	})
	live := idleAs(t, "sleep")
	mustWritePending := func(pid int, newest map[string]time.Time) string {
		t.Helper()
		path := m.pendingPath(pid)
		if err := writePending(path, newest); err != nil {
			t.Fatal(err)
		}
		return path
	}
	mustWritePending(live.Process.Pid, map[string]time.Time{
		"/other": now.Add(-time.Second), "/other-stale": now.Add(-time.Minute), "/future": now.Add(time.Minute)})
	dead := mustWritePending(2147483647, map[string]time.Time{"/dead": now})
	mustWriteFile(t, m.pendingPath(os.Getppid()), "not json")
	mustWriteFile(t, filepath.Join(m.pendingDir(), ".pending-tmp"), "{}")
	got, err := m.busyWorktrees(now)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]bool{"/own": true, "/other": true}; !maps.Equal(got, want) {
		t.Fatalf("busy = %v, want %v", got, want)
	}
	if _, err := os.Stat(dead); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a dead manager's file survived: %v", err)
	}
	broken := newTestManager(t)
	mustWriteFile(t, broken.pendingDir(), "a file where the directory belongs")
	if _, err := broken.busyWorktrees(now); err == nil {
		t.Fatal("an unreadable pending directory read as nothing pending")
	}
}

// Under synctest, Wait returns once the publisher has written and gone back
// to waiting for the next change, so each file state is asserted exactly
// rather than polled for.
func TestPublishPendingFollowsOperations(t *testing.T) {
	bubble(t, func(t *testing.T) {
		m := newTestManager(t)
		m.pending = newPendingCalls()
		path := m.pendingPath(os.Getpid())
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() { defer close(done); m.publishPending(ctx) }()
		published := func() map[string]int64 {
			t.Helper()
			synctest.Wait()
			data, err := os.ReadFile(path)
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			var newest map[string]int64
			if err != nil || json.Unmarshal(data, &newest) != nil {
				t.Fatalf("pending file %q: %v", data, err)
			}
			return newest
		}

		key := operationKey{id: mustID(t, "published")}
		m.beginPending(key, "/published")
		if got := published(); got["/published"] != time.Now().UnixMilli() {
			t.Fatalf("published %v, want /published started now", got)
		}
		m.endPending(key)
		if got := published(); got != nil {
			t.Fatalf("file kept after the last call ended: %v", got)
		}

		// Routers on many goroutines: the publisher coalesces whatever burst
		// it finds and settles on the final state.
		var routers sync.WaitGroup
		for i := range 64 {
			routers.Go(func() {
				key := operationKey{id: mustID(t, fmt.Sprintf("burst-%d", i))}
				m.beginPending(key, fmt.Sprintf("/burst-%d", i%4))
				m.endPending(key)
			})
		}
		m.beginPending(key, "/published")
		routers.Wait()
		if got := published(); len(got) != 1 || got["/published"] == 0 {
			t.Fatalf("after the burst published %v, want only /published", got)
		}

		cancel()
		<-done
		if got := published(); got != nil {
			t.Fatalf("file kept after shutdown: %v", got)
		}
	})
}
