package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Why a budget rather than a fix: gopls mcp hard-codes its fsnotify watcher
// over the roots answerRoots reports, and on darwin fsnotify is kqueue, which
// holds one descriptor open for every file and directory under the watched tree
// — markdown, python and yaml included, since gopls filters events, not
// watches. A gopls therefore costs about as many descriptors as its worktree
// has entries, and nothing on this side of the MCP connection can change that
// short of withholding roots, which would leave gopls blind to new files.
// What can be bounded is how many such trees are watched at once: maintenance
// evicts the least frequently used servers while their lsof rows together
// exceed GOPLS_MANAGER_MAX_OPEN_FILES, and the next call to an evicted worktree
// starts it again.
const (
	// useHalfLife ages usage, so a worktree busy yesterday does not outrank
	// one busy now. Exponential in wall time rather than halved per sweep,
	// because every manager sweeping the map would otherwise halve it again.
	useHalfLife = time.Hour
	// evictionWindow spares a new server until its first uses have had a few
	// sweeps to reach the map; without it the server a cold call just started
	// would rank last and be the first one evicted. W-TinyLFU's window, for the
	// same reason.
	evictionWindow = 5 * time.Minute
	// openFilesTimeout bounds one lsof over every recorded pid.
	openFilesTimeout = 5 * time.Second
)

// useCounts holds this manager's tools/call deliveries per worktree until the
// next sweep adds them to the map.
type useCounts struct {
	mu     sync.Mutex
	counts map[string]int
}

// used counts one delivered tools/call. Nil-safe: routers built in tests have
// no manager, or one without counts.
func (m *manager) used(worktree string) {
	if m == nil || m.uses == nil {
		return
	}
	m.uses.mu.Lock()
	m.uses.counts[worktree]++
	m.uses.mu.Unlock()
}

// flushUses is a withMap body adding the pending counts to their records.
// Terminating records are left alone: signalTerminating matches the exact
// record it persisted.
func (m *manager) flushUses(records []record) ([]record, error) {
	if m.uses == nil {
		return records, nil
	}
	m.uses.mu.Lock()
	counts := m.uses.counts
	if len(counts) != 0 {
		m.uses.counts = make(map[string]int)
	}
	m.uses.mu.Unlock()
	now := time.Now()
	for i, r := range records {
		if n := counts[r.Worktree]; n > 0 && !r.Terminating {
			records[i].Uses = r.frequency(now) + float64(n)
			records[i].UsedAt = now.Unix()
		}
	}
	return records, nil
}

// frequency is Uses aged to now. A future UsedAt is not aged at all, rather
// than grown.
func (r record) frequency(now time.Time) float64 {
	age := max(now.Sub(time.Unix(r.UsedAt, 0)), 0)
	return r.Uses * math.Exp2(-age.Hours()/useHalfLife.Hours())
}

// evictionVictims flushes this manager's usage, then names the servers to
// evict. Only the flush can fail the sweep: a missing or failing lsof skips
// the budget and leaves deleted-worktree reaping to go ahead.
func (m *manager) evictionVictims(ctx context.Context) ([]record, error) {
	records, err := m.withMap(ctx, m.flushUses)
	if err != nil || m.openFiles == nil || len(records) == 0 {
		return nil, err
	}
	now := time.Now()
	files, err := m.openFiles(ctx, records)
	var busy map[string]bool
	if err == nil {
		busy, err = m.busyWorktrees(now)
	}
	if err != nil {
		if !errors.Is(err, exec.ErrNotFound) && ctx.Err() == nil {
			fmt.Fprintln(os.Stderr, "gopls-mcp-manager: open-file budget skipped:", err)
		}
		return nil, nil
	}
	victims, total := overBudget(records, files, busy, m.limits.OpenFiles, now)
	if total > m.limits.OpenFiles {
		fmt.Fprintf(os.Stderr, "gopls-mcp-manager: gopls servers hold %d open files after eviction, above GOPLS_MANAGER_MAX_OPEN_FILES=%d\n", total, m.limits.OpenFiles)
	}
	return victims, nil
}

// overBudget picks the least frequently used records whose open files take the
// total above budget, and returns the total left once they are gone.
// Terminating records still hold their files and count towards it, but are
// already leaving; records inside evictionWindow, and busy worktrees, are spared.
//
// ponytail: exact aged counters, not W-TinyLFU's count-min sketch — the sketch
// saves memory over millions of keys, and the map holds at most
// GOPLS_MANAGER_MAX_SERVERS.
func overBudget(records []record, files map[int]int, busy map[string]bool, budget int, now time.Time) ([]record, int) {
	total := 0
	counted := make(map[int]bool, len(records))
	var candidates []record
	for _, r := range records {
		if !counted[r.PID] {
			counted[r.PID] = true
			total += files[r.PID]
		}
		age := now.Sub(time.Unix(r.StartedAt, 0))
		if !r.Terminating && !busy[r.Worktree] && files[r.PID] > 0 && (age < 0 || age >= evictionWindow) {
			candidates = append(candidates, r)
		}
	}
	slices.SortStableFunc(candidates, func(a, b record) int {
		return cmp.Or(cmp.Compare(a.frequency(now), b.frequency(now)), cmp.Compare(a.UsedAt, b.UsedAt))
	})
	var victims []record
	for _, r := range candidates {
		if total <= budget {
			break
		}
		victims = append(victims, r)
		total -= files[r.PID]
	}
	return victims, total
}

// countOpenFiles counts lsof's rows per recorded pid: the rows
// `lsof | grep '^gopls '` counts, descriptors plus cwd, txt and mapped entries.
func countOpenFiles(ctx context.Context, records []record) (map[int]int, error) {
	ctx, cancel := context.WithTimeout(ctx, openFilesTimeout)
	defer cancel()
	list := make([]string, len(records))
	for i, r := range records {
		list[i] = strconv.Itoa(r.PID)
	}
	out, err := exec.CommandContext(ctx, "lsof", "-n", "-P", "-w", "-F", "f", "-p", strings.Join(list, ",")).Output()
	// lsof exits 1 when a listed pid has gone, and still lists the others.
	var exit *exec.ExitError
	if err != nil && (!errors.As(err, &exit) || exit.ExitCode() != 1) {
		return nil, err
	}
	return parseOpenFiles(string(out)), nil
}

// parseOpenFiles reads lsof -F f output: a p<pid> line, then an f<fd> line for
// each of its files.
func parseOpenFiles(out string) map[int]int {
	counts := make(map[int]int)
	pid := 0
	for line := range strings.SplitSeq(out, "\n") {
		switch {
		case strings.HasPrefix(line, "p"):
			pid, _ = strconv.Atoi(line[1:])
		case strings.HasPrefix(line, "f") && pid > 0:
			counts[pid]++
		}
	}
	return counts
}
