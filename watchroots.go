package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"
	"golang.org/x/mod/modfile"
)

// A gopls watches every root answerRoots reports recursively, and on darwin
// every entry under a watched directory costs it a descriptor (see the
// descriptor reservations in supervisor.go). So instead of the worktree itself,
// answerRoots reports the smallest set of directories that covers the worktree
// minus what its filters exclude: the go.mod ignore directives of the worktree's
// modules, then GOPLS_MANAGER_DIRECTORY_FILTERS. With no rule excluding
// anything the plan is the worktree itself, exactly as before.
//
// What no root covers is the entries directly inside a directory that has an
// excluded descendant — a "partial" directory, the worktree itself whenever
// anything is excluded. gopls catches edits to Go files there anyway, because
// every file-scoped MCP tool compares the mtimes of its package's files first,
// but not a file added or removed, a new subdirectory, or a go.mod/go.sum/
// go.work change. A manager therefore keeps the Go-relevant entry names of each
// partial directory, and maintenance condemns a gopls whose partial
// directories gained or lost one, or whose module files changed after it
// started; the next call starts a fresh one. See watchChanged.

// watchPlan is what answerRoots tells a gopls to watch.
type watchPlan struct {
	err       error                     // a failed plan must never be reported as complete roots
	started   time.Time                 // admission order, so a late plan cannot replace a newer generation
	roots     []string                  // absolute directories gopls watches recursively
	partial   map[string]partialEntries // keyed by absolute directory
	uncertain bool
}

// These are manager-metadata bounds, not gopls watcher or RSS limits. One
// operation shares the counters across all directories/modules, preventing a
// many-small-inputs bypass of an individual file or directory ceiling.
const (
	maxWatchEntries       = 100_000
	maxWatchNameBytes     = 32 << 20
	maxWatchDepth         = 128
	maxWatchRoots         = 4096
	maxWatchPartials      = 16_384
	maxWatchMetadataFiles = 256
	maxWatchMetadataBytes = 16 << 20
	maxWatchFilters       = 1024
	maxWatchFilterBytes   = 1 << 20
	watchDirectoryBatch   = 128
)

var errWatchLimit = errors.New("roots planning resource limit exceeded")

type watchLimits struct {
	entries, nameBytes, depth, roots, partials int
	metadataFiles, metadataBytes               int
	filters, filterBytes                       int
}

func defaultWatchLimits() watchLimits {
	return watchLimits{entries: maxWatchEntries, nameBytes: maxWatchNameBytes,
		depth: maxWatchDepth, roots: maxWatchRoots, partials: maxWatchPartials,
		metadataFiles: maxWatchMetadataFiles, metadataBytes: maxWatchMetadataBytes,
		filters: maxWatchFilters, filterBytes: maxWatchFilterBytes}
}

type watchBudget struct{ remaining watchLimits }

func takeWatchBudget(remaining *int, n int, resource string) error {
	if n > *remaining {
		return fmt.Errorf("%w: %s", errWatchLimit, resource)
	}
	*remaining -= n
	return nil
}

func (b *watchBudget) nameBytes(values ...string) error {
	for _, value := range values {
		if err := takeWatchBudget(&b.remaining.nameBytes, len(value), "entry name/path bytes"); err != nil {
			return err
		}
	}
	return nil
}

func (b *watchBudget) rule(rule string) error {
	if err := takeWatchBudget(&b.remaining.filters, 1, "filter count"); err != nil {
		return err
	}
	return takeWatchBudget(&b.remaining.filterBytes, len(rule), "filter bytes")
}

// readDir batches before retaining entries. os.ReadDir would allocate the
// entire directory before a limit could reject it. At most one fixed batch is
// transiently live beyond the retained-entry ceiling. Close before recursing
// so depth does not also grow the number of open directory descriptors.
func (b *watchBudget) readDir(dir, rel string, depth int) ([]os.DirEntry, error) {
	if depth > b.remaining.depth {
		return nil, fmt.Errorf("%w: directory depth", errWatchLimit)
	}
	// The enumerated directory may have become a FIFO or symlink before
	// recursion. Check the opened descriptor, with flags that cannot park on
	// a FIFO and cannot follow a replacement out of the enumerated tree.
	f, err := os.OpenFile(dir, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, syscall.ENOTDIR
	}
	var entries []os.DirEntry
	for {
		batch, err := f.ReadDir(watchDirectoryBatch)
		for _, entry := range batch {
			if err := takeWatchBudget(&b.remaining.entries, 1, "tree entries"); err != nil {
				return nil, err
			}
			name := entry.Name()
			// Charge the name and both joined paths before constructing them.
			if err := b.nameBytes(name, dir, "/", name, rel, "/", name); err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// partialEntries is what a partial directory held when the plan was made.
type partialEntries struct {
	// names are its Go-relevant entries, sorted; see relevantNames.
	names []string
	// files is false for an excluded directory walked only for the + rules
	// below it: its own files are excluded, so their changes do not count.
	files bool
}

// watchSnapshot is the plan a manager last reported for a worktree, and when.
type watchSnapshot struct {
	planned time.Time
	partial map[string]partialEntries
	taken   time.Time
	// server is the gopls the roots went to, unknown when the connection did
	// not say; see rememberWatch.
	server    serverID
	uncertain bool
}

// watchPlans holds this manager's snapshots by worktree.
type watchPlans struct {
	mu    sync.Mutex
	plans map[string]watchSnapshot
}

// skippedByWatcher reports whether gopls' watcher never descends into a
// directory of this name (filewatcher.skipDir), so it neither needs excluding
// nor makes its parent partial. .git is the case that matters: a worktree's own
// .git directory is never watched, whatever a filter says.
func skippedByWatcher(name string) bool {
	return strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata"
}

// watchedFile reports whether gopls' watcher reports changes to a file of this
// name (filewatcher.skipFile).
func watchedFile(name string) bool {
	switch strings.TrimPrefix(filepath.Ext(name), ".") {
	case "go", "mod", "sum", "work", "s":
		return true
	}
	return false
}

// moduleFile reports whether a change to this file changes gopls' module graph,
// which it learns of only from its watcher.
func moduleFile(name string) bool {
	switch name {
	case "go.mod", "go.sum", "go.work", "go.work.sum":
		return true
	}
	return false
}

// rulePath is the path a filter rule names, slash-cleaned and relative to the
// worktree: "" for a rule covering the whole worktree.
func rulePath(rule string) string {
	return strings.TrimPrefix(path.Clean("/"+rule[1:]), "/")
}

// pathIncludeFunc implements the directory-filter contract: rules are evaluated in order
// and the last match wins, a rule matches its path and everything below it, and
// a "**" segment matches zero or more segments. Paths are slash-separated and
// relative to the worktree.
func pathIncludeFunc(rules []string) (func(string) bool, error) {
	budget := watchBudget{remaining: defaultWatchLimits()}
	for _, rule := range rules {
		if err := budget.rule(rule); err != nil {
			return nil, err
		}
	}
	matchers := make([]*regexp.Regexp, 0, len(rules))
	included := make([]bool, 0, len(rules))
	for _, rule := range rules {
		if !usableFilterRule(rule) {
			continue
		}
		prefix := rulePath(rule)
		pattern := ".*"
		if prefix != "" {
			var b strings.Builder
			b.WriteString("^/")
			for seg := range strings.SplitSeq(prefix, "/") {
				if seg == "**" {
					b.WriteString("(?:[^/]+/)*")
				} else {
					b.WriteString(regexp.QuoteMeta(seg))
					b.WriteString("/")
				}
			}
			pattern = b.String()
		}
		matcher, err := regexp.Compile(pattern)
		if err != nil {
			continue
		}
		matchers = append(matchers, matcher)
		included = append(included, rule[0] == '+')
	}
	return func(rel string) bool {
		rel = "/" + strings.Trim(rel, "/") + "/"
		include := true
		for i, m := range matchers {
			if m.MatchString(rel) {
				include = included[i]
			}
		}
		return include
	}, nil
}

func usableFilterRule(rule string) bool {
	return len(rule) > 0 && len(rule) <= config.MaxDirectoryFilterBytes && utf8.ValidString(rule) && (rule[0] == '+' || rule[0] == '-')
}

// dirFilter judges directories by one rule list: include is pathIncludeFunc's
// verdict, and mayInclude whether an excluded directory is worth looking into,
// that is, whether some + rule could include it or something below it. A rule
// with a "**" segment may match anywhere, so it always says yes; any other rule
// names one path, which an excluded directory can lead to only when it is that
// path or an ancestor of it. Without + rules nothing excluded is worth a look.
type dirFilter struct {
	include    func(string) bool
	mayInclude func(string) bool
	hasRules   bool
}

func newDirFilter(rules []string) (dirFilter, error) {
	include, err := pathIncludeFunc(rules)
	if err != nil {
		return dirFilter{}, err
	}
	var plus []string
	for _, rule := range rules {
		if !usableFilterRule(rule) || rule[0] != '+' {
			continue
		}
		prefix := rulePath(rule)
		if prefix == "" || slices.Contains(strings.Split(prefix, "/"), "**") {
			plus = append(plus, "**")
			break
		}
		plus = append(plus, prefix)
	}
	return dirFilter{
		include: include,
		mayInclude: func(rel string) bool {
			rel = strings.Trim(rel, "/")
			return slices.ContainsFunc(plus, func(prefix string) bool {
				return prefix == "**" || prefix == rel || strings.HasPrefix(prefix, rel+"/")
			})
		},
		hasRules: len(rules) != 0,
	}, nil
}

// worthListing reports whether the appearance of subdirectory rel of an
// excluded directory matters: it is included, or may lead to something that is.
func (f dirFilter) worthListing(rel string) bool {
	return f.include(rel) || f.mayInclude(rel)
}

// Match cmd/go ignore semantics: ./dir is relative, bare dir matches at any depth.
// Missing or malformed metadata contributes no filters; gopls reports it.
func (b *watchBudget) moduleIgnoreFilters(worktree string) ([]string, error) {
	moduleDirs := []string{"."}
	if data, err := b.readModuleFile(filepath.Join(worktree, "go.work")); err == nil {
		if work, err := modfile.ParseWork("go.work", data, nil); err == nil {
			for _, use := range work.Use {
				if !utf8.ValidString(use.Path) {
					continue
				}
				dir := path.Clean(filepath.ToSlash(use.Path))
				if !path.IsAbs(dir) && dir != ".." && !strings.HasPrefix(dir, "../") && !slices.Contains(moduleDirs, dir) {
					// Include the root go.mod and the already-read go.work in the
					// same file-count ceiling before retaining another module path.
					if len(moduleDirs) >= b.remaining.metadataFiles {
						return nil, fmt.Errorf("%w: module metadata files", errWatchLimit)
					}
					if err := b.nameBytes(dir); err != nil {
						return nil, err
					}
					moduleDirs = append(moduleDirs, dir)
				}
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var filters []string
	appendFilter := func(rule string) error {
		if err := b.rule(rule); err != nil {
			return err
		}
		filters = append(filters, rule)
		return nil
	}
	for _, dir := range moduleDirs {
		gomod := filepath.Join(worktree, filepath.FromSlash(dir), "go.mod")
		data, err := b.readModuleFile(gomod)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			continue
		}
		file, err := modfile.ParseLax(gomod, data, nil)
		if err != nil {
			continue
		}
		for _, ignore := range file.Ignore {
			if !utf8.ValidString(ignore.Path) {
				continue
			}
			pattern, relative := strings.CutPrefix(filepath.ToSlash(ignore.Path), "./")
			pattern = strings.Trim(path.Clean("/"+pattern), "/")
			if pattern == "" {
				continue
			}
			if !relative {
				if dir != "." {
					// "dir/**/pattern" needs a segment for ** to stand for, so
					// it alone would miss a pattern directory directly in dir.
					if err := appendFilter("-" + path.Join(dir, pattern)); err != nil {
						return nil, err
					}
				}
				pattern = "**/" + pattern
			}
			if err := appendFilter("-" + path.Join(dir, pattern)); err != nil {
				return nil, err
			}
		}
	}
	return filters, nil
}

// Metadata is repository-controlled. Nonblocking open avoids parking on a FIFO;
// descriptor checks reject devices, and the reader also bounds concurrent growth.
const maxModuleFileBytes = 4 << 20

func (b *watchBudget) readModuleFile(path string) ([]byte, error) {
	if err := b.nameBytes(path); err != nil {
		return nil, err
	}
	if err := takeWatchBudget(&b.remaining.metadataFiles, 1, "module metadata files"); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, os.ErrInvalid
	}
	limit := min(maxModuleFileBytes, b.remaining.metadataBytes)
	if info.Size() > int64(limit) {
		return nil, fmt.Errorf("%w: module metadata bytes", errWatchLimit)
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if len(data) > limit {
		return nil, fmt.Errorf("%w: module metadata bytes", errWatchLimit)
	}
	if err != nil {
		return nil, err
	}
	if err := takeWatchBudget(&b.remaining.metadataBytes, len(data), "module metadata bytes"); err != nil {
		return nil, err
	}
	return data, nil
}

// planWatch computes the roots a gopls for worktree is told to watch.
func planWatch(worktree string, global []string) watchPlan {
	return planWatchWithLimits(worktree, global, defaultWatchLimits())
}

func planWatchWithLimits(worktree string, global []string, limits watchLimits) watchPlan {
	b := &watchBudget{remaining: limits}
	if err := b.nameBytes(worktree); err != nil {
		return watchPlan{err: err}
	}
	filter, err := b.watchFilter(worktree, global)
	if err != nil {
		return watchPlan{err: err}
	}
	w := planner{filter: filter, budget: b, plan: watchPlan{partial: make(map[string]partialEntries)}}
	if !filter.hasRules {
		err = w.addRoot(worktree)
	} else {
		var clean bool
		clean, err = w.walk(worktree, "", 0)
		if err == nil && clean {
			err = w.addRoot(worktree)
		}
	}
	if err != nil {
		return watchPlan{err: err}
	}
	if len(w.plan.partial) == 0 {
		w.plan.partial = nil
	}
	slices.Sort(w.plan.roots)
	return w.plan
}

type planner struct {
	filter dirFilter
	budget *watchBudget
	plan   watchPlan
}

func (w *planner) addRoot(dir string) error {
	if err := takeWatchBudget(&w.budget.remaining.roots, 1, "roots"); err != nil {
		return err
	}
	if err := w.budget.nameBytes(dir); err != nil {
		return err
	}
	w.plan.roots = append(w.plan.roots, dir)
	return nil
}

func (w *planner) addPartial(dir string, names []string, files bool) error {
	if err := takeWatchBudget(&w.budget.remaining.partials, 1, "partial directories"); err != nil {
		return err
	}
	if err := w.budget.nameBytes(dir); err != nil {
		return err
	}
	w.plan.partial[dir] = partialEntries{names: names, files: files}
	return nil
}

// walk reports whether the tree at dir has no excluded directory, adding the
// roots and partial directories that cover it to the plan when it does. A
// directory read error fails the plan rather than certifying a partial view.
func (w *planner) walk(dir, rel string, depth int) (bool, error) {
	entries, err := w.budget.readDir(dir, rel, depth)
	if err != nil {
		return false, err
	}
	var clean []string
	partial := false
	for _, e := range entries {
		if !e.IsDir() || skippedByWatcher(e.Name()) {
			continue
		}
		child, childRel := filepath.Join(dir, e.Name()), path.Join(rel, e.Name())
		switch {
		case !w.filter.include(childRel):
			partial = true
			if w.filter.mayInclude(childRel) {
				if err := w.walkExcluded(child, childRel, depth+1); err != nil {
					return false, err
				}
			}
		default:
			childClean, err := w.walk(child, childRel, depth+1)
			if err != nil {
				return false, err
			}
			if childClean {
				clean = append(clean, child)
			} else {
				partial = true
			}
		}
	}
	if !partial {
		return true, nil
	}
	for _, dir := range clean {
		if err := w.addRoot(dir); err != nil {
			return false, err
		}
	}
	names, err := relevantNames(entries, rel, w.filter, true, w.budget)
	if err != nil {
		return false, err
	}
	return false, w.addPartial(dir, names, true)
}

// walkExcluded looks inside an excluded directory for descendants a later +
// rule includes again; the caller has checked that one may exist. Only its
// subdirectories are recorded, those that may hold such a descendant, since a
// new one may: its own files are excluded, so a change among them, or a new
// subdirectory no + rule can reach, must not condemn a gopls.
func (w *planner) walkExcluded(dir, rel string, depth int) error {
	entries, err := w.budget.readDir(dir, rel, depth)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || skippedByWatcher(e.Name()) {
			continue
		}
		child, childRel := filepath.Join(dir, e.Name()), path.Join(rel, e.Name())
		if !w.filter.include(childRel) {
			if w.filter.mayInclude(childRel) {
				if err := w.walkExcluded(child, childRel, depth+1); err != nil {
					return err
				}
			}
		} else {
			clean, err := w.walk(child, childRel, depth+1)
			if err != nil {
				return err
			}
			if clean {
				if err := w.addRoot(child); err != nil {
					return err
				}
			}
		}
	}
	names, err := relevantNames(entries, rel, w.filter, false, w.budget)
	if err != nil {
		return err
	}
	return w.addPartial(dir, names, false)
}

// relevantNames lists the entries of a partial directory whose appearance or
// disappearance gopls would have needed to hear about, subdirectories with a
// trailing slash. With files set (a directory that is itself included) that is
// its included subdirectories and watched files; without (an excluded one), the
// subdirectories a + rule may reach (dirFilter.worthListing).
func relevantNames(entries []os.DirEntry, rel string, filter dirFilter, files bool, budget *watchBudget) ([]string, error) {
	names := []string{}
	for _, e := range entries {
		name := e.Name()
		switch {
		case e.IsDir() && !skippedByWatcher(name):
			child := path.Join(rel, name)
			if files && filter.include(child) || !files && filter.worthListing(child) {
				if err := budget.nameBytes(name, "/"); err != nil {
					return nil, err
				}
				names = append(names, name+"/")
			}
		case files && e.Type().IsRegular() && watchedFile(name):
			if err := budget.nameBytes(name); err != nil {
				return nil, err
			}
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names, nil
}

// gopls only adds watches and emits no event for files predating registration.
// Keep each known server's first view so a later session cannot hide an
// intervening partial-directory change. Unknown identities may replace it;
// uncertain delivery stays sticky until the generation is retired.
func (m *manager) rememberWatch(worktree string, server serverID, plan watchPlan) {
	if m == nil || m.watch == nil || plan.err != nil {
		return
	}
	m.watch.mu.Lock()
	defer m.watch.mu.Unlock()
	if current, ok := m.watch.plans[worktree]; ok {
		same := current.server == server
		if same && (server.known() || current.uncertain) || !plan.started.IsZero() && current.planned.After(plan.started) {
			if same && plan.uncertain {
				current.uncertain = true
				m.watch.plans[worktree] = current
			}
			return
		}
	}
	if len(plan.partial) == 0 && !server.known() && !plan.uncertain {
		delete(m.watch.plans, worktree)
		return
	}
	// A later acknowledgement cannot prove which watcher view was received first.
	m.watch.plans[worktree] = watchSnapshot{partial: plan.partial, taken: time.Now(), planned: plan.started, server: server, uncertain: plan.uncertain}
}

// watchChanged reports whether r's gopls has missed a change no root of its plan
// covers: a partial directory whose relevant entries differ from the snapshot,
// or a module file in one modified after the server started. Only a snapshot of
// r's own server describes r's watcher. With both identities known that is
// compared; otherwise a snapshot taken before r started belongs to a server this
// manager reported roots to earlier. It returns the snapshot to hand to
// forgetWatch once r is condemned rather than dropping it, because a caller may
// still spare the server (a busy worktree) and so must not use the change up:
// one change condemns one server.
func (m *manager) watchChanged(r record) (watchSnapshot, bool, error) {
	return m.watchChangedWithLimits(r, defaultWatchLimits())
}

func (m *manager) watchChangedWithLimits(r record, limits watchLimits) (watchSnapshot, bool, error) {
	if m.watch == nil {
		return watchSnapshot{}, false, nil
	}
	m.watch.mu.Lock()
	snapshot, ok := m.watch.plans[r.Worktree]
	m.watch.mu.Unlock()
	if !ok {
		return watchSnapshot{}, false, nil
	}
	if id := r.server(); id.known() && snapshot.server.known() {
		if id != snapshot.server {
			return watchSnapshot{}, false, nil
		}
	} else if snapshot.taken.Unix() < r.StartedAt {
		return watchSnapshot{}, false, nil
	}
	if snapshot.uncertain {
		return snapshot, true, nil
	}
	if len(snapshot.partial) == 0 {
		return watchSnapshot{}, false, nil
	}
	b := &watchBudget{remaining: limits}
	filter, err := b.watchFilter(r.Worktree, m.limits.DirectoryFilters)
	if err != nil {
		return watchSnapshot{}, false, err
	}
	for dir, want := range snapshot.partial {
		if err := takeWatchBudget(&b.remaining.partials, 1, "partial directories"); err != nil {
			return watchSnapshot{}, false, err
		}
		if err := b.nameBytes(dir); err != nil {
			return watchSnapshot{}, false, err
		}
		changed, err := partialChanged(r, dir, want, filter, b)
		if err != nil {
			return watchSnapshot{}, false, err
		}
		if changed {
			return snapshot, true, nil
		}
	}
	return watchSnapshot{}, false, nil
}

// partialChanged reports whether partial directory dir of r's worktree no
// longer holds the relevant entries it did, or holds a module file modified
// after r started. A record from before StartedAt existed skips the mtime half.
// Read/stat failures report uncertainty; they cannot certify a fresh watcher.
func partialChanged(r record, dir string, want partialEntries, filter dirFilter, budget *watchBudget) (bool, error) {
	rel, err := filepath.Rel(r.Worktree, dir)
	if err != nil {
		return false, err
	}
	rel = filepath.ToSlash(rel)
	if rel == "." {
		rel = ""
	}
	depth := 0
	if rel != "" {
		depth = strings.Count(rel, "/") + 1
	}
	entries, err := budget.readDir(dir, rel, depth)
	if err != nil {
		return false, err
	}
	names, err := relevantNames(entries, rel, filter, want.files, budget)
	if err != nil {
		return false, err
	}
	if !slices.Equal(want.names, names) {
		return true, nil
	}
	started, ok := r.startedPrecisely()
	if !want.files || !ok {
		return false, nil
	}
	for _, e := range entries {
		if !moduleFile(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return false, err
		}
		if info.ModTime().After(started) {
			return true, nil
		}
	}
	return false, nil
}

func (b *watchBudget) watchFilter(worktree string, global []string) (dirFilter, error) {
	rules, err := b.moduleIgnoreFilters(worktree)
	if err != nil {
		return dirFilter{}, err
	}
	for _, rule := range global {
		if err := b.rule(rule); err != nil {
			return dirFilter{}, err
		}
		rules = append(rules, rule)
	}
	return newDirFilter(rules)
}
