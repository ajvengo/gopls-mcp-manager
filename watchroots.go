package main

import (
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/mod/modfile"
)

// A gopls watches every root answerRoots reports recursively, and on darwin
// every entry under a watched directory costs it a descriptor (see the
// open-file budget in maintenance.go). So instead of the worktree itself,
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
// started; the next call starts a fresh one. See watchStale.

// watchPlan is what answerRoots tells a gopls to watch.
type watchPlan struct {
	roots   []string                  // absolute directories gopls watches recursively
	partial map[string]partialEntries // keyed by absolute directory
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
	partial map[string]partialEntries
	taken   time.Time
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

// pathIncludeFunc is gopls' cache.PathIncludeFunc: rules are evaluated in order
// and the last match wins, a rule matches its path and everything below it, and
// a "**" segment matches zero or more segments. Paths are slash-separated and
// relative to the worktree.
func pathIncludeFunc(rules []string) func(string) bool {
	matchers := make([]*regexp.Regexp, len(rules))
	included := make([]bool, len(rules))
	for i, rule := range rules {
		prefix := strings.TrimPrefix(path.Clean("/"+rule[1:]), "/")
		pattern := ".*"
		if prefix != "" {
			var b strings.Builder
			b.WriteString("^/")
			for seg := range strings.SplitSeq(prefix, "/") {
				if seg == "**" {
					b.WriteString(".*")
				} else {
					b.WriteString(regexp.QuoteMeta(seg))
				}
				b.WriteString("/")
			}
			pattern = strings.TrimPrefix(b.String(), "^/.*")
		}
		matchers[i] = regexp.MustCompile(pattern)
		included[i] = rule[0] == '+'
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
	}
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
}

func newDirFilter(rules []string) dirFilter {
	var plus []string
	for _, rule := range rules {
		if rule[0] != '+' {
			continue
		}
		prefix := strings.TrimPrefix(path.Clean("/"+rule[1:]), "/")
		if prefix == "" || slices.Contains(strings.Split(prefix, "/"), "**") {
			plus = append(plus, "**")
			break
		}
		plus = append(plus, prefix)
	}
	return dirFilter{
		include: pathIncludeFunc(rules),
		mayInclude: func(rel string) bool {
			rel = strings.Trim(rel, "/")
			return slices.ContainsFunc(plus, func(prefix string) bool {
				return prefix == "**" || prefix == rel || strings.HasPrefix(prefix, rel+"/")
			})
		},
	}
}

// worthListing reports whether the appearance of subdirectory rel of an
// excluded directory matters: it is included, or may lead to something that is.
func (f dirFilter) worthListing(rel string) bool {
	return f.include(rel) || f.mayInclude(rel)
}

// moduleIgnoreFilters turns the ignore directives of the worktree's go.mod, and
// of every module its go.work uses, into directory filters. cmd/go reads
// "./dir" as relative to the module root and a bare "dir" as a directory of
// that path at any depth (search.IgnorePatterns). A file that is missing or
// does not parse contributes nothing: gopls reports that, not this.
func moduleIgnoreFilters(worktree string) []string {
	moduleDirs := []string{"."}
	if data, err := os.ReadFile(filepath.Join(worktree, "go.work")); err == nil {
		if work, err := modfile.ParseWork("go.work", data, nil); err == nil {
			for _, use := range work.Use {
				dir := path.Clean(filepath.ToSlash(use.Path))
				if !path.IsAbs(dir) && dir != ".." && !strings.HasPrefix(dir, "../") && !slices.Contains(moduleDirs, dir) {
					moduleDirs = append(moduleDirs, dir)
				}
			}
		}
	}
	var filters []string
	for _, dir := range moduleDirs {
		gomod := filepath.Join(worktree, filepath.FromSlash(dir), "go.mod")
		data, err := os.ReadFile(gomod)
		if err != nil {
			continue
		}
		file, err := modfile.ParseLax(gomod, data, nil)
		if err != nil {
			continue
		}
		for _, ignore := range file.Ignore {
			pattern, relative := strings.CutPrefix(filepath.ToSlash(ignore.Path), "./")
			pattern = strings.Trim(path.Clean("/"+pattern), "/")
			if pattern == "" {
				continue
			}
			if !relative {
				if dir != "." {
					// "dir/**/pattern" needs a segment for ** to stand for, so
					// it alone would miss a pattern directory directly in dir.
					filters = append(filters, "-"+path.Join(dir, pattern))
				}
				pattern = "**/" + pattern
			}
			filters = append(filters, "-"+path.Join(dir, pattern))
		}
	}
	return filters
}

// planWatch computes the roots a gopls for worktree is told to watch.
func planWatch(worktree string, global []string) watchPlan {
	rules := append(moduleIgnoreFilters(worktree), global...)
	if len(rules) == 0 {
		return watchPlan{roots: []string{worktree}}
	}
	w := planner{
		filter: newDirFilter(rules),
		plan:   watchPlan{partial: make(map[string]partialEntries)},
	}
	if w.walk(worktree, "") {
		return watchPlan{roots: []string{worktree}}
	}
	slices.Sort(w.plan.roots)
	return w.plan
}

type planner struct {
	filter dirFilter
	plan   watchPlan
}

// walk reports whether the tree at dir has no excluded directory, adding the
// roots and partial directories that cover it to the plan when it does. A
// directory that cannot be read counts as clean: gopls watches what it can.
func (w *planner) walk(dir, rel string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return true
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
				w.walkExcluded(child, childRel)
			}
		case w.walk(child, childRel):
			clean = append(clean, child)
		default:
			partial = true
		}
	}
	if !partial {
		return true
	}
	w.plan.roots = append(w.plan.roots, clean...)
	w.plan.partial[dir] = partialEntries{names: relevantNames(entries, rel, w.filter, true), files: true}
	return false
}

// walkExcluded looks inside an excluded directory for descendants a later +
// rule includes again; the caller has checked that one may exist. Only its
// subdirectories are recorded, those that may hold such a descendant, since a
// new one may: its own files are excluded, so a change among them, or a new
// subdirectory no + rule can reach, must not condemn a gopls.
func (w *planner) walkExcluded(dir, rel string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || skippedByWatcher(e.Name()) {
			continue
		}
		child, childRel := filepath.Join(dir, e.Name()), path.Join(rel, e.Name())
		if !w.filter.include(childRel) {
			if w.filter.mayInclude(childRel) {
				w.walkExcluded(child, childRel)
			}
		} else if w.walk(child, childRel) {
			w.plan.roots = append(w.plan.roots, child)
		}
	}
	w.plan.partial[dir] = partialEntries{names: relevantNames(entries, rel, w.filter, false)}
}

// relevantNames lists the entries of a partial directory whose appearance or
// disappearance gopls would have needed to hear about, subdirectories with a
// trailing slash. With files set (a directory that is itself included) that is
// its included subdirectories and watched files; without (an excluded one), the
// subdirectories a + rule may reach (dirFilter.worthListing).
func relevantNames(entries []os.DirEntry, rel string, filter dirFilter, files bool) []string {
	names := []string{}
	for _, e := range entries {
		name := e.Name()
		switch {
		case e.IsDir():
			if !skippedByWatcher(name) && (files && filter.include(path.Join(rel, name)) || !files && filter.worthListing(path.Join(rel, name))) {
				names = append(names, name+"/")
			}
		case files && e.Type().IsRegular() && watchedFile(name):
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// rememberWatch records the plan just reported for worktree. Nil-safe: routers
// built in tests may have no manager.
func (m *manager) rememberWatch(worktree string, plan watchPlan) {
	if m == nil || m.watch == nil {
		return
	}
	m.watch.mu.Lock()
	defer m.watch.mu.Unlock()
	if len(plan.partial) == 0 {
		delete(m.watch.plans, worktree)
		return
	}
	m.watch.plans[worktree] = watchSnapshot{partial: plan.partial, taken: time.Now()}
}

// watchStale reports whether r's gopls has missed a change no root of its plan
// covers: a partial directory whose relevant entries differ from the snapshot,
// or a module file in one modified after the server started. Only a snapshot
// taken since r started describes r's watcher; an older one belongs to a server
// this manager reported roots to before. A stale verdict drops the snapshot, so
// one change condemns one server.
func (m *manager) watchStale(r record) bool {
	snapshot, stale := m.watchChanged(r)
	if stale {
		m.forgetWatch(r.Worktree, snapshot)
	}
	return stale
}

// watchChanged is watchStale without dropping the snapshot, for a caller that
// may still spare the server (a busy worktree) and so must not use the change
// up: it returns the snapshot to hand to forgetWatch once r is condemned.
func (m *manager) watchChanged(r record) (watchSnapshot, bool) {
	if m.watch == nil {
		return watchSnapshot{}, false
	}
	m.watch.mu.Lock()
	snapshot, ok := m.watch.plans[r.Worktree]
	m.watch.mu.Unlock()
	if !ok || snapshot.taken.Unix() < r.StartedAt {
		return watchSnapshot{}, false
	}
	filter := m.watchFilter(r.Worktree)
	for dir, want := range snapshot.partial {
		if partialChanged(r, dir, want, filter) {
			return snapshot, true
		}
	}
	return watchSnapshot{}, false
}

// forgetWatch drops snapshot, unless a newer one has replaced it meanwhile.
func (m *manager) forgetWatch(worktree string, snapshot watchSnapshot) {
	m.watch.mu.Lock()
	defer m.watch.mu.Unlock()
	if current, ok := m.watch.plans[worktree]; ok && current.taken.Equal(snapshot.taken) {
		delete(m.watch.plans, worktree)
	}
}

// partialChanged reports whether partial directory dir of r's worktree no
// longer holds the relevant entries it did, or holds a module file modified
// after r started. A record from before StartedAt existed skips the mtime half.
// A directory that cannot be read is the worktree-reaping sweep's business.
func partialChanged(r record, dir string, want partialEntries, filter dirFilter) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	rel, _ := filepath.Rel(r.Worktree, dir)
	rel = strings.TrimPrefix(filepath.ToSlash(rel), ".")
	if !slices.Equal(want.names, relevantNames(entries, rel, filter, want.files)) {
		return true
	}
	if !want.files || r.StartedAt == 0 {
		return false
	}
	started := time.Unix(r.StartedAt, 0)
	for _, e := range entries {
		if !moduleFile(e.Name()) {
			continue
		}
		if info, err := e.Info(); err == nil && info.ModTime().After(started) {
			return true
		}
	}
	return false
}

// watchFilter rebuilds the directory filter for worktree, so that a new
// subdirectory is judged by the rules in force now.
func (m *manager) watchFilter(worktree string) dirFilter {
	return newDirFilter(append(moduleIgnoreFilters(worktree), m.limits.DirectoryFilters...))
}
