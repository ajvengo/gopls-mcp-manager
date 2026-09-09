package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

type pathArguments struct {
	File  string   `json:"file"`
	Dir   string   `json:"dir"`
	Files []string `json:"files"`
}

func parsePathArguments(params json.RawMessage) pathArguments {
	var call struct {
		Arguments pathArguments `json:"arguments"`
	}
	if json.Unmarshal(params, &call) != nil {
		return pathArguments{}
	}
	return call.Arguments
}

// toolCallWorktrees reports the worktrees owning the path arguments of a
// tools/call, in the order the arguments name them and without repeats. A call
// naming no path we can resolve reports none.
//
// Every path is resolved rather than just the first, because more than one
// answer is not a tie to be broken: tools like go_diagnostics take a files
// array, no single gopls knows a tree it was not started for, and routing such
// a call by its first path answers about that worktree while saying nothing at
// all about the files in the other — an omission the client cannot see, since
// what comes back is a well-formed result for the files it did cover. Both are
// reported so that route can refuse it instead. The extra work is a memo lookup
// per path on the hit that already covers the routing path (see worktreeOf).
func (r *router) toolCallWorktrees(params json.RawMessage) []string {
	args := parsePathArguments(params)
	// The scalar arguments are ranged over as an array rather than concatenated
	// with Files into one slice: this runs on the goroutine that routes every
	// worktree's calls, and the combined slice was a heap allocation on every
	// tools/call — including the overwhelmingly common one naming a single file.
	var found []string
	for _, path := range [...]string{args.File, args.Dir} {
		found = r.appendWorktreeOf(found, path)
	}
	for _, path := range args.Files {
		found = r.appendWorktreeOf(found, path)
	}
	return found
}

// appendWorktreeOf adds the worktree owning path to found, unless the path is
// no evidence or names a worktree already there.
func (r *router) appendWorktreeOf(found []string, path string) []string {
	if r.ctx.Err() != nil {
		return found
	}
	// An absent argument is no evidence, and a relative one is worse: it would
	// resolve against this process's own cwd — the home worktree — and beat the
	// sticky routing that was right, for every worktree but home. gopls's
	// schemas ask for absolute paths.
	if !filepath.IsAbs(path) {
		return found
	}
	worktree := r.worktreeOf(path)
	if worktree == "" || slices.Contains(found, worktree) {
		return found
	}
	return append(found, worktree)
}

// worktreeOf resolves one path argument, memoizing the answer under the
// directory holding it rather than the path itself: resolution shells out to
// git at ~13ms a call, every path in one directory has the same answer, and a
// session names many files under the same handful of directories. Only
// successes are cached, so a path that becomes resolvable later still gets its
// own lookup; a worktree removed mid-session keeps answering until the routed
// gopls reports the path itself.
//
// The verbatim path is memoized in front of that, because reaching the
// directory memo is not free: containingDir lstats every component of the
// argument, and a profile put two thirds of the routing path there — time the
// reader goroutine spends routing nobody else's call. Kept as a second map so
// that a path-cache rollover need not discard cached directory resolutions.
func (r *router) worktreeOf(path string) string {
	r.memoMu.Lock()
	if worktree, ok := r.paths[path]; ok {
		r.memoMu.Unlock()
		return worktree
	}
	r.memoMu.Unlock()
	dir := containingDir(path)
	r.memoMu.Lock()
	worktree, ok := r.worktrees[dir]
	r.memoMu.Unlock()
	if !ok {
		var err error
		if worktree, err = worktreeOfDir(r.ctx, dir); err != nil {
			return ""
		}
		r.memoMu.Lock()
		r.memoize(r.worktrees, dir, worktree)
		r.memoMu.Unlock()
	}
	r.memoMu.Lock()
	r.memoize(r.paths, path, worktree)
	r.memoMu.Unlock()
	return worktree
}

// Clear at capacity instead of retaining a second eviction index. A discarded
// entry is resolved afresh, including symlinks; failures remain uncached.
// Caller holds memoMu. Each of the two caches has its own entry bound.
func (r *router) memoize(cache map[string]string, key, value string) {
	limit := r.limits.CacheEntries
	if limit <= 0 {
		limit = defaultCallLimits().CacheEntries
	}
	if len(cache) >= limit {
		clear(cache)
	}
	cache[key] = value
}

// containingDir is input itself when it names a directory, and its parent
// otherwise — including when it cannot be stat'd at all, since a path we were
// handed and cannot see is a file far more often than a directory.
//
// Cleaned either way, because this is a memo key: a dir argument spelled with a
// trailing separator names the same directory as one without, and must not fork
// git twice. filepath.Dir cleans on its own.
//
// Symlinks are resolved first, because the reduction is lexical: a link to a
// file in another worktree is not a directory, so its parent would be the
// link's own and the named tree would never be asked. Resolving also makes the
// key physical, which lets two spellings of one tree share an entry (R4, R5).
//
// A failure leaves input alone: EvalSymlinks needs every component to exist,
// and a path that does not exist yet must still resolve through its parent
// (R2) — a file created moments ago is the ordinary case.
func containingDir(input string) string {
	resolved, err := filepath.EvalSymlinks(input)
	if err == nil {
		input = resolved
	}
	if info, err := os.Stat(input); err == nil && info.IsDir() {
		return filepath.Clean(input)
	}
	dir := filepath.Dir(input)
	// A missing file cannot be resolved as a whole, but its existing parent
	// still has a physical spelling. Never climb past a missing parent (R2).
	if err != nil {
		if resolved, err := filepath.EvalSymlinks(dir); err == nil {
			return resolved
		}
	}
	return dir
}

// worktreePath resolves a file or directory to the root of the worktree holding
// it. A path that does not exist, or is in no repository, resolves to an error:
// callers routing a tool call treat that as "no evidence, keep the current
// server", which then reports its own error for the path.
//
// The reduction to a directory happens here and in worktreeOf, which needs it
// for its memo key anyway — never twice on one argument: a second pass over a
// directory that does not exist climbs to its parent, resolving a path against
// a tree the caller never named. worktreeOfDir is what makes that unsayable.
func worktreePath(ctx context.Context, input string) (string, error) {
	return worktreeOfDir(ctx, containingDir(input))
}

// worktreeOfDir resolves a directory — never a file — to the root of the
// worktree holding it.
//
// The context is the caller's own — the session's on the routing path — so that
// a git parked on a dead mount is cancelled when the session ends rather than
// outliving it: this runs on the goroutine that reads the client, and a hang
// here is a teardown that waits out the timeout below with nothing left to
// serve. The timeout stays on top of it, because the caller's context may have
// no deadline at all.
func worktreeOfDir(ctx context.Context, dir string) (string, error) {
	// Generous, since rev-parse reads no tree: only a hang should ever reach it —
	// the same failure H5 bounds for the handshake.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// git -C resolves its own cwd physically, so the toplevel it prints already
	// has the symlinks taken out of it — and a relative dir resolves against
	// this process's cwd, which is what the "." default means.
	out, err := exec.CommandContext(ctx, "git", "-C", dir, "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse --show-toplevel: %w", err)
	}
	worktree, err := filepath.EvalSymlinks(strings.TrimSpace(string(out)))
	if err != nil {
		return "", err
	}
	// Refused at the one place a worktree is minted, so no port is allocated and
	// no gopls spawned for a path the map cannot hold: a Unix path is arbitrary
	// bytes, but the map file is JSON, and json.Marshal replaces invalid UTF-8
	// with U+FFFD rather than failing (§10). writeMap refuses it too — this is
	// what keeps it from ever getting that far.
	if !utf8.ValidString(worktree) {
		return "", fmt.Errorf("worktree path is not valid UTF-8: %q", worktree)
	}
	return worktree, nil
}
