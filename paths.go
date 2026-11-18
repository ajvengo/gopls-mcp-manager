package main

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/ajvengo/gopls-mcp-manager/internal/protocol"
)

type pathArguments struct {
	File  string   `json:"file"`
	Dir   string   `json:"dir"`
	Files []string `json:"files"`
}

// Whole-memo epochs avoid a timestamp or eviction node for every path. Both
// levels expire together so a directory hit cannot renew a stale path forever.
type memoState struct {
	expires                              time.Time
	generation                           uint64
	hits, misses, expirations, rollovers int64
	// Skip the rewrite scan until a lookup has found a physical alias (R10).
	aliased atomic.Bool
}

// Caller holds memoMu. Expiry is lazy; metrics snapshots also sweep idle memos.
func (r *router) expireMemosLocked() {
	now := time.Now()
	if !r.memo.expires.IsZero() && now.Before(r.memo.expires) {
		return
	}
	if !r.memo.expires.IsZero() {
		clear(r.paths)
		clear(r.worktrees)
		r.memo.expirations++
	}
	r.memo.expires = now.Add(r.limits.CacheTTL)
	r.memo.generation++
}

func parsePathArguments(params jsontext.Value) pathArguments {
	var call struct {
		Arguments pathArguments `json:"arguments"`
	}
	if json.Unmarshal(params, &call) != nil {
		return pathArguments{}
	}
	return call.Arguments
}

// Resolve every path so routing can reject a call spanning worktrees (R1).
func (r *router) toolCallWorktrees(ctx context.Context, params jsontext.Value) []string {
	args := parsePathArguments(params)
	// Keep scalar arguments separate to avoid allocating a combined slice.
	var found []string
	for _, path := range [...]string{args.File, args.Dir} {
		found = r.appendWorktreeOf(ctx, found, path)
	}
	for _, path := range args.Files {
		found = r.appendWorktreeOf(ctx, found, path)
	}
	return found
}

func (r *router) appendWorktreeOf(ctx context.Context, found []string, path string) []string {
	if ctx.Err() != nil {
		return found
	}
	// Relative paths would resolve against the manager's cwd and override sticky routing.
	if !filepath.IsAbs(path) {
		return found
	}
	worktree := r.worktreeOf(ctx, path)
	if worktree == "" || slices.Contains(found, worktree) {
		return found
	}
	return append(found, worktree)
}

// Keep routing and rewriting in one memo entry so they expire together (R10).
type pathMemo struct {
	worktree string
	physical string
}

// The verbatim path memo avoids filesystem calls. The physical directory memo
// shares Git lookups across paths. Cache only successful resolutions (R5).
func (r *router) worktreeOf(ctx context.Context, path string) string {
	r.memoMu.Lock()
	r.expireMemosLocked()
	generation := r.memo.generation
	if memo, ok := r.paths[path]; ok {
		r.memoMu.Unlock()
		return memo.worktree
	}
	r.memoMu.Unlock()
	dir, physical := containingDir(path)
	if physical == path {
		physical = "" // nothing to substitute
	}
	r.memoMu.Lock()
	worktree, ok := r.worktrees[dir]
	r.memoMu.Unlock()
	if !ok {
		var err error
		if worktree, err = worktreeOfDir(ctx, dir); err != nil {
			return ""
		}
	}
	r.memoMu.Lock()
	// A cancelled/slow lookup must not repopulate a newer cache epoch.
	r.expireMemosLocked()
	if ctx.Err() == nil && generation == r.memo.generation {
		memoize(r, r.worktrees, dir, worktree)
		memoize(r, r.paths, path, pathMemo{worktree: worktree, physical: physical})
		if physical != "" {
			r.memo.aliased.Store(true)
		}
	}
	r.memoMu.Unlock()
	return worktree
}

// Clear at capacity instead of retaining a second eviction index. A discarded
// entry is resolved afresh, including symlinks; failures remain uncached.
// Caller holds memoMu. Each of the two caches has its own entry bound.
func memoize[V any](r *router, cache map[string]V, key string, value V) {
	if _, exists := cache[key]; !exists && len(cache) >= r.limits.CacheEntries {
		clear(cache)
		r.memo.rollovers++
	}
	cache[key] = value
}

// Resolve symlinks before selecting the directory memo key so a file symlink
// routes to its target's worktree. Return the physical input for R10.
func containingDir(input string) (dir, physical string) {
	physical, err := filepath.EvalSymlinks(input)
	if err == nil {
		input = physical
	}
	if info, err := os.Stat(input); err == nil && info.IsDir() {
		return filepath.Clean(input), physical
	}
	dir = filepath.Dir(input)
	// A missing file inherits its immediate parent's physical spelling (R2).
	if err != nil {
		if resolved, err := filepath.EvalSymlinks(dir); err == nil {
			return resolved, filepath.Join(resolved, filepath.Base(input))
		}
	}
	return dir, physical
}

// Select the directory once. Repeating the reduction could climb past a
// missing parent and route into a worktree the caller never named.
func worktreePath(ctx context.Context, input string) (string, error) {
	dir, _ := containingDir(input)
	return worktreeOfDir(ctx, dir)
}

func worktreeOfDir(ctx context.Context, dir string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// Git reports the physical worktree root.
	out, err := boundedCommandOutput(ctx, 64<<10, "git", "-C", dir, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", fmt.Errorf("git rev-parse --show-toplevel: %w", err)
	}
	worktree, err := filepath.EvalSymlinks(strings.TrimSpace(string(out)))
	if err != nil {
		return "", err
	}
	// Reject paths the JSON registry cannot represent before reserving capacity.
	if !utf8.ValidString(worktree) {
		return "", fmt.Errorf("worktree path is not valid UTF-8: %q", worktree)
	}
	return worktree, nil
}

// Rewrite only memoized aliases to keep filesystem waits off this path (R10).
func (r *router) canonicalToolCall(params jsontext.Value) (jsontext.Value, bool) {
	if !r.memo.aliased.Load() {
		return params, false
	}
	file, dir, files := r.physicalSpellings(parsePathArguments(params))
	return protocol.WithPhysicalPaths(params, file, dir, files)
}

// Reuse the routing epoch. A concurrent sweep may leave the input unchanged.
func (r *router) physicalSpellings(args pathArguments) (file, dir string, files []string) {
	r.memoMu.Lock()
	defer r.memoMu.Unlock()
	file, dir = r.paths[args.File].physical, r.paths[args.Dir].physical
	for i, path := range args.Files {
		if physical := r.paths[path].physical; physical != "" {
			if files == nil {
				files = slices.Clone(args.Files)
			}
			files[i] = physical
		}
	}
	return file, dir, files
}
