package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"time"
)

const routingBudget = 10 * time.Second

type resolution struct {
	ctx    context.Context
	params json.RawMessage
	reply  chan []string
}

// One worker, no buffered jobs: a filesystem call that the OS cannot cancel
// occupies at most this worker. A timed-out job cannot commit sticky state.
// Work is committed on the reader in wire order, preserving pathless routing
// and cancellation-after-admission semantics.
type pathResolver struct {
	jobs   chan resolution
	lookup func(context.Context, json.RawMessage) []string
}

func (p *pathResolver) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-p.jobs:
			if job.ctx.Err() != nil {
				continue
			}
			job.reply <- p.lookup(job.ctx, job.params)
		}
	}
}

func (r *router) resolveWorktrees(parent context.Context, params json.RawMessage) ([]string, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if found, complete := r.cachedWorktrees(params); complete {
		return found, parent.Err()
	}
	if r.resolver == nil {
		// Unsynchronised, and safe for the same reason the worker is single:
		// resolveWorktrees has one caller per router — the stdio reader, or the
		// HTTP backend's one cold-resolution goroutine — so the first miss of a
		// session is the only thing that ever reaches this branch. Built here
		// rather than in newRouter so a session that never misses the memo, which
		// is most of them, starts no goroutine at all.
		//
		// Only this worker performs filesystem lookups. It touches the memo maps
		// and their lock, never request tracking or lane state — and it runs
		// under the job's own context, not the session's, so a routing budget
		// that expires cuts the lookup it paid for and no other.
		r.resolver = &pathResolver{jobs: make(chan resolution), lookup: r.toolCallWorktrees}
		go r.resolver.run(r.ctx)
	}
	ctx, cancel := context.WithTimeout(parent, routingBudget)
	defer cancel()
	job := resolution{ctx: ctx, params: params, reply: make(chan []string, 1)}
	select {
	case r.resolver.jobs <- job:
	case <-ctx.Done():
		return nil, fmt.Errorf("routing budget exhausted: %w", ctx.Err())
	}
	select {
	case found := <-job.reply:
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("routing budget exhausted: %w", err)
		}
		return found, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("routing budget exhausted: %w", ctx.Err())
	}
}

// Parsing and memo hits never wait for a filesystem syscall held by the worker.
func (r *router) cachedWorktrees(params json.RawMessage) ([]string, bool) {
	args := parsePathArguments(params)
	r.memoMu.Lock()
	defer r.memoMu.Unlock()
	r.expireMemosLocked()
	// Written out rather than as a closure over found, for the reason
	// toolCallWorktrees gives: this runs on the goroutine that routes every
	// worktree's calls, and a closure capturing found heap-allocates both on
	// each tools/call, memo hit included.
	var found []string
	var ok bool
	for _, path := range [...]string{args.File, args.Dir} {
		if found, ok = r.appendMemoizedWorktree(found, path); !ok {
			return nil, false
		}
	}
	for _, path := range args.Files {
		if found, ok = r.appendMemoizedWorktree(found, path); !ok {
			return nil, false
		}
	}
	return found, true
}

// appendMemoizedWorktree is appendWorktreeOf without the filesystem: it adds
// only what the memo already knows, and reports whether path needed nothing
// more. Caller holds memoMu.
func (r *router) appendMemoizedWorktree(found []string, path string) ([]string, bool) {
	if !filepath.IsAbs(path) {
		return found, true
	}
	memo, ok := r.paths[path]
	if !ok {
		r.memo.misses++
		return found, false
	}
	r.memo.hits++
	if !slices.Contains(found, memo.worktree) {
		found = append(found, memo.worktree)
	}
	return found, true
}
