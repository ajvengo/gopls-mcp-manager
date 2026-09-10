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
		// Only this worker performs filesystem lookups. Its view shares memo
		// maps and their lock, never request tracking or lane state.
		view := &router{paths: r.paths, physical: r.physical, worktrees: r.worktrees, memoMu: r.memoMu, memo: r.memo, limits: r.limits}
		r.resolver = &pathResolver{jobs: make(chan resolution)}
		r.resolver.lookup = func(ctx context.Context, raw json.RawMessage) []string {
			view.ctx = ctx
			return view.toolCallWorktrees(raw)
		}
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
	var found []string
	add := func(path string) bool {
		if !filepath.IsAbs(path) {
			return true
		}
		worktree, ok := r.paths[path]
		if ok {
			r.memo.hits++
		} else {
			r.memo.misses++
		}
		if ok && !slices.Contains(found, worktree) {
			found = append(found, worktree)
		}
		return ok
	}
	if !add(args.File) || !add(args.Dir) {
		return nil, false
	}
	for _, path := range args.Files {
		if !add(path) {
			return nil, false
		}
	}
	return found, true
}
