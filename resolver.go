package main

import (
	"context"
	"encoding/json"
	"fmt"
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

func (r *router) resolveWorktrees(params json.RawMessage) ([]string, error) {
	if r.resolver == nil {
		// The resolver alone owns the memos after this point. A small routing
		// view shares only those maps, never the router's mutex or lane state.
		view := &router{paths: r.paths, worktrees: r.worktrees}
		r.resolver = &pathResolver{jobs: make(chan resolution)}
		r.resolver.lookup = func(ctx context.Context, raw json.RawMessage) []string {
			view.ctx = ctx
			return view.toolCallWorktrees(raw)
		}
		go r.resolver.run(r.ctx)
	}
	ctx, cancel := context.WithTimeout(r.ctx, routingBudget)
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
