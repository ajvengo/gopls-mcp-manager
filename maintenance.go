package main

import (
	"context"
	"errors"
	"os"
	"slices"
	"syscall"
	"time"
)

// sweepMaintenance reaps servers whose worktree is gone and evicts those over
// the open-file budget. Both reach a signal only through identityVerdict and
// withRecords' persisted termination, like any other condemned record.
func (m *manager) sweepMaintenance(ctx context.Context) ([]record, error) {
	victims, err := m.evictionVictims(ctx)
	if err != nil {
		return nil, err
	}
	maintenance := *m
	maintenance.alive = func(ctx context.Context, r record) probeVerdict {
		verdict := m.alive(ctx, r)
		if verdict != probeLive || withinStartGrace(r) {
			return verdict
		}
		// Asked again just before the verdict: a call may have reached this
		// server since the victims were picked.
		if slices.Contains(victims, r) {
			if busy, err := m.busyWorktrees(time.Now()); err == nil && !busy[r.Worktree] {
				return identityVerdict(ctx, r)
			}
		}
		info, err := os.Stat(r.Worktree)
		if err == nil && info.IsDir() {
			return verdict
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ENOTDIR) {
			return probeUncertain
		}
		return identityVerdict(ctx, r)
	}
	return maintenance.withRecords(ctx, keepRecords)
}

// A signal is not an available slot. Confirm exits before startup admission,
// but keep uncertain/slow processes recorded instead of exceeding the cap.
func (m *manager) prepareAdmission(ctx context.Context, worktree string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*exitGrace)
	defer cancel()
	for {
		records, err := m.sweepMaintenance(ctx)
		if err != nil {
			return err
		}
		pending := false
		requestedPending := false
		for _, r := range records {
			if r.Worktree == worktree && !r.Terminating {
				return nil
			}
			pending = pending || r.Terminating
			requestedPending = requestedPending || (r.Worktree == worktree && r.Terminating)
		}
		if !pending || (!requestedPending && len(records) < m.limits.SharedServers) {
			return nil
		}
		if err := waitContext(ctx, 20*time.Millisecond); err != nil {
			return err
		}
	}
}
