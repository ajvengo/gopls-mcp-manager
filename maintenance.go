package main

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"
)

func (m *manager) sweepMaintenance(ctx context.Context) ([]record, error) {
	maintenance := *m
	maintenance.alive = func(ctx context.Context, r record) probeVerdict {
		verdict := m.alive(ctx, r)
		if verdict != probeLive || withinStartGrace(r) {
			return verdict
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
