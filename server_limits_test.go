package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
)

func TestSharedServerCapSerializesConcurrentManagers(t *testing.T) {
	t.Parallel()
	first := newTestManager(t)
	first.limits.SharedServers = 1
	var starts atomic.Int32
	first.start = func(context.Context, string, int) (*childProcess, error) {
		starts.Add(1)
		return &childProcess{Process: &os.Process{Pid: 12345}, done: make(chan struct{})}, nil
	}
	first.alive = func(context.Context, record) probeVerdict { return probeLive }
	second := first // independent manager, shared registry and admission policy
	done := make(chan error, 2)
	for i, m := range []*manager{&first, &second} {
		go func() {
			_, _, err := m.claimPort(t.Context(), fmt.Sprintf("/repo/%d", i))
			done <- err
		}()
	}
	var rejected int
	for range 2 {
		if err := mustRecv(t, done, "manager claim did not finish"); err != nil {
			rejected++
		}
	}
	if rejected != 1 || starts.Load() != 1 {
		t.Fatalf("capacity race: rejected=%d starts=%d", rejected, starts.Load())
	}
	records, _, err := readMap(first.mapPath)
	if err != nil || len(records) != 1 {
		t.Fatalf("registry = %+v, %v", records, err)
	}
	got, child, err := first.claimPort(t.Context(), records[0].Worktree)
	if err != nil || child != nil || got != records[0] || starts.Load() != 1 {
		t.Fatalf("existing endpoint not reusable at capacity: %+v, %v", got, err)
	}
}
