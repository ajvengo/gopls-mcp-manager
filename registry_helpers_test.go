package main

import (
	"sync"
)

// cleanRecords drops the records whose server is gone. Every record is offered
// to alive, which reaps what it rejects, so none may be skipped — including a
// second record for a worktree ensure will never answer with, since the record
// is the only handle on that process and its port really is taken.
//
// The probes run at once, so alive has to be safe to call from several
// goroutines. Every caller holds the map's exclusive flock across this and a
// probe waits out its own timeout, so in sequence a few servers too busy to
// answer would keep every other invocation of this tool — and so every other
// worktree's next tool call — waiting for the sum of them. Nothing bounds the
// fan-out because nothing needs to: the records are one per live gopls, and a
// machine that could run enough of those to matter here has already run out of
// memory.
func cleanRecords(records []record, alive func(record) bool) []record {
	live := make([]bool, len(records))
	var probes sync.WaitGroup
	for i, r := range records {
		probes.Go(func() { live[i] = alive(r) })
	}
	probes.Wait()

	kept := make([]record, 0, len(records))
	for i, r := range records {
		if live[i] {
			kept = append(kept, r)
		}
	}
	return kept
}
