package transport

import (
	"errors"
	"sync"
)

// ErrMessageTooLarge reports a wire value or event exceeding its configured limit.
var ErrMessageTooLarge = errors.New("MCP message exceeds configured byte limit")

// Budget charges allocated frame capacity, not only payload length. Exhaustion rejects
// the stream instead of deadlocking several partially assembled events.
type Budget struct {
	mu                sync.Mutex
	limit, used, peak int
}

// NewBudget creates a shared capacity budget. Nonpositive limits admit no frames.
func NewBudget(limit int) *Budget { return &Budget{limit: limit} }

// Snapshot returns current and peak live frame capacity in bytes.
func (b *Budget) Snapshot() (used, peak int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.peak
}

func (b *Budget) acquire(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.limit-b.used {
		return false
	}
	b.used += n
	b.peak = max(b.peak, b.used)
	return true
}

func (b *Budget) release(n int) {
	b.mu.Lock()
	b.used -= n
	b.mu.Unlock()
}
