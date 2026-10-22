// Package background tracks work that outlives the request that started it,
// so the server can wait for it before exiting.
package background

import (
	"context"
	"sync"

	"github.com/diggerhq/digger/backend/logging"
)

// A WaitGroup would panic if a late request calls Go while Wait runs after a
// shutdown timeout, so this counts by hand.
var (
	mu      sync.Mutex
	pending int
	idle    = closedChannel()
)

func closedChannel() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

// Go runs fn in a goroutine that Wait waits for. fn gets ctx's values and
// request logger but not its cancellation, because the work outlives the
// request.
func Go(ctx context.Context, fn func(context.Context)) {
	ctx = context.WithoutCancel(ctx)
	mu.Lock()
	if pending == 0 {
		idle = make(chan struct{})
	}
	pending++
	mu.Unlock()

	go func() {
		defer finish()
		defer logging.InheritRequestLogger(ctx)()
		fn(ctx)
	}()
}

func finish() {
	mu.Lock()
	defer mu.Unlock()
	pending--
	if pending == 0 {
		close(idle)
	}
}

// Wait blocks until no work started with Go is running, or ctx is done. It
// returns the number of goroutines still running.
func Wait(ctx context.Context) int {
	mu.Lock()
	done := idle
	mu.Unlock()

	select {
	case <-done:
		return 0
	case <-ctx.Done():
		mu.Lock()
		defer mu.Unlock()
		return pending
	}
}
