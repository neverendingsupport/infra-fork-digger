package background

import (
	"context"
	"testing"
	"time"
)

func TestWaitReturnsWhenWorkFinishes(t *testing.T) {
	release := make(chan struct{})
	Go(context.Background(), func(context.Context) { <-release })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if n := Wait(ctx); n != 1 {
		t.Fatalf("Wait with work running = %d, want 1", n)
	}

	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if n := Wait(ctx); n != 0 {
		t.Fatalf("Wait after work finished = %d, want 0", n)
	}
}

func TestWaitWithNoWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n := Wait(ctx); n != 0 {
		t.Fatalf("Wait with no work = %d, want 0", n)
	}
}

func TestGoKeepsValuesButNotCancellation(t *testing.T) {
	type key struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "v"))
	cancel()
	got := make(chan context.Context, 1)
	Go(ctx, func(ctx context.Context) { got <- ctx })
	c := <-got
	if v := c.Value(key{}); v != "v" {
		t.Fatalf("context value = %v, want v", v)
	}
	if c.Err() != nil {
		t.Fatalf("context err = %v, want nil", c.Err())
	}
	Wait(context.Background())
}
