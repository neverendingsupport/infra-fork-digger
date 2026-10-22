package bootstrap

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/background"
)

func listen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestRunWaitsForRequestsAndBackgroundWork(t *testing.T) {
	var finished atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		background.Go(r.Context(), func(context.Context) {
			<-release
			finished.Add(1)
		})
		close(started)
		<-release
		finished.Add(1)
	})

	l := listen(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, &http.Server{Handler: mux}, l, 5*time.Second) }()

	reqErr := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + l.Addr().String() + "/")
		if err == nil {
			resp.Body.Close()
		}
		reqErr <- err
	}()
	select {
	case <-started:
	case err := <-reqErr:
		t.Fatalf("request failed before the handler ran: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("handler never ran")
	}
	cancel()

	select {
	case <-done:
		t.Fatal("run returned while a request and its background work were running")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("run = %v, want nil", err)
	}
	if n := finished.Load(); n != 2 {
		t.Fatalf("finished = %d, want 2", n)
	}
}

func TestRunGivesUpAfterTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() {
		close(release)
		background.Wait(context.Background())
	})
	background.Go(context.Background(), func(context.Context) { <-release })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, &http.Server{Handler: http.NewServeMux()}, listen(t), 50*time.Millisecond) }()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run didn't return after its shutdown timeout")
	}
}

func TestRunReturnsServeErrors(t *testing.T) {
	l := listen(t)
	l.Close()
	if err := run(context.Background(), &http.Server{}, l, time.Second); err == nil {
		t.Fatal("run on a closed listener = nil, want an error")
	}
}

func TestShutdownTimeout(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"", defaultShutdownTimeout},
		{"45s", 45 * time.Second},
		{"30", defaultShutdownTimeout},
		{"-1s", defaultShutdownTimeout},
	} {
		t.Setenv("DIGGER_SHUTDOWN_TIMEOUT", tc.value)
		if got := shutdownTimeout(); got != tc.want {
			t.Errorf("shutdownTimeout with %q = %v, want %v", tc.value, got, tc.want)
		}
	}
}
