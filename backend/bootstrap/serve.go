package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/diggerhq/digger/backend/background"
	"github.com/getsentry/sentry-go"
	"github.com/gin-gonic/gin"
)

const defaultShutdownTimeout = 15 * time.Second

// Serve runs r on port until SIGTERM or SIGINT. It then stops accepting
// connections and waits, up to DIGGER_SHUTDOWN_TIMEOUT, for in-flight
// requests and the work they started with background.Go.
func Serve(r *gin.Engine, port int) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	// Restore default signal handling, so a second signal stops the process.
	context.AfterFunc(ctx, stop)

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		slog.Error("Could not listen", "port", port, "error", err)
		os.Exit(1)
	}
	slog.Info("Listening", "addr", l.Addr().String())

	srv := &http.Server{Handler: r.Handler(), ReadHeaderTimeout: 10 * time.Second}
	err = run(ctx, srv, l, shutdownTimeout())
	sentry.Flush(2 * time.Second)
	if err != nil {
		slog.Error("Server stopped", "error", err)
		os.Exit(1)
	}
}

// run serves on l until ctx is done, then shuts down within timeout. It
// returns an error only if the server fails before ctx is done.
func run(ctx context.Context, srv *http.Server, l net.Listener, timeout time.Duration) error {
	errs := make(chan error, 1)
	go func() { errs <- srv.Serve(l) }()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}

	slog.Info("Shutting down", "timeout", timeout)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("Requests still running at shutdown", "error", err)
	}
	if n := background.Wait(shutdownCtx); n > 0 {
		slog.Warn("Background work still running at shutdown", "count", n)
	} else {
		slog.Info("Shutdown complete")
	}
	return nil
}

func shutdownTimeout() time.Duration {
	v := os.Getenv("DIGGER_SHUTDOWN_TIMEOUT")
	if v == "" {
		return defaultShutdownTimeout
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		slog.Warn("Ignoring invalid DIGGER_SHUTDOWN_TIMEOUT", "value", v)
		return defaultShutdownTimeout
	}
	return d
}
