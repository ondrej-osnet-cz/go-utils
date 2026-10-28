// Command e2e-ui is an interactive browser for the E2E tests. It lists the
// tests, runs the one you click and shows its progress, output and every HTTP
// request and response it made, as recorded by package record.
//
//	go run ./cmd/e2e-ui    # then open http://127.0.0.1:9100
//
// It runs `go test` on request, so it only listens on a loopback address.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	defaultAddr     = "127.0.0.1:9100"
	defaultPackages = "./e2e/..."
	shutdownTimeout = 5 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	var addr, pkgs, dir string
	fs := flag.NewFlagSet("e2e-ui", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&addr, "addr", defaultAddr, "listen address, loopback only")
	fs.StringVar(&pkgs, "pkg", defaultPackages, "space separated package patterns with the tests")
	fs.StringVar(&dir, "dir", ".", "directory in which go test runs (the module root)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if !isLoopbackAddr(addr) {
		return fmt.Errorf("refusing to listen on %q: e2e-ui runs go test on request, use a loopback address", addr)
	}
	patterns := strings.Fields(pkgs)
	if len(patterns) == 0 {
		return errors.New("no package patterns")
	}

	log := slog.New(slog.NewJSONHandler(stderr, nil))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := newServer(ctx, goTool{dir: dir}, patterns, "http://"+ln.Addr().String(), log)
	srv := &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// Event streams end with ctx; otherwise Shutdown would wait for them.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	// Cancelling on return also stops the shutdown goroutine when Serve fails.
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil {
			log.Error("shutdown", "err", err)
		}
	}()

	log.Info("listening", "addr", ln.Addr().String(), "packages", patterns)
	if _, err := fmt.Fprintf(stdout, "E2E UI on http://%s\n", ln.Addr()); err != nil {
		log.Warn("announce listen address", "err", err)
	}
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	<-done
	s.wait()
	return nil
}

// isLoopbackAddr reports whether addr only accepts connections from this machine.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	return err == nil && isLoopbackHost(host)
}
