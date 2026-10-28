package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/ondrej-osnet-cz/go-utils/e2eui/record"
)

const (
	historyLimit   = 50_000
	maxIngestBody  = 8 << 20
	maxRequestBody = 64 << 10
)

//go:embed ui/index.html
var indexHTML []byte

// testRunner lists and runs tests. goTool is the real one.
type testRunner interface {
	List(ctx context.Context, pkgs []string) ([]pkgTests, error)
	Run(ctx context.Context, pkg, pattern string, env []string, report func(testEvent)) error
}

var errBusy = errors.New("a run is already in progress")

// server runs one test run at a time and streams its progress to browsers.
type server struct {
	ctx        context.Context // cancelled on shutdown; stops the active run
	runner     testRunner
	patterns   []string // package patterns to list, e.g. ./e2e/...
	ingestBase string   // base URL of this server, as the tests reach it
	log        *slog.Logger
	hub        *hub
	wg         sync.WaitGroup

	mu     sync.Mutex
	known  []string // packages of the last listing
	cancel context.CancelFunc
	runs   int
}

func newServer(ctx context.Context, runner testRunner, patterns []string, ingestBase string, log *slog.Logger) *server {
	return &server{ctx: ctx, runner: runner, patterns: patterns, ingestBase: ingestBase, log: log, hub: newHub(historyLimit)}
}

// wait blocks until the active run, if any, has finished.
func (s *server) wait() { s.wg.Wait() }

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/tests", s.listTests)
	mux.HandleFunc("GET /api/events", s.events)
	mux.HandleFunc("POST /api/run", s.run)
	mux.HandleFunc("POST /api/stop", s.stop)
	mux.HandleFunc("POST /api/ingest", s.ingest)
	return localOnly(mux)
}

// localOnly rejects requests that a web page from elsewhere could make: the
// server runs go test on request. A foreign Host means DNS rebinding, a
// foreign Origin a cross-site request, and requiring a JSON body on POST
// forces a CORS preflight that this server never answers.
func localOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isLoopbackHost(r.Host) {
			writeError(w, http.StatusForbidden, "host %q is not a loopback address", r.Host)
			return
		}
		if o := r.Header.Get("Origin"); o != "" {
			if u, err := url.Parse(o); err != nil || !isLoopbackHost(u.Host) {
				writeError(w, http.StatusForbidden, "origin %q is not allowed", o)
				return
			}
		}
		if r.Method == http.MethodPost {
			if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
				writeError(w, http.StatusUnsupportedMediaType, "want Content-Type application/json")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// isLoopbackHost reports whether hostport (with an optional port) names this machine.
func isLoopbackHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *server) listTests(w http.ResponseWriter, r *http.Request) {
	list, err := s.refreshList(r.Context())
	if err != nil {
		s.log.Error("list tests", "err", err)
		writeError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *server) refreshList(ctx context.Context) ([]pkgTests, error) {
	list, err := s.runner.List(ctx, s.patterns)
	if err != nil {
		return nil, err
	}
	known := make([]string, len(list))
	for i, p := range list {
		known[i] = p.Package
	}
	s.mu.Lock()
	s.known = known
	s.mu.Unlock()
	return list, nil
}

// runRequest selects what to run. An empty Package runs every known package,
// an empty Test the whole package. Tests runs several top-level tests of the
// package at once (a group in the page) instead of one Test.
type runRequest struct {
	Package string   `json:"package"`
	Test    string   `json:"test"`
	Tests   []string `json:"tests"`
}

// selection returns the tests to run; nil means all.
func (r runRequest) selection() ([]string, error) {
	switch {
	case r.Test != "" && len(r.Tests) > 0:
		return nil, errors.New("test and tests are exclusive")
	case r.Test != "":
		return []string{r.Test}, nil
	}
	for _, t := range r.Tests {
		if t == "" || strings.Contains(t, "/") {
			return nil, fmt.Errorf("tests must be top-level test names, got %q", t)
		}
	}
	return r.Tests, nil
}

func (s *server) run(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	tests, err := req.selection()
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	if req.Package == "" && tests != nil {
		writeError(w, http.StatusBadRequest, "a test needs its package")
		return
	}
	pkgs, err := s.packagesFor(r.Context(), req.Package)
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	id, err := s.start(pkgs, tests)
	if errors.Is(err, errBusy) {
		writeError(w, http.StatusConflict, "%v", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]int{"run": id})
}

// packagesFor resolves pkg against the listing, which it refreshes when pkg
// is not known yet. Only listed packages run, never an arbitrary path.
func (s *server) packagesFor(ctx context.Context, pkg string) ([]string, error) {
	s.mu.Lock()
	known := s.known
	s.mu.Unlock()
	if known == nil || (pkg != "" && !slices.Contains(known, pkg)) {
		if _, err := s.refreshList(ctx); err != nil {
			return nil, fmt.Errorf("list tests: %w", err)
		}
		s.mu.Lock()
		known = s.known
		s.mu.Unlock()
	}
	if pkg == "" {
		if len(known) == 0 {
			return nil, errors.New("no packages with tests")
		}
		return known, nil
	}
	if !slices.Contains(known, pkg) {
		return nil, fmt.Errorf("unknown package %q", pkg)
	}
	return []string{pkg}, nil
}

// start begins a run of tests (nil runs all) in the background and returns its ID.
func (s *server) start(pkgs, tests []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return 0, errBusy
	}
	s.runs++
	id := s.runs
	ctx, cancel := context.WithCancel(s.ctx)
	s.cancel = cancel
	s.hub.publish(event{Type: "run-start", Run: id, Packages: pkgs, Tests: tests})

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var runErr error
		for _, pkg := range pkgs {
			if ctx.Err() != nil {
				break
			}
			ingest := s.ingestBase + "/api/ingest?" + url.Values{"run": {strconv.Itoa(id)}, "pkg": {pkg}}.Encode()
			err := s.runner.Run(ctx, pkg, runPattern(tests), []string{record.URLEnv + "=" + ingest}, func(ev testEvent) {
				pkgName := ev.Package
				if pkgName == "" {
					pkgName = pkg
				}
				s.hub.publish(event{Type: "test", Run: id, Package: pkgName, Test: ev.Test,
					Action: ev.Action, Elapsed: ev.Elapsed, Output: ev.Output})
			})
			if err != nil && ctx.Err() == nil {
				s.log.Error("run tests", "package", pkg, "err", err)
				runErr = errors.Join(runErr, err)
			}
		}
		stopped := ctx.Err() != nil

		// Free the slot before announcing the end, so the browser can start
		// the next run as soon as it sees run-end.
		s.mu.Lock()
		s.cancel = nil
		s.mu.Unlock()
		cancel()
		end := event{Type: "run-end", Run: id, Stopped: stopped}
		if runErr != nil {
			end.Error = runErr.Error()
		}
		s.hub.publish(end)
	}()
	return id, nil
}

func (s *server) stop(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// ingest receives one exchange recorded by package record in the test process.
func (s *server) ingest(w http.ResponseWriter, r *http.Request) {
	run, err := strconv.Atoi(r.URL.Query().Get("run"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid run")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxIngestBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body: %v", err)
		return
	}
	var ex struct {
		Test string `json:"test"`
	}
	if err := json.Unmarshal(body, &ex); err != nil {
		writeError(w, http.StatusBadRequest, "invalid exchange: %v", err)
		return
	}
	s.hub.publish(event{Type: "exchange", Run: run, Package: r.URL.Query().Get("pkg"), Test: ex.Test, Exchange: body})
	w.WriteHeader(http.StatusNoContent)
}

// events streams the history and then every new event as server-sent events.
// A reset event comes first, so a reconnecting page starts from scratch.
func (s *server) events(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	rc := http.NewResponseController(w)
	history, live, cancel := s.hub.subscribe()
	defer cancel()

	if _, err := io.WriteString(w, "event: reset\ndata: {}\n\n"); err != nil {
		return
	}
	for _, b := range history {
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return
		}
	}
	if err := rc.Flush(); err != nil {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case b, ok := <-live:
			if !ok {
				return
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, format string, args ...any) {
	writeJSON(w, status, map[string]string{"error": fmt.Sprintf(format, args...)})
}
