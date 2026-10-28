package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ondrej-osnet-cz/go-utils/e2eui/record"
)

// fakeRunner lists fixed packages. Run reports a pass for the pattern, or
// blocks until cancelled when block is set.
type fakeRunner struct {
	block bool

	mu    sync.Mutex
	calls []fakeCall
}

type fakeCall struct {
	pkg, pattern string
	env          []string
}

func (f *fakeRunner) List(context.Context, []string) ([]pkgTests, error) {
	return []pkgTests{{Package: "example.com/e2e", Tests: []string{"TestA", "TestB"}}}, nil
}

func (f *fakeRunner) Run(ctx context.Context, pkg, pattern string, env []string, report func(testEvent)) error {
	f.mu.Lock()
	f.calls = append(f.calls, fakeCall{pkg, pattern, env})
	f.mu.Unlock()
	report(testEvent{Action: "run", Package: pkg, Test: "TestA"})
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	elapsed := 0.5
	report(testEvent{Action: "pass", Package: pkg, Test: "TestA", Elapsed: &elapsed})
	return nil
}

func (f *fakeRunner) runs() []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCall(nil), f.calls...)
}

// startServer serves s on a loopback listener whose address is also the
// ingest base, the way main wires it.
func startServer(t *testing.T, runner testRunner, patterns ...string) (*server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	base := "http://" + ln.Addr().String()
	s := newServer(ctx, runner, patterns, base, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := &httptest.Server{Listener: ln, Config: &http.Server{Handler: s.handler(), ReadHeaderTimeout: 10 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}}
	ts.Start()
	t.Cleanup(func() {
		cancel()
		ts.Close()
		s.wait()
	})
	return s, base
}

// postJSON posts body and returns the response status.
func postJSON(t *testing.T, url, body string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// eventStream reads the server-sent events of /api/events.
type eventStream struct {
	sc *bufio.Scanner
}

func openEvents(t *testing.T, base string) *eventStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q, want text/event-stream", ct)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(nil, 16<<20)
	return &eventStream{sc: sc}
}

// until reads events up to and including the first one for which stop is
// true, and returns them all. The reset marker is skipped.
func (es *eventStream) until(t *testing.T, stop func(event) bool) []event {
	t.Helper()
	var got []event
	for es.sc.Scan() {
		data, ok := strings.CutPrefix(es.sc.Text(), "data: ")
		if !ok || data == "{}" {
			continue
		}
		var ev event
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			t.Fatalf("decode event %q: %v", data, err)
		}
		got = append(got, ev)
		if stop(ev) {
			return got
		}
	}
	t.Fatalf("event stream ended (%v) after %+v", es.sc.Err(), got)
	return nil
}

func isType(typ string) func(event) bool { return func(ev event) bool { return ev.Type == typ } }

func TestServer_RunsTestAndStreamsEvents(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{}
	_, base := startServer(t, runner, "./e2e/...")
	events := openEvents(t, base)

	if status := postJSON(t, base+"/api/run", `{"package":"example.com/e2e","test":"TestA"}`); status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", status)
	}
	got := events.until(t, isType("run-end"))

	var types []string
	for _, ev := range got {
		types = append(types, ev.Type+":"+ev.Action)
	}
	if want := "run-start: test:run test:pass run-end:"; strings.Join(types, " ") != want {
		t.Errorf("events = %q, want %q", strings.Join(types, " "), want)
	}
	if ev := got[2]; ev.Package != "example.com/e2e" || ev.Test != "TestA" || ev.Elapsed == nil || *ev.Elapsed != 0.5 {
		t.Errorf("pass event = %+v", ev)
	}
	calls := runner.runs()
	if len(calls) != 1 || calls[0].pattern != "^TestA$" || len(calls[0].env) != 1 ||
		!strings.HasPrefix(calls[0].env[0], record.URLEnv+"="+base+"/api/ingest?") {
		t.Errorf("runner calls = %+v", calls)
	}
}

func TestServer_RunsGroup(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{}
	_, base := startServer(t, runner)
	events := openEvents(t, base)

	if status := postJSON(t, base+"/api/run", `{"package":"example.com/e2e","tests":["TestA","TestB"]}`); status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", status)
	}
	got := events.until(t, isType("run-end"))
	if start := got[0]; start.Type != "run-start" || strings.Join(start.Tests, ",") != "TestA,TestB" {
		t.Errorf("run-start = %+v, want the group", start)
	}
	if calls := runner.runs(); len(calls) != 1 || calls[0].pattern != "^(?:TestA|TestB)$" {
		t.Errorf("runner calls = %+v", calls)
	}
}

func TestServer_HistoryIsReplayed(t *testing.T) {
	t.Parallel()
	_, base := startServer(t, &fakeRunner{})
	first := openEvents(t, base)
	postJSON(t, base+"/api/run", `{}`)
	first.until(t, isType("run-end"))

	// A page opened later sees the finished run.
	replayed := openEvents(t, base).until(t, isType("run-end"))
	if len(replayed) != 4 || replayed[0].Type != "run-start" {
		t.Errorf("replayed %+v, want the whole run", replayed)
	}
}

func TestServer_Ingest(t *testing.T) {
	t.Parallel()
	_, base := startServer(t, &fakeRunner{})
	events := openEvents(t, base)

	exchange := `{"test":"TestA/sub","request":{"method":"GET","url":"http://x/v1/health"}}`
	if status := postJSON(t, base+"/api/ingest?run=3&pkg=example.com/e2e", exchange); status != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", status)
	}

	ev := events.until(t, isType("exchange"))[0]
	if ev.Run != 3 || ev.Package != "example.com/e2e" || ev.Test != "TestA/sub" || string(ev.Exchange) != exchange {
		t.Errorf("exchange event = %+v", ev)
	}
}

func TestServer_StopCancelsRun(t *testing.T) {
	t.Parallel()
	_, base := startServer(t, &fakeRunner{block: true})
	events := openEvents(t, base)

	postJSON(t, base+"/api/run", `{}`)
	events.until(t, func(ev event) bool { return ev.Action == "run" }) // the runner is blocked now
	if status := postJSON(t, base+"/api/run", `{}`); status != http.StatusConflict {
		t.Errorf("second run: status = %d, want 409", status)
	}
	if status := postJSON(t, base+"/api/stop", `{}`); status != http.StatusNoContent {
		t.Errorf("stop: status = %d, want 204", status)
	}

	end := events.until(t, isType("run-end"))
	if ev := end[len(end)-1]; !ev.Stopped || ev.Error != "" {
		t.Errorf("run-end = %+v, want stopped without error", ev)
	}
	if status := postJSON(t, base+"/api/run", `{}`); status != http.StatusAccepted {
		t.Errorf("run after stop: status = %d, want 202", status)
	}
}

func TestServer_RejectsBadRequests(t *testing.T) {
	t.Parallel()
	_, base := startServer(t, &fakeRunner{})
	tests := []struct {
		name   string
		method string
		path   string
		header map[string]string
		body   string
		want   int
	}{
		{"unknown package", http.MethodPost, "/api/run", nil, `{"package":"./../evil"}`, http.StatusBadRequest},
		{"test without package", http.MethodPost, "/api/run", nil, `{"test":"TestA"}`, http.StatusBadRequest},
		{"tests without package", http.MethodPost, "/api/run", nil, `{"tests":["TestA"]}`, http.StatusBadRequest},
		{"test and tests", http.MethodPost, "/api/run", nil, `{"package":"example.com/e2e","test":"TestA","tests":["TestB"]}`, http.StatusBadRequest},
		{"subtest in tests", http.MethodPost, "/api/run", nil, `{"package":"example.com/e2e","tests":["TestA/x","TestB"]}`, http.StatusBadRequest},
		{"form post", http.MethodPost, "/api/run", map[string]string{"Content-Type": "text/plain"}, `{}`, http.StatusUnsupportedMediaType},
		{"foreign origin", http.MethodPost, "/api/run", map[string]string{"Origin": "https://evil.example"}, `{}`, http.StatusForbidden},
		{"dns rebinding", http.MethodGet, "/api/tests", map[string]string{"Host": "evil.example:9100"}, "", http.StatusForbidden},
		{"ingest without run", http.MethodPost, "/api/ingest", nil, `{}`, http.StatusBadRequest},
		{"ingest not json", http.MethodPost, "/api/ingest?run=1", nil, `nope`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequestWithContext(t.Context(), tt.method, base+tt.path, strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			for k, v := range tt.header {
				req.Header.Set(k, v)
			}
			if h, ok := tt.header["Host"]; ok {
				req.Host = h
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestServer_ServesPage(t *testing.T) {
	t.Parallel()
	_, base := startServer(t, &fakeRunner{})

	resp, err := http.Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "/api/events") {
		t.Errorf("GET / = %d, %d bytes; want the page", resp.StatusCode, len(body))
	}
}

func TestIsLoopbackAddr(t *testing.T) {
	t.Parallel()
	for addr, want := range map[string]bool{
		"127.0.0.1:9100": true, "localhost:0": true, "[::1]:9100": true,
		":9100": false, "0.0.0.0:9100": false, "192.168.1.5:9100": false, "nonsense": false,
	} {
		if got := isLoopbackAddr(addr); got != want {
			t.Errorf("isLoopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}

// The whole chain for real: go test runs a sample suite that uses record,
// its events and recorded exchanges reach the browser stream.
func TestServer_RealGoTest(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go command not found")
	}
	t.Parallel()
	s, base := startServer(t, goTool{dir: "."}, "./testdata/sample")
	events := openEvents(t, base)

	list, err := s.refreshList(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || strings.Join(list[0].Tests, ",") != "TestSamplePass,TestSampleFail" {
		t.Fatalf("list = %+v", list)
	}
	pkg := list[0].Package

	postJSON(t, base+"/api/run", `{"package":"`+pkg+`"}`)
	got := events.until(t, isType("run-end"))

	status := map[string]string{}
	var exchanges []event
	for _, ev := range got {
		if ev.Type == "test" && (ev.Action == "pass" || ev.Action == "fail") {
			status[ev.Test] = ev.Action
		}
		if ev.Type == "exchange" {
			exchanges = append(exchanges, ev)
		}
	}
	want := map[string]string{"": "fail", "TestSamplePass": "pass", "TestSamplePass/health": "pass", "TestSampleFail": "fail"}
	for name, action := range want {
		if status[name] != action {
			t.Errorf("%q: %q, want %q (all: %v)", name, status[name], action, status)
		}
	}
	if len(exchanges) != 1 || exchanges[0].Test != "TestSamplePass/health" || exchanges[0].Package != pkg ||
		!strings.Contains(string(exchanges[0].Exchange), `/health`) {
		t.Errorf("exchanges = %+v, want the one health call of the subtest", exchanges)
	}
	if end := got[len(got)-1]; end.Error != "" || end.Stopped {
		t.Errorf("run-end = %+v, want a normal end", end)
	}
}
