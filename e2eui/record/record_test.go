package record

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// exchangeSink collects the exchanges posted to URLEnv.
type exchangeSink struct {
	mu   sync.Mutex
	got  []Exchange
	errs []string
}

func (s *exchangeSink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ct := r.Header.Get("Content-Type"); r.Method != http.MethodPost || ct != "application/json" {
		s.errs = append(s.errs, r.Method+" with content type "+ct)
	}
	var ex Exchange
	if err := json.NewDecoder(r.Body).Decode(&ex); err != nil {
		s.errs = append(s.errs, err.Error())
	}
	s.got = append(s.got, ex)
	w.WriteHeader(http.StatusNoContent)
}

func (s *exchangeSink) exchanges(t *testing.T) []Exchange {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.errs) > 0 {
		t.Fatalf("bad posts to the sink: %q", s.errs)
	}
	return append([]Exchange(nil), s.got...)
}

func startSink(t *testing.T) *exchangeSink {
	t.Helper()
	sink := &exchangeSink{}
	srv := httptest.NewServer(sink)
	t.Cleanup(srv.Close)
	t.Setenv(URLEnv, srv.URL) // not parallel: changes the environment
	return sink
}

func TestTransport_RecordsExchange(t *testing.T) {
	sink := startSink(t)
	seen := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seen <- string(b)
		w.Header().Set("X-Reply", "yes")
		_, _ = w.Write([]byte("pong"))
	}))
	t.Cleanup(srv.Close)
	client := &http.Client{Transport: Transport(t, http.DefaultTransport)}

	resp, err := client.Post(srv.URL+"/echo", "text/plain", strings.NewReader("ping"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if got := <-seen; got != "ping" {
		t.Errorf("server got body %q, want ping", got)
	}
	got := sink.exchanges(t)
	if len(got) != 1 {
		t.Fatalf("recorded %d exchanges, want 1", len(got))
	}
	ex := got[0]
	if ex.Test != t.Name() || ex.Request.Method != http.MethodPost || !strings.HasSuffix(ex.Request.URL, "/echo") ||
		ex.Request.Body != "ping" || ex.Started.IsZero() {
		t.Errorf("request = %+v of %q, want POST /echo ping of %s", ex.Request, ex.Test, t.Name())
	}
	if ex.Response == nil || ex.Response.Status != http.StatusOK || ex.Response.Body != "pong" ||
		http.Header(ex.Response.Header).Get("X-Reply") != "yes" {
		t.Errorf("response = %+v, want 200 pong with X-Reply", ex.Response)
	}
}

func TestTransport_ErrorIsRecorded(t *testing.T) {
	sink := startSink(t)
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens there any more
	client := &http.Client{Transport: Transport(t, http.DefaultTransport)}

	if resp, err := client.Get(url); err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected a connection error")
	}

	got := sink.exchanges(t)
	if len(got) != 1 || got[0].Error == "" || got[0].Response != nil {
		t.Errorf("recorded %+v, want one exchange with an error and no response", got)
	}
}

func TestTransport_LargeBodyIsTruncated(t *testing.T) {
	sink := startSink(t)
	big := strings.Repeat("x", maxRecordedBody+10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(big))
	}))
	t.Cleanup(srv.Close)
	client := &http.Client{Transport: Transport(t, http.DefaultTransport)}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || len(body) != len(big) {
		t.Fatalf("test read %d bytes (%v), want the whole %d", len(body), err, len(big))
	}

	got := sink.exchanges(t)
	if len(got) != 1 || got[0].Response == nil {
		t.Fatalf("recorded %+v, want one exchange", got)
	}
	if r := got[0].Response; len(r.Body) != maxRecordedBody || !r.BodyTruncated {
		t.Errorf("recorded body of %d bytes, truncated=%v; want %d, true", len(r.Body), r.BodyTruncated, maxRecordedBody)
	}
}

func TestTransport_DisabledByDefault(t *testing.T) {
	t.Setenv(URLEnv, "")
	base := http.DefaultTransport

	if got := Transport(t, base); got != base {
		t.Errorf("transport = %T, want the base transport unchanged", got)
	}
}
