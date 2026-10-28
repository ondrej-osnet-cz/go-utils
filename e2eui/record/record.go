// Package record records the HTTP exchanges of a test for e2e-ui. Wrap the
// transport of the client a test uses:
//
//	client := &http.Client{Transport: record.Transport(t, http.DefaultTransport)}
//
// Without URLEnv set, Transport returns the base unchanged and nothing is
// recorded, so the wrapper can stay in the suite for good.
package record

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
)

// URLEnv names the environment variable with a URL to which every HTTP
// exchange of a test is posted as JSON (Exchange). The interactive E2E browser
// (cmd/e2e-ui) sets it; without it nothing is recorded.
const URLEnv = "E2E_RECORD_URL"

// maxRecordedBody limits how much of one request or response body is recorded.
const maxRecordedBody = 1 << 20

const postTimeout = 5 * time.Second

// Exchange is one recorded HTTP request with its response. It is posted when
// the response body is fully read or closed, or when the request fails.
type Exchange struct {
	Test       string            `json:"test"`
	Started    time.Time         `json:"started"`
	DurationMS float64           `json:"durationMs"`
	Request    RecordedRequest   `json:"request"`
	Response   *RecordedResponse `json:"response,omitempty"`
	Error      string            `json:"error,omitempty"`
}

// RecordedRequest is the request part of an Exchange.
type RecordedRequest struct {
	Method        string              `json:"method"`
	URL           string              `json:"url"`
	Header        map[string][]string `json:"header"`
	Body          string              `json:"body,omitempty"`
	BodyTruncated bool                `json:"bodyTruncated,omitempty"`
}

// RecordedResponse is the response part of an Exchange.
type RecordedResponse struct {
	Status        int                 `json:"status"`
	Header        map[string][]string `json:"header"`
	Body          string              `json:"body,omitempty"`
	BodyTruncated bool                `json:"bodyTruncated,omitempty"`
}

// Transport wraps base so that it records the test's exchanges when URLEnv is
// set, and returns base unchanged otherwise.
func Transport(t testing.TB, base http.RoundTripper) http.RoundTripper {
	t.Helper()
	target := os.Getenv(URLEnv)
	if target == "" {
		return base
	}
	if u, err := url.Parse(target); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		t.Fatalf("%s=%q is not an http(s) URL", URLEnv, target)
	}
	return recordingTransport{test: t.Name(), target: target, base: base}
}

// recordingTransport posts every exchange that passes through it to target.
type recordingTransport struct {
	test   string
	target string
	base   http.RoundTripper
}

func (rt recordingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	ex := &Exchange{
		Test:    rt.test,
		Started: time.Now(),
		Request: RecordedRequest{Method: r.Method, URL: r.URL.String(), Header: r.Header.Clone()},
	}
	if r.Body != nil && r.Body != http.NoBody {
		body, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("record request body: %w", err)
		}
		ex.Request.Body, ex.Request.BodyTruncated = truncate(body)
		r = r.Clone(r.Context())
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	}

	resp, err := rt.base.RoundTrip(r)
	if err != nil {
		ex.Error = err.Error()
		rt.post(ex)
		return nil, err
	}
	ex.Response = &RecordedResponse{Status: resp.StatusCode, Header: resp.Header.Clone()}
	resp.Body = &recordingBody{ReadCloser: resp.Body, done: func(body []byte, truncated bool) {
		ex.Response.Body, ex.Response.BodyTruncated = string(body), truncated
		rt.post(ex)
	}}
	return resp, nil
}

// CloseIdleConnections lets http.Client.CloseIdleConnections reach the base.
func (rt recordingTransport) CloseIdleConnections() {
	if c, ok := rt.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

// post sends the finished exchange. A failure is only logged: recording must
// not change the outcome of the test.
func (rt recordingTransport) post(ex *Exchange) {
	ex.DurationMS = float64(time.Since(ex.Started).Microseconds()) / 1000
	payload, err := json.Marshal(ex)
	if err != nil {
		slog.Warn("record exchange", "test", rt.test, "err", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), postTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rt.target, bytes.NewReader(payload))
	if err != nil {
		slog.Warn("record exchange", "test", rt.test, "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Warn("record exchange", "test", rt.test, "err", err)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		slog.Warn("record exchange", "test", rt.test, "status", resp.StatusCode)
	}
}

// recordingBody keeps a copy of what the test reads and reports it once, at
// EOF or on Close. Streamed responses (MCP over SSE) are thus recorded whole.
type recordingBody struct {
	io.ReadCloser
	done func(body []byte, truncated bool)

	mu        sync.Mutex
	buf       []byte
	truncated bool
	finished  bool
}

func (b *recordingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if room := maxRecordedBody - len(b.buf); n > room {
		b.buf = append(b.buf, p[:room]...)
		b.truncated = true
	} else {
		b.buf = append(b.buf, p[:n]...)
	}
	if err != nil {
		b.finishLocked()
	}
	return n, err
}

func (b *recordingBody) Close() error {
	err := b.ReadCloser.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.finishLocked()
	return err
}

func (b *recordingBody) finishLocked() {
	if b.finished {
		return
	}
	b.finished = true
	b.done(b.buf, b.truncated)
}

func truncate(body []byte) (string, bool) {
	if len(body) > maxRecordedBody {
		return string(body[:maxRecordedBody]), true
	}
	return string(body), false
}
