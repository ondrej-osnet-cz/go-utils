// Package sample_test is a tiny suite for the e2e-ui integration test. It is
// under testdata, so `go test ./...` skips it. TestSampleFail fails on purpose.
package sample_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ondrej-osnet-cz/go-utils/e2eui/record"
)

func TestSamplePass(t *testing.T) {
	t.Run("health", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"status":"ok"}`)
		}))
		t.Cleanup(srv.Close)
		client := &http.Client{Transport: record.Transport(t, http.DefaultTransport)}

		resp, err := client.Get(srv.URL + "/health")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
	})
}

func TestSampleFail(t *testing.T) {
	t.Error("failing on purpose")
}
