package main

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ondrej-osnet-cz/go-utils/dbbackup/internal/s3/s3test"
)

func testEnv(srv *s3test.Server, root string) map[string]string {
	return map[string]string{
		"BACKUP_S3_ENDPOINT":          srv.URL,
		"BACKUP_S3_REGION":            s3test.Region,
		"BACKUP_S3_BUCKET":            s3test.Bucket,
		"BACKUP_S3_PATH_STYLE":        "true",
		"BACKUP_S3_ACCESS_KEY_ID":     s3test.AccessKeyID,
		"BACKUP_S3_SECRET_ACCESS_KEY": s3test.SecretAccessKey,
		"BACKUP_ROOT":                 root,
		"BACKUP_DATABASES":            "app.db, tenants/*.db",
		"BACKUP_S3_PREFIX":            "prod",
	}
}

func runCmd(t *testing.T, env map[string]string, args ...string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), args, func(k string) string { return env[k] }, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func createDB(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+url.PathEscape(p)+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE t (v TEXT); INSERT INTO t VALUES ('hello')"); err != nil {
		t.Fatal(err)
	}
}

func TestOnceListRestore(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	createDB(t, filepath.Join(root, "app.db"))
	createDB(t, filepath.Join(root, "tenants", "taxpayer-1.db"))
	srv := s3test.New(t)
	var pings atomic.Int32
	hb := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { pings.Add(1) }))
	t.Cleanup(hb.Close)
	env := testEnv(srv, root)
	env["BACKUP_HEARTBEAT_URL"] = hb.URL

	if _, stderr, err := runCmd(t, env, "once", "-tmp-dir", t.TempDir()); err != nil {
		t.Fatalf("once: %v\n%s", err, stderr)
	}
	if got := len(srv.Keys()); got != 2 {
		t.Errorf("%d backups stored, want 2: %q", got, srv.Keys())
	}
	if pings.Load() != 1 {
		t.Errorf("%d heartbeats, want 1", pings.Load())
	}

	out, _, err := runCmd(t, env, "list")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "prod/app.db/") || !strings.Contains(out, "tenants/taxpayer-1.db") {
		t.Errorf("list output:\n%s", out)
	}

	dst := filepath.Join(t.TempDir(), "restored.db")
	if _, stderr, err := runCmd(t, env, "restore", "-db", "tenants/taxpayer-1.db", "-out", dst); err != nil {
		t.Fatalf("restore: %v\n%s", err, stderr)
	}
	entries, err := os.ReadDir(filepath.Dir(dst))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("restore left extra files: %v", entries)
	}
}

func TestOnce_FailureExitsWithError(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	createDB(t, filepath.Join(root, "app.db"))
	srv := s3test.New(t)
	srv.SetFailPut(true)
	if _, _, err := runCmd(t, testEnv(srv, root), "once"); err == nil {
		t.Error("once succeeded although the upload failed")
	}
}

func TestConfigErrors(t *testing.T) {
	t.Parallel()
	srv := s3test.New(t)
	tests := []struct {
		name   string
		change func(map[string]string)
		args   []string
		want   string
	}{
		{"no secret", func(e map[string]string) { delete(e, "BACKUP_S3_SECRET_ACCESS_KEY") }, []string{"once"}, "access key"},
		{"no databases", func(e map[string]string) { delete(e, "BACKUP_DATABASES") }, []string{"once"}, "no databases"},
		{"bad env", func(e map[string]string) { e["BACKUP_KEEP"] = "many" }, []string{"once"}, "BACKUP_KEEP"},
		{"short interval", func(map[string]string) {}, []string{"run", "-interval", "1s"}, "interval"},
		{"restore without out", func(map[string]string) {}, []string{"restore", "-db", "app.db"}, "-out"},
		{"unknown command", func(map[string]string) {}, []string{"backup"}, "unknown command"},
		{"flag wins over env", func(map[string]string) {}, []string{"once", "-s3-bucket", ""}, "bucket"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env := testEnv(srv, t.TempDir())
			tt.change(env)
			_, _, err := runCmd(t, env, tt.args...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestRun_BacksUpUntilStopped(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	createDB(t, filepath.Join(root, "app.db"))
	srv := s3test.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The heartbeat after the first round stops the service.
	hb := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { cancel() }))
	t.Cleanup(hb.Close)
	env := testEnv(srv, root)
	env["BACKUP_HEARTBEAT_URL"] = hb.URL

	var stderr bytes.Buffer
	err := run(ctx, []string{"run"}, func(k string) string { return env[k] }, &bytes.Buffer{}, &stderr)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, stderr.String())
	}
	if len(srv.Keys()) != 1 {
		t.Errorf("keys = %q, want one backup", srv.Keys())
	}
	if !strings.Contains(stderr.String(), "backup service stopped") {
		t.Errorf("log:\n%s", stderr.String())
	}
}

func TestFlagOverridesEnv(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	createDB(t, filepath.Join(root, "other.db"))
	srv := s3test.New(t)
	if _, stderr, err := runCmd(t, testEnv(srv, root), "once", "-databases", "other.db", "-s3-prefix", "x"); err != nil {
		t.Fatalf("once: %v\n%s", err, stderr)
	}
	keys := srv.Keys()
	if len(keys) != 1 || !strings.HasPrefix(keys[0], "x/other.db/") {
		t.Errorf("keys = %q", keys)
	}
}
