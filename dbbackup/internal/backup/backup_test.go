package backup_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ondrej-osnet-cz/go-utils/dbbackup/internal/backup"
	"github.com/ondrej-osnet-cz/go-utils/dbbackup/internal/s3"
	"github.com/ondrej-osnet-cz/go-utils/dbbackup/internal/s3/s3test"
)

// s3Store adapts the S3 client to backup.Store, as the command does.
type s3Store struct{ *s3.Client }

func (s s3Store) List(ctx context.Context, prefix string) ([]backup.StoredObject, error) {
	objects, err := s.Client.List(ctx, prefix)
	out := make([]backup.StoredObject, 0, len(objects))
	for _, o := range objects {
		out = append(out, backup.StoredObject{Key: o.Key, Size: o.Size})
	}
	return out, err
}

func newStore(t *testing.T) (*s3test.Server, backup.Store) {
	t.Helper()
	srv := s3test.New(t)
	c, err := s3.New(s3.Config{
		Endpoint: srv.URL, Region: s3test.Region, Bucket: s3test.Bucket, PathStyle: true,
		AccessKeyID: s3test.AccessKeyID, SecretAccessKey: s3test.SecretAccessKey,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return srv, s3Store{c}
}

// createDB makes a WAL database with n rows in table t, as the applications use.
func createDB(t *testing.T, p string, n int) *sql.DB {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	dsn := (&url.URL{Scheme: "file", Opaque: url.PathEscape(p), RawQuery: "_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (?)", strings.Repeat("x", i)); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func countRows(t *testing.T, p string) int {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+url.PathEscape(p)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func newBackuper(t *testing.T, cfg backup.Config, store backup.Store) *backup.Backuper {
	t.Helper()
	if cfg.TempDir == "" {
		cfg.TempDir = t.TempDir()
	}
	b, err := backup.New(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestBackupAll_BacksUpAndRestoresWhileAppWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	central := createDB(t, filepath.Join(root, "app.db"), 10)
	createDB(t, filepath.Join(root, "app-tenants", "taxpayer-1.db"), 3)
	createDB(t, filepath.Join(root, "app-tenants", "taxpayer 2.db"), 5)
	if err := os.WriteFile(filepath.Join(root, "other.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv, store := newStore(t)
	b := newBackuper(t, backup.Config{Root: root, Patterns: []string{"app.db", "app-tenants/*.db"}, Prefix: "/prod/"}, store)

	// An open write transaction of the application must not end up in the backup.
	tx, err := central.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t (v) VALUES ('uncommitted')"); err != nil {
		t.Fatal(err)
	}
	results, err := b.BackupAll(ctx)
	if err != nil {
		t.Fatalf("BackupAll: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("results = %+v, want 3", results)
	}
	for _, k := range srv.Keys() {
		if !strings.HasPrefix(k, "prod/") || !strings.HasSuffix(k, "Z.db.gz") {
			t.Errorf("unexpected key %q", k)
		}
	}

	all, err := b.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	var dbs []string
	for _, s := range all {
		dbs = append(dbs, s.DB)
	}
	if want := []string{"app-tenants/taxpayer 2.db", "app-tenants/taxpayer-1.db", "app.db"}; !slices.Equal(dbs, want) {
		t.Errorf("listed dbs = %q, want %q", dbs, want)
	}

	for db, want := range map[string]int{"app.db": 10, "app-tenants/taxpayer 2.db": 5} {
		out := filepath.Join(t.TempDir(), "restored.db")
		if _, err := b.Restore(ctx, db, "", out); err != nil {
			t.Fatalf("restore %s: %v", db, err)
		}
		if got := countRows(t, out); got != want {
			t.Errorf("%s: restored %d rows, want %d", db, got, want)
		}
	}
}

func TestBackup_KeepDeletesOldest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	createDB(t, filepath.Join(root, "a.db"), 1)
	createDB(t, filepath.Join(root, "b.db"), 1)
	srv, store := newStore(t)
	b := newBackuper(t, backup.Config{Root: root, Patterns: []string{"*.db"}, Keep: 2}, store)

	now := time.Date(2026, 10, 28, 3, 0, 0, 0, time.UTC)
	backup.SetClock(b, func() time.Time { return now })
	for range 4 {
		if _, err := b.BackupAll(ctx); err != nil {
			t.Fatal(err)
		}
		now = now.Add(24 * time.Hour)
	}
	want := []string{
		"a.db/20261030T030000Z.db.gz", "a.db/20261031T030000Z.db.gz",
		"b.db/20261030T030000Z.db.gz", "b.db/20261031T030000Z.db.gz",
	}
	if got := srv.Keys(); !slices.Equal(got, want) {
		t.Errorf("keys = %q, want %q", got, want)
	}

	out := filepath.Join(t.TempDir(), "old.db")
	got, err := b.Restore(ctx, "a.db", "20261030T030000Z", out)
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != "a.db/20261030T030000Z.db.gz" {
		t.Errorf("restored %s", got.Key)
	}
}

func TestBackupAll_OneFailureDoesNotStopOthers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	createDB(t, filepath.Join(root, "a.db"), 1)
	if err := os.WriteFile(filepath.Join(root, "b.db"), []byte("not a database"), 0o600); err != nil {
		t.Fatal(err)
	}
	createDB(t, filepath.Join(root, "c.db"), 1)
	srv, store := newStore(t)
	b := newBackuper(t, backup.Config{Root: root, Patterns: []string{"*.db"}}, store)

	results, err := b.BackupAll(ctx)
	if err == nil || !strings.Contains(err.Error(), "b.db") {
		t.Fatalf("err = %v, want failure of b.db", err)
	}
	if len(results) != 3 || results[0].Err != nil || results[1].Err == nil || results[2].Err != nil {
		t.Errorf("results = %+v", results)
	}
	if n := len(srv.Keys()); n != 2 {
		t.Errorf("%d objects uploaded, want 2", n)
	}
}

func TestBackup_FailedUploadKeepsOldBackups(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	createDB(t, filepath.Join(root, "a.db"), 1)
	srv, store := newStore(t)
	b := newBackuper(t, backup.Config{Root: root, Patterns: []string{"a.db"}, Keep: 1}, store)
	if _, err := b.BackupAll(ctx); err != nil {
		t.Fatal(err)
	}
	before := srv.Keys()

	srv.SetFailPut(true)
	if _, err := b.BackupAll(ctx); err == nil {
		t.Fatal("no error with failing upload")
	}
	if got := srv.Keys(); !slices.Equal(got, before) {
		t.Errorf("keys = %q, want the old backup %q", got, before)
	}
}

func TestBackupAll_NothingFound(t *testing.T) {
	t.Parallel()
	_, store := newStore(t)
	b := newBackuper(t, backup.Config{Root: t.TempDir(), Patterns: []string{"*.db"}}, store)
	if _, err := b.BackupAll(context.Background()); err == nil {
		t.Error("no error when no database matches")
	}
}

func TestRestore_NeverOverwrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	createDB(t, filepath.Join(root, "a.db"), 1)
	_, store := newStore(t)
	b := newBackuper(t, backup.Config{Root: root, Patterns: []string{"a.db"}}, store)
	if _, err := b.BackupAll(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Restore(ctx, "a.db", "", filepath.Join(root, "a.db")); err == nil {
		t.Error("restore over an existing file succeeded")
	}
	if _, err := b.Restore(ctx, "a.db", "20000101T000000Z", filepath.Join(t.TempDir(), "x.db")); err == nil {
		t.Error("restore of a missing time succeeded")
	}
}

func TestFind_SkipsSideFilesAndDuplicates(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	db := createDB(t, filepath.Join(root, "a.db"), 1)
	// Keep the WAL file around while globbing.
	if _, err := db.Exec("INSERT INTO t (v) VALUES ('y')"); err != nil {
		t.Fatal(err)
	}
	_, store := newStore(t)
	b := newBackuper(t, backup.Config{Root: root, Patterns: []string{"a.db*", "*.db"}}, store)
	got, err := b.Find()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a.db"}; !slices.Equal(got, want) {
		t.Errorf("Find = %q, want %q", got, want)
	}
}

func TestNew_RejectsPatternsOutsideRoot(t *testing.T) {
	t.Parallel()
	for _, p := range []string{"../x.db", "/data/x.db", "a/../../x.db", "[.db"} {
		_, err := backup.New(backup.Config{Root: "/data", Patterns: []string{p}}, nil, slog.Default())
		if err == nil {
			t.Errorf("pattern %q accepted", p)
		}
	}
}
