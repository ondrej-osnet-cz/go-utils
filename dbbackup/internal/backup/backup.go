// Package backup snapshots SQLite databases and keeps the snapshots in object
// storage.
//
// A snapshot is taken with VACUUM INTO, which reads one consistent state of
// the database while the application keeps writing (also in WAL mode). The
// copy must pass PRAGMA integrity_check before it is gzipped and uploaded as
//
//	<prefix>/<path relative to root>/<UTC time>.db.gz
//
// e.g. backups/osnet-invoicing-tenants/taxpayer-1.db/20261028T030000Z.db.gz.
package backup

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite" // driver "sqlite"
)

// stampLayout names a backup by its UTC time; names sort by time.
const stampLayout = "20060102T150405Z"

const suffix = ".db.gz"

var backupName = regexp.MustCompile(`^\d{8}T\d{6}Z\.db\.gz$`)

// Store keeps the backups (an S3 bucket).
type Store interface {
	Put(ctx context.Context, key string, body io.Reader, size int64, payloadHash, contentType string) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	List(ctx context.Context, prefix string) ([]StoredObject, error)
	Delete(ctx context.Context, key string) error
}

// StoredObject is one object of a Store listing.
type StoredObject struct {
	Key  string
	Size int64
}

// Config says what to back up and where to.
type Config struct {
	// Root is the directory the patterns are relative to. The path of a
	// database relative to Root names its backups.
	Root string
	// Patterns are filepath.Match globs relative to Root, e.g.
	// "app.db" or "app-tenants/*.db". Only BackupAll needs them.
	Patterns []string
	// Prefix of all keys, without slashes at the ends; may be empty.
	Prefix string
	// Keep is how many newest backups of a database to keep; older ones are
	// deleted after a successful backup. 0 keeps all.
	Keep int
	// TempDir holds the snapshot while it is checked and compressed;
	// empty means os.TempDir().
	TempDir string
}

// Backuper backs up the configured databases.
type Backuper struct {
	cfg   Config
	store Store
	log   *slog.Logger
	now   func() time.Time
}

// New checks cfg and returns a Backuper.
func New(cfg Config, store Store, log *slog.Logger) (*Backuper, error) {
	if cfg.Root == "" {
		return nil, errors.New("root is empty")
	}
	for _, p := range cfg.Patterns {
		if _, err := filepath.Match(p, ""); err != nil {
			return nil, fmt.Errorf("pattern %q: %w", p, err)
		}
		if filepath.IsAbs(p) || !filepath.IsLocal(p) {
			return nil, fmt.Errorf("pattern %q: want a path inside the root", p)
		}
	}
	if cfg.Keep < 0 {
		return nil, errors.New("keep is negative")
	}
	cfg.Prefix = strings.Trim(cfg.Prefix, "/")
	return &Backuper{cfg: cfg, store: store, log: log, now: time.Now}, nil
}

// Find returns the databases matching the patterns, as slash separated
// paths relative to the root, sorted and without duplicates. SQLite's own
// side files (-wal, -shm, -journal) are left out.
func (b *Backuper) Find() ([]string, error) {
	var found []string
	for _, p := range b.cfg.Patterns {
		matches, err := filepath.Glob(filepath.Join(b.cfg.Root, p))
		if err != nil {
			return nil, fmt.Errorf("pattern %q: %w", p, err)
		}
		if len(matches) == 0 {
			b.log.Warn("pattern matches no file", "pattern", p, "root", b.cfg.Root)
		}
		for _, m := range matches {
			if strings.HasSuffix(m, "-wal") || strings.HasSuffix(m, "-shm") || strings.HasSuffix(m, "-journal") {
				continue
			}
			if fi, err := os.Stat(m); err != nil || !fi.Mode().IsRegular() {
				continue
			}
			rel, err := filepath.Rel(b.cfg.Root, m)
			if err != nil {
				return nil, fmt.Errorf("relative path of %s: %w", m, err)
			}
			found = append(found, filepath.ToSlash(rel))
		}
	}
	sort.Strings(found)
	return slices.Compact(found), nil
}

// Result is the outcome of backing up one database.
type Result struct {
	DB       string
	Key      string
	Size     int64
	Duration time.Duration
	Deleted  int
	Err      error
}

// BackupAll backs up every database Find returns, one after another. A
// failed database does not stop the others; the error joins all failures.
func (b *Backuper) BackupAll(ctx context.Context) ([]Result, error) {
	dbs, err := b.Find()
	if err != nil {
		return nil, err
	}
	if len(dbs) == 0 {
		return nil, fmt.Errorf("no database found in %s for %q", b.cfg.Root, b.cfg.Patterns)
	}
	var results []Result
	var errs []error
	for _, db := range dbs {
		if err := ctx.Err(); err != nil {
			return results, errors.Join(append(errs, err)...)
		}
		res := b.Backup(ctx, db)
		results = append(results, res)
		if res.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", db, res.Err))
			b.log.ErrorContext(ctx, "backup failed", "db", db, "error", res.Err)
			continue
		}
		b.log.InfoContext(ctx, "backup done", "db", db, "key", res.Key, "bytes", res.Size,
			"duration", res.Duration.Round(time.Millisecond).String(), "deleted_old", res.Deleted)
	}
	return results, errors.Join(errs...)
}

// Backup snapshots the database db (relative to the root), uploads it and
// then deletes backups over Keep.
func (b *Backuper) Backup(ctx context.Context, db string) Result {
	start := b.now()
	res := Result{DB: db, Key: b.key(db, start.UTC().Format(stampLayout))}
	res.Size, res.Err = b.upload(ctx, db, res.Key)
	if res.Err == nil && b.cfg.Keep > 0 {
		res.Deleted, res.Err = b.prune(ctx, db)
	}
	res.Duration = b.now().Sub(start)
	return res
}

func (b *Backuper) upload(ctx context.Context, db, key string) (int64, error) {
	dir, err := os.MkdirTemp(b.cfg.TempDir, "db-backup-")
	if err != nil {
		return 0, fmt.Errorf("create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	snapshot := filepath.Join(dir, "snapshot.db")
	if err := Snapshot(ctx, filepath.Join(b.cfg.Root, filepath.FromSlash(db)), snapshot); err != nil {
		return 0, err
	}
	if err := Check(ctx, snapshot); err != nil {
		return 0, fmt.Errorf("check snapshot: %w", err)
	}
	gz := snapshot + ".gz"
	hash, err := compress(snapshot, gz)
	if err != nil {
		return 0, err
	}
	f, err := os.Open(gz) // #nosec G304 -- our own temp file
	if err != nil {
		return 0, fmt.Errorf("open compressed snapshot: %w", err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat compressed snapshot: %w", err)
	}
	if err := b.store.Put(ctx, key, f, fi.Size(), hash, "application/gzip"); err != nil {
		return 0, fmt.Errorf("upload: %w", err)
	}
	return fi.Size(), nil
}

// prune deletes the oldest backups of db so that Keep remain.
func (b *Backuper) prune(ctx context.Context, db string) (int, error) {
	backups, err := b.List(ctx, db)
	if err != nil {
		return 0, fmt.Errorf("list for retention: %w", err)
	}
	deleted := 0
	for len(backups)-deleted > b.cfg.Keep {
		if err := b.store.Delete(ctx, backups[deleted].Key); err != nil {
			return deleted, fmt.Errorf("delete old backup: %w", err)
		}
		deleted++
	}
	return deleted, nil
}

// Stored is one backup in the store.
type Stored struct {
	DB   string
	Key  string
	Time time.Time
	Size int64
}

// List returns the backups of db, oldest first; an empty db lists the
// backups of all databases, by database and time.
func (b *Backuper) List(ctx context.Context, db string) ([]Stored, error) {
	objects, err := b.store.List(ctx, b.dbPrefix(db))
	if err != nil {
		return nil, err
	}
	var out []Stored
	for _, o := range objects {
		rest := strings.TrimPrefix(o.Key, b.dbPrefix(""))
		dbPath, name := path.Split(rest)
		dbPath = strings.TrimSuffix(dbPath, "/")
		if !backupName.MatchString(name) || dbPath == "" || db != "" && dbPath != db {
			continue
		}
		t, err := time.Parse(stampLayout, strings.TrimSuffix(name, suffix))
		if err != nil {
			continue
		}
		out = append(out, Stored{DB: dbPath, Key: o.Key, Time: t, Size: o.Size})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DB != out[j].DB {
			return out[i].DB < out[j].DB
		}
		return out[i].Time.Before(out[j].Time)
	})
	return out, nil
}

// Restore downloads the backup of db taken at stamp (empty = the newest)
// into the new file out and checks it. It never overwrites a file.
func (b *Backuper) Restore(ctx context.Context, db, stamp, out string) (Stored, error) {
	backups, err := b.List(ctx, db)
	if err != nil {
		return Stored{}, err
	}
	if len(backups) == 0 {
		return Stored{}, fmt.Errorf("no backup of %s", db)
	}
	chosen := backups[len(backups)-1]
	if stamp != "" {
		i := slices.IndexFunc(backups, func(s Stored) bool { return s.Time.Format(stampLayout) == stamp })
		if i < 0 {
			return Stored{}, fmt.Errorf("no backup of %s at %s", db, stamp)
		}
		chosen = backups[i]
	}
	if _, err := os.Lstat(out); err == nil {
		return Stored{}, fmt.Errorf("%s already exists; restore into a new file", out)
	}

	body, err := b.store.Get(ctx, chosen.Key)
	if err != nil {
		return Stored{}, fmt.Errorf("download: %w", err)
	}
	defer func() { _ = body.Close() }()
	tmp := out + ".restoring"
	if err := decompress(body, tmp); err != nil {
		_ = os.Remove(tmp)
		return Stored{}, err
	}
	if err := Check(ctx, tmp); err != nil {
		_ = os.Remove(tmp)
		return Stored{}, fmt.Errorf("check restored database: %w", err)
	}
	if err := os.Rename(tmp, out); err != nil {
		_ = os.Remove(tmp)
		return Stored{}, fmt.Errorf("move restored database: %w", err)
	}
	return chosen, nil
}

func (b *Backuper) dbPrefix(db string) string {
	p := db + "/"
	if db == "" {
		p = ""
	}
	if b.cfg.Prefix != "" {
		p = b.cfg.Prefix + "/" + p
	}
	return p
}

func (b *Backuper) key(db, stamp string) string {
	return b.dbPrefix(db) + stamp + suffix
}

// Snapshot writes a consistent copy of the database src into the new file
// dst. It opens src read-only and waits for a busy writer.
func Snapshot(ctx context.Context, src, dst string) error {
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("database: %w", err)
	}
	db, err := open(src, "mode=ro", "_pragma=busy_timeout(30000)")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "VACUUM INTO ?", dst); err != nil {
		return fmt.Errorf("snapshot %s: %w", src, err)
	}
	return nil
}

// Check runs PRAGMA integrity_check on the database file p.
func Check(ctx context.Context, p string) error {
	db, err := open(p, "mode=ro")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return fmt.Errorf("integrity check: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var problems []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return fmt.Errorf("integrity check: %w", err)
		}
		if line != "ok" {
			problems = append(problems, line)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("integrity check: %w", err)
	}
	if len(problems) > 0 {
		return fmt.Errorf("integrity check: %s", strings.Join(problems, "; "))
	}
	return nil
}

func open(p string, params ...string) (*sql.DB, error) {
	dsn := (&url.URL{Scheme: "file", Opaque: url.PathEscape(p), RawQuery: strings.Join(params, "&")}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", p, err)
	}
	// One connection: VACUUM INTO and the check need no more.
	db.SetMaxOpenConns(1)
	return db, nil
}

// compress gzips src into dst and returns the hex SHA-256 of dst.
func compress(src, dst string) (string, error) {
	in, err := os.Open(src) // #nosec G304 -- our own temp file
	if err != nil {
		return "", fmt.Errorf("open snapshot: %w", err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- our own temp file
	if err != nil {
		return "", fmt.Errorf("create compressed snapshot: %w", err)
	}
	defer func() { _ = out.Close() }()
	h := sha256.New()
	zw := gzip.NewWriter(io.MultiWriter(out, h))
	if _, err := io.Copy(zw, in); err != nil {
		return "", fmt.Errorf("compress snapshot: %w", err)
	}
	if err := zw.Close(); err != nil {
		return "", fmt.Errorf("compress snapshot: %w", err)
	}
	if err := out.Close(); err != nil {
		return "", fmt.Errorf("write compressed snapshot: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// decompress gunzips r into the new file dst.
func decompress(r io.Reader, dst string) error {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("read backup: %w", err)
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- path chosen by the operator
	if err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	defer func() { _ = out.Close() }()
	// #nosec G110 -- the backup is our own upload, not untrusted input
	if _, err := io.Copy(out, zr); err != nil {
		return fmt.Errorf("decompress backup: %w", err)
	}
	if err := zr.Close(); err != nil {
		return fmt.Errorf("decompress backup: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("write %s: %w", dst, err)
	}
	return nil
}
