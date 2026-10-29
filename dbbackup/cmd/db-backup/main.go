// Command db-backup backs up SQLite databases to S3 compatible storage.
//
//	db-backup run       # back up now and then every -interval, until stopped
//	db-backup once      # back up once; exit status 1 if any database failed
//	db-backup list      # list the stored backups
//	db-backup restore -db app.db -out /tmp/app.db   # download and check a backup
//
// Settings come from flags or BACKUP_* environment variables (a flag wins).
// The S3 keys are read only from the environment.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/ondrej-osnet-cz/go-utils/dbbackup/internal/backup"
	"github.com/ondrej-osnet-cz/go-utils/dbbackup/internal/s3"
)

// version is set at build time with -ldflags "-X main.version=…".
var version = "dev"

const usage = `usage: db-backup <command> [flags]

commands:
  run       back up now and then every -interval until stopped (SIGINT/SIGTERM)
  once      back up once and exit; status 1 if any database failed
  list      list stored backups (-db to show one database)
  restore   download a backup into a new file and check it (-db, -out, -at)
  version   print the version

Run "db-backup <command> -h" for the flags of a command.
`

const heartbeatTimeout = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return errors.New("no command")
	}
	cmd, args := args[0], args[1:]
	log := slog.New(slog.NewJSONHandler(stderr, nil))

	switch cmd {
	case "version":
		_, _ = fmt.Fprintln(stdout, version)
		return nil
	case "run", "once", "list", "restore":
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprint(stdout, usage)
		return nil
	default:
		_, _ = fmt.Fprint(stderr, usage)
		return fmt.Errorf("unknown command %q", cmd)
	}

	fs := flag.NewFlagSet("db-backup "+cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cfg config
	env := envReader{getenv: getenv}
	cfg.registerStorage(fs, &env)
	var db, at, out string
	switch cmd {
	case "run", "once":
		cfg.registerBackup(fs, &env, cmd == "run")
	case "list":
		fs.StringVar(&db, "db", "", "show only this database (path relative to the root, e.g. app.db)")
	case "restore":
		fs.StringVar(&db, "db", "", "database to restore (path relative to the root, e.g. app.db)")
		fs.StringVar(&at, "at", "", "time of the backup as listed, e.g. 20261028T030000Z (default the newest)")
		fs.StringVar(&out, "out", "", "new file to write the database to; never overwritten")
	}
	if env.err != nil {
		return env.err
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q", fs.Args())
	}

	b, err := cfg.backuper(getenv, cmd == "run" || cmd == "once", log)
	if err != nil {
		return err
	}
	switch cmd {
	case "once":
		log.InfoContext(ctx, "backup started", "version", version)
		_, err := b.BackupAll(ctx)
		if err == nil {
			ping(ctx, cfg.heartbeatURL, log)
		}
		return err
	case "run":
		if cfg.interval < time.Minute {
			return fmt.Errorf("interval %s: want at least 1m", cfg.interval)
		}
		return loop(ctx, b, cfg.interval, cfg.heartbeatURL, log)
	case "list":
		return list(ctx, b, db, stdout)
	default:
		if db == "" || out == "" {
			return errors.New("restore needs -db and -out")
		}
		s, err := b.Restore(ctx, db, at, out)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "restored %s from %s into %s (integrity check ok)\n", db, s.Key, out)
		return nil
	}
}

// loop backs up right away and then every interval until ctx ends. A failed
// round is logged and the loop goes on; only a successful one pings.
func loop(ctx context.Context, b *backup.Backuper, interval time.Duration, heartbeatURL string, log *slog.Logger) error {
	log.InfoContext(ctx, "backup service started", "version", version, "interval", interval.String())
	for {
		if _, err := b.BackupAll(ctx); err == nil {
			ping(ctx, heartbeatURL, log)
		} else if ctx.Err() == nil {
			log.ErrorContext(ctx, "backup round failed", "error", err)
		}
		t := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			t.Stop()
			log.Info("backup service stopped")
			return nil
		case <-t.C:
		}
	}
}

func list(ctx context.Context, b *backup.Backuper, db string, stdout io.Writer) error {
	backups, err := b.List(ctx, db)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "DATABASE\tTIME\tBYTES\tKEY")
	for _, s := range backups {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", s.DB, s.Time.Format("20060102T150405Z"), s.Size, s.Key)
	}
	return tw.Flush()
}

// ping tells a monitoring service (e.g. healthchecks.io) that a round
// succeeded; a missing ping is what raises the alarm.
func ping(ctx context.Context, url string, log *slog.Logger) {
	if url == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err == nil {
		var resp *http.Response
		if resp, err = http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				err = fmt.Errorf("HTTP %d", resp.StatusCode)
			}
		}
	}
	if err != nil {
		// The URL may carry a token, so it is not logged.
		log.WarnContext(ctx, "heartbeat failed", "error", err)
	}
}

type config struct {
	root, databases, tmpDir, heartbeatURL string
	interval                              time.Duration
	keep                                  int
	endpoint, region, bucket, prefix      string
	pathStyle                             bool
}

func (c *config) registerStorage(fs *flag.FlagSet, env *envReader) {
	fs.StringVar(&c.endpoint, "s3-endpoint", env.str("BACKUP_S3_ENDPOINT", ""), "S3 endpoint URL, e.g. https://s3.eu-central-1.amazonaws.com (env BACKUP_S3_ENDPOINT)")
	fs.StringVar(&c.region, "s3-region", env.str("BACKUP_S3_REGION", "us-east-1"), "S3 region (env BACKUP_S3_REGION)")
	fs.StringVar(&c.bucket, "s3-bucket", env.str("BACKUP_S3_BUCKET", ""), "S3 bucket (env BACKUP_S3_BUCKET)")
	fs.StringVar(&c.prefix, "s3-prefix", env.str("BACKUP_S3_PREFIX", ""), "key prefix of all backups, e.g. prod/app (env BACKUP_S3_PREFIX)")
	fs.BoolVar(&c.pathStyle, "s3-path-style", env.boolean("BACKUP_S3_PATH_STYLE", false), "bucket in the URL path instead of the host name; MinIO needs it (env BACKUP_S3_PATH_STYLE)")
	fs.StringVar(&c.root, "root", env.str("BACKUP_ROOT", "/data"), "directory the database patterns are relative to (env BACKUP_ROOT)")
	fs.IntVar(&c.keep, "keep", env.integer("BACKUP_KEEP", 0), "newest backups to keep per database, older are deleted; 0 keeps all (env BACKUP_KEEP)")
}

func (c *config) registerBackup(fs *flag.FlagSet, env *envReader, daemon bool) {
	fs.StringVar(&c.databases, "databases", env.str("BACKUP_DATABASES", ""), "comma separated globs of databases relative to -root, e.g. app.db,app-tenants/*.db (env BACKUP_DATABASES)")
	fs.StringVar(&c.tmpDir, "tmp-dir", env.str("BACKUP_TMP_DIR", ""), "directory for the snapshot while it is checked and compressed (env BACKUP_TMP_DIR, default the system temp dir)")
	fs.StringVar(&c.heartbeatURL, "heartbeat-url", env.str("BACKUP_HEARTBEAT_URL", ""), "URL to GET after every successful round, for monitoring (env BACKUP_HEARTBEAT_URL)")
	if daemon {
		fs.DurationVar(&c.interval, "interval", env.duration("BACKUP_INTERVAL", 24*time.Hour), "time between backups (env BACKUP_INTERVAL)")
	}
}

func (c *config) backuper(getenv func(string) string, backingUp bool, log *slog.Logger) (*backup.Backuper, error) {
	client, err := s3.New(s3.Config{
		Endpoint:        c.endpoint,
		Region:          c.region,
		Bucket:          c.bucket,
		PathStyle:       c.pathStyle,
		AccessKeyID:     getenv("BACKUP_S3_ACCESS_KEY_ID"),
		SecretAccessKey: getenv("BACKUP_S3_SECRET_ACCESS_KEY"),
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("s3 (BACKUP_S3_* settings; keys only in env BACKUP_S3_ACCESS_KEY_ID and BACKUP_S3_SECRET_ACCESS_KEY): %w", err)
	}
	cfg := backup.Config{Root: c.root, Prefix: c.prefix, Keep: c.keep, TempDir: c.tmpDir}
	if backingUp {
		for _, p := range strings.Split(c.databases, ",") {
			if p = strings.TrimSpace(p); p != "" {
				cfg.Patterns = append(cfg.Patterns, p)
			}
		}
		if len(cfg.Patterns) == 0 {
			return nil, errors.New("no databases: set -databases or BACKUP_DATABASES")
		}
	}
	return backup.New(cfg, s3Store{client}, log)
}

// s3Store adapts the S3 client to backup.Store.
type s3Store struct{ *s3.Client }

func (s s3Store) List(ctx context.Context, prefix string) ([]backup.StoredObject, error) {
	objects, err := s.Client.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]backup.StoredObject, 0, len(objects))
	for _, o := range objects {
		out = append(out, backup.StoredObject{Key: o.Key, Size: o.Size})
	}
	return out, nil
}

// envReader turns environment variables into flag defaults and keeps the
// first invalid value as err.
type envReader struct {
	getenv func(string) string
	err    error
}

func (e *envReader) str(name, def string) string {
	if v := e.getenv(name); v != "" {
		return v
	}
	return def
}

func (e *envReader) parse(name string, parse func(string) error) {
	v := e.getenv(name)
	if v == "" {
		return
	}
	if err := parse(v); err != nil && e.err == nil {
		e.err = fmt.Errorf("env %s=%q: %w", name, v, err)
	}
}

func (e *envReader) boolean(name string, def bool) bool {
	e.parse(name, func(v string) (err error) { def, err = strconv.ParseBool(v); return err })
	return def
}

func (e *envReader) integer(name string, def int) int {
	e.parse(name, func(v string) (err error) { def, err = strconv.Atoi(v); return err })
	return def
}

func (e *envReader) duration(name string, def time.Duration) time.Duration {
	e.parse(name, func(v string) (err error) { def, err = time.ParseDuration(v); return err })
	return def
}
