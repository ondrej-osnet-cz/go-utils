# dbbackup

`db-backup` is a small service that backs up the SQLite databases of other
applications to S3 compatible storage (AWS S3, MinIO, Cloudflare R2,
Backblaze B2, Wasabi, …). It runs beside the application, typically as a
second container on the same data volume, and backs up one or more databases
on a regular interval.

- **Consistent while the application runs.** Each database is copied with
  `VACUUM INTO`, which reads one consistent state even while the application
  writes (also in WAL mode); open transactions are not in the copy.
- **Checked before upload.** The copy must pass `PRAGMA integrity_check`, then
  it is gzipped and uploaded. The upload is signed with its SHA-256, so the
  storage rejects a corrupted transfer.
- **Patterns, not a fixed list.** Databases are globs relative to a root
  directory, so a database that appears later (a new tenant) is backed up
  without a config change. `-wal`, `-shm` and `-journal` files are skipped.
- **One failure does not stop the rest.** Every database is tried; the round
  reports which failed.
- **Retention** keeps the newest N backups of each database (or all of them,
  leaving cleanup to a bucket lifecycle rule). Old backups are deleted only
  after a new one was uploaded.
- **Restore** downloads a backup into a new file and checks it. It never
  overwrites a file, the live database included.

The standard library does the S3 part (Signature Version 4, single PUT up to
5 GiB per compressed database); the only dependency is the pure-Go SQLite
driver `modernc.org/sqlite`, so the binary is static and needs no CGO.

## Layout of the bucket

```
<prefix>/<path relative to root>/<UTC time>.db.gz

prod/osnet-invoicing.db/20261028T030000Z.db.gz
prod/osnet-invoicing-tenants/taxpayer-1.db/20261028T030000Z.db.gz
```

## Commands

```sh
db-backup run       # back up now and then every -interval until SIGINT/SIGTERM
db-backup once      # back up once and exit; status 1 if any database failed (for cron)
db-backup list      # list stored backups; -db app.db shows one database
db-backup restore -db app.db -out /tmp/app.db [-at 20261028T030000Z]
db-backup version
```

`run` logs one JSON line per database (`backup done` or `backup failed`) to
stderr. A failed round is logged and the service goes on; set
`-heartbeat-url` to get alerted when backups stop succeeding.

## Settings

A flag wins over its environment variable.

| Flag              | Environment            | Default     | Meaning |
|-------------------|------------------------|-------------|---------|
| `-databases`      | `BACKUP_DATABASES`     | (required)  | comma separated globs relative to the root, e.g. `app.db,app-tenants/*.db` (`run`, `once`) |
| `-root`           | `BACKUP_ROOT`          | `/data`     | directory the globs are relative to; the relative path names the backups |
| `-interval`       | `BACKUP_INTERVAL`      | `24h`       | time between rounds of `run`, at least `1m` |
| `-keep`           | `BACKUP_KEEP`          | `0`         | newest backups kept per database, older deleted; `0` keeps all |
| `-tmp-dir`        | `BACKUP_TMP_DIR`       | system temp | where the copy is checked and compressed; needs room for the largest database |
| `-heartbeat-url`  | `BACKUP_HEARTBEAT_URL` |             | URL to `GET` after every fully successful round (e.g. healthchecks.io); not logged |
| `-s3-endpoint`    | `BACKUP_S3_ENDPOINT`   | (required)  | e.g. `https://s3.eu-central-1.amazonaws.com`, `https://<account>.r2.cloudflarestorage.com`, `http://minio:9000` |
| `-s3-region`      | `BACKUP_S3_REGION`     | `us-east-1` | region used in the signature (`auto` for R2) |
| `-s3-bucket`      | `BACKUP_S3_BUCKET`     | (required)  | bucket; it must exist |
| `-s3-prefix`      | `BACKUP_S3_PREFIX`     |             | prefix of all keys, e.g. `prod` |
| `-s3-path-style`  | `BACKUP_S3_PATH_STYLE` | `false`     | bucket in the path (`endpoint/bucket/key`) instead of the host name; MinIO needs it |
|                   | `BACKUP_S3_ACCESS_KEY_ID`, `BACKUP_S3_SECRET_ACCESS_KEY` | (required) | the keys, **only** from the environment so they never show in a process list |

The keys need `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` and, with
`-keep`, `s3:DeleteObject` on the bucket (or prefix). Without `-keep`, a key
without delete rights (plus bucket versioning or object lock) keeps backups
safe even if the server is compromised.

## Running beside an application (docker compose)

The service mounts the same volume as the application. It needs write access
to it although it only reads the databases: in WAL mode a reader writes the
`-shm` file beside the database. The image runs as the same `nonroot` user
(uid 65532) as distroless application images.

```yaml
services:
  app:
    # …
    volumes:
      - app-data:/data

  backup:
    build: https://github.com/ondrej-osnet-cz/go-utils.git#dbbackup/v0.1.0:dbbackup
    restart: unless-stopped
    environment:
      BACKUP_DATABASES: osnet-invoicing.db,osnet-invoicing-tenants/*.db
      BACKUP_INTERVAL: ${BACKUP_INTERVAL:-6h}
      BACKUP_KEEP: ${BACKUP_KEEP:-60}
      BACKUP_S3_ENDPOINT: ${BACKUP_S3_ENDPOINT}
      BACKUP_S3_REGION: ${BACKUP_S3_REGION:-us-east-1}
      BACKUP_S3_BUCKET: ${BACKUP_S3_BUCKET}
      BACKUP_S3_PREFIX: ${BACKUP_S3_PREFIX:-prod}
      BACKUP_S3_ACCESS_KEY_ID: ${BACKUP_S3_ACCESS_KEY_ID}
      BACKUP_S3_SECRET_ACCESS_KEY: ${BACKUP_S3_SECRET_ACCESS_KEY}
      BACKUP_HEARTBEAT_URL: ${BACKUP_HEARTBEAT_URL:-}
    volumes:
      - app-data:/data

volumes:
  app-data:
```

Restore from the running container into the volume, then stop the
application and move the file into place:

```sh
docker compose exec backup /db-backup list -db osnet-invoicing.db
docker compose exec backup /db-backup restore -db osnet-invoicing.db -out /data/restored.db
```

Without Docker: `go install github.com/ondrej-osnet-cz/go-utils/dbbackup/cmd/db-backup@latest`.

## Module

`dbbackup` is its own Go module (`github.com/ondrej-osnet-cz/go-utils/dbbackup`),
so its SQLite driver does not enter the module graph of projects that use
the rest of go-utils. It is released by tags `dbbackup/vX.Y.Z`.

## License

MIT
