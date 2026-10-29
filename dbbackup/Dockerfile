ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
# Pure-Go SQLite driver: no CGO needed, so the binary is fully static.
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/db-backup ./cmd/db-backup

# nonroot (uid 65532) like the applications whose /data it reads: in WAL mode
# even a reader writes the -shm file beside the database.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/db-backup /db-backup
ENV BACKUP_ROOT=/data
ENTRYPOINT ["/db-backup"]
CMD ["run"]
