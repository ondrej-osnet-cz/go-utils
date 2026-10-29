# go-utils

Shared tools and packages for Go projects. One module, one version for all of
them; pick only what you import. A whole application with its own
dependencies (`dbbackup`) is a module of its own, tagged `<dir>/vX.Y.Z`.

| Directory           | What it is                                                                         |
|---------------------|------------------------------------------------------------------------------------|
| [`e2eui`](e2eui/)   | Interactive browser for E2E tests: run a test by click, see every HTTP request and response |
| [`dbbackup`](dbbackup/) | Service that backs up SQLite databases to S3 compatible storage on an interval; own module |

Add a tool with `go get -tool github.com/ondrej-osnet-cz/go-utils/<path>@latest`
and a package with a plain import. Each directory has a README on how to set it
up in a project.

## Adding a utility

- A new top-level directory with its own README; link it in the table above.
- Prefer the standard library. A dependency is added only when really needed,
  since every project using go-utils sees it in its module graph.
- `go vet ./...` and `go test -race ./...` must pass.
- Release by tagging `vX.Y.Z` (`dbbackup/vX.Y.Z` for dbbackup); a change that
  breaks a utility's API bumps the minor version while below v1.
- In `dbbackup` also `golangci-lint run` with its `.golangci.yml`.

## License

MIT
