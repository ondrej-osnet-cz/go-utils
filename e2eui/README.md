# e2eui

An interactive browser for the end-to-end tests of a Go project. It lists the
tests, runs the one you click and shows its progress, its output and every HTTP
request and response it made, with headers and bodies.

- Tests come from `go test -list`; ▶ runs a test, a group, a package or
  everything. Subtests appear after the first run. One run at a time, ■ stops it.
- A test runs exactly as on the command line:
  `go test -json -count=1 -run '^TestX$' <package>`. The tests are not changed.
- Tests are grouped by the prefix before the first underscore:
  `TestHealth_API_OK` and `TestHealth_MCP_OK` form the group `Health` with
  `API OK` and `MCP OK`. A prefix with a single test is no group.
- Requests to `/mcp` are labelled with their JSON-RPC method (MCP over
  streamable HTTP); streamed responses are recorded whole once closed.

The page is in Czech.

## Setup in a project

Requires Go 1.24 or newer.

1. Add the tool and the library to `go.mod`:

   ```sh
   go get -tool github.com/ondrej-osnet-cz/go-utils/e2eui/cmd/e2e-ui@latest
   ```

2. Record the HTTP traffic of the tests. Wrap the transport of the
   `http.Client` the tests use (best in one shared helper):

   ```go
   import "github.com/ondrej-osnet-cz/go-utils/e2eui/record"

   client := &http.Client{Transport: record.Transport(t, http.DefaultTransport)}
   ```

   For an `httptest.Server` client, wrap its transport:
   `c := *srv.Client(); c.Transport = record.Transport(t, c.Transport)`.
   Clients built on top of this transport (an MCP client, an OAuth client, …)
   are recorded too. Without the wrapper the tests still run in the UI, only
   without requests.

3. Run it from the module root and open http://127.0.0.1:9100:

   ```sh
   go tool e2e-ui -pkg ./e2e/...
   ```

   A Makefile target keeps it at hand:

   ```make
   e2e-ui:
   	go tool e2e-ui -pkg ./e2e/... $(E2E_UI_ARGS)
   ```

Update with the same `go get -tool …@latest`; it updates the whole go-utils module.

## Flags

| Flag    | Default          | Meaning                                         |
|---------|------------------|-------------------------------------------------|
| `-addr` | `127.0.0.1:9100` | listen address, loopback only                   |
| `-pkg`  | `./e2e/...`      | space separated package patterns with the tests |
| `-dir`  | `.`              | directory in which `go test` runs (module root) |

## How recording works

The UI starts `go test` with `E2E_RECORD_URL` (`record.URLEnv`) pointing at
itself. `record.Transport` posts every finished exchange there as JSON
(`record.Exchange`). Bodies are kept up to 1 MiB. Without the variable,
`record.Transport` returns the base transport unchanged, so the wrapper costs
nothing in a normal `go test` run. A failed post is only logged and never fails
the test.

## Security

The server runs `go test` on request. It therefore listens only on a loopback
address and rejects foreign `Host` and `Origin` headers and POSTs that are not
`application/json`.

## License

MIT
