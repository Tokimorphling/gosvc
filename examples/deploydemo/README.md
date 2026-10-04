# Persistent task board

A small single-user application demonstrating a complete gosvc service: an
embedded browser UI, authenticated REST and JSON-RPC, durable tasks, health
checks, metrics, structured logs and graceful shutdown. This is a standalone
Go module consuming the published framework dependency; it does not use a
local `replace` directive.

## Run locally

From this directory:

```sh
go run ./cmd/deploydemo -c config.toml
```

Open http://127.0.0.1:18080/. The development configuration disables
authentication. To try authentication locally, start with a disposable key:

```sh
DEMO_AUTH_API_KEYS=local-test-key \
  go run ./cmd/deploydemo -c config.toml
```

Enter that key in the browser UI. The UI stores it in the current tab's
`sessionStorage` and sends it in `X-API-Key`; it is not included in asset files
or URLs. The homepage, scripts, styles and health probes are public. All task
operations, status and JSON-RPC require authentication when enabled.

## API

All paths below are relative to the service base URL. Behind a path-prefix
proxy, strip the prefix before forwarding and redirect the prefix without a
trailing slash to its slash-terminated form so relative assets resolve.

| Method and path | Request | Success response |
| --- | --- | --- |
| `GET /api/v1/status` | — | `{ "service": "gosvc-demo", "version": "dev", "uptimeSeconds": 12, "storage": "JSON file" }` |
| `GET /api/v1/tasks` | — | An array of tasks, ordered by increasing ID; empty is `[]` |
| `POST /api/v1/tasks` | `{ "title": "Ship the service" }` | Task, HTTP 201 |
| `GET /api/v1/tasks/:id` | — | Task |
| `POST /api/v1/tasks/:id/complete` | — | Completed task; repeated calls are idempotent |
| `DELETE /api/v1/tasks/:id` | — | `{ "deleted": true }` |

A task is `{ "id": 1, "title": "Ship the service", "done": false,
"createdAt": "2026-10-04T00:00:00Z" }`. Errors use the framework's
`{ "error": { "code": "...", "message": "...", "requestId": "..." } }`
shape. The defaults allow 1,000 tasks and 160 Unicode characters per title.
HTTP bodies are limited to 16 KiB and request contexts have a five-second
budget.

Example:

```sh
curl http://127.0.0.1:18080/api/v1/tasks \
  -H 'Content-Type: application/json' -H 'X-API-Key: local-test-key' \
  --data '{"title":"Ship the service"}'
```

The same business service registers JSON-RPC methods `tasks.create`,
`tasks.get`, `tasks.list`, `tasks.complete`, and `tasks.delete` on `/rpc`.
Create accepts `{ "title": "..." }`, ID methods accept `{ "id": 1 }`,
and list accepts `{}`. Authentication also applies to this endpoint.

## Persistence and deployment

`tasks.dataFile` selects the JSON state file. Relative paths resolve from the
process working directory, not the configuration directory. Every mutation
writes a private temporary file in the same directory, syncs it, atomically
renames it over the previous state, and syncs the directory before returning
success. New directories use mode `0700` and files use `0600`. Startup rejects
invalid state instead of silently clearing it. A failed write before rename
keeps the previous in-memory state. If syncing the directory fails after
rename, the new state is visible but the request returns an error because
crash durability cannot be guaranteed.

Use **one process per data file** on a local filesystem that supports atomic
rename and directory fsync. A mutex coordinates requests inside that process;
there is no distributed lock or cross-process coordination. Writes rewrite
the bounded task collection, which is appropriate for this small personal
board. Use a database-backed repository for high write volume or multiple
replicas. The service does not configure backups automatically.

For production, set `auth.enabled = true`, supply a random key through
`DEMO_AUTH_API_KEYS`, and store the state on a persistent writable directory.
Keep all listeners on loopback and expose only the HTTP listener through
Caddy. Defaults are HTTP `127.0.0.1:18080`, gRPC `127.0.0.1:19090` and admin
`127.0.0.1:16060`. Admin includes metrics and debugging endpoints and should
remain private. TLS can terminate at Caddy. Configuration changes require a
process restart.

```sh
go build -trimpath -ldflags '-s -w -X main.version=demo-v1' \
  -o deploydemo ./cmd/deploydemo
./deploydemo -healthcheck http://127.0.0.1:16060/readyz
go test -race ./...
go vet ./...
```

Business logic lives in `service.go`, filesystem persistence in
`persistence.go`, transport/asset bindings in `app.go`, and runtime startup in
`cmd/deploydemo/main.go`. Static assets are embedded in the executable, so the
deployed service needs only its binary, configuration and persistent data
directory.

The temporary production verification unit and lifecycle notes live in
[`deploy/`](deploy/README.md). The completed, closed test is recorded in
[`deploy/verification.md`](deploy/verification.md).
