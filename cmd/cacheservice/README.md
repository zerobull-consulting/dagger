# cacheservice: remote cache protocol spike

A spike for the S3-compatible remote cache
([#3](https://github.com/zerobull-consulting/dagger/issues/3), under
[#1](https://github.com/zerobull-consulting/dagger/issues/1)). It is the
smallest service an engine talks to. It stores nothing. Its job is to show what
the engine does on the wire before we design storage.

## What it serves

| Address | Path | Purpose |
| --- | --- | --- |
| `-listen` (`:8090`) | `/v1/engine` | The engine WebSocket (`protocol.EnginePath`) |
| `-listen` | `/v1/logs`, `/v1/traces`, `/v1/metrics` | OTLP/HTTP receivers for the engine's Cloud telemetry |
| `-listen` | `PUT /blobs/{digest}` | A stand-in for presigned PUTs: checks the blob against its digest, logs it, keeps nothing |
| `-debug-listen` (`127.0.0.1:8091`) | `GET /debug/engines` | Lists the welcomed engines |
| `-debug-listen` | `POST /debug/engines/{conn}/request?type=<t>` | Sends a request to one engine and returns its reply |

On the engine WebSocket, the service:

- takes the HTTP Basic username as the engine token. It accepts any non-empty
  token and logs only a short SHA-256 fingerprint of it.
- reads `hello`, replies `welcome`, and records the engine's cache identity.
- answers WebSocket pings (the library does this; `-v` logs each one).
- answers `renew` with `{"unavailable": true}`, since nothing is stored.
- answers any other request from the engine with an `error` reply.
- logs every envelope in both directions: direction, type, `id`, `re`, message
  size, body size, and a short summary for the known body types.

`-dump-dir DIR` also writes every envelope body and engine event body to a
numbered file in `DIR`.

The debug API has no authentication. It listens on localhost only by default.

## Running it against a local engine

1. Start the service:

   ```bash
   go run ./cmd/cacheservice -v -dump-dir /tmp/cacheservice-dump
   ```

2. Build and start a dev engine (see `hack/dev`). Then restart its container
   with these variables set:

   ```bash
   _EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL=http://<host>:8090
   DAGGER_CLOUD_TOKEN=spike-token
   DAGGER_CLOUD_URL=http://<host>:8090
   ```

   The remote cache connection turns on only when both the URL and
   `DAGGER_CLOUD_TOKEN` are set. `DAGGER_CLOUD_TOKEN=oidc` keeps it off. Any
   other string works as the token.

   Set `DAGGER_CLOUD_URL` too. Turning on the remote cache also turns on the
   engine's cache-event export (`cmd/engine/remotecache.go`), and those events
   go to `DAGGER_CLOUD_URL`. If it is unset, they go to
   `https://api.dagger.cloud` under the spike token.

   If `hack/dev` can't run (it loads Dagger modules from GitHub), build the
   binaries from this repo and copy them into the release image instead:

   ```bash
   CGO_ENABLED=0 go build -tags dfexcludepatterns,dfparents -o dagger-engine ./cmd/engine
   ```

   Do the same for `./cmd/init-session` and `./cmd/dagger`. Then build an image
   `FROM registry.dagger.io/engine:<release>` that copies them to
   `/usr/local/bin/`. Run it with `--privileged`, and point the CLI at it with
   `_EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://<name>`. On a host
   with little free disk, also mount `{"gc":{"enabled":false}}` at
   `/etc/dagger/engine.json`, or the engine prunes every result right after its
   session.

3. Run some pipelines through the engine, then probe it with requests by hand:

   ```bash
   curl -s localhost:8091/debug/engines
   ```

   ```bash
   curl -s -XPOST 'localhost:8091/debug/engines/1/request?type=export' -d '{"roots":[1,2,3]}'
   ```

   Take entry numbers from the `result` fields of the `cache span` log lines,
   or from the `engine.share` and `engine.prune` events.

## Expected behavior, read from the code

This section comes from reading `engine/remotecache`, `cmd/engine` and
`engine/server`. The live run below confirms most of it; see
[Observed behavior](#observed-behavior) for where it differs.

### On the WebSocket, what the engine sends without being asked

- **`hello`**, as the first message on every connection, carrying `cacheID`,
  `generation`, `engineVersion` and `engineName`. It waits up to 30s for
  `welcome`.
- **WebSocket pings** every 15s (`protocol.PingInterval`). A ping with no pong
  within 15s closes the connection.
- **`renew`**, only when a read of a part that the service offered earlier
  needs fresh download addresses. This service offers nothing, so a live run
  should never show one.

The engine sends nothing else on its own initiative. It does not announce new
cache entries over the WebSocket.

### On the WebSocket, what the engine sends only when asked

| Service sends | Engine answers | Notes |
| --- | --- | --- |
| `export` | `exported` | Holds the bundle's chains under `exportID` until `upload` or the end of the connection. At most 4 exports run at once. |
| `upload` | `uploaded` | One presigned PUT per blob. At most 8 PUTs run at once across all exports. An empty map releases the chains. |
| `merge` | `merged` | |
| `offer` | `offered` | |

### On reconnect

Each connection starts fresh with `hello`. Exports in flight, and the chains
held for them, are released when the connection ends. The engine waits 1s
before reconnecting, doubling up to 30s, with ±20% jitter. The wait resets once
the engine has been welcomed.

### Over OTLP, how the service learns the engine's cache

The WebSocket carries no notice of new cache entries. The service learns the
engine's cache from telemetry, posted to `DAGGER_CLOUD_URL`:

- **Engine cache events**, as log records at `/v1/logs`
  (`engine/telemetryattrs`, scope `dagger.io/engine.cache`):
  - `engine.start`: the cache opened and was restored.
  - `engine.share`: a snapshot-sharing pass completed parts, with each part's
    entry number, dependencies and replacement count.
  - `engine.prune`: retention edges were dropped.
  - `engine.stop`: the cache closed.

  Each event's body is JSON. It carries `dagger.io/engine.cache` (the cache
  identity and generation) as an attribute.
- **Session spans**, at `/v1/traces`. Spans that touched a cache entry carry
  `dagger.io/cache.result.id` and the entry's dependencies, parts, type,
  retention and expiry. Session telemetry goes out through each session's own
  exporters, so it may arrive from the client side, not only from the engine.

So the likely flow for a real service is: learn entry numbers from events and
spans, then `export` the ones worth keeping, then `upload` their blobs to S3.

## Observed behavior

From a live run on 2026-10-31: an engine built from this branch
(v1.0.0-beta.23), its binaries copied into the
`registry.dagger.io/engine:v1.0.0-beta.22` image and run under Docker, with
the service on the same host. The pipelines were small `dagger query` calls:
`container.from(alpine).withExec(...)` with a 2MB random file, and a
`directory.withNewFile.withNewDirectory`.

### Connection

- The engine connected as soon as it started, before any session. It sent
  `hello` (`cacheID`, `generation` 1, `engineVersion`, `engineName` "vm") and
  was welcomed. It asked for `permessage-deflate` with no context takeover. The
  dial's User-Agent is the Go default.
- WebSocket pings arrived every 15s.
- **Reconnects work.** When the service restarted, the engine reconnected
  within about a second, with a new `hello` carrying the same `cacheID` and
  `generation`.
- **A clean engine stop does not close the WebSocket cleanly.** On
  `docker stop`, the service saw the connection end with EOF and no close
  frame. `engine.stop` (`{"clean":true,"savedEntries":4858}`) arrived over OTLP
  about 0.6s later. On the WebSocket alone, the service cannot tell an engine
  shutdown from a network drop.
- After a clean restart, `hello` carried the same `cacheID` with `generation`
  2, and `engine.start` said `{"restored":true,"restoredEntries":11}`.
- After an unclean stop (`docker rm -f`), the next start had a new `cacheID`.
  Its `engine.start` named the old one: `"wipedCache":"<old cacheID>"`.

### What the engine sent unprompted

- Over the WebSocket: only `hello` and pings. No `renew`, as expected, since
  nothing was offered.
- Over OTLP: `engine.start`, `engine.prune` and `engine.stop` events, at
  `/v1/logs`. No `engine.share` was seen in these runs. Session logs and
  metrics arrived too, and the engine sent `HEAD /v1/traces` to probe the URL.
- **No spans arrived at `/v1/traces`**, though the engine logged "session
  telemetry published to Cloud". The cause is not yet known. Until it is,
  spans can't be relied on to learn entry numbers. This needs a follow-up
  before designing on top of spans.

### What the engine sent only when asked (debug API probes)

- **`export` without outputs** returns a `ValueBundle` (`version` 3) with only
  `roots` and `values`. Each value has the engine's `senderNumber`, its
  `dependencyIDs` (by bundle ordinal), and a record: the call (`field`,
  `args`) and an `objectJSON` with the object's metadata and its parts
  (`fs`, `execMeta`, ...) marked `pending`. The bundle includes the roots'
  whole dependency closure. A `Container.withExec` export was 3 values and
  about 4.4KB.
- Numbers the engine no longer has come back in `skipped` with reason `gone`,
  in the same `exported` reply as the survivors.
- **`export` with outputs** (`{"number": N, "address": {"part": "fs"}}`) adds
  `outputs` to the bundle: each selected part as `completed`, with its value
  (a directory at `/` for `linux/amd64`) and its layer chain (OCI
  `tar+gzip` layer descriptors: digest, size, uncompressed digest). It also
  includes parts along the dependency closure, such as `Container.from`'s
  `fs` (the alpine layer).
- **`upload`** with a URL per layer digest made the engine PUT each blob
  (`Content-Length` set, no `Content-Type`). All three digests verified. It
  answered `uploaded` with all three in `done`. An `upload` with an empty map
  released the export and answered `{"done":[]}`.
- **`offer`** with no parts answered each item. A live entry came back as
  `{"number": 5009}`, and an unknown one as `{"gone": true}`.
- **`merge`** with an empty bundle was refused with an error reply,
  `bundle has no roots`. The connection stayed up.
- Entry numbers stayed the same across a clean restart with the same
  `cacheID`.

### What survives a session

- About 4,850 entries were alive between sessions, nearly all typedefs
  (`Function`, `ObjectTypeDef`, `FunctionArg`, ...), numbered from 1. The
  engine keeps these for its whole life. A full scan of all numbers in chunks
  of 500 produced about 400–700KB of bundle JSON per chunk.
- Results of persistable fields (such as `Container.from`/`withExec` and
  `Directory.withNewFile`) kept their entries after the session, and a repeat
  run was a cache hit.
- **In this container, the engine's disk-space GC pruned them about a second
  after each session ended**, and reported it as `engine.prune`. Its default
  policy wanted more free space than the host had (target ~28GB). The runs
  above used `{"gc":{"enabled":false}}` in `/etc/dagger/engine.json`. A real
  service will see prunes like these, and should export entries promptly or
  expect them to be gone.

### Takeaways for the storage design

- Learning which entries exist is the open problem. The WebSocket never says
  so, spans did not arrive, and `engine.share` was not seen. Scanning numbers
  with `export` works, but it is crude.
- Blobs are ordinary OCI layer blobs, addressed by digest and PUT whole. That
  maps directly onto S3 objects keyed by digest. Layers repeat across outputs
  (alpine's base layer appeared under two entries), so dedupe by digest.
- The records (bundles) are small JSON and separate from the blobs. They need
  their own store, keyed by the engine's `cacheID` and entry number.
