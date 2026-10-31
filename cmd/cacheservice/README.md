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
`engine/server`. **It has not yet been checked against a live engine.** The
live run (step 2 above) still needs to be done, and this section updated with
what it shows.

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

_To be filled in from a live run._
