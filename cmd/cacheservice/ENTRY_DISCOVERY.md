# How the cache service learns which entries exist

Design note for [#4](https://github.com/zerobull-consulting/dagger/issues/4).
It covers what the service takes in, how each input reaches it, how it tracks
entries, and how it picks export roots. It is based on the engine at
`v1.0.0-beta.23` and on a live run described at the end.

## Summary

- The engine tells the service about cache entries in **session spans** only.
  Engine cache events (`engine.start`, `engine.prune`, `engine.share`,
  `engine.stop`) never announce a new entry, and the WebSocket carries no
  notice of one either.
- Session spans are sent under the **client's** Cloud settings, not the
  engine's. The engine publishes a session's spans only when the CLI that
  started the session has Cloud telemetry configured, through
  `DAGGER_CLOUD_TOKEN` or a `dagger login` credential, and it sends them to the CLI's `DAGGER_CLOUD_URL`. This is why the protocol
  spike (#3) saw no spans: its CLI did not point at the service.
- With the CLI's `DAGGER_CLOUD_URL` pointed at the service, spans arrive. A
  `withExec` entry learned only from its span was exported, with its layers,
  after its session had ended.
- `engine.share` reports only rows **imported** from the remote cache. The
  spike never merged anything, so it never saw one. That is expected.
- The service should export every **retained** entry it sees in a session's
  spans, in one `export` per session, as soon as the session ends. It should
  drop entries named by `engine.prune` and by `gone` skips.

The one real gap is sessions whose clients send no telemetry to the service.
Their entries are invisible. See [Gaps and options](#gaps-and-options).

## What reaches the service, and how

| Input | Transport | Sent to / under | What it says about entries |
| --- | --- | --- | --- |
| WebSocket (`/v1/engine`) | engine's own connection | `_EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL`, engine token | Nothing unprompted: `hello` and pings only. |
| Engine cache events | OTLP logs, `/v1/logs` | engine's `DAGGER_CLOUD_URL`, engine token | Start, prune drops, imported-row sharing, stop. No new entries. |
| Session spans | OTLP traces, `/v1/traces` | **client's** `DAGGER_CLOUD_URL`, **client's** token | Every entry a call touched: number, type, deps, parts, retention, expiry. |

### Engine cache events

Turning on the remote cache turns on engine events
(`cmd/engine/remotecache.go`). The engine builds a logger provider of its own
for them (`newEngineEventExport` in `cmd/engine/telemetry.go`). It posts to
the engine's `DAGGER_CLOUD_URL` with `DAGGER_CLOUD_TOKEN` as Basic auth, and
discards its span and metric exporters.

Each record has instrumentation scope `dagger.io/engine.cache`. Its body is
the JSON of the event type. It carries two attributes:
`dagger.io/engine.event` (the kind) and `dagger.io/engine.cache`
(`<cacheID>/<generation>`). The record timestamp is the engine's clock at the
event. Types are in `engine/telemetryattrs/attrs.go`.

| Kind | Body | Use to the service |
| --- | --- | --- |
| `engine.start` | `engineVersion`, `engineName`, `restored`, `restoredEntries`, `wipedCache` | A new generation. `wipedCache` names a cache ID whose entries are all gone. |
| `engine.prune` | `drops[]`: `resultId`, `droppedAtUnixNano` | These entries lost their retention edge. Stop considering them, and expect `gone` if exported. |
| `engine.share` | `parts[]`: `resultId`, `part`, `deps`, `replacements` | An **imported** row got a local snapshot from an equivalent entry. Only fires on engines that can hold imports. Useful later for import bookkeeping (#8), not for discovery. |
| `engine.stop` | `clean`, `savedEntries` | `clean: false` means the next start wipes the cache. |

The emitter drops events when its 4096-slot queue is full and counts the drops
(`engineEventEmitter`). The service can't assume it saw every prune.

Why `engine.share` was never seen: it is reported by early snapshot sharing
(`dagql/cache_snapshot_sharing.go`). That gives an *imported* row ownership of
a local equivalent's snapshot. With nothing merged there are no imported rows,
so there is nothing to share.

### Session spans

Spans are stamped in `core/telemetry.go` (`cacheStateAttrs`) and in
`dagql/otelprof_lazy.go` (lazy evaluation). The span's **resource** carries
`dagger.io/engine.cache` (`<cacheID>/<generation>`). The span carries:

| Attribute | Meaning |
| --- | --- |
| `dagger.io/cache.result.id` | Entry number. With the cache ID, names the entry for the cache's lifetime. |
| `dagger.io/cache.type` | Type name, such as `Container` or `String`. |
| `dagger.io/cache.outcome` | `hit`, `executed`, ...; absent on lazy-evaluation spans. |
| `dagger.io/cache.deps` | Entry numbers of its dependencies. |
| `dagger.io/cache.retained` | `"true"` when it has a retention edge, so it outlives the session. |
| `dagger.io/cache.retention.expires` | Retention edge's expiry, Unix seconds. |
| `dagger.io/cache.expires` | When the entry stops serving hits, Unix seconds. |
| `dagger.io/cache.parts` | Complete part addresses (`fs`, `execMeta`, ...). |
| `dagger.io/cache.replacements` | Times the value was replaced under the same number. |
| `dagger.io/cache.session.spans` | On the session-end carrier span: how many of the session's spans carry a result ID. |

A lazily evaluated entry reports its parts later, on separate spans. In the
live run, the `Container.withExec` call span had no parts. Two later spans
named `resume withExec (metadata)` and `resume withExec (execOutputs)` carried
the same result number with 1 and 3 parts.

#### Who publishes them, and where

The engine publishes a session's telemetry to Cloud only when the session's
main client asks it to (`sessionPublishesToCloud` in
`engine/server/session_cloud_telemetry.go`). The CLI asks only when it has
Cloud telemetry configured (`cliCloudTelemetry.configured()` in
`internal/cmd/dagger/engine.go`). That takes a credential from
`DAGGER_CLOUD_TOKEN` or, when it is unset, from `dagger login` with its
current org (`GetCloudAuth` in `internal/cloud/auth/auth.go`). It then hands
over:

- its own `DAGGER_CLOUD_URL` as `ClientMetadata.CloudURL`;
- its own credential as `ClientMetadata.CloudAuth`.

The engine builds per-session exporters from those
(`initializeSessionCloudTelemetry`) and forwards the main client's telemetry
store through them (`session_cloud_forwarder.go`). The engine's own
`DAGGER_CLOUD_URL` is used only when the client's is empty
(`ResolveCloudURL`).

Consequences:

- A CLI with neither `DAGGER_CLOUD_TOKEN` nor a `dagger login` credential
  sends no session spans anywhere. The engine logs
  nothing about it.
- A CLI with a credential but no `DAGGER_CLOUD_URL` sends spans to the engine's
  `DAGGER_CLOUD_URL` if set, else to `https://api.dagger.cloud`.
- Span requests are authorized with the **user's** token, not the engine's.
  Auth in #10 must accept both kinds, or tell them apart.

## Tracking entries

Key every entry by **cache ID and result number**. Keep the generation, but
don't put it in the key:

- A **clean restart** keeps the cache ID, bumps the generation, and keeps
  entry numbers (observed in #3). Entries the service knew stay valid.
- A **forced kill** starts a new cache ID. The new `engine.start` names the
  old one in `wipedCache`; forget every entry under it.
- **`replacements`** changes when an entry's value is replaced in place. Treat
  a higher count as a new value: export it again.
- **Fence by generation.** Spans of one session can still be in flight when
  a newer generation starts reporting, and concurrent sessions' spans arrive
  in any order. Keep the newest generation seen per cache ID, and ignore
  observations from an older one. Within a generation, drop an observation
  whose `replacements` count is lower than the stored one. A higher count
  starts a new value: reset the entry's parts and deps to that observation's.
  An equal count is merged as below.

Group observations by **session** as well. Every session span's resource
carries `dagger.io/engine.session`, beside `dagger.io/engine.cache`. Several
sessions share one engine and can touch the same entry, so candidates and the
session-end carrier span are tracked per session ID and generation. One
session's carrier closes only that session's candidates. The persistent entry
key stays cache ID and result number.

Per entry, keep: type, deps, parts, retained, retention expiry, expiry,
replacements, the session that last touched it, and the export state
(`pending`, `exported` with the bundle key and the parts it covered, `busy`,
`refused`, `gone`).

**Two kinds of span.** A **state span** is a call span: it carries
`dagger.io/cache.outcome` and the entry's whole state at the span's end. On
a state span, an omitted `retained`, `retention.expires` or `expires` is a
fact: not retained, or no expiry. A **lazy-evaluation span** carries no
outcome. It reports only the result ID, deps, parts, the output content
digest when there is one, and the replacement count when it isn't 0
(`endOTelLazyOp` in `dagql/otelprof_lazy.go`). What else it leaves out says
nothing, and the stored value stays. On both kinds, an omitted
`dagger.io/cache.replacements` means 0, so the replacement fence above
applies to every span before any merge. Spans of the same value arrive in any order, so they
are merged per field, never replace the whole entry:

- **Parts** are a union. A later span can only add parts to a value, as the
  live run shows (`withExec` with 0, then 1, then 3 parts).
- **Deps** keep the longest list seen for the value. An empty list on a span
  that carries the attribute is a fact, but it never shrinks a known list.
- **Type** takes the latest non-empty value.
- **Retention expiry** and **expiry** come from the state span with the
  latest end time, including its omissions. Lazy-evaluation spans never
  change them.
- **Retained** follows the retention watermark below.

**Order retention by time.** Prunes come through the engine's own exporter
and spans through the session's, so a span that saw `retained = "true"`
before a prune can arrive after it. Keep a per-entry retention watermark:
the `droppedAtUnixNano` of the latest `engine.prune` drop for it. A state
span sets `retained`, to true or to false, only if it ended after both the
watermark and the state span stored last. Span end times and
drop times come from the same engine clock.

## Choosing export roots

1. **Candidates.** An entry is a candidate for a session when any of that
   session's spans (by `dagger.io/engine.session`) showed
   `dagger.io/cache.retained = "true"`. Entries without a retention edge are
   released with their session and come back `gone`.
2. **Skip** candidates already exported with the same `replacements` count
   and a part set that covers every part now known. An entry whose known
   parts grew since its export is exported again. Also skip candidates
   marked `refused` for the same `replacements` count and parts, and those
   where either expiry has passed: the entry's own
   (`dagger.io/cache.expires`) or its retention edge's
   (`dagger.io/cache.retention.expires`). A missing or zero value means no
   expiry. A lapsed retention edge means the entry is no longer retained,
   even if an earlier span said it was and no `engine.prune` has arrived.
3. **When.** Export when the session ends. The carrier span with
   `dagger.io/cache.session.spans` marks the end and says how many cache spans
   to expect. The live run didn't check that this span reaches the service,
   since the spike logs only spans with a result ID. If it doesn't come, fall
   back to a short quiet period.
   Don't wait longer: the engine's GC can prune retained entries right after
   the session (seen in #3 with GC on).
4. **One request per session.** Send all of a session's candidates as `roots`
   of one `export`. The bundle carries each root's whole dependency closure,
   and shared dependencies are encoded once.
5. **Outputs.** For each root, request the parts listed by its latest
   `dagger.io/cache.parts`, merged with parts reported by lazy-evaluation
   spans. Parts that own no bytes (absent or metadata parts) need no upload.
   Layers dedupe by digest in S3 (#5), so asking for a part already stored
   costs only the `upload` round trip.
6. **Results.** Handle each `skipped` root by its reason
   (`engine/remotecache/protocol/protocol.go`):
   - `gone`: the engine no longer has it. Mark it gone.
   - `busy`: it couldn't be captured now. Keep it pending and retry with
     backoff, in a later `export`.
   - `refused`: the engine would refuse it again. Mark it refused with its
     message, and don't retry until its `replacements` count or parts
     change.

   For the roots that made it into the bundle, store the bundle (#6) and
   upload the blobs (#7), or send an empty URL map to release the export.
   Releasing an export never changes a `busy` root's pending state.
7. **Prunes.** On `engine.prune`, mark each dropped `resultId` as no longer
   retained, and raise its retention watermark to the drop time. Don't delete what was already stored; retention of stored bundles
   is #11's job.

## Gaps and options

**Sessions whose clients send no telemetry to the service are invisible.**
The engine holds their retained entries, but nothing tells the service about
them. Options:

- **(a) Deployment requirement, no engine change.** Every client has Cloud
  telemetry configured (`DAGGER_CLOUD_TOKEN` or `dagger login`) and sets
  `DAGGER_CLOUD_URL` pointing at the service. The **engine's** own
  `DAGGER_CLOUD_URL` must point at the service too: engine cache events
  (`engine.start`, `engine.prune`) go there, not to the clients' URL, and
  without them the service misses cache wipes and prunes. Even then, the
  emitter drops events when its queue is full, so wipes and prunes are best
  effort: an export answer of `gone` remains the final word. Works today;
  this note's live run used it, with both URLs on the service. Costs: clients lose Dagger Cloud
  traces unless the service forwards them on. The service takes user tokens
  on `/v1/traces`. One client left unconfigured is a silent miss.
- **(b) Small engine change.** Add an engine event, such as `engine.retain`,
  listing entries that gained a retention edge: number, type, deps, parts,
  expiry, replacements. It would ride the existing event export, under the
  engine's token and URL, and cover every session. The change would sit next
  to `emitPruneEvent` in `engine/server/engine_events.go`, plus a hook where
  retention edges are added in `dagql`.
- **(c) Scan as a fallback.** Send `export` over ranges of entry numbers and
  keep what isn't `gone`. Crude: most live entries are core typedefs (about
  4,850 in #3). Only worth it for catching up after the service was down.

**Decision: go with (a) for now.** It needs no engine change, and #7 and #6
can proceed against it. To keep clients' Dagger Cloud traces and metrics, the
service forwards what it receives to the real Cloud URL; see
[Forwarding to Dagger Cloud](#forwarding-to-dagger-cloud). (b) stays a
candidate follow-up, (c) is for recovery, and the engine's workload export is
recorded below as an alternative.

### Forwarding to Dagger Cloud

Under (a), the service is the clients' `DAGGER_CLOUD_URL`. It reads session
spans to learn entries and passes everything on to the real Cloud URL, so
clients keep their traces and metrics. Requirements, from the client and
engine code:

- **Proxy every path, not only `/v1/*`.** The CLI uses `DAGGER_CLOUD_URL` as
  the base of all Cloud APIs: login and org lookups
  (`internal/cloud/auth/auth.go`), the GraphQL client
  (`internal/cloud/client.go`) and trace reads, `GET /v1/traces/{id}`
  (`internal/cloud/otlp.go`). Tap only `POST /v1/traces` and `POST /v1/logs`.
  Pass every other request through as is.
- **Forward synchronously and return Cloud's answer.** The engine's session
  forwarder advances its cursor only past rows Cloud accepted. It stops on an
  unusable credential or a permanent refusal (`session_cloud_forwarder.go`).
  If the service answered 200 itself, rows Cloud refused would be lost
  silently. Return Cloud's status and body, including partial-success bodies.
  The engine bounds each request at 10s (`CloudExportTimeout`), so the extra
  hop must stay well under that.
- **Keep the headers.** Pass `Authorization` (an engine token as Basic auth,
  an OIDC token, or an OAuth bearer that the engine refreshes itself),
  `X-Dagger-Org` and `X-Dagger-Export` (the exporter's sequence) through
  unchanged.
- **Tapping must not block forwarding.** Parse a copy of the body after the
  request has been forwarded. A parse failure is logged, not returned.
- **Don't forward engine cache events by default.** They arrive under the
  engine's `DAGGER_CLOUD_TOKEN`, the same token that authenticates the
  WebSocket. Forwarding them needs a real Cloud engine token. It's also
  unknown how Cloud treats `engine.*` events from an engine that isn't on its
  own remote cache. They come from their own logger provider, so a request
  whose records are all in scope `dagger.io/engine.cache` can be answered
  locally. Make forwarding them an option.
- **Known breakage:** with `DAGGER_CLOUD_URL` set to anything other than
  `api.dagger.cloud`, the CLI sends the GitHub app install and the OAuth
  redirect to dev targets, including `localhost:3000`
  (`internal/cmd/dagger/integration_cloud.go`,
  `internal/cmd/dagger/workspace.go`). `dagger cloud integration` and
  workspace GitHub setup won't work for clients behind the service.
- **Still a silent miss** for any client without a Cloud credential
  (`DAGGER_CLOUD_TOKEN` or `dagger login`) and this `DAGGER_CLOUD_URL`.
- **`dagger login` goes through the service too.** The CLI's auth package
  uses `DAGGER_CLOUD_URL` as its API base, so logging in and refreshing
  tokens pass through the proxy.

### Alternative: the engine's workload export (not tested)

The engine has an internal setting, `telemetry.workloadExport`
(`engine/config/config.go`, hidden from the schema with `jsonschema:"-"`).
It exports every session's spans from the engine to the standard
`OTEL_EXPORTER_OTLP_*` endpoint (`engine/telemetry/workload_export.go`).
Pointed at the service, it would deliver every session's spans under the
engine's own config. Clients change nothing and Dagger Cloud is untouched, so
no forwarding is needed.

Limits:

- **Only a fixed list of span fields is kept** (`workloadSpanAttribute` in
  `engine/telemetry/workload_spans.go`). It includes
  `dagger.io/cache.result.id` and `dagger.io/cache.outcome`. It drops
  `retained`, `deps`, `parts`, `type`, `expires` and `replacements`.
- **The resource has no `dagger.io/engine.cache`.** It has
  `service.instance.id`, which engine cache events also carry, so the cache ID
  can be joined from `engine.start`.
- **It shares the OTLP environment variables** with the engine's resource
  metrics export.
- **It is internal and undocumented.** It may change without notice.

How it could work without an engine change: after a session ends, export
every entry number its spans named. Entries without a retention edge come
back `gone`, which filters them out. The parts come from the bundle rather
than the span. With a one-line engine change, adding the
`dagger.io/cache.*` state fields to the allowlist, it would carry everything
session spans carry today.

This hasn't been run live. Before choosing it, confirm entry numbers arrive
at the service, and that export-then-filter-`gone` finds the retained entries
of a session.

## Live run

On 2026-10-31, with the engine, init-session and CLI built from this branch
and copied into `registry.dagger.io/engine:v1.0.0-beta.22`. The engine ran in
Docker with host networking and `{"gc":{"enabled":false}}`, and with
`_EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL`, `DAGGER_CLOUD_TOKEN` and
`DAGGER_CLOUD_URL` pointing at `cacheservice` on `127.0.0.1:8090`. Each CLI
run was `dagger core container from --address=alpine:3.20 with-exec
--args=sh,-c,'echo ...' stdout`.

| Case | CLI environment | At the service |
| --- | --- | --- |
| A | no `DAGGER_CLOUD_TOKEN`, no `DAGGER_CLOUD_URL` | No spans, no session logs. Only the engine's own `engine.start`. |
| C | `DAGGER_CLOUD_TOKEN=spike-token`, `DAGGER_CLOUD_URL=http://127.0.0.1:8090` | `HEAD /v1/traces` probe, then span and log batches. 7 cache spans. |

The variant with a token but no URL was not run. It would send the token to
`https://api.dagger.cloud`.

Cache spans from case C (cache `a68ced68-…/1`):

| Span | Result | Outcome | Type | Retained | Deps | Parts |
| --- | --- | --- | --- | --- | --- | --- |
| `Query.container` | 5315 | hit | Container | | 0 | 3 |
| `Container.from` | 5316 | hit | Container | true | 1 | 3 |
| `Container.from` | 5316 | executed | Container | true | 1 | 3 |
| `Container.withExec` | 5642 | executed | Container | true | 1 | 0 |
| `resume withExec (metadata)` | 5642 | | | | 1 | 1 |
| `Container.stdout` | 5643 | executed | String | | 1 | 0 |
| `resume withExec (execOutputs)` | 5642 | | | | 1 | 3 |

`Container.from` (5316) had been pulled by case A, which sent nothing. The
service learned of it only because case C touched it again.

After the session ended, `export` with `roots: [5642]` and an `fs` output for
5642 returned a version 3 bundle: 3 values, 5 layer digests, about 6KB. An
`upload` with no URLs released the export.
