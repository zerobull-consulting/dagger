# cacheservice: remote cache service

The S3-compatible remote cache service
([#1](https://github.com/zerobull-consulting/dagger/issues/1)). It began as a
protocol spike ([#3](https://github.com/zerobull-consulting/dagger/issues/3))
that stored nothing. With `CACHESERVICE_S3_BUCKET` set, it now stores the
entries engines build in S3 (see [Export path](#export-path)) and hands them to
other engines (see [Import path](#import-path)). Without it, it is still the
spike, and only logs what the engine does on the wire.

## What it serves

| Address | Path | Purpose |
| --- | --- | --- |
| `-listen` (`:8090`) | `/v1/engine` | The engine WebSocket (`protocol.EnginePath`) |
| `-listen` | `/v1/logs`, `/v1/traces`, `/v1/metrics` | OTLP/HTTP receivers for the engine's Cloud telemetry |
| `-listen` | `PUT /blobs/{digest}` | A stand-in for presigned PUTs: checks the blob against its digest, logs it, keeps nothing |
| `-debug-listen` (`127.0.0.1:8091`) | `GET /debug/engines` | Lists the welcomed engines |
| `-debug-listen` | `POST /debug/engines/{conn}/request?type=<t>` | Sends a request to one engine and returns its reply |
| `-debug-listen` | `POST /debug/engines/{conn}/export` | Exports now: the posted `{"roots":[...],"outputs":[...]}`, or every tracked entry worth it. Answers with what was stored |
| `-debug-listen` | `GET /debug/engines/{conn}/entries` | What the export path tracks of the engine's cache |
| `-debug-listen` | `POST /debug/engines/{conn}/import` | Merges and offers now what the engine lacks. Answers with what was merged, failed and offered |
| `-debug-listen` | `GET /debug/engines/{conn}/imports` | What the import path merged into the engine, and the entries it landed on |

On the engine WebSocket, the service:

- takes the HTTP Basic username as the engine token. It accepts any non-empty
  token and logs only a short SHA-256 fingerprint of it.
- reads `hello`, replies `welcome`, and records the engine's cache identity.
- answers WebSocket pings (the library does this; `-v` logs each one).
- answers `renew` with fresh presigned GETs when the import path is on and
  every layer is stored, and with `{"unavailable": true}` otherwise.
- answers any other request from the engine with an `error` reply.
- logs every envelope in both directions: direction, type, `id`, `re`, message
  size, body size, and a short summary for the known body types.

`-dump-dir DIR` also writes every envelope body and engine event body to a
numbered file in `DIR`.

The debug API has no authentication. It listens on localhost only by default.

## Blob store

[`blobstore`](blobstore) stores layer blobs in S3 for the service
([#5](https://github.com/zerobull-consulting/dagger/issues/5)). The export path
uses it. Blobs never pass through the service: it hands the
engine presigned URLs, so the engine never holds S3 credentials.

- Keys are content-addressed: `<prefix>/blobs/<algorithm>/<hex>`, such as
  `team/blobs/sha256/4f1e...`.
- `Has` checks a blob with `HeadObject`. A 404 means missing; any other error
  is an error.
- `PresignPut` signs a PUT for an exact size: `Content-Length` is a signed
  header, so S3 refuses a body of any other size. It signs no other headers,
  matching the engine's upload. It refuses size 0, because SigV4 doesn't sign
  a zero `Content-Length` and the URL would take a body of any size.
- `PresignGet` signs a GET. PUTs expire after 15 minutes and GETs after an hour
  by default. A URL signed with temporary credentials (STS, an instance role)
  stops working when they expire, so its `ExpiresAt` is capped at theirs.
- `UploadURLs` builds an `upload` request's URL map: it checks each blob with
  `Has` (16 at a time) and presigns PUTs only for the missing ones, so a blob
  already stored is not requested again.

**Known gap: content isn't verified.** S3 checks a PUT's size, not its
content. An engine that uploads a same-size body that doesn't hash to the
digest stores it under that digest. `Has` then reports the blob, and
`UploadURLs` never asks for the right one again. Downloading engines refuse it
(containerd checks the digest), so builds stay correct, but that cache entry
fails until the object is deleted. `TestMinIOStoreKnownGapMisdeclaredDigest`
pins this behavior. The planned fix needs no engine changes: uploads go to a
per-session staging prefix, and the service verifies each blob after the
engine's `uploaded` reply and only then promotes it to its digest key.

`S3ConfigFromEnv` reads endpoint, region, bucket, prefix, path-style
addressing, expiries and static credentials from `CACHESERVICE_S3_*`. Without
static credentials it uses the AWS SDK's default chain.

## Metadata store

[`metastore`](metastore) stores the cache's metadata in the same bucket as the
blobs, with no separate database
([#6](https://github.com/zerobull-consulting/dagger/issues/6)). The export path
records into it. Each scope (a team, a tenant) has:

- **bundles**, at `<prefix>/<scope>/bundles/<id>.json`: one per exported
  `dagql.ValueBundle`. The ID is the SHA-256 of the bundle's JSON, so a bundle
  is written once (`If-None-Match: *`) and never changes. Storing it again
  writes nothing. Reading it checks its content against the ID. Only bundles
  of the version this engine imports (`dagql.ValueBundleVersion`) are stored.
- **an index**, at `<prefix>/<scope>/index.json`: every stored entry (the
  bundle and ordinal holding its record, its source engine entry, replacement
  count, expiry, and the entries it keeps alive: its dependencies and the
  owners of its pending offers and outputs, as dagql's importer walks them), the retained roots and their expiries, and,
  per engine `cacheID`, which engine entry numbers map to which service
  numbers, with the latest generation each was recorded in.

The scope `blobs` is refused, since it would clash with the blob store's keys
under the same prefix.

**Index updates are atomic.** `UpdateIndex` reads the index and its ETag,
applies a change, and writes it back with `If-Match` on that ETag, or
`If-None-Match: *` when there was no index yet. A writer that loses (412, or
AWS's 409 for a conditional write in flight) waits with jittered exponential
backoff, reads the new index, and applies its change again. After
`MaxAttempts` (50 by default) it gives up with `ErrContention`. So concurrent
writers, in one process or in several replicas, never lose each other's
updates. All state is in the bucket, so a restarted service loses nothing.

MinIO honors these conditional writes (`TestMinIOConditionalWrites`,
`TestMinIOMetastoreConditionalWrites`), and so does AWS S3 since 2024.
`VerifyConditionalWrites` checks a store at startup, with a probe object it
deletes afterwards. A store that ignored them
would silently let writers overwrite each other, so a service should refuse
to start if the check fails. For such a store, the alternative is to keep the
index in DynamoDB, with bundles still in S3: one item per scope, holding the
same index JSON and a version attribute, written with `PutItem` and a
condition expression on that version (`attribute_not_exists(version)` for the
first write). The read, change and conditional write loop stays the same, so
only `Objects` would change. An item is limited to 400KB, though, so a large
scope would need one item per entry instead, written in a transaction with
the counter item.

**Service numbers are stable.** The service names stored entries with its own
numbers: the `SenderNumber` of a merge, an offer's `CloudNumber`, and the
number an engine keeps as its Cloud holding (see
`dagql/cache_cloud_holding.go`). Engines keep these across their own restarts
and re-key a holding when a message names another number, so numbers that
changed on every service restart would make engines re-key everything. So:

- The index's `nextNumber` is the next number to hand out. Numbers start at 1,
  only go up, and are never reused, even after their entry is dropped.
- A number is handed out inside an index update. If two writers hand out the
  same number, only one conditional write wins, and the other retries with
  the new `nextNumber`.
- An engine entry already in the index, by `cacheID` and engine number, keeps
  its service number when it is exported again. Engines keep their `cacheID`
  and entry numbers across a clean restart (the generation goes up), so this
  holds across engine restarts too. An unclean restart gets a new `cacheID`,
  and its entries get new numbers.
- A record replaces the stored one unless the stored one is newer: from a
  later generation, or from the same generation with more replacements. So a
  late export from an older generation never overwrites a newer record.
- The index is refused unless its mappings and entries agree both ways: each
  mapping names an entry from that cache and engine number, and each entry is
  its source's mapping.

`DropExpired` drops expired roots, expired entries and, transitively, the
entries that depend on them, then every entry no remaining root depends on,
with their mappings. Bundles stay in the bucket; collecting unreferenced ones
is left for later.

**Limits.** The whole index is one object, rewritten on every update, so its
size and the update rate per scope are bounded by how fast one object can be
read and written. Writers to one scope take turns; writers to different scopes
don't contend.

## Export path

[`exporter`](exporter) pushes engines' cache entries to S3
([#7](https://github.com/zerobull-consulting/dagger/issues/7)). It is on when
`CACHESERVICE_S3_BUCKET` is set; the other `CACHESERVICE_S3_*` settings are as
for the blob store. At startup the service checks the bucket's conditional
writes (`VerifyConditionalWrites`) and refuses to start if they fail.
Exported entries go into one scope, `-scope` (`default`), until auth and
namespacing ([#10](https://github.com/zerobull-consulting/dagger/issues/10))
picks one per engine.

**Triggers and roots.** The tracker follows
[ENTRY_DISCOVERY.md](ENTRY_DISCOVERY.md). It learns entries from session
spans at `/v1/traces`, and prunes and cache wipes from engine events at
`/v1/logs`. A session's roots are the entries its spans showed retained. Its
outputs are every part those spans reported. A session is exported:

- when it ends: its carrier span (`dagger.io/cache.session.spans`) arrived,
  and so did every cache span it counted, or `-export-end-grace` (2s) passed;
- or, if its end isn't seen, after `-export-quiet` (10s) without a cache span
  from it;
- or when asked: `POST /debug/engines/{conn}/export`.

Entries already stored with the same replacement count and parts are left
out. So are entries whose expiry or retention has lapsed, and entries refused
for the same value. The export goes to the engine connected with the
session's cache ID. If none is, the export waits and is tried again.

**One export.**

1. `export` with the roots and outputs. `exported` names skipped roots:
   `gone` ones are forgotten, `busy` ones are tried again with backoff (2s
   doubling to 5m), and `refused` ones are not tried again until their value
   changes.
2. The layer blobs the bundle's outputs need, each once, less those S3
   already has (`UploadURLs`), go in `upload` as presigned PUTs. When S3 has
   them all, the map is empty, and the engine releases the chains.
3. `uploaded`: if any blob failed, or wasn't answered, nothing is published.
   The engine released the chains with its answer, so the export is tried
   again from step 1 after a backoff (1s, doubling). Blobs stored by the
   earlier try aren't asked for again. After 3 tries the roots count as
   failed. The tracker tries them again later, up to 5 times per value.
4. Once every blob is stored, the bundle is published and recorded in the
   index (`metastore.Record`).

A repeated export of stored entries uploads no blob again, and keeps the
entries' service numbers. An unchanged bundle is not written again.

**Limits.** The engine works on 4 exports at once and queues the rest, so the
service sends at most 4 at a time to each engine. The engine uploads 8 blobs at
once across its exports; the service waits up to an hour for `uploaded`.
Neither side sends a message over 64 MiB. When the engine answers an export
with `too large`, or the service's own `export` or `upload` would be over the
limit, the roots are split in two and each half is exported alone (an
`upload` too large to send is preceded by an empty one, which releases the
chains). One root that is too large on its own fails. A batch holds at most
1,000 roots.

Presigned PUTs last `CACHESERVICE_S3_PUT_EXPIRY` (15m). In an export with so
many blobs that the engine, uploading 8 at a time, starts some after that,
those uploads fail and the export is tried again with fresh URLs for the
blobs still missing. For exports that large, raise the expiry. An export cut
short because its engine disconnected, or the service stopped, isn't counted
as a failure: its roots wait for the engine and are tried again.

**Known gaps.** Nothing is authenticated yet
([#10](https://github.com/zerobull-consulting/dagger/issues/10)): the
WebSocket takes any token, and the OTLP receivers take any request. Anyone who
can reach the service can forge spans, which make it export (or stop
exporting) a connected engine's entries, or connect as an engine and offer it
bundles of its own. Run the export path only on a trusted network until then.
Uploaded content isn't verified against its digest
([#23](https://github.com/zerobull-consulting/dagger/issues/23)). Sessions whose
clients send no spans to the service are invisible (see
[ENTRY_DISCOVERY.md](ENTRY_DISCOVERY.md#gaps-and-options)). The tracker's state
is in memory, so after a service restart entries are learned again from new
spans; what is in S3 is not lost, and a re-export uploads nothing already
stored.

## Import path

[`importer`](importer) makes engines reuse what other engines exported
([#8](https://github.com/zerobull-consulting/dagger/issues/8)). It is on with
the export path, reads the same scope, and is turned off with `-import=false`.

**When.** Each welcomed connection gets a sync loop. It syncs on `hello`,
again whenever another engine's export is stored, and every `-import-poll`
(30s), which picks up bundles stored by other service replicas. `POST
/debug/engines/{conn}/import` syncs now.

**One sync.**

1. **Plan.** The index's live roots, grouped by the bundle that holds each
   one's current record and the engine cache that exported it (identical
   exports from two caches share a bundle, but not sender numbers). Roots that have expired, or whose entry has, are
   left out, and so are an engine's own exports: its cache has them. A bundle
   already merged into this cache, at this generation, with every root it
   needs now, is not merged again.
2. **Merge.** Each bundle is rewritten for the engine: each value's
   `SenderNumber` becomes its service number, which the engine keeps as its
   Cloud holding; the roots are the planned ones with the index's expiries;
   and each output's chain gets a presigned GET per layer, with the scope as
   its renewal key. Records name each other by ordinal, so values are not
   dropped or renumbered, and the engine refuses a bundle with a value
   outside its roots' closure. So the bundle's other roots are sent too,
   already expired: the engine validates the bundle with them, then skips
   them and whatever only they need. A value only those need, whose entry the
   index has dropped, gets a placeholder number above every stored one, and
   is never merged. The engine answers `merged`: the entry each record
   landed on, its replacement count, expiry, dependencies, and complete and
   offered parts, and which roots it retained or skipped as expired. The
   importer keeps that, by service number (`GET
   /debug/engines/{conn}/imports`).
3. **Offer.** A stored part a merged entry has neither complete nor offered
   is offered to that entry, by the engine's number, with fresh GETs and the
   owner's dependencies mapped to the engine's numbers. `accepted` and
   `alreadyComplete` are recorded. `executionStarted` and `unavailable` are
   tried again at the next sync, up to 3 times. `invalid` is not tried again.
   An entry the engine answers `gone` for is forgotten. An offer over the
   message limit is split in two until each fits.

A merge refused by the engine is tried again at later syncs, up to 3 times per
bundle and engine generation. A merge over the 64 MiB message limit is not
sent. A new engine generation, or an `engine.start` that names a wiped cache,
starts that cache over; `engine.prune` forgets the dropped entries. A merged
reply that arrives after its cache's state was reset is discarded.

**Renew.** Presigned GETs last `CACHESERVICE_S3_GET_EXPIRY` (1h). When the
engine reads an offered part after they expired, it sends `renew` with the
chain's layers; the service checks each is stored and answers with fresh GETs,
or `unavailable` when one is missing or the renewal key isn't its scope.

**Known gaps.** Bundles are merged whole, so a bundle whose roots were partly
superseded by a later export still sends the older records of its other
values; the engine keeps whichever value it merged first, as the same recipe
is interchangeable. Parts are offered only at an entry's own address, not at
an output path into it. What the importer knows of each engine is in memory:
after a service restart, every connected engine is merged everything again,
which the engine keeps as it is. An engine's own exports are never merged back
into it, even after it pruned them. Entries an engine prunes, or answers
`gone` for, are not merged into it again until it restarts: merging them back
on the next poll would undo its garbage collection, which prunes them again.

## Running it against a local engine

For a local S3-compatible store (MinIO), see
[`hack/remote-cache`](../../hack/remote-cache/README.md).

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

   Take entry numbers from the `result` fields of the `cache span` log lines.
   Cache spans arrive only when the CLI itself has Cloud telemetry configured,
   through `DAGGER_CLOUD_TOKEN` or `dagger login`, and has `DAGGER_CLOUD_URL`
   pointing at the service.

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
    entry number, dependencies and replacement count. It covers only rows
    imported from the remote cache, so it says nothing about new entries.
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
  telemetry published to Cloud". The cause, found later: the engine sends a
  session's spans to the **CLI's** `DAGGER_CLOUD_URL`, under the CLI's token,
  not to its own. With the CLI's `DAGGER_CLOUD_URL` pointed at the service,
  spans arrive. See [ENTRY_DISCOVERY.md](ENTRY_DISCOVERY.md).

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

- Learning which entries exist was the open problem. The WebSocket never says
  so, spans did not arrive, and `engine.share` was not seen. Both are now
  explained, and the service can learn entries from session spans: see
  [ENTRY_DISCOVERY.md](ENTRY_DISCOVERY.md).
- Blobs are ordinary OCI layer blobs, addressed by digest and PUT whole. That
  maps directly onto S3 objects keyed by digest. Layers repeat across outputs
  (alpine's base layer appeared under two entries), so dedupe by digest.
- The records (bundles) are small JSON and separate from the blobs. They need
  their own store, keyed by the engine's `cacheID` and entry number.
