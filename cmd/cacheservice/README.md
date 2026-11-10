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
| `-listen` | `/v1/logs`, `/v1/traces`, `/v1/metrics` | OTLP/HTTP receivers for the engine's Cloud telemetry, each body bounded (see [OTLP limits](#otlp-limits)) |
| `-listen` | `PUT /blobs/{digest}` | A stand-in for presigned PUTs: checks the blob against its digest, logs it, keeps nothing |
| `-listen` | `POST /v1/engines/{cacheID}/flush?timeout=<d>` | Exports what is pending for one engine cache now, through the scheduler, within `timeout` (default `10s`, at most `10m`), and answers with what is left. Authenticated as the engine's requests are. 409 on a read-only instance or with the export path off, 503 while draining or stopping (see [Flushing one engine cache](#flushing-one-engine-cache)) |
| `-listen` | `POST /v1/engines/flush?timeout=<d>` | The same, for the engine cache of the connections that authenticated with the request's token: the form for a sidecar, which doesn't know its engine's cache ID. 409 when that token's connections hold more than one cache |
| `-debug-listen` (`127.0.0.1:8091`) | `GET /debug/engines` | Lists the welcomed engines |
| `-debug-listen` | `POST /debug/engines/{conn}/request?type=<t>` | Sends a request to one engine and returns its reply |
| `-debug-listen` | `POST /debug/engines/{conn}/export` | Exports now: the posted `{"roots":[...],"outputs":[...]}`, or every tracked entry worth it. Answers with what was stored. It takes the engine cache's export slot: it waits for the export of that cache in flight to end, and the scheduler starts none beside it (see [Export path](#export-path)). 503 while draining; 409 on a read-only instance (`-export=false`) |
| `-debug-listen` | `POST /debug/engines/{conn}/flush?timeout=<d>` | The flush of a connection's engine cache, as `POST /v1/engines/{cacheID}/flush` does it, keyed like the other debug routes |
| `-debug-listen` | `GET /debug/engines/{conn}/entries` | What the export path tracks of the engine's cache, with each entry's `dirty` and `dueAt` |
| `-debug-listen` | `POST /debug/engines/{conn}/import` | Merges and offers now what the engine lacks. Answers with what was merged, failed and offered. 503 while draining |
| `-debug-listen` | `GET /debug/engines/{conn}/imports` | What the import path merged into the engine, and the entries it landed on |
| `-debug-listen` | `POST /debug/gc[?dry-run]` | Runs garbage collection now, over every namespace, and answers with what it did (see [Retention and garbage collection](#retention-and-garbage-collection)). 503 while draining; 409 on a read-only instance (`-export=false`) |
| `-debug-listen` | `GET /debug/identity` | The service's identity: its base release (or `dev`), the engine versions it accepts and its revision (see [Engine versions](#engine-versions)) |
| `-debug-listen` | `GET /debug/drain` | Whether the service is draining, what is left to export while it does, and the drain's report once it ended (see [Stopping and draining](#stopping-and-draining)) |
| `-debug-listen`, and `-metrics-listen` (off) | `GET /metrics` | Prometheus metrics (see [Metrics](#metrics)). `-metrics-listen` serves them alone, without credentials, for a scraper that shouldn't reach the debug API |

Every `-debug-listen` route, `/metrics` included, takes the debug token as
`Authorization: Bearer <token>` when `-debug-token-file` is set, and answers
401 without it (see [The debug API](#the-debug-api)).

On the engine WebSocket, the service:

- takes the HTTP Basic username as the engine token. With `-tokens-file`, it
  refuses an unknown token with 401 before the upgrade, and the token's
  namespace decides everything the connection exports, imports, is offered
  and renews (see [Auth and namespaces](#auth-and-namespaces)). Without it,
  it accepts any non-empty token, in the `-scope` namespace. It logs only a
  short SHA-256 fingerprint of a token.
- reads `hello`, refuses an engine whose version it doesn't accept (see
  [Engine versions](#engine-versions)), replies `welcome` with the
  service's identity, and records the engine's cache identity.
- answers WebSocket pings (the library does this; `-v` logs each one).
- answers `renew` with fresh presigned GETs when the import path is on and
  every layer is stored, and with `{"unavailable": true}` otherwise.
- answers any other request from the engine with an `error` reply.
- logs every envelope in both directions: direction, type, `id`, `re`, message
  size, body size, and a short summary for the known body types.

`-dump-dir DIR` also writes every envelope body and engine event body to a
numbered file in `DIR`.

The debug API sees and drives every namespace. It listens on localhost only
by default. With `-debug-token-file`, every request needs that token; the
service refuses to start without one when it has `-tokens-file`, or when
`-debug-listen` isn't a loopback address (see
[The debug API](#the-debug-api)).

## Engine versions

Each service release accepts a verified list of engine versions and refuses
every other engine
([zerobull-consulting/dagger#117](https://github.com/zerobull-consulting/dagger/issues/117)).
The list is the manifest [`compat/engines.txt`](compat/engines.txt),
embedded in the binary:

```
base v1.0.0-beta.25
accept v1.0.0-beta.25 sha256:39abecfe… verified: https://github.com/zerobull-consulting/dagger/actions/runs/…
reject v1.0.0-beta.NN zerobull-consulting/dagger#NNN
```

- **`base`** is the upstream release the service is built from.
- **`accept`** lines name an engine version, the digest of its stock
  `registry.dagger.io/engine` image, and the CI run of the e2e and the
  sidecar demo against that image that verified it. A version is accepted
  only after such a run is green. The first one is the engine CI's pull
  requests, the e2e harness and the demo run. At most 100.
- **`reject`** lines name a version that was tried and fails, and the issue
  saying why. It is refused like any unlisted version.

The parser is strict: an unknown line, a version on two lines, a malformed
version, digest, run URL or issue, no `base` or no `accept` line, or more
than 100 `accept` lines fails the unit tests, and the service at startup.
Every digest-pinned engine in this README, `hack/remote-cache` and the
sidecar action is checked against the `accept` lines
(`TestPinsMatchManifest`), and CI reads its pin from the manifest
(`hack/remote-cache/engines.sh pin`).

**New versions are verified by the release check**
([Release check](../../hack/remote-cache/README.md#release-check)): every
hour it looks for an upstream `v1` release newer than every line here,
waits for its image, and runs the e2e and the sidecar demo of this service
against it. A green run becomes an `accept` line through a PR; a red one is
tried again on a rebuild of the service on that release, and failing that
becomes a `reject` line and a `compat-break` issue. To verify a version by
hand, see [Engine](../../hack/remote-cache/README.md#engine).

**The match** is an exact string match of the hello's `engineVersion`
against the `accept` lines: no ranges, prefixes or normalization. An empty
version, a dev engine's (`v1.0.0-beta.26-dev-<commit>`, `+dirty`), a
rejected one or any release not listed is refused. It applies to every
engine, whatever its token or namespace, and before anything else of the
connection: a refused engine is never registered, owns no cache ID, and
is never tracked, merged into or exported from.

**A refusal** happens right after the hello, before the welcome:

- an `error` reply to the hello, whose message names the engine's version
  (quoted, cut at 64 bytes), the service's base and revision, the versions
  it accepts, and then: for a version on a `reject` line, its issue; for
  another version of a release's shape (at most 64 bytes), the hint to run
  `dagger-remote-cache:<engineVersion>` once that version is verified (the
  floating tag of the service's [image](#image) that accepts that engine);
- then a close with status **4002** (`server.CloseEngineVersionRefused`;
  4001 is a revoked token) and a short reason, within a close frame's 123
  bytes;
- a warning in the service's log, `engine refused: its version is not
  accepted`, with `engineVersion`, `accept`, `base` and `closeStatus=4002`,
  and `engine_version_mismatch_total{outcome="refused"}` (no version label:
  the version is the engine's to choose).

An accepted engine gets the welcome, and the service logs `engine welcomed`,
with `engineVersion` (cut at 64 bytes) and `versionAccepted`. `engine
connected` comes earlier, at the WebSocket upgrade, before the hello says
whether the engine is accepted: wait for `engine welcomed` or the refusal,
as the sidecar action's start does
([zerobull-consulting/dagger#146](https://github.com/zerobull-consulting/dagger/issues/146)).

The stock engine logs the refusal at warning level, `remote cache
connection ended; reconnecting`, with the error reply as its error:
`hello: hello: engine version "…" is not accepted; this cache service (base
…, revision …) accepts only …` (should the close frame reach it first, its
error names status 4002 and the reason instead). Its client treats 4002 as
any other close: it reconnects with its backoff, from 1s doubling to 30s,
which an unwelcomed connection never resets, so a refused engine retries
every 30s or so and is refused each time. Its builds run on without the
remote cache.

**`-allow-engine-version-mismatch`** is for development only: it welcomes
an engine of any version. The service logs `ENGINE VERSION CHECK OFF` at
startup, and `ENGINE VERSION NOT ACCEPTED, welcomed anyway` for each such
engine, counted as `engine_version_mismatch_total{outcome="allowed"}`;
`GET /debug/engines` shows it with `"versionAccepted": false`. The e2e
harness turns it on for `E2E_BUILD_ENGINE=1`, whose engines are built from
the checkout, and says so in the run's output. The default is strict.

**The service's identity** is its base release, its accept list and its
revision, not `internal/version/VERSION` (which tracks upstream main):

```json
{"base": "dev", "accept": ["v1.0.0-beta.25", "v1.0.0-beta.24"], "revision": "<commit>"}
```

It is in the welcome (`{"service": {...}}`, alongside `protocol.Welcome`'s
fields, of which there are none; the stock engine doesn't decode the
welcome's body, so nothing in `engine/` changed for it), at
`GET /debug/identity`, in refusals and in the startup log line `identity`.
`base` is the manifest's `base` only in a build stamped as that release
(the image's `--build-arg RELEASE=<tag>`, which sets `compat.Release`), as
every published [image](#image) is; any other build, such as a local one,
reports `dev`. A build
stamped as another release than the manifest's `base` refuses to start.
The revision is the image's `REVISION`, else the build's VCS revision, else
`dev`.

## Code layout

The service lives in package [`server`](server/), so it can run outside this
command: `cmd/cacheservice` (`main.go`) only parses flags, reads the
`CACHESERVICE_S3_*` settings and listens. To run the service in-process, as
tests do:

```go
svc := server.New(logger, "")
// Optional: store exports in S3 and merge them into engines.
if err := svc.EnableS3(ctx, s3cfg, server.S3Options{Scope: "default"}); err != nil {
	return err
}
engines := httptest.NewServer(svc.Handler())     // the -listen address
// Optional: require a token on the debug API (needed with tokens).
if err := svc.UseDebugTokenFile("debug-token"); err != nil {
	return err
}
debug := httptest.NewServer(svc.DebugHandler())  // the -debug-listen address
// To stop: cancel ctx, then
svc.CloseEngines()
svc.Wait()
```

`EnableExport` and `EnableImport` take stores directly, such as the in-memory
ones the unit tests use. The stores and the export and import logic are in
[`blobstore`](blobstore/), [`metastore`](metastore/),
[`exporter`](exporter/) and [`importer`](importer/), and garbage collection
in [`gc`](gc/). What the service measures is in [`metrics`](metrics/). Every
key the service writes is in [`layout`](layout/), and the ownership marker
and its startup check in [`ownership`](ownership/). The one-shot
`cacheservice lifecycle` subcommand, which sets the bucket's lifecycle rules,
is [`lifecycle`](lifecycle/) (see
[The lifecycle command](#the-lifecycle-command)).

## Bucket ownership

**The service owns everything under `CACHESERVICE_S3_PREFIX`**, or the whole
bucket when the prefix is empty
([zerobull-consulting/dagger#89](https://github.com/zerobull-consulting/dagger/issues/89)).
Nothing else may write there: not another application, not another copy of
the service with another prefix nested inside this one, not a person
copying files in by hand. **Give the service a bucket of its own** where you
can; a prefix of its own in a shared bucket works too, as long as nothing
else writes under it.

Rather than tell its keys apart from other applications', the service
asserts that nothing else is there, and stops when that's false.

**The marker.** `<prefix>/cacheservice.json` makes a prefix the service's:

```json
{"layout": 2, "created": "2026-11-05T12:00:00Z", "serviceVersion": "<revision>"}
```

`layout` is the version of everything under the prefix; `created` is when a
writer claimed the prefix, and `serviceVersion` the build that did (the
image's revision, stamped at build time, or the binary's VCS revision, or
`dev`). The marker is written once, and never rewritten or deleted by the
service.

**The layout** (version 2), with every shape in [`layout`](layout/), which
the writers and GC both use:

```
<prefix>/cacheservice.json                                the ownership marker
<prefix>/ns/<ns>/index.json                               the namespace's index
<prefix>/ns/<ns>/gc-epoch.json                            GC's published epoch
<prefix>/ns/<ns>/bundles/<sha256>.json                    bundles
<prefix>/ns/<ns>/resolutions/<sha256>.json                resolutions
<prefix>/ns/<ns>/blobs/<alg>/<hex>                        verified blobs
<prefix>/ns/<ns>/staging/<stage>/<alg>/<hex>              uploads not yet verified
<prefix>/ns/<ns>/conditional-write-check/<16 hex>         the startup probe, deleted after it
```

Incomplete multipart uploads exist only at blob keys (a promotion's copy).
Namespaces live only under `ns/`, with the names
[the tokens file](#the-tokens-file) takes. Anything else under the prefix
isn't the service's.

### At startup

Before anything else touches the bucket (`server.EnableS3`), every instance
checks the prefix:

| Marker | Under the prefix | Writer (export on) | Read-only (`-export=false`) |
| --- | --- | --- | --- |
| layout 2 | anything | starts | starts |
| another layout | anything | exits 1 | exits 1 |
| none | nothing | creates the marker (`If-None-Match: *`), starts; unless a multipart upload is in progress there (exit 1) | starts, with nothing to import; writes nothing |
| none | keys | exits 1 | exits 1 |

- **Claiming without starting a writer:** `cacheservice lifecycle claim`
  makes the same claim (this table's "none, nothing" row, as a writer) and
  stops, for an operator who has only the admin key (see
  [The `lifecycle` command](#the-lifecycle-command)).
- **Two writers on an empty prefix** both start: both create the marker
  with `If-None-Match: *`, one wins, and the other reads the winner's marker
  (`TestWritersRaceOnEmptyPrefix`, `TestMinIOBinaryWritersRaceOnEmptyPrefix`).
  A writer whose listing finds keys reads the marker again before refusing,
  since another writer may have claimed the prefix, and written under it, in
  between (`TestWriterListsAfterAnotherClaimed`).
- **Another layout:** `<prefix>/cacheservice.json records layout version 3,
  and this service uses layout 2: it can't use this store. Wipe the prefix
  (then re-run the cache-warming job), or set another CACHESERVICE_S3_PREFIX`.
- **Keys and no marker:** `prefix cache/ holds keys and no ownership marker
  (cacheservice.json), so it isn't this service's: <up to 10 keys>, ... The
  service needs a prefix of its own, which nothing else writes to: wipe it,
  or set another CACHESERVICE_S3_PREFIX`. There's no override flag.
- **A cache of the layout before this one** (`default/index.json`,
  `blobs/sha256/...`) is refused like another application's files, on
  purpose: there is no migration, and reading the old layout to recognize
  it would be one (see [Changing layouts](#changing-layouts);
  `TestNonEmptyPrefixRefused`, `TestMinIOBinaryRefusesPrefixNotOwned`).
- **A prefix nested in another service's** is refused by both kinds of
  instance: with no marker of its own, an instance also reads the marker
  position of each ancestor prefix (`cache/` for `cache/team`, and the
  bucket's top), and refuses if any holds one, of any content: that
  service owns everything under it (`TestNestedPrefixRefused`). What it
  does on each answer for an ancestor's marker:
  - **found** (of any content): refused, naming the ancestor.
  - **not found** (404): skipped; that ancestor holds no store.
  - **access denied** (403, for a key allowed only its own prefix):
    skipped. A nested claim it misses costs no data, only the ancestor's
    GC, which stops on the nested prefix's keys.
  - **any other answer or error** (a 5xx, a 401, a store-specific code, a
    timeout error, a dropped connection): refused, after the S3 client's
    own retries, with `can't read <key>, an ancestor's ownership marker
    position, to check that prefix <prefix>/ isn't inside another
    service's: <the store's error>`. The check doesn't guess, and adds no
    retries of its own: start again once the store answers
    (`TestAncestorUnreadRefused`, `TestStartupAncestorMarkerErrors`,
    `TestClaimAncestorMarkerErrors`).

  Only those two answers are skipped, so a key allowed only its own prefix
  must be denied the ancestors' markers with a 403. A key that a store
  refuses with a 401, or with a code of its own, can't start or claim
  there. Any 403
  counts as denied, whatever its code: a marker that exists but that a
  store answers with a 403 of another code (`InvalidObjectState`, say) is
  skipped too (`TestClaimAncestorInvalidObjectStateSkippedKnownGap`). An
  interrupt, or the caller's own deadline, ending the check while it reads
  an ancestor's marker stops it with that error, not this refusal.
- **An upload in progress counts too.** An object listing doesn't show
  multipart uploads, so a writer about to claim an empty prefix also lists
  them, and refuses one with any, after reading the marker once more, since a
  writer racing it may have claimed the prefix and started a promotion
  since (`upload:<key>` in the error; `TestWriterUploadListingAfterAnotherClaimed`;
  `TestWriterRefusesPrefixWithUpload`,
  `TestMinIOBinaryWriterRefusesPrefixWithUpload`). A read-only instance
  claims nothing and doesn't list them, so its key needn't be allowed to.
- **A marker of layout 2 lacking `created` or `serviceVersion`** wasn't
  written by the service: it is refused as unreadable, never taken as a
  claim (`TestIncompleteMarkerRefused`).
- **A folder placeholder counts as a key.** The zero-byte `cache/` object
  that the AWS and Spaces consoles create with "Create folder" makes the
  prefix not empty, and a writer refuses it, listing `cache/`. Intended:
  empty means no key at all. Delete the placeholder; the service needs no
  folder (`TestFolderPlaceholderRefused`).
- Refusals write and delete nothing (the tests compare full listings), and
  exit 1, the code for a store error; flag errors exit 2. They are an exit
  code and an `ERROR` line (`msg="export path"` or `msg="import path
  (read-only)"`, `error="ownership check: ..."`); there is no metric, since
  the process stops before anything scrapes it.

"Empty" is decided by one bounded listing (`ListObjectsV2`, at most 11
keys), and for a writer about to claim it, the multipart upload listing. What a startup costs, in requests to the store:

| Case | Ownership check | Then |
| --- | --- | --- |
| marker there (every start after the first) | 1 GET | writer: the conditional write probe, 4 PUTs and 1 DELETE |
| empty prefix, writer | 1 GET, 1 LIST, 1 GET per ancestor prefix, 1 upload listing (2 on MinIO, `FilterAllUploads`), 1 PUT (+1 GET if another writer won) | the probe |
| empty prefix, read-only | 1 GET, 1 LIST, 1 GET per ancestor prefix | nothing |
| refused | 1 GET, 1 LIST, 1 GET | nothing |

The ownership check and the conditional write probe stay separate: the probe
needs an object it can overwrite and delete, and the marker is never
rewritten or deleted. The probe runs second, in the scope's namespace, so a
refused prefix sees no write at all.

### GC relies on it

GC runs only under a marker of layout 2. It finds namespaces under `ns/`
only, and on a key or upload of no known shape it changes nothing in that
namespace, or, outside the namespaces, nothing at all: it logs up to 10 of
the keys and raises `gc_unexpected_keys`. It never deletes or aborts what it
didn't recognize as the service's, a violation included: whoever wrote it
removes it. See [One run, per namespace](#one-run-per-namespace).

## Changing layouts

A prefix holds one layout, the marker's. A service of another layout refuses
it at startup, and there is no migration code.

**Today: wipe and rebuild.** To move to a new layout, or to another prefix,
wipe the prefix (or point `CACHESERVICE_S3_PREFIX` at a new, empty one) and
re-run the cache-warming job. The first writer claims the empty prefix, and
the cache fills again. Pull-request jobs before that run start with nothing
to import (see [Read-only instances](#read-only-instances)).

```bash
aws s3 rm --recursive s3://dagger-cache/cache/   # or: mc rm --recursive --force local/dagger-cache/cache/
```

**Later: import from one prefix, export to another.** Images are pinned by
SHA, so an old image can still read the old prefix: run an engine with a
sidecar of the old image on the old prefix (read-only), and a sidecar of the
new image on the new prefix (a writer), and build through it. **The catch:**
imports are lazy. An engine holds an imported entry's bytes only once a
build has read them, and it can export only what it has, so this carries
over what builds touch, not the whole cache. Not built; it is to be tried
and documented in an issue under
[zerobull-consulting/dagger#1](https://github.com/zerobull-consulting/dagger/issues/1).

## Blob store

[`blobstore`](blobstore) stores layer blobs in S3 for the service
([#5](https://github.com/zerobull-consulting/dagger/issues/5)). The export path
uses it. Engines upload and download blobs directly: the service hands them
presigned URLs, so an engine never holds S3 credentials, and its uploads and
downloads never pass through the service. An upload goes to a staging key,
and reaches the blob's digest key only once the service has checked its
content ([#23](https://github.com/zerobull-consulting/dagger/issues/23)).
Usually the store hashes it as it copies it; the exception is a
service-hashed promotion (a blob over 5 GiB, `CACHESERVICE_S3_HASH_IN_SERVICE`,
or a store that returns no checksum), which reads the blob once through the
service, over its network to S3, with a 32 KiB buffer (see
[Verification and promotion](#verification-and-promotion)).

**Keys.** The service uses each namespace's view of the store (`Namespace`),
next to the namespace's metadata and apart from every other namespace's blobs
(see [Auth and namespaces](#auth-and-namespaces)):

```
<prefix>/ns/<ns>/blobs/<alg>/<hex>                    a blob, verified: its digest key
<prefix>/ns/<ns>/staging/<stage>/<alg>/<hex>          an upload, not yet verified
```

such as `cache/ns/team-a/blobs/sha256/4f1e...`. A **stage** is 128 random bits
(32 hex digits), new for each try of each export: two exports of one blob
never write the same staging key, and a staging key can't be guessed from a
digest. Engine-supplied strings never name a key: digests are validated, and
stages are the service's own.

- `Has` checks a blob's **digest key** with `HeadObject`. A 404 means
  missing; any other error is an error.
- `PresignPut` signs a PUT of a **staging key**, for an exact size:
  `Content-Length` is a signed header, so S3 refuses a body of any other
  size. It signs no other headers, matching the engine's upload (a bare PUT
  with `Content-Length`, unchanged by #23). It refuses size 0, because SigV4
  doesn't sign a zero `Content-Length` and the URL would take a body of any
  size.
- `PresignGet` signs a GET of a **digest key**. PUTs expire after 15 minutes
  and GETs after an hour by default. A URL signed with temporary credentials
  (STS, an instance role) stops working when they expire, so its `ExpiresAt`
  is capped at theirs.
- `UploadURLs` builds an `upload` request's URL map: it checks each blob with
  `Has` (16 at a time) and presigns PUTs, under the try's stage, only for the
  missing ones, so a verified blob is not requested again, and a blob whose
  upload failed verification is.
- `Promote` verifies one staged upload and moves it to its digest key (below).

`Has`, `UploadURLs`, and every presigned GET (the import path's merges and
offers, and `renew`) look at digest keys alone. So "stored" means
"verified", and a staged upload is never reported, offered, merged or renewed
(`TestVerifyStagingNeverServed`).

### Verification and promotion

The engine's `uploaded` reply lists the blobs it uploaded. For each one the
service asked for, the exporter calls `Promote`. It:

1. checks the staged object (`HeadObject`): **missing** if the engine reported
   a blob it never uploaded;
2. checks the digest key: **existing** if the blob is already stored, by an
   earlier upload or another export racing this one;
3. hashes the staged object (below), and copies it to the digest key only if
   the hash is the digest and the size the declared one, else **mismatch**;
4. completes the copy with `If-None-Match: *`, so the digest key appears only
   now, and a blob already there is never overwritten: when two exports race,
   the first verified copy wins and the other is **existing**
   (`TestMinIOConditionalPromotion`, `TestVerifyRacingExports`);
5. deletes the staged object once it has decided (promoted, existing or
   mismatch). After a store error it leaves the object to the
   [lifecycle rule](#lifecycle-rule-for-staging).

A blob that isn't promoted counts as failed: the export publishes nothing,
tries again with backoff under a new stage, and asks for that blob again.
The bundle and index are written only once every blob of the export is
promoted, so no stored bundle names a blob that wasn't verified
(`TestMinIOStoreMisdeclaredDigestNeverStored`, which replaced the known-gap
test `TestMinIOStoreKnownGapMisdeclaredDigest`;
`TestVerifyLyingUploadNotPublished`; the real-engine `verify` scenario).

**Who hashes (open question 1).** Plain S3-compatible storage can: MinIO
computes a full-object SHA-256 when it copies an object with
`ChecksumAlgorithm=SHA256`, from an object uploaded with no checksum at all,
and so does AWS S3 for objects up to 5 GB (`TestMinIOStoreHashesCopies`).

- A SHA-256 blob of up to 5 GiB is promoted by a multipart upload of one
  `UploadPartCopy`, created with `ChecksumAlgorithm=SHA256`. The store hashes
  the bytes as it copies them and returns the part's checksum, which for one
  part is the whole blob's, **before** the upload completes. A mismatch aborts
  the upload, so nothing is written to the digest key. No byte passes through
  the service.
- A blob over 5 GiB needs several parts (`UploadPartCopy`'s limit), whose
  checksums are per part and can't be combined into the blob's. The service
  then reads the staged object and hashes it itself, and copies it in parts
  of 1 GiB (larger when that would make over S3's 10,000 parts), each
  conditioned on the ETag it read (`CopySourceIfMatch`), so a staged object
  overwritten in between is not what gets promoted
  (`TestMinIOPromoteManyParts`, with the limits lowered to 5 MiB). A store
  may ignore that condition, as DigitalOcean Spaces does, so the service also
  takes each part's MD5 as it reads and compares it with the ETag the store
  returns for that part's copy. A difference is a mismatch, and the upload
  never completes (`TestPromoteChecksCopiedParts`,
  `TestMinIOPromoteStagedOverwritten`). This can't apply to a store whose
  ETags aren't MD5s, such as AWS S3 with SSE-KMS; the staged object's own ETag
  shows that, and only the condition protects the copy there. Since MD5 isn't
  collision resistant, someone who holds the presigned PUT, prepares a
  same-MD5 pair and wins the race could still get past it on a store that
  ignores the condition
  ([zerobull-consulting/dagger#67](https://github.com/zerobull-consulting/dagger/issues/67)).
- So is every blob with `CACHESERVICE_S3_HASH_IN_SERVICE=true`, for a store
  whose copy checksums can't be trusted, and every blob once a store returns
  a part copy without a checksum: the service then hashes from that point
  on (`TestMinIOPromoteStoreWithoutChecksums`).

**Why a multipart copy.** MinIO ignores `If-None-Match` on `CopyObject` (a
second conditional copy overwrote the key), but honors it on
`CompleteMultipartUpload` (412), and honors `CopySourceIfMatch`
(`TestMinIOConditionalPromotion`). AWS S3 documents both conditions on
those calls too. Spaces ignores both, so racing exports may both write there
([what each store does](../../hack/remote-cache/README.md#what-each-store-does)).
Correctness doesn't
rest on the condition: only verified copies are ever promoted, so a store
that ignored it would overwrite a blob with identical bytes.

**Bounds.** At most 8 promotions run at once per service, across every
namespace (`CACHESERVICE_S3_MAX_PROMOTIONS`), and one export promotes at most
8 at a time. A store-hashed promotion holds no blob data in the service; a
service-hashed one streams the object through a 32 KiB buffer, so at most
8 × 32 KiB in all.

**Time to promotion.** A blob reaches the shared cache after the engine's
`uploaded` reply (which waits for every upload of the export), plus its own
promotion. Promotion costs a fixed ~13 ms (6 requests: a HEAD of each key, the
multipart upload's create, copy and complete, and the staged object's
delete) plus the copy, which grows with the size. Measured against the local MinIO
(`TestMinIOPromoteLatency`, 2026-11-02; `CACHESERVICE_TEST_PROMOTE_MB=1024`
adds the 1 GiB row, which takes about 50s and writes 4 GiB):

| Blob size | Store-hashed | Service-hashed |
| --- | --- | --- |
| 64 KiB | 13 ms | 14 ms |
| 1 MiB | 20 ms | 21 ms |
| 16 MiB | 103 ms | 147 ms |
| 64 MiB | 381 ms | 465 ms |
| 1 GiB | 5.5 s | 6.9 s |

So roughly `13 ms + size / 185 MiB/s` when the store hashes, and
`13 ms + size / 150 MiB/s` when the service does, on one local disk. Store
copies on AWS S3 run in the store, at its internal copy rate; a
service-hashed promotion also reads the blob once over the service's network
to S3. `blob_promotion_duration_seconds` measures it in production (see
[Metrics](#metrics)).

**A restart between `uploaded` and promotion** leaves the staged uploads
where they are. The service doesn't scan staging on startup, or when a
namespace is first used: a staged object belongs to an export whose state
was lost with the restart
([#45](https://github.com/zerobull-consulting/dagger/issues/45)), so nothing
would publish a bundle for it. The next export of the entries asks for the
blobs again, under a new stage, and promotes those; the orphans are never
read, and the lifecycle rule expires them. The cost is the blobs' upload
again (`TestVerifyRestartBetweenUploadedAndPromotion`).

### Lifecycle rule for staging

Abandoned staged uploads (an export cut short, an engine that uploaded and
never answered, a store error during promotion) and incomplete multipart
copies are best expired by a bucket lifecycle rule, which costs the service
no request. Garbage collection covers a namespace without one: it reports
it (`gc_staging_rule_missing`, and a warning), and deletes its staged
uploads older than an export's upload timeout plus an hour itself, and it aborts stale
multipart uploads in every namespace (see
[Retention and garbage collection](#retention-and-garbage-collection)). One
day is safe for the rule: an export's uploads and promotions finish within
its upload timeout (1h).

S3 lifecycle filters match a key prefix, not a wildcard segment, so one rule
can't match `<prefix>/ns/*/staging/`. The options, and the choice:

- **A tag set on staging objects** would need the engine to send
  `x-amz-tagging` with its PUT: the presigned URL signs it as a header, and a
  bare PUT then fails (tried against MinIO: 400). Ruled out, since the engine
  doesn't change.
- **Staging outside the namespace** (`<prefix>/staging/<ns>/...`) would take
  one rule, but #23's comment from #52 settles staging under the namespace,
  so that promotion never crosses a namespace's prefix.
- **One rule per namespace**, on `<prefix>/ns/<ns>/staging/`: the choice. A
  namespace is added in the tokens file, so its rule can be generated from
  the same file (below). A namespace without a rule is covered by garbage
  collection, which deletes its abandoned uploads itself and reports the
  namespace ([#11](https://github.com/zerobull-consulting/dagger/issues/11)).
  AWS allows 1,000 rules per bucket.

Plus one rule for the whole prefix that aborts incomplete multipart uploads
(a promotion aborts its own, but a service killed mid-copy leaves its parts).

#### The `lifecycle` command

`cacheservice lifecycle` generates these rules from the tokens file, merges
them into the bucket's configuration, and checks a bucket against them
([zerobull-consulting/dagger#140](https://github.com/zerobull-consulting/dagger/issues/140)).
It is a subcommand of the service's binary, so it ships in the image, and it
reads the same `CACHESERVICE_S3_*` settings:

```bash
cacheservice lifecycle plan  -tokens-file tokens   # the default: print the rules and the diff; changes nothing
cacheservice lifecycle claim -confirm=dagger-cache/cache   # once per prefix: creates the ownership marker on an empty prefix
cacheservice lifecycle apply -tokens-file tokens -confirm=dagger-cache/cache
cacheservice lifecycle check -tokens-file tokens   # read-only: exit 3 if a rule is missing or too broad
```

or, from the image (`docker run --rm --env-file s3.env -v "$PWD/tokens:/tokens:ro"
ghcr.io/zerobull-consulting/dagger-remote-cache:<tag> lifecycle plan -tokens-file /tokens`).
Without `-tokens-file`, the one namespace is `-scope` (`default`), as for
the service.

**Setting up a fresh bucket or prefix**, with the admin key, from your own
shell, in this order:

1. `lifecycle claim -confirm=<bucket>/<prefix>`: `apply` refuses a prefix
   without the service's ownership marker, and on a fresh bucket the admin
   key is the only key there is, which must never go in the service's
   environment, so no writer can claim the prefix first. `claim` makes the
   claim a writer makes at startup. It doesn't need the admin key (a
   Read/Write/Delete key can claim), only a key that can list the prefix and
   its multipart uploads, and `s3:GetObject` and `s3:PutObject` on the marker
   (`<prefix>/cacheservice.json`); on a fresh bucket, that is the admin key.
   `s3:GetObject` on each ancestor prefix's marker is optional (see the keys
   table): on a 403, the nested-prefix check skips that ancestor. Any other
   error reading it (a 5xx, a timeout error) refuses the claim; run `claim`
   again.
2. `lifecycle apply -tokens-file tokens -confirm=<bucket>/<prefix>` (on
   MinIO, with `-abort-rule=false`).
3. `lifecycle check -tokens-file tokens`: exit 0.

Then start the service with its own key (the Read/Write/Delete one). A prefix
a writer already started on needs no `claim`; running it anyway is harmless.

**Run `apply` with the admin key, interactively.** Setting lifecycle rules
takes the bucket's most powerful key (on DigitalOcean Spaces, a full-access
key), since a rule can expire anything in the bucket. `claim` doesn't need
that power, only a key that can list the prefix and its multipart uploads,
and `s3:GetObject` and `s3:PutObject` on the marker; on a fresh bucket the admin
key is the only key there is, so that is the one to use. Run `apply` once when a
bucket is set up, and again when the tokens file's namespaces change, from
your own shell, with the key in your environment for that one command. Never
put the admin key in the service's secrets or a CI job's: no job needs it.
`plan` and `check` only read, so they run with the writer's key when it has
`s3:GetLifecycleConfiguration`; on Spaces a limited key doesn't, and they need
the admin key (see [the keys](#the-keys)). See
[Retention and garbage collection](#retention-and-garbage-collection) for
the three keys.

What it does:

- **`claim`** creates the prefix's ownership marker, `<prefix>/cacheservice.json`,
  by the same check a writer makes at startup (`ownership.Check` in writer
  mode, with `If-None-Match: *`), and does nothing else: it starts no
  server and runs no conditional-write probe, and the marker is the only
  object it writes. It needs `-confirm=<bucket>/<prefix>` (`<bucket>/` for an
  empty prefix), like `apply`, and reads no tokens file (`-tokens-file`, `-scope`, `-allow-removals`, `-abort-rule` and `-max-rules` are usage errors, exit 2). It is idempotent: with
  a marker of this layout there already, it prints `already claimed`, writes
  nothing and exits 0. It exits 1, writing nothing, on a prefix that holds keys
  and no marker, one with a marker of another layout, a prefix inside another
  service's, an upload in progress under it, or a store error (a 403 is
  explained as needing a key that can list the prefix and its uploads and read
  and write the marker). There is no
  `apply -claim`: keeping the steps apart keeps `apply`'s refusal of an
  unowned prefix a real check.
- **`plan`** prints the bucket and prefix, the namespaces and where they came
  from, the ownership marker, then one line per rule of the command's: `=`
  kept, `+` added, `~` updated (same ID, other content) or replaced (another
  ID of the command's for the same prefix and action, such as the former
  hand-made IDs below), `-` removed. Then how many other rules there are, the
  total against the store's limit, and the command's rules as JSON. It warns
  of any rule that expires a namespace's live data, and ends with the `apply`
  command for this plan: the same `-tokens-file` or `-scope`, the same
  options, and `-allow-removals` when it removes rules.
- **`apply` merges.** `PutBucketLifecycleConfiguration` replaces the whole
  configuration, so `apply` reads it, replaces only the rules it owns, and
  writes every other rule back as it read it. It owns a rule whose ID
  starts with `cacheservice-<hash>-`, the hash of its prefix (see Rule IDs
  below), and whose filter's prefix is under the service's prefix, or one
  with a former hand-made ID on exactly that ID's target (below): two
  services on two prefixes of one bucket, nested or not, each own their
  own, and other tools' rules are never touched. After writing, it reads
  the configuration back and checks that its rules are as wanted and the
  others exactly as they were. S3 has no conditional write for the
  configuration, so a change another tool makes between `apply`'s read and
  its write is lost, and that check reports it: don't run `apply` while
  something else changes the rules. When nothing changes, it writes
  nothing.
- **`apply` refuses**, exit 2 and nothing written: without `-confirm`
  repeating the bucket and the prefix (`<bucket>/<prefix>`, `<bucket>/` for
  an empty prefix); and when it would remove a rule nothing replaces, such
  as the staging rule of a namespace no longer in the tokens file, unless
  `-allow-removals` is passed. A removal costs nothing (GC expires that
  namespace's staged uploads itself), but a wrong tokens file, or a missing
  `-tokens-file`, would otherwise remove every namespace's rule at once.
- **`apply` refuses**, exit 1: a prefix without the service's ownership marker
  of this layout, as GC does (on an empty prefix, run `lifecycle claim` first,
  or start a writer on it); a configuration it can't read (it never writes one it
  couldn't merge); a result over the store's limit (`-max-rules`, 1,000 as
  on AWS); a wanted ID that a rule it doesn't own already has; and any rule
  broader than a namespace's staging prefix, except the rule that only aborts
  uploads, on the prefix itself. That last check (`lifecycle.CheckSafe`) runs
  on every rule it would write, so no rule it writes can expire live data.
- **`check`** reads the configuration and exits 3 when a namespace lacks its
  staging rule (what `gc_staging_rule_missing` reports, by the same rule
  parsing), when a rule expires a namespace's live data (GC's `liveRules`
  warning, and also a rule that expires on a date, filters by a tag or
  size too, or transitions objects to another storage class, which can
  leave them unreadable), or when a rule of the command's is too broad. It exits 1 when
  the configuration can't be read (`TestMinIOLifecycleCheckUnreadable`). The
  abort rule's absence is a note, not a failure. It doesn't run GC, and it
  checks the tokens file's namespaces, not those with data in the bucket:
  GC's metric covers those.

Exit codes: 0 ok, 1 an error or a refusal, 2 a flag or confirmation error
(`apply` or `claim` without a matching `-confirm`), 3 `check` found something. Every request is bounded (30s), and the whole
run by `-timeout` (2m). It prints no key or token: the tokens file is read
for its namespaces only.

**Rule IDs** are `cacheservice-<hash>-staging-<namespace>` and
`cacheservice-<hash>-incomplete-uploads`, where `<hash>` is 8 hex digits of
the SHA-256 of the prefix with its trailing slash (`cache/` gives
`daa886c7`). The hash keeps IDs unique between prefixes of one bucket, as S3
requires. Namespaces are validated as the service validates them (at most
128 letters, digits, `.`, `_` or `-`), so an ID is at most 158 characters,
under S3's 255; `CheckSafe` checks every ID again before writing.

**Stores.** MinIO refuses a rule with `AbortIncompleteMultipartUpload` (400
`InvalidArgument`, see `hack/remote-cache/README.md`, "What each store
does"). `apply` fails, writes nothing, and names that as the likely cause
of a 400: run it with `-abort-rule=false` there. MinIO drops stale uploads itself after 24 hours (its `api
stale_uploads_expiry`), and GC aborts stale ones at blob keys. A Spaces key
that isn't full-access gets 403, which `apply` explains as needing the admin
key. Tested against MinIO (`TestMinIOLifecycle`: plan, apply, a foreign rule
kept, a second apply writing nothing, a removed namespace losing its rule
with `-allow-removals`, check before and after, the abort rule refused;
`TestMinIOLifecycleClaim`: claim, then apply and check, a second claim
writing nothing; `TestMinIOLifecycleClaimRefusesKeys`) and in unit tests.

On DigitalOcean Spaces, as observed on 2026-11-07 on a bucket in sfo3, prefix
`cache`, namespace `default` (the first run on Spaces): with a prefix claimed
by a writer, a full-access key ran `lifecycle apply` (2 rules added, read
back) and `lifecycle check` (ok, exit 0). A limited Read/Write/Delete key
could not read the lifecycle configuration (`GetBucketLifecycleConfiguration`
returned 403 `AccessDenied`), so `lifecycle check` needs the admin key.

Run again on 2026-11-08 on another Spaces bucket (nyc3), with a full-access,
a limited `Read/Write/Delete` and a limited `Read` key: `lifecycle claim` on an
empty prefix created the marker with the full-access key (a second one wrote
nothing; a limited key claimed an empty prefix at a writer's startup, by the
same `ownership.Check`, but `lifecycle claim` itself was run with the
full-access key only), and `plan`, `apply` (2 rules added, read back) and `check` (ok) followed.
`claim` on a prefix holding a key exited 1 and wrote nothing. Every key-level
fact in [The keys](#the-keys) and [A read-only key](#a-read-only-key) was
seen again there.

#### The rules, for reference

On AWS, with the service's prefix `cache` and namespaces `team-a` and
`team-b`, `plan` prints these:

```json
{
  "Rules": [
    {"ID": "cacheservice-daa886c7-staging-team-a", "Status": "Enabled",
     "Filter": {"Prefix": "cache/ns/team-a/staging/"}, "Expiration": {"Days": 1}},
    {"ID": "cacheservice-daa886c7-staging-team-b", "Status": "Enabled",
     "Filter": {"Prefix": "cache/ns/team-b/staging/"}, "Expiration": {"Days": 1}},
    {"ID": "cacheservice-daa886c7-incomplete-uploads", "Status": "Enabled",
     "Filter": {"Prefix": "cache/"}, "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}
  ]
}
```

Before the command, this section built them by hand from the tokens file
(an `awk` and `jq` pipeline) and applied them with `aws s3api
put-bucket-lifecycle-configuration` or `mc ilm rule add`, with IDs
`cacheservice-staging-<namespace>` and `cacheservice-incomplete-copies`.
`apply` takes a rule with one of those IDs over as its own only on exactly
its target under the prefix (`<prefix>/ns/<namespace>/staging/`, or
`<prefix>/`), and replaces it with the IDs above. Use the
command rather than writing the configuration by hand: that call replaces
the bucket's whole configuration, and a hand-made merge can drop another
tool's rules.

### Repairing blobs stored before verification

Blobs stored at digest keys before verification shipped were never checked
(#23's open question 3). The service trusts them: `Has` reports them, and a
correct upload's promotion finds the key taken. It never re-verifies, deletes
or rewrites a digest key on its own, and never because an engine reported a
failure (a `renew`, or an offer that failed): that is one engine's say-so
([#23](https://github.com/zerobull-consulting/dagger/issues/23#issuecomment-6561085241)).
This gap is pinned by `TestMinIOStoreKnownGapUnverifiedOldObjects`, and doing
it in the service is
[#53](https://github.com/zerobull-consulting/dagger/issues/53).

To repair a bucket once, after upgrading: hash every object at a digest key
and compare it with its key. This reads every blob once:

```bash
# Lists the digest keys whose content doesn't match; deletes nothing.
mc find local/dagger-cache/cache --path '*/blobs/sha256/*' | while read -r obj; do
  want="${obj##*/}"
  got="$(mc cat "$obj" | sha256sum | cut -d' ' -f1)"
  [ "$got" = "$want" ] || echo "MISMATCH $obj ($got)"
done
```

(With the AWS CLI: `aws s3 ls --recursive s3://dagger-cache/cache/` and
`aws s3 cp s3://dagger-cache/<key> - | sha256sum`.) Delete the keys it
reports. The next export that needs such a blob uploads it again, and this
time it is verified; until then, an import of an entry naming it finds the
layer missing, as for any deleted blob.

### Configuration

`S3ConfigFromEnv` reads endpoint, region, bucket, prefix (`CACHESERVICE_S3_PREFIX`:
the service owns everything under it, the whole bucket when it is empty;
see [Bucket ownership](#bucket-ownership)), path-style addressing, expiries,
static credentials,
`CACHESERVICE_S3_MAX_PROMOTIONS` (8) and `CACHESERVICE_S3_HASH_IN_SERVICE`
(false) from `CACHESERVICE_S3_*`. Without static credentials it uses the AWS
SDK's default chain. The service's credentials need `s3:GetObject` and
`s3:DeleteObject` on staging keys (a copy reads its source), `s3:PutObject`
and `s3:AbortMultipartUpload` on digest keys (the multipart copy), besides
what the metadata store needs, and what garbage collection needs (see
[Retention and garbage collection](#retention-and-garbage-collection)). The
startup's ownership check needs `s3:ListBucket` on the prefix, and, on an
empty prefix, `s3:ListBucketMultipartUploads` (which GC needs anyway) and
`s3:PutObject` on `<prefix>/cacheservice.json`.

## Metadata store

[`metastore`](metastore) stores the cache's metadata in the same bucket as the
blobs, with no separate database
([#6](https://github.com/zerobull-consulting/dagger/issues/6)). The export path
records into it. Each scope (a team, a tenant) has:

- **bundles**, at `<prefix>/ns/<scope>/bundles/<id>.json`: one per exported
  `dagql.ValueBundle`. The ID is the SHA-256 of the bundle's JSON, so a bundle
  is written once (`If-None-Match: *`) and never changes. Storing it again
  writes nothing. Reading it checks its content against the ID. Only bundles
  of the version the engine imports are stored: the service's own
  `metastore.BundleVersion`, which a test pins to what `dagql`'s capture writes.
- **resolutions**, at `<prefix>/ns/<scope>/resolutions/<id>.json`: one per
  export, the service number each of the bundle's values resolved to when
  the export was recorded, which merges of that bundle send (see
  [Imported entries](#imported-entries)). Content-addressed and written once,
  like bundles.
- **an index**, at `<prefix>/ns/<scope>/index.json`: every stored entry (the
  bundle and ordinal holding its record, the resolution of the export that
  wrote it, its source engine entry, replacement
  count, expiry, and the entries it keeps alive: its dependencies and the
  owners of its pending offers and outputs, as dagql's importer walks them,
  and the parts its bundle holds outputs of), the retained roots and their
  expiries, and, per engine `cacheID`, which engine entry numbers map to
  which service numbers, with the latest generation each was recorded in:
  the entries the cache exported (`numbers`), and the stored entries merged
  into it (`imported`, see [Imported entries](#imported-entries)).

A scope is a namespace (see [Auth and namespaces](#auth-and-namespaces)), and
its blobs are under it too, at `<prefix>/ns/<scope>/blobs/`. Every scope
lives under `<prefix>/ns/`, so no name is reserved; `blobs` is a name like
any other. The GC epoch object is at `<prefix>/ns/<scope>/gc-epoch.json`,
and the startup probe (below) at
`<prefix>/ns/<scope>/conditional-write-check/<nonce>`. All of these shapes
are listed once, in [`layout`](layout), and the ownership marker,
`<prefix>/cacheservice.json`, sits above them (see
[Bucket ownership](#bucket-ownership)).

**Index updates are atomic.** `UpdateIndex` reads the index and its ETag,
applies a change, and writes it back with `If-Match` on that ETag, or
`If-None-Match: *` when there was no index yet. A writer that loses (412, or
AWS's 409 for a conditional write in flight) waits with jittered exponential
backoff, reads the new index, and applies its change again. After
`MaxAttempts` (50 by default) it gives up with `ErrContention`. So concurrent
writers, in one process or in several replicas, never lose each other's
updates. All state is in the bucket, so a restarted service loses nothing.

MinIO and DigitalOcean Spaces honor these conditional writes
(`TestMinIOConditionalWrites`, `TestMinIOMetastoreConditionalWrites`), and so
does AWS S3 since 2024, by its documentation. The ETag goes in `If-Match`
without its quotes: Spaces answers 412 to the quoted ETag it returned itself,
even when it is current. MinIO takes either; AWS S3 hasn't been run with this
yet, and `VerifyConditionalWrites` would refuse it at startup if it didn't.
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
- A record of the same value (as many replacements, from the same generation
  or a later one: engines keep replacement counts across a clean restart)
  replaces the stored one only if its bundle holds outputs of every part the
  stored record does. Otherwise the stored record stays, and a root the
  bundle has of it is refreshed as recorded (#133). A bundle carries outputs
  for its roots only (the tracker asks for their parts alone), so a stored
  value carried again as a dependency of a later root, which is how most
  values are seen again, holds fewer parts there; without this rule its
  entry moved to the later bundle with fewer parts, and the bundle holding
  its outputs, named by no entry, was collected (`TestDependencyKeepsItsBundle`).
  The kept entry's dependencies become those of both records: an engine can
  find more dependencies of a value at the same replacement count, and a merge
  of the later bundle sends its own record of the value, which names them
  (`TestRecordKeptRecordKeepsNewDependencies`).
  A record of a new value (more replacements) replaces the stored one
  whatever its parts. A record with other parts, neither more nor fewer, keeps
  the stored one too: an entry names one record
  (`TestRecordSameValueOtherPartsKeepsStored`). The new part reaches the
  index once an export holds both. While the tracker tracks the value, its
  next export does, since it keeps every part it has seen; after a service
  restart it knows only the parts the engine's spans report again, and an
  engine that no longer has the stored part leaves the new one out of the
  index, as with imports (#37). The kept record keeps its
  expiry, as an imported value's does (see below).
- A late export from a generation older than the cache's latest recorded one
  adds nothing the index has no mapping for: if any of its values has no
  stored entry, the whole record is refused (`metastore.ErrStaleGeneration`,
  #59), and the exporter settles its roots as refused rather than retry
  them. The exporter checks this against the index as soon as the engine
  answers export, before it asks for any upload, so a stale export uploads
  and publishes nothing (`TestExportStaleGenerationUploadsNothing`, #111).
  That check is only a shortcut: the one in the index update decides, since
  a newer generation can be recorded between the two
  (`TestExportStaleGenerationAfterEarlyCheck`). See [Max age](#max-age).
- A cache's record (its `generation`) outlives its mappings, for that check:
  once it has none, exported or imported, it stays until
  `metastore.CacheRecordRetention` (7 days) after it last changed
  (`updatedAtUnix`), then `Expire` drops it. A late export after that is
  recorded as new. Each kept record is about a hundred bytes of index.
- The index is refused unless its mappings and entries agree both ways: each
  exported mapping names an entry from that cache and engine number, and
  each entry is its source's mapping. Each imported mapping names a stored
  entry from another cache, on an engine entry the cache didn't export, and
  a stored entry is imported at most once per cache.

**Imported entries.** An engine that had a stored entry merged into it holds
the value on an entry of its own, and its sessions name that entry when they
hit it. So, after each merge, the importer records the engine entry each
stored entry landed on as the cache's import (`Index.RecordImports`). A
read-only instance doesn't (see [Read-only instances](#read-only-instances)):

- `Record` gives a value whose engine entry is imported the imported entry's
  service number, whether it is a root or in another root's closure. The
  same value is never stored twice under two numbers, and a new entry over
  imported ones depends on their original numbers.
- An import doesn't change the stored entry's source. A record from the
  importing cache replaces the stored one only if its bundle holds outputs of
  every part the stored record does, and more. The entry then keeps its
  number, its source becomes the importing cache, and the old source's
  mapping becomes an import. Generations and replacement counts of two
  caches don't compare, so nothing else decides between their records.
- An import that isn't recorded: of an entry no longer stored, of an entry
  the cache exported itself, or onto an engine entry the cache exported,
  which keeps its own number. A stored entry imported onto another engine
  entry than before moves there.
- A wiped cache (`engine.start` naming it) loses its imports; its exported
  entries stay, and so does its record, with its generation (see above). `engine.prune` does not drop imports: a prune drops
  retention edges, and an entry another one depends on lives on, so a later
  export may still name it in a closure. Dropping its import then would
  store it again under a new number, or leave that bundle unmergeable. Engine
  numbers are never reused within a cache, so a stale import names nothing.
- An imported entry keeps its record's expiry when the importing cache
  exports it again, even if that cache's record says it expires later
  (`TestRecordImportKeepsRecordExpiry`). This is intentional: merges send the
  record with the expiry it holds, so a later expiry in the index would offer
  engines an entry whose record they treat as expired. So an imported value's
  retention ends when its record expires: the root takes the importing
  cache's later expiry, but `DropExpired` drops a root whose entry has
  expired, whatever the root's own expiry. The importing engine's copy came
  from the merge with the record's expiry, so it expires then too, unless
  that engine built the value itself. The tracker
  likewise doesn't export an unchanged import for a later expiry, as it
  doesn't for a stored entry (`TestTrackerImportedIgnoresExpiry`).
- **Known gap
  ([#37](https://github.com/zerobull-consulting/dagger/issues/37)):** a
  record of an imported value that holds a part the stored record lacks, but
  lacks one the stored record holds, is not recorded
  (`TestRecordKnownGapImportWithOtherParts`). Replacing the stored record
  would lose its part, and an entry names one record, so two bundles' parts
  can't be combined. The export still reports the root as stored, so the
  tracker doesn't try it again while the value is unchanged
  (`TestExportKnownGapImportWithOtherParts`). The new part stays in that
  engine and doesn't reach others. Recording it would need entries that name
  a record per part.
- Indexes written before imports have entries with no parts. A record of an
  imported value with any part replaces such an entry once.

**Each export's resolution is stored
([#47](https://github.com/zerobull-consulting/dagger/issues/47)).** A bundle
names its values by the exporting engine's numbers (`SenderNumber`), and
those of imported values resolve through the cache's imported mapping, which
doesn't last: an import moves when the engine has the stored entry merged
onto another entry (it pruned it, restarted, and had it merged again), and a
wipe drops them all. Resolving a stored bundle through the mapping at merge
time then failed with "value N, which a root needs, is no longer stored", for
every engine, at every sync. Bundles never change, so how they resolved
doesn't either: `Store.Record` stores it, and merges use it.

- **What and where.** A `metastore.Resolution` is the bundle's ID, the
  exporting `cacheID`, and the service number of each value by ordinal. It
  is its own immutable object, keyed by the SHA-256 of its JSON, and each
  record the export writes names it (`Entry.Resolution`). A bundle two caches
  exported resolves to different numbers, so each gets its own resolution
  (the `cacheID` is in it); two exports that resolve a bundle the same way
  share one. This is the shape of
  [#46](https://github.com/zerobull-consulting/dagger/issues/46)'s record
  descriptions (immutable, one per export, keyed by content), which can
  absorb it by adding each record's dependencies, parts, generation and
  replacements, with entries naming a (description, ordinal) as they name a
  (resolution, ordinal) now.
- **Cost.** One more conditional PUT per recorded export (`If-None-Match:
  *`; a resolution already stored costs a GET to check it), about 8 bytes of
  JSON per value plus the IDs, and one more when a lost index write changes
  the numbers. The index grows by one ID, about 80 bytes, per entry. The
  importer reads a resolution once per bundle and keeps it in memory, like
  bundles.
- **Write order.** Bundle, then resolution, then index. The resolution is
  stored inside the index update, after the change resolved the values
  against the index it read and before the conditional write. A write that
  loses reads the newer index, resolves again (numbers may change), and
  stores that resolution, if it differs, before trying again. So the index
  never names a resolution that isn't stored, and `UpdateIndex`'s
  compare-and-swap is unchanged. A service that stops between the resolution
  and the index write leaves a resolution nothing names, and the export
  isn't recorded, as when it stops after storing the bundle
  (`TestRecordStopsAfterResolution`, `TestRecordWritesResolutionBeforeIndex`).
- **The index still decides what is stored.** A merge sends a value's
  resolved number only while the index has that entry; otherwise it gets a
  placeholder, or the merge fails if a merged root needs it. So a resolution
  keeps no entry alive, and never names one `DropExpired` dropped: numbers
  are never reused (`TestResolutionAndDropExpired`,
  `TestBuildMergeDroppedValues`). It doesn't depend on imports at all, so
  #12's generation fence on recording imports, and wipes racing them, don't
  change it. An entry still names one record (#37): the resolution is of the
  export that wrote it.
- **Records stored before resolutions** name none, and their bundles resolve
  through the cache's current mappings, as before
  (`TestLegacyBundleResolvesThroughCurrentMappings`). There is nothing to
  backfill from once an import has moved, a backfill at startup would cost a
  pass over every entry and bundle, and no deployment held such bundles. A
  record gets a resolution when its value is exported again.
- **Index version 2.** Resolutions bumped the index format to version 2. A
  version 1 index reads as one whose records name no resolution, and is
  written back as version 2. A service that reads only version 1 refuses a
  version 2 index rather than rewrite it without the field, so during a
  rolling upgrade older replicas stop updating the scope instead of erasing
  the resolutions newer ones recorded.

`DropExpired` drops expired roots, expired entries and, transitively, the
entries that depend on them, then every entry no remaining root depends on,
with their mappings, exported and imported. `Expire` does the same under a
namespace's max age, which also expires a root that long after it was
recorded (`Root.RecordedAtUnix`). Bundles, resolutions and blobs stay in the
bucket; garbage collection runs both on a schedule, and deletes what no
entry names any more (see
[Retention and garbage collection](#retention-and-garbage-collection)).

- **Index version 4.** Each cache record carries the time the index last
  changed it (`updatedAtUnix`, required), so a record with no mappings is
  kept for its generation and then dropped (#59). A version 1 to 3 index
  reads with every cache record stamped with the reader's clock, so an
  empty one is kept a whole retention from the upgrade. A service that
  reads only version 3 refuses a version 4 index, rather than write it back
  without the times.
- **Index version 3.** Roots carry the time an export last recorded them
  (`recordedAtUnix`), and the index holds GC's deletions (`gc`). A version 1
  or 2 index reads as one whose roots have no recording time (GC stamps them
  at its first run, so they get a whole max age from then) and no
  deletions. A service that reads only version 2 refuses a version 3 index:
  it doesn't check GC's deletions when it records an export, so during a
  rolling upgrade the older replicas stop updating the scope instead of
  naming an object GC is deleting.

**Limits.** The whole index is one object, rewritten on every update, so its
size and the update rate per scope are bounded by how fast one object can be
read and written. Writers to one scope take turns; writers to different scopes
don't contend.

## Export path

[`exporter`](exporter) pushes engines' cache entries to S3
([#7](https://github.com/zerobull-consulting/dagger/issues/7)). It is on when
`CACHESERVICE_S3_BUCKET` is set, unless `-export=false`; the other
`CACHESERVICE_S3_*` settings are as for the blob store. At startup the
service checks that the prefix is its own (see
[Bucket ownership](#bucket-ownership)), then the bucket's conditional writes
(`VerifyConditionalWrites`), and refuses to start if either fails. `-export=false` (default true, mirroring
`-import`) turns the export path off and makes the instance read-only: it
only imports, and never writes to or deletes from the bucket (see
[Read-only instances](#read-only-instances)).
Exported entries go into the scope of the engine's namespace: its token's,
with `-tokens-file`, or `-scope` (`default`) without (see
[Auth and namespaces](#auth-and-namespaces)). Each namespace has its own
tracker and exporter, and its blobs, bundles and index under its own prefix.

**Triggers and roots.** The tracker follows
[ENTRY_DISCOVERY.md](ENTRY_DISCOVERY.md). It learns entries from session
spans at `/v1/traces`, and prunes and cache wipes from engine events at
`/v1/logs`. The roots are the entries spans showed retained, and each
root's outputs are every part its spans reported. Exports stream: an entry
is exported while its session still runs, once its spans settle
([zerobull-consulting/dagger#134](https://github.com/zerobull-consulting/dagger/issues/134)).
An entry is **dirty** from the first span that changes its value (a new
replacement count, a new part, deps that grew, or retained turning true)
until an export takes it. A dirty entry worth exporting is due:

- once `-export-settle` (1s) passed with no span naming it; each span naming
  it starts the wait again;
- or once it has been dirty for `-export-max-delay` (10s), counted from its
  first change since its last export, so an entry a long, busy session keeps
  naming is still exported;
- or at once when its session ends: the carrier span
  (`dagger.io/cache.session.spans`) arrived, and so did every cache span it
  counted. A carrier with spans missing changes nothing, and nothing waits
  for a session that sends none;
- or when asked: `POST /debug/engines/{conn}/export`;
- or when the service stops cleanly (SIGTERM): the drain exports every dirty
  entry at once, without waiting for it to settle. See
  [Stopping and draining](#stopping-and-draining).

A value that changes while its export is in flight is dirty again, and is
exported again once it settles, or `-export-max-delay` after the change, so a
value that keeps changing isn't exported back to back; the next export
covers its current parts. A non-retained entry is forgotten 10s after a span
last named it, and so is a session.

**One export per engine cache at a time**
([zerobull-consulting/dagger#135](https://github.com/zerobull-consulting/dagger/issues/135)).
Each engine cache of a namespace has one export slot. While an export of it
is in flight, the entries that come due wait (`export_roots_waiting`), and
go in its next batch, which starts when that export ends. So batches follow
load: small and quick when the build is idle, larger when it's busy, with
one export's compression and uploads on the engine's host at a time. A batch
of more than 1,000 roots due is split, and the parts run one after another.
The scheduler wakes at the next entry's due time, when an export ends, and
at least every second. The explicit hook takes the slot too, waiting for the
export in flight.

| Flag | Default | |
| --- | --- | --- |
| `-export-settle` | `1s` | Export a changed entry once this long passed with no cache span naming it |
| `-export-max-delay` | `10s` | Export a changed entry at the latest this long after its first change since its last export |
| `-export-quiet` | | Deprecated, removed in a later release: sets `-export-settle` (and raises `-export-max-delay` to it) when `-export-settle` isn't set, with a warning |
| `-export-end-grace` | | Deprecated, removed in a later release: ignored, with a warning |

More, smaller exports than per-session ones mean more bundles and index
records, and a dependency shared by roots of separate exports is in each
bundle. Values exported before they are complete are exported again; the
index keeps a record's parts when a later one lacks some
([zerobull-consulting/dagger#142](https://github.com/zerobull-consulting/dagger/issues/142)).

A trace request over its limit is refused whole (413), and the engine then
stops publishing that session's telemetry: its entries aren't exported from
that session. Real engines' requests are thousands of times under the limit;
see [OTLP limits](#otlp-limits).

Entries already stored with the same replacement count and parts are left
out, and so are entries merged into the engine by the import path that still
have the replacement count and no part beyond what the merge left: they are
the stored values. So are entries whose expiry or retention has lapsed, and
entries refused for the same value. An import that changed is exported, and
`Record` keeps the stored entry's number for it (see
[Imported entries](#imported-entries)). The export goes to the engine connected with the
entry's cache ID. If none is, the export waits and is tried again.

**One export.**

1. `export` with the roots and outputs. `exported` names skipped roots:
   `gone` ones are forgotten, `busy` ones are tried again with backoff (2s
   doubling to 5m), and `refused` ones are not tried again until their value
   changes.
2. The layer blobs the bundle's outputs need, each once, less those S3
   already has at their digest keys (`UploadURLs`), go in `upload` as
   presigned PUTs to staging keys, under a stage of this try's own. When S3
   has them all, the map is empty, and the engine releases the chains.
3. `uploaded`: the service verifies each blob the engine reports done, and
   promotes it to its digest key only if its content hashes to the digest
   (see [Verification and promotion](#verification-and-promotion)). A blob
   that failed, wasn't answered, or failed verification (a mismatch, or
   nothing uploaded) is not stored, and nothing is published. The engine
   released the chains with its answer, so the export is tried again from
   step 1 after a backoff (1s, doubling), under a new stage. Blobs stored by
   the earlier try aren't asked for again; those that failed are. After 3
   tries the roots count as failed. The tracker tries them again later, up to
   5 times per value.
4. Once every blob is stored (promoted, so verified), the bundle is published
   and recorded in the index (`metastore.RecordFenced`). Each try reads
   GC's epoch (`<prefix>/ns/<ns>/gc-epoch.json`, one small GET, at most once per 10s) before it
   checks any blob, and its index write is refused if it names a blob,
   bundle or resolution GC is deleting, or deleted after that read: it
   checked the object before it was deleted. The export is then tried again
   from step 1, and asks for what is gone (`export_collected_total`; see
   [Retention and garbage collection](#retention-and-garbage-collection)).
   The layers of its records' pending offers, which the engine can't
   upload, are checked too, after the epoch: an export naming one that is
   no longer stored fails, since its bundle can't be stored whole
   (`TestExportChecksOfferedLayers`).
   The roots it records are stamped with the time, from which the
   namespace's max age counts.

A repeated export of stored entries uploads no blob again, and keeps the
entries' service numbers. An unchanged bundle is not written again. Before a
bundle is stored, the service drops what belongs to one export rather than to
the values (`stripVolatile` in `exporter`): every chain's download addresses,
and the engine-internal `buildkit/createdat` layer annotation. An engine that
had entries merged into it exports their unpulled parts as pending offers in
its records, with the presigned GETs it was given: stored, they would reach
every engine the bundle is merged into, as GETs that work until they expire
and are never renewed. The offers stay, with their layers and renewal key, so
a receiver renews them through its own session. The engine annotates a layer
with its creation time only once its snapshot's metadata has one, so the
first export of an exec's layers lacks the annotation and later exports have
it, which made a repeated export a new bundle. Both were seen on the
real-engine run ([Real-engine run](#real-engine-run)).

A blob deleted from the bucket is uploaded again by the next export that
needs it from an engine that has it: `UploadURLs` asks for every blob the
store lacks.

**Limits.** The service sends one export at a time to each engine cache
(above); the engine itself would work on 4 at once and queue the rest. The
engine uploads 8 blobs at once across its exports; the service waits up to an hour for `uploaded`.
Neither side sends a message over 64 MiB. When the engine answers an export
with `too large`, or the service's own `export` or `upload` would be over the
limit, the roots are split in two and each half is exported alone (an
`upload` too large to send is preceded by an empty one, which releases the
chains). One root that is too large on its own fails. A batch holds at most
1,000 roots.

The import path can't split a stored bundle (records name each other by
ordinal), and adds a presigned GET, about 1 KB, per layer of each output's
chain when it merges one. So the exporter stores a bundle only if its merge
would fit in a message too: `importer.MergeSize` bounds it from the bundle's
size, a GET's size (measured by signing one) and what a merge rewrites. An
export whose bundle would be over is split like one too large for a message,
before any blob is uploaded, and a root whose bundle alone would be over fails
as `too large to import`
([#12](https://github.com/zerobull-consulting/dagger/issues/12); see
[Resilience](#resilience)).

Presigned PUTs last `CACHESERVICE_S3_PUT_EXPIRY` (15m). In an export with so
many blobs that the engine, uploading 8 at a time, starts some after that,
those uploads fail and the export is tried again with fresh URLs for the
blobs still missing. For exports that large, raise the expiry. An export cut
short because its engine disconnected, or the service stopped, isn't counted
as a failure: its roots wait for the engine (`NoEngineWait`, 10s) and are
tried again on its next connection, from `export`: the engine released the
chains when the connection ended. A clean engine restart keeps the cache ID
and entry numbers, so this holds across one too, on the new generation.

**S3 errors.** The service's own requests (`HeadObject` for which blobs to
ask for, bundle and index writes) go through the AWS SDK's standard retryer,
which retries throttling (503 `SlowDown`) and 5xx answers with backoff, and
so do a promotion's copies. The engine doesn't retry a failed blob PUT
itself; it reports it in `uploaded`, and the export is tried again as above.
Nothing is published until every blob is stored, verified, and the index is
written last, so a failure at any step leaves no index entry naming a
missing bundle, and no stored bundle naming a missing or unverified blob; at
worst, blobs or a bundle nothing names yet, which the next export reuses or
garbage collection deletes, and staged uploads, which the lifecycle rule or
garbage collection expires. An export that fails as a whole (S3 down) counts a failure of its
roots, which are tried again with backoff (2s doubling), up to 5 times per
value.

**Known gaps.** Without `-tokens-file`, nothing is authenticated: the
WebSocket takes any token, and the OTLP receivers take any request, so anyone
who can reach the service can forge spans, which make it export (or stop
exporting) a connected engine's entries, or connect as an engine and offer it
bundles of its own. Run it that way only on a trusted network. With tokens,
the same holds within a namespace (see
[Auth and namespaces](#auth-and-namespaces)).
Blobs stored before uploads were verified are trusted as they are
([#53](https://github.com/zerobull-consulting/dagger/issues/53); see
[Repairing blobs stored before verification](#repairing-blobs-stored-before-verification)).
Sessions whose
clients send no spans to the service are invisible (see
[ENTRY_DISCOVERY.md](ENTRY_DISCOVERY.md#gaps-and-options)). The tracker's state
is in memory, so after a service restart entries are learned again from new
spans; what is in S3 is not lost, and a re-export uploads nothing already
stored. An export pending or in flight when the service stops is lost until a
later session names its entries
([#45](https://github.com/zerobull-consulting/dagger/issues/45), pinned by
`TestKnownGapServiceRestartForgetsPendingExport`).

## Import path

[`importer`](importer) makes engines reuse what other engines exported
([#8](https://github.com/zerobull-consulting/dagger/issues/8)). It is on with
the export path, reads the same namespace's scope, and is turned off with
`-import=false`. An engine is merged and offered only its own namespace's
bundles, with presigned GETs of its own namespace's blobs.

**When.** Each welcomed connection gets a sync loop. It syncs on `hello`,
again whenever another engine's export is stored, and every `-import-poll`
(30s), which picks up bundles stored by other service replicas. `POST
/debug/engines/{conn}/import` syncs now.

**One sync.**

1. **Plan.** The index's live roots, grouped by the bundle that holds each
   one's current record, the engine cache that exported it (identical
   exports from two caches share a bundle, but not sender numbers), and the
   resolution the record names. Roots
   that have expired, or whose entry or any entry it depends on has, are
   left out, as `DropExpired` would drop them, and so are an engine's own
   exports: its cache has them. A root past the namespace's max age, counted
   from when it was recorded, is left out too, as `Expire` would drop it,
   whether or not GC has run. A bundle
   already merged into this cache, at this generation, with every root it
   needs now, is not merged again.
2. **Merge.** Each bundle is rewritten for the engine: each value's
   `SenderNumber` becomes its service number, which the engine keeps as its
   Cloud holding. The number is the bundle's stored resolution's: how its
   values resolved when it was recorded, not through the exporting cache's
   mappings now, which move as that cache's imports do (see
   [Imported entries](#imported-entries)); the roots are the planned ones with the index's expiries,
   each capped at the namespace's max age after it was recorded, so engines
   release imported roots on their own (most stored roots have no expiry);
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
   /debug/engines/{conn}/imports`), records each landed entry in the index as
   the cache's import, and tells the export path's tracker the ones the index
   took (not those that landed on an entry the cache exported itself). If
   the index can't be updated, the merge stays committed, since the engine
   has it, and only the index write is tried again, at each sync, until it
   lands; the tracker is told then. If the cache was wiped while the imports were
   written, they are dropped again.
3. **Offer.** A stored part a merged entry has neither complete nor offered
   is offered to that entry, by the engine's number, with fresh GETs and the
   owner's dependencies and the value's services mapped to the engine's
   numbers. `accepted` and `alreadyComplete` are recorded. `executionStarted`
   and `unavailable` are tried again at the next sync, up to 3 times, unless
   the stored entry has expired by then, or its engine entry has moved or
   been forgotten. `invalid` is not tried again. Parts that offers place on
   imported entries are passed to the tracker too, so a span naming one
   doesn't count as a change.
   An entry the engine answers `gone` for is forgotten. An offer over the
   message limit is split in two until each fits.

A merge refused by the engine is tried again at later syncs, up to 3 times per
bundle and engine generation. A merge over the 64 MiB message limit is not
sent; the export path no longer stores a bundle whose merge would be (see
[Export path](#export-path)), but one stored before that, or by a service
with a higher limit, is never imported. A new engine generation, or an `engine.start` that names a wiped cache,
starts that cache over (a wiped cache's imports leave the index too);
`engine.prune` forgets the dropped entries, but keeps their imports in the
index. A merged
reply that arrives after its cache's state was reset is discarded.

**Renew.** Presigned GETs last `CACHESERVICE_S3_GET_EXPIRY` (1h). When the
engine reads an offered part after they expired, it sends `renew` with the
chain's layers; the service checks each is stored in the connection's
namespace and answers with fresh GETs of that namespace's keys, or
`unavailable` when one is missing there or the renewal key isn't the
namespace. Renewals aren't limited to the digests offered to the connection:
an engine renews offers merged before a reconnect or a service restart, which
this process never offered it, and within its namespace a token could have
those blobs merged anyway (`TestRenewNotLimitedToOfferedDigests`).
The engine checks an address's expiry itself, and renews without trying an
expired GET; it renews a whole chain at once, and only once per read. When
renewal answers `unavailable`, the read fails and the engine computes the part
again, as for a cache miss (seen on the real-engine run).

**Known gaps.** Bundles are merged whole, so a bundle whose roots were partly
superseded by a later export still sends the older records of its other
values; the engine keeps whichever value it merged first, as the same recipe
is interchangeable. The real-engine run's `superseded` scenario checks which record an
empty engine ends with ([The scenarios](#the-scenarios)). Parts are offered only at an entry's own address, not at
an output path into it. What the importer knows of each engine is in memory:
after a service restart, every connected engine is merged everything again,
once, including entries it had pruned. This is accepted rather than fixed
with a per-cache marker in the store
([#12](https://github.com/zerobull-consulting/dagger/issues/12)): it happens
once per restart, not per poll; a merge carries records and GETs, not layers;
an engine whose GC pruned the entries prunes them again if they stay unused;
and a marker would add a store write to every prune event. Pinned by
`TestServiceRestartMergesPrunedEntriesAgainOnce`; the real-engine
`service-restart` scenario measures it ([Resilience](#resilience)). An
engine's own exports are never merged back into it, even after it pruned
them. Entries an engine prunes, or answers
`gone` for, are not merged into it again until it restarts: merging them back
on the next poll would undo its garbage collection, which prunes them again.

## Read-only instances

`-export=false` is for jobs that should use the cache but never change it,
such as pull-request jobs, while a scheduled job writes it
([#65](https://github.com/zerobull-consulting/dagger/issues/65)). The
promise: **a read-only instance never writes to or deletes from the
bucket.** So it runs with a read-only key, and that is testable.

What it does, and doesn't:

- **No export path.** No tracker and no exporter: session spans are received
  (200, within the same [OTLP limits](#otlp-limits)) and dropped, so
  nothing is recorded, exported or uploaded. `POST
  /debug/engines/{conn}/export` and `GET /debug/engines/{conn}/entries`
  answer 409, `the export path is off: the instance is read-only
  (-export=false)`, distinct from a service with no store (`... no store is
  configured`).
- **No conditional write check at startup.** It checks the index's
  conditional writes, which only the export path makes, and it writes. The
  service logs that it skipped it, and why.
- **No GC.** GC deletes, so it is forced off whatever `-gc-interval` says,
  and `POST /debug/gc` answers 409, `gc is off: the instance is read-only
  (-export=false), and never deletes from the bucket`. This goes beyond
  [#65](https://github.com/zerobull-consulting/dagger/issues/65)'s "GC
  unchanged": a read-only key couldn't delete anyway, and a GC that fails
  every delete is noise, not retention. Run GC from a writer.
- **Imports, offers and renewals as usual**, with one difference: the import
  path records no imports in the index. A writer records, after each merge,
  which engine entry each stored value landed on, so that a later export of
  that entry reuses the stored one instead of storing it again (see
  [Imported entries](#imported-entries)); a read-only instance never exports,
  so it skips that index write, and a wiped cache's imports have nothing to
  drop. The cost: if the same engine cache later exports through a writer,
  it stores copies of what it imported, under new numbers, rather than
  reusing them. Nothing is lost; a fresh engine per job never hits it.
- **The drain has nothing to flush,** and returns at once
  (`drain: nothing to export (read-only: -export=false)`), unless an
  explicit import admitted before it still runs: it waits for that, up to
  its deadline (`drain: waiting for explicit requests`; see
  [Stopping and draining](#stopping-and-draining)).

Three combinations are refused at startup, with exit code 2 and an error
naming both settings, before anything connects to the bucket:

- `-export=false -import=false`: with both paths off, the service has
  nothing to do.
- `-export=false` without `CACHESERVICE_S3_BUCKET`: a read-only instance
  only imports from the bucket, so it would have nothing to do either, and a
  typo in the variable would otherwise go unnoticed.
- `-export=false` with an explicit nonzero `-gc-interval` (even `1h`, the
  default, when it's set on the command line): it asks for GC, which a
  read-only instance never runs. The default alone is fine; `-gc-interval=0`
  too.

The startup log has one line of the mode, `msg=mode export=off import=on
gc=off readOnly=true`, and `cacheservice_export_enabled` is 0, so a
dashboard can tell a read-only sidecar from a writer that is broken.

**On a prefix with no store yet** (a pull-request job before the first
cache-warming run): a read-only instance checks ownership as a writer does
(see [Bucket ownership](#bucket-ownership)), but never writes the marker.
With no marker and nothing under the prefix, it starts, logs `no store
exists under the prefix yet` at warning level, and has nothing to merge:
the index it reads doesn't exist. It writes nothing
(`TestReadOnlyStartsOnEmptyPrefix`, `TestMinIOBinaryReadOnlyEmptyPrefix`).
Once a writer creates the store, the same instance merges from it as usual:
it doesn't check the marker again. With keys under the prefix and no
marker, or a marker of another layout, it exits 1, as a writer does.

### A read-only key

**The flag is trust hygiene; the read-only key is the boundary.** A job that
holds a key that can write can still write, whatever flag it passes the
service. Give pull-request jobs a key that can only read.

The import path needs, on the objects under the prefix (`<prefix>/*`):

- `s3:GetObject`: the index, bundles and resolutions it reads, the
  `HeadObject` that checks a layer is stored before a renewal, and the
  presigned GETs it hands engines, which are signed with this key;
- `s3:ListBucket` on the bucket, for the prefix: the startup's ownership
  check lists the prefix once, for at most 11 keys, when it finds no marker
  (see [Bucket ownership](#bucket-ownership)); and without it S3 answers
  403 instead of 404 for a missing key, such as the marker or the index of
  a namespace nothing was stored in yet. The condition below,
  `s3:prefix` like `cache/*`, covers that listing (its prefix is `cache/`).
  With an empty `CACHESERVICE_S3_PREFIX`, the listing is of the whole
  bucket, and the condition must allow it (drop it, or use `*`).

That is all: no `s3:PutObject`, `s3:DeleteObject` or bucket configuration.
`TestMinIOBinaryReadOnlyKey` runs a read-only instance with a MinIO user
that has only these (as below, for the test prefix): it merges a writer's
entries into a fresh engine, the engine pulls their layers and renews their
addresses, and the bucket's listing is unchanged. With the export path on,
the same key fails at startup with `AccessDenied`, exit 1: on an empty
prefix at the ownership check's listing of multipart uploads, before it
would create the marker, and on a writer's prefix at the conditional write
check (`TestMinIOBinaryReadOnlyKeyRefusedAsWriter`): that
is what stops a
pull-request job that forgot `-export=false` from running as a writer whose
every export fails. MinIO answers 404 for a missing key even without
`s3:ListBucket` (checked with a `GetObject`-only user); so does Spaces with a
limited `Read` key (below); AWS S3 documents the 403, which hasn't been run
here.

**MinIO** (what `hack/remote-cache/compose.yaml`'s `create-readonly-user`
does), with `dagger-cache` and `cache` for your bucket and prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::dagger-cache/cache/*"]},
    {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::dagger-cache"],
     "Condition": {"StringLike": {"s3:prefix": ["cache/*"]}}}
  ]
}
```

```bash
mc admin policy create local cacheservice-readonly readonly.json
mc admin user add local cacheservice-pr '<secret>'
mc admin policy attach local cacheservice-readonly --user cacheservice-pr
```

**AWS S3:** the same document is an IAM policy: attach it to the IAM user
or role the pull-request jobs use (`aws iam put-user-policy --user-name
cacheservice-pr --policy-name cacheservice-readonly --policy-document
file://readonly.json`), and give the writer its own credentials. A bucket
policy that grants a write to everyone in the account would still apply:
check the bucket policy grants nothing wider.

**DigitalOcean Spaces:** Spaces has no IAM policies; it has per-bucket
[limited access keys](https://docs.digitalocean.com/products/spaces/how-to/manage-access/),
each bucket with `Read` or `Read/Write/Delete`. Create one with `Read` on the
cache's bucket only. Observed on 2026-11-07 (sfo3, prefix `cache`): a limited
`Read` key could GET, HEAD and list objects and list multipart uploads; PUT,
DELETE and CreateMultipart returned 403; a missing key returned 404
`NoSuchKey`, not 403; and `GetBucketLifecycleConfiguration` returned 403. A
read-only instance (`-export=false`) with that key started cleanly, and the
same key without `-export=false` exited 1 with 403 on the conditional-write
check. It is per bucket, not per prefix: it reads every prefix of the
bucket, other namespaces' included, so give the cache a bucket of its own.
Spaces also refuses a limited access key on a bucket with a bucket policy
(`PutBucketPolicy`).

Spaces keys come at three levels, which match the
[three keys](#the-keys): a limited key with `Read` here, a limited key with
`Read/Write/Delete` for the writer, and a full-access key only for setting
lifecycle rules (`cacheservice lifecycle apply`), which a limited key can't
(403). Keep the full-access key out of the jobs' secrets.

## OTLP limits

The OTLP receivers bound what a request can make the service read and hold
([zerobull-consulting/dagger#77](https://github.com/zerobull-consulting/dagger/issues/77),
item 1). Before, each body was read whole, and gunzipped whole with
`Content-Encoding: gzip`, so a gzip bomb of a few kilobytes could run the
service out of memory, with no token in unauthenticated mode. With tokens,
the token is checked before any of the body is read, once the request and its
headers are reserved from the budget (below), and a request without a known
one is refused with 401 (`TestOTLPUnauthenticatedBodyNotRead`). net/http then
reads up to 256 KiB more of the body after the handler, and discards it, to
reuse the connection; the 401 cuts the connection's read deadline to 1s
(`otlpRefusalDrain`), so a client can't hold the connection, or the headers
net/http holds with it, by trickling or stalling the body after a 401
(`TestOTLPRefusedTokenSlowBodyCutOff`).

| Route | Limit, as received and decoded | Largest request in six e2e runs | Margin |
| --- | --- | --- | --- |
| `POST /v1/traces` | 64 MiB (`server.MaxOTLPTracesBytes`) | 19,489 bytes | 3,443x |
| `POST /v1/logs` | 64 MiB (`server.MaxOTLPLogsBytes`) | 9,466 bytes | 7,089x |
| `POST /v1/metrics` | 16 MiB (`server.MaxOTLPMetricsBytes`) | 1,437 bytes | 11,675x |

- **Two limits per request.** The body as received is limited
  (`http.MaxBytesReader`), and so is the decoded stream: the service reads
  at most one byte past the limit from it, so a gzip bomb stops there however
  well it compresses (`TestOTLPGzipBomb`). Over either: **413**, the
  connection closes, and nothing of the request is recorded: no span
  tracked, no log record, no partial decode, since a body is decoded only
  once read whole (`TestOTLPOverLimitTracksNothing`).
- **A shared budget.** The OTLP requests being read at once hold at most
  128 MiB (`server.OTLPDecodeBudget`, twice the largest limit) across the
  three routes, and each reserves what it holds before allocating it:
  - **itself**, 16 KiB (its goroutine and the server's buffers), before
    anything is read, so the budget also bounds how many requests are read
    at once, stalled ones included: about 7,000 with the engines' usual
    headers, and at least 2,600 with the largest headers allowed (see the
    next item) (`TestOTLPStalledRequestsBounded`);
  - its **headers**, with it, before any of the body is read: net/http has
    parsed and holds them while the handler runs. They are estimated from
    the request (`otlpHeaderReserve`): the bytes of every key and value, 112
    per key and 32 per value (measured against the heap of parsed requests:
    `TestOTLPHeaderReserveCoversParsedHeaders`), the host, and the request URI
    twice. Real engines' requests come to about 1.5 to 3.2 KiB (see **Headers are
    bounded**, below), small beside the 16 KiB; but a client sending 24 KiB
    of headers and then stalling its body holds that much of the budget
    beside the 16 KiB, not the 16 KiB alone (one over 32 KiB is refused, below)
    ([zerobull-consulting/dagger#85](https://github.com/zerobull-consulting/dagger/issues/85),
    `TestOTLPRequestHeadersInBudget`, `TestOTLPHeaderReserve`). The estimate
    counts the names a chunked request declares in its `Trailer` header too,
    which net/http moves out of the headers (68 KB of them held 1.5 MB; they
    reserved nothing before). It is not bounded by `MaxHeaderBytes`: 68 KB of
    5-byte headers would reserve over 1 MB, which is also about what they
    hold. So a request whose headers would reserve more than 32 KiB
    (`server.MaxOTLPHeaderReserve`, ten times what the largest real
    requests reserve) is answered **431**, counted as `too_large`, before it
    reserves anything (`TestOTLPHeaderReserveCap`). The most one request
    holds while it waits on its body is then its headers' 32 KiB, its 16 KiB
    and its first buffer: about 49 KiB, so the budget holds at least 2,600
    stalled requests, not the 90 that 1.4 MB each would allow. A refused
    request's unread body is drained for at most 1s (`otlpRefusalDrain`),
    not until the 30s read timeout: net/http holds the request, headers
    included, while it drains, and 200 refused requests with 40 KiB headers
    and stalled bodies held 12.7 MB for the whole timeout, with nothing
    reserved (`TestOTLPRefusedStalledBodyConnectionClosed`). The request and
    its headers are reserved before the token is checked, so a request with
    a missing or unknown token holds them in the budget too while its handler
    runs, and is answered 431 or 503 as above, before the check, when they
    don't fit; it gives them back on the 401, and its drain is cut to 1s
    like any refusal's. That drain comes after the handler, once the
    reservation is given back: for up to 1s, a refused request's headers
    are held outside the budget. 200 such requests with 24 KiB headers and a stalled
    body held 9.6 MB of heap for the whole 30s, with nothing reserved; now
    0.3 MB is left 3s after they are sent
    ([zerobull-consulting/dagger#219](https://github.com/zerobull-consulting/dagger/issues/219),
    `TestOTLPRefusedTokenHeadersInBudget`, `TestOTLPFullBudgetRefusedBeforeToken`,
    `TestOTLPHeaderReserveCapBeforeToken`);
  - a gzipped body's **decompressor**, 64 KiB, before it is built
    (`TestOTLPGzipDecompressorReserved`);
  - the body's **buffer**, doubling from 512 bytes up to the limit as the
    body arrives, the old buffer staying reserved until it is copied into the
    new (`TestOTLPBodyReadOnlyIntoReservedBytes`,
    `TestOTLPBufferGrowthReservesBoth`): at most twice what the client sent,
    three times while it grows;
  - the **decoded message**, estimated from the body before it is decoded,
    from each OTLP message's Go struct size, each repeated element and each
    string (`TestOTLPDecodedSizeEstimate`), each element of a repeated string
    field (`EntityRef.id_keys`, the only one) at its length and a quarter, for
    the allocator's size classes, and 64 bytes, for its 16-byte header in a
    slice that append grows, even when it is empty
    (`TestOTLPAppendPeakHeaders`, `TestOTLPEmptyRepeatedStringsCoverDecodePeak`,
    [zerobull-consulting/dagger#217](https://github.com/zerobull-consulting/dagger/issues/217)),
    and each field the message
    doesn't know, or that arrives with the wrong wire type for its number,
    at three times its wire size and at least 16 bytes, for the slice the
    decoder appends it to (`TestOTLPUnknownFieldsEstimateCoversDecodePeak`,
    `TestOTLPWrongWireTypeChargedAsUnknown`). Two gaps: a singular string is
    charged its length and 16 bytes, not its size class, so a 32,769-byte one
    that takes 40,960 is charged 32,785
    (`TestOTLPSingularStringSizeClassKnownGap`); and the fix for repeated
    strings covers protobuf only, as a JSON body is charged 16 bytes for each
    comma, so an empty element, `"",`, is charged 19 bytes, under what append
    holds for it (`TestOTLPJSONEmptyRepeatedStringsKnownGap`). A body decodes to more than its
    size, and can decode to far more: one of empty entries, 2 bytes each on
    the wire, decodes to about 70 times its size. A body whose message could
    never fit in the budget is refused with **413** before it is decoded
    (`TestOTLPAmplifiedBodyRefused`).

- **Nesting is bounded.** A body that nests deeper than the service decodes
  is refused with **400**, counted as `too_deep`, before it is decoded
  (`TestOTLPDeeplyNestedBodyRefused`, `TestOTLPNestedGroupsRefused`,
  `TestOTLPDeeplyNestedJSONBodyRefused`,
  [zerobull-consulting/dagger#86](https://github.com/zerobull-consulting/dagger/issues/86)).
  The estimate and `proto.Unmarshal` each recurse once per level, and the
  stack that takes was not in the decode budget. The limit was 10,000 levels,
  which a body of 40 KB reached, and the walk and the decode took a few MiB of
  stack (scratch measurements: the goroutine's stack grew to 2 to 4 MiB for
  the walk, and 4 to 8 MiB for the decode).
  - **Protobuf: 64 levels** (`otlpMaxDepth`, the request being level 0),
    counting nested groups, which the walk follows itself, without recursion,
    since `protowire.ConsumeFieldValue` would walk them uncounted
    (`TestOTLPNestedGroupsRefused`: 9,000 nested groups, 36 KB, were taken).
    OTLP's own messages nest 8 levels down to the first `AnyValue`
    (`TestOTLPFixedNestingDepth`), and an `AnyValue` nested through arrays adds
    2 levels each, so 64 leaves 28 arrays below the deepest attribute. A body
    at 64 levels is taken (`TestOTLPBodyAtMaxDepthTaken`).
  - **JSON: 128 objects and arrays deep** (`otlpMaxJSONDepth`, twice the
    other, since a message in a repeated field takes an array and an object),
    counted outside strings (`TestOTLPJSONBracesInStringsNotCounted`).
  - **The stack is reserved** before the body is walked (a request whose
    stack doesn't fit is answered 503 without being walked,
    `TestOTLPDecodeStackReservedBeforeWalk`), and apart from the 16 KiB each
    request reserves: 64 KiB for a protobuf body (`otlpDecodeStack`) and
    128 KiB for JSON (`otlpJSONDecodeStack`). At the limits, the goroutine's
    stack grew to 16 to 32 KiB for the walk and 32 to 64 KiB for the decode of
    a protobuf body, and 64 to 128 KiB for the decode of a JSON one (a stack
    grows by doubling, so these are ranges). These are scratch measurements,
    not a test.
  - The e2e's `otlp-sizes` scenario logs how deep the real engines' requests
    nest (`nestingDepth` in the `otlp request accepted` record) and fails if
    the deepest is over half of 64 (`server.MaxOTLPNesting`), or if any
    request taken was JSON, whose depth is not measured (`-1`).

  Since the decoded message counts too, the route limits are not the only
  bound on a body: one that decodes to more than the budget can hold is
  refused whatever its size. The e2e's requests estimated at up to 5.6 times
  their size, so a body as dense as theirs is refused past about 17 MiB
  (its 32 MiB buffer and its message), and one made mostly of a single
  string only just short of the limit. The forwarder's 4 MiB batches
  estimate at about 23 MiB: several fit at once.

  A request that doesn't fit in what is left: **503** with `Retry-After: 1`,
  and nothing recorded (`TestOTLPDecodeBudget`). Only what requests hold
  counts, so any number of the engines' usual requests fit, and an at-limit
  one beside them; two at-limit requests at once may have one refused, and
  retried. Without the budget, N requests at the limit would hold N times
  the limit, and their messages more.
- **Idle connections are closed.** Every listener (`server.NewHTTPServer`)
  sets `IdleTimeout` to 120s (`server.IdleTimeout`): a keep-alive connection
  with no request on it is closed after that
  (`TestIdleKeepAliveConnectionClosed`). It is above the 90s the engine's
  Cloud exporters keep an idle connection (`IdleConnTimeout` of
  `cloudExportTransport` in `engine/telemetry`, measured: a test server saw
  that transport close an idle connection after 90.08s), so the exporters'
  client closes first. Were the server's below the client's, a client reusing
  a connection as the server closed it would get a transport error that Go
  doesn't retry for a POST: in a test with the server's at half the client's,
  5 of 5 runs failed with `EOF` or `connection reset by peer`
  (`TestExporterClientPostsAcrossIdleGaps`, over HTTP/1.1). Over HTTP/2
  (`-tls-cert`) the race did not occur in a run: a client with
  `ForceAttemptHTTP2` and an idle timeout of 300ms made 24 POSTs with the
  server's idle timeout at the real ratio above it, and 24 with it at half the
  client's, and none failed (the server ends an idle HTTP/2 connection with a
  GOAWAY, and the client dials another). That run is not a test, as it passes
  in both orders. The engine WebSocket
  is unaffected, as a hijacked connection is out of the server's hands. The
  same timeout applies to the debug and metrics listeners, whose clients are
  scrapers sending GETs: Go's transport retries an idempotent request that
  fails on a reused connection the server closed, so those are covered (read
  from the code, not run; not checked for other scrapers). There
  is no cap on the number of connections: a client can still hold many open
  while it is sending, and what bounds that is left to the network.
- **Headers are bounded.** Every listener (`server.NewHTTPServer`: the
  engine and OTLP listener, the debug listener and the metrics listener)
  sets `MaxHeaderBytes` to 64 KiB (`server.MaxHeaderBytes`), where net/http's
  default is 1 MiB. net/http allows 4 KiB more than that for its read buffer,
  so a request line and headers of up to about 68 KiB are read; over
  HTTP/1 a larger one is answered **431** before any handler runs, so it
  reserves nothing and records nothing (`TestOTLPRequestOverHeaderLimit`).
  Over HTTP/2 (`-tls-cert`) the server advertises the limit
  (`SETTINGS_MAX_HEADER_LIST_SIZE`), and a client that honors it, as Go's does,
  refuses to send a larger header list ("request header list larger than
  peer's advertised limit"); a client that doesn't was not tried. Trailers
  are bounded by net/http too, to a few KiB: a chunked request whose trailer
  is 1 KiB is taken, one of 4 KiB or more is answered **400** and records
  nothing (`TestOTLPRequestOverTrailerLimit`), inside the 16 KiB each request
  reserves. Before
  ([zerobull-consulting/dagger#85](https://github.com/zerobull-consulting/dagger/issues/85)),
  up to 1 MiB of headers per request was held outside the budget, so about
  8,000 requests stalled after their headers held about 8 GB it didn't count.
  The limit was chosen from what real callers send, measured by running the
  engine's own client code against a server that logged each request's raw
  header block:
  - the engine's OTLP exporters (`newCloudExporters`, `POST /v1/traces`;
    `/v1/logs` and `/v1/metrics` send the same headers): **402 bytes** with
    a Basic engine token (`Authorization`, `Content-Type`, `User-Agent`,
    `X-Dagger-Export`, `X-Dagger-Org`, `Accept-Encoding`, `Content-Length`),
    **1,811 bytes** with a 1.5 KiB bearer token (an OIDC or OAuth token,
    which dominates the size);
  - the engine's WebSocket upgrade (`remotecache.dialService`): **431
    bytes** with a 64-character token (`Authorization`, and the
    WebSocket upgrade headers);
  - the debug API is called by operators with `curl` or a script, with a
    bearer token: a few hundred bytes. A browser's cookies could add more,
    but a debug client is not a browser, and 64 KiB is far over it.

  64 KiB is over 30 times the largest of these on the wire (1,811 bytes), and
  about 20 times the largest reserve (3.2 KiB: the 1,811 bytes, nine headers
  at 144 bytes each, and the host and URI). The e2e's `otlp-sizes`
  scenario logs the headers' reserve of every OTLP request the real engines
  sent (`headerBytes` in the `otlp request accepted` record) and fails if the
  largest is within a tenth of `MaxOTLPHeaderReserve`; every real engine's
  WebSocket upgrade also has to pass the limit for the run to connect at all.
  The reserve is never less than the headers' wire bytes (each header adds at
  least 144 bytes against at most 4 of framing), so the check bounds the wire
  too: it can fail early for many short headers, never late.
  The cap is the same on the debug and metrics listeners, with no setting to
  raise it: operators' `curl` and scripts send a few hundred bytes, but a
  browser or proxy adding large cookies or forwarded-for chains to the debug
  API could meet it.
- **A read timeout.** Each body must arrive within 30s of its handler
  starting (`server.OTLPReadTimeout`), a read deadline on the OTLP routes
  alone, set before the token is checked (`http.ResponseController`, per
  stream over HTTP/2 with `-tls-cert`): past it, **408**, and what it held
  of the budget is given back (`TestOTLPSlowBodyCutOff`). The engine's Cloud
  exporters give up on a request after 10s themselves. Every listener also
  bounds a request's headers to 10s (`server.ReadHeaderTimeout`). Neither
  applies to the engine WebSocket on the same listener, which stays
  connected past both (same test): the server sets no `ReadTimeout`, which
  would cut it off. Idle keep-alive connections are closed after
  `server.IdleTimeout` (above).
- **Engine IDs are validated before they key anything.** An engine cache ID
  (the `<cacheID>` of a span's or event's `dagger.io/engine.cache`, an
  engine start's wiped cache, an engine's hello) and a span's session
  (`dagger.io/engine.session`), when it has one, must be 1 to 128 letters,
  digits, `.`, `_` or `-` (`server.MaxEngineIDLen`), as a flush's cache ID
  already had to be; and `dagger.io/engine.cache` must be
  `<cacheID>/<generation>`
  ([zerobull-consulting/dagger#88](https://github.com/zerobull-consulting/dagger/issues/88)).
  Real engines send far less: a cache ID is a UUID, 36 characters
  (`uuid.NewString` in dagql's `Cache.openIdentity`), and a session ID 26
  (`crypto/rand.Text` in the engine client); the e2e's `otlp-sizes` scenario
  logs the longest of each the run's engines sent, and fails if any was
  dropped. Before, a cache ID or session of any length or content (with a
  numeric generation) became a key of the tracker's state, which the
  tracker keeps for a cache no engine connects under. That
  bounds what each cache ID costs, but not how many the tracker keeps: it
  never forgets a cache no engine connects under
  ([zerobull-consulting/dagger#203](https://github.com/zerobull-consulting/dagger/issues/203)). Now
  a request's spans under an invalid one are dropped, not tracked; an event
  of any kind with one is dropped; a start naming an invalid wiped cache is
  taken without it; a hello with one is answered with an error and the
  connection closed. Each is counted in
  `cacheservice_engine_ids_refused_total`, by source (`span`, `event`,
  `wiped_cache`, `hello`), and logged, at warning level at most once a
  minute per source; the refusal's record gives the values' lengths, not
  the values
  (`TestTrackSpansValidatesEngineIDs`, `TestTrackEventValidatesEngineIDs`,
  `TestHelloValidatesCacheID`).
- **Every refusal is counted**, by route and reason, in
  `cacheservice_otlp_refused_total`, and logged: at warning level at most
  once a minute per route and reason, as refused tokens are, the rest at
  debug level, so a flood of them can't flood the log; the sizes
  of the requests taken are in `cacheservice_otlp_request_bytes` (see
  [Metrics](#metrics)), to check the limits against real traffic.

**Known gaps**, each pinned by a test:

- Walking a deeply nested body (OTLP values nest) takes stack the budget
  doesn't count; its structs are counted
  ([zerobull-consulting/dagger#86](https://github.com/zerobull-consulting/dagger/issues/86),
  `TestOTLPDeeplyNestedBodyTaken`).

**How they were chosen.** The e2e run (`./hack/remote-cache/dev.sh e2e`,
every scenario) logs every request taken, and its `otlp-sizes` scenario
reports the largest per route (above, the largest of six runs). The
engines sent protobuf, not gzipped. In the last run, which counts the
decoded messages too, the largest estimated at 84,231 bytes (traces), and
at most 131 KiB of the budget was in use at once. The limits don't follow from those alone: the engine's Cloud
forwarder, which sends session spans and logs, keeps each request to 4 MiB
of protobuf, and sends larger only for a single span or log record larger
than that (`cloudForwardMaxPayloadSize` in
`engine/server/session_cloud_forwarder.go`). 64 MiB is 16 times that bound,
and thousands of times what the run sent. The engine's own cache events
(`/v1/logs`, under the engine's token) come in batches of at most 128
records, one event each. The largest are a prune run's, about 60 bytes per
dropped entry, and a sharing pass's, a few hundred bytes per part shared:
it would take one prune run dropping about a million entries, or one pass
sharing a few hundred thousand parts, to reach 64 MiB. Metrics are only counted, and the engine sends a few
kilobytes of them. The margin check (`otlpMargin`, 10x) fails the e2e run if
an engine change brings a request within a tenth of its limit, or the
budget's peak within a tenth of it: measure again then, before changing
either.

They are constants, not flags: real requests are three to four orders of
magnitude under them, and the engine bounds its own requests, so no operator
needs to raise them; and lowering one below 4 MiB would refuse a real
session's telemetry, which costs more than the memory it saves (below).

**What a refused request costs.** The OTLP/HTTP spec doesn't let an exporter
retry a 413, so its data is lost, and more than the request: the engine's
forwarder treats a 4xx other than 401, 403, 408 and 429 as a permanent
refusal, and stops publishing that session's telemetry, every remaining span
and log of it. A refused trace batch can therefore cost the export path that
session's entries: their spans never arrive, so they aren't tracked or
exported from that session, until a later session's spans name them again.
A refused batch of engine events loses up to 128 of them; a lost prune event
leaves the service tracking entries the engine dropped, which an export then
reports `gone`. For real engines this is practically impossible: their
requests are thousands of times under the limits, and the forwarder's own
4 MiB bound keeps them there. A 503 costs session telemetry nothing: the
exporters retry it after `Retry-After`, and the forwarder retries from where
it was, however long the refusals last. The engine's cache events are
retried by their exporter alone, for up to a minute by default: a budget
kept full longer than that, by a flood, drops their batches as a 413 does. A 408 is retried by the forwarder too, and only a
client that takes 30s to send a body gets one.

## Auth and namespaces

[#10](https://github.com/zerobull-consulting/dagger/issues/10). A **token**
authenticates an engine, and maps it to a **namespace**: a team, a tenant.
The namespace is the trust boundary. Everything a connection does is in its
namespace's scope:

- **exports:** what it records, in the namespace's index, and where its
  blobs and bundles go, under `<prefix>/<namespace>/`;
- **imports:** the index and bundles the importer plans and merges into it;
- **offers:** of the namespace's stored parts, with GETs of its blobs;
- **renew:** only blobs stored in the namespace, under the namespace's
  renewal key.

One service serves many namespaces at once. Each has its own tracker,
exporter, importer, in-memory caches and blob store view, created on its
first connection; the metrics' `scope` label is the namespace.

### The tokens file

`-tokens-file FILE` turns authentication on. One token and its namespace per
line, separated by white space; blank lines and `#` comments are ignored:

```text
# team A's engines and CI
3f9c0e2a7d...   team-a
b72d41c09e...   team-a
# team B
5e01aa83f4...   team-b
```

A token may not contain white space or `:` (HTTP Basic ends the username at
the first colon), and may appear once. A namespace is what
a scope may be: 1-128 letters, digits, `.`, `_` or `-`, starting with a letter
or digit, without `..`. Many tokens may share a namespace. Generate tokens
long and random (`openssl rand -hex 32`), and keep the file readable by the
service only. The service keeps only the tokens' SHA-256 hashes, compares
them in constant time, and logs only a short fingerprint (the hash's first 4
bytes).

An engine sends its token as the HTTP Basic username, from
`DAGGER_CLOUD_TOKEN`
([#13](https://github.com/zerobull-consulting/dagger/issues/13) will let its
config set it). On the WebSocket, an unknown or missing token is refused with
401 before the upgrade, so it never gets a protocol message. A token is
checked again when its connection says `hello`, against the tokens of that
moment.

**The OTLP receivers** take the same tokens, as HTTP Basic usernames, and
refuse others with 401 (`HEAD /v1/traces`, the engine's credential-less probe,
is still answered). Each request feeds only its token's namespace. The engine
sends two kinds, under different tokens:

- **Engine events** (`engine.start`, `engine.prune`, on `/v1/logs`) come from
  the engine's own log exporter, under the engine's `DAGGER_CLOUD_TOKEN`
  (`cmd/engine/telemetry.go`), the token of its WebSocket. The service takes
  an event about an engine cache only from the token of that cache's latest
  connection, or about a cache no connection has named yet (an engine's
  `engine.start` can arrive before its `hello`). An `engine.start` that
  reports wiping a cache whose connection had another token is taken without
  the wipe. The others are dropped, logged (`engine event ignored`) and
  counted (`engine_events_ignored_total`). A cache's owner is kept while it
  is connected and for an hour after, and at most 4096 owners of
  disconnected caches are kept per namespace, so cache IDs that never come
  back don't pile up.
- **Session spans** (`/v1/traces`) come under the token of the client that
  ran the session: the CLI forwards them, or the engine publishes them for it
  with the client's credential (`engine/server/session_cloud_telemetry.go`).
  So spans are taken from any token of the namespace: a CI client's token
  must be in the file, in its engines' namespace, for its sessions to be
  exported. A token that doesn't belong to the engine's namespace reaches
  only its own namespace, where that cache is a stranger.

A request with another namespace's token, or none, can't change what the
importer or tracker knows of an engine (`TestForgedTelemetryIgnored`). Within
a namespace, tokens trust each other further: a connection may say `hello`
with another engine's cache ID, and then takes over its exports and its
engine events. That is a known gap
([#51](https://github.com/zerobull-consulting/dagger/issues/51),
`TestKnownGapSameNamespaceTokenClaimsCacheID`); give engines you don't trust
with each other different namespaces.

### Reloading and revocation

`SIGHUP` reloads the file. (A file watcher would also fire on half-written
files and editors' temporary renames; a signal is explicit, and works the
same with a file mounted from a secret.) A file that can't be read or parsed
changes nothing: the tokens loaded before stay, and the service logs an
error. After a reload, every connection whose token is gone, or now maps to
another namespace, is closed with WebSocket status 4001
(`protocol.CloseTokenRevoked`), and is no longer listed, so no export or merge
picks it while its close handshake runs.

The engine treats 4001 as any other close: it logs it and reconnects with its
backoff (1s, doubling to 30s). The reconnection is refused with 401, as is
every later one. Those refusals are logged at warning level once per token a
minute, and at most 10 times a minute in all, then at debug level, never at
error level; `auth_rejected_total` counts them all.

Nothing stored is deleted or changed when a token goes: its namespace's data
stays, for its other tokens, and for the token if it comes back. Garbage
collection still collects it, by its limits like any other namespace's:
"no tokens" never means "delete" (`TestGCNamespaceWithoutTokens`).

### The namespaces file

`-namespaces-file FILE` sets per-namespace retention limits (see
[Retention and garbage collection](#retention-and-garbage-collection)). One
namespace per line, then `key=value` settings; blank lines and `#` comments
are ignored:

```text
# defaults for namespaces with no line of their own
*        max-age=720h
team-a   max-age=168h max-bytes=500GiB
team-b   max-bytes=2TiB
```

- `max-age` is a Go duration (`168h`), at least `1s`; `max-bytes` a size in
  bytes, with `B`, `KB`, `MB`, `GB`, `TB` (powers of 1000) or `KiB`, `MiB`,
  `GiB`, `TiB` (powers of 1024). `0` or `none` is no limit.
- `-max-age` and `-max-bytes` are the defaults; a `*` line overrides them,
  and a namespace's line overrides the keys it sets. Without a file, every
  namespace has the flags' limits; both are 0 (no limit) by default.
- A namespace is what a scope may be, as in the tokens file, and may appear
  once.
- `SIGHUP` reloads it with the tokens file. A file that can't be read or
  parsed changes nothing: the limits loaded before stay, and the service
  logs an error. The import path reads a namespace's max age at each sync,
  and GC at each run.

A `#` that starts a field begins a comment, to the end of its line, so a
comment may follow a namespace's settings (`TestParseLimitsTrailingComment`).

A namespace in no line, and a namespace with no tokens, is collected with
the defaults: neither file decides that a namespace's data goes.

**Why a file of its own**, not columns in the tokens file:

- Limits belong to a namespace, not a token. A namespace has many tokens,
  and repeating its limits on each line invites conflicts.
- The tokens file holds secrets: it is mode 0600, rotated often, and
  distributed differently. Limits are plain configuration anyone operating
  the bucket can read.
- A namespace can have limits and no tokens: `-scope` without a tokens file,
  or a namespace whose tokens were all revoked, whose data still needs
  collecting.
- It is the one list of namespaces an operator maintains. The staging
  lifecycle rules come from the tokens file, though
  ([The lifecycle command](#the-lifecycle-command)): only a namespace with
  tokens has engines that stage uploads, and one whose tokens were all
  revoked keeps its rule until `lifecycle apply -allow-removals` runs.

### Blobs are kept per namespace

Each namespace's blobs are stored under its own prefix,
`<prefix>/<namespace>/blobs/<algorithm>/<hex>`, even when two namespaces
store the same layer. The service checks, presigns PUTs and GETs for, and
renews only the connection's namespace's keys (`EnableExport` and
`EnableImport` refuse a blob store that can't keep namespaces apart), so one namespace can never get
a GET for another's blob, nor have its uploads skipped because another
namespace has the blob (`TestNamespacesKeepExportsApart`,
`TestRenewAcrossNamespacesRefused`, `TestMinIONamespaceKeys`).

The trade-off is storage: a base image layer every team uses is stored once
per namespace. Staging keys are per namespace too
(`<prefix>/ns/<ns>/staging/<stage>/...`), and a promotion copies within one
namespace's prefix, so verification never crosses tenants
(`TestVerifyNamespacesApart`).

Before #23, sharing a prefix would have let one tenant's bad or malicious
same-size upload break a layer for every tenant, with `Has` keeping the
others from ever uploading the right one. Now that only verified blobs are
promoted ([#23](https://github.com/zerobull-consulting/dagger/issues/23)),
that no longer holds, so blobs could be shared per group of namespaces that
accept each other's uploads: the promotion target would be a group's prefix
outside `<prefix>/ns/<ns>/` (say `<prefix>/shared/<group>/blobs/`), and `Has`,
GETs and `renew` would read it. Not built: sharing would still tell a tenant
which digests the others in its group hold (an upload skipped is a blob
someone stored), and GC would have to count references across the group's
namespaces ([#11](https://github.com/zerobull-consulting/dagger/issues/11)).

Without a tokens file, the one namespace is `-scope`, so blobs are at
`<prefix>/ns/<scope>/blobs/`. A prefix of an earlier layout is refused at
startup (see [Bucket ownership](#bucket-ownership) and
[Changing layouts](#changing-layouts)).

### Unauthenticated mode

Without `-tokens-file`, the service takes any non-empty token on the
WebSocket and any OTLP request, and puts every engine in the `-scope`
namespace, as before namespaces existed. It logs a warning at startup:

```text
level=WARN msg="UNAUTHENTICATED: no -tokens-file, so the service takes any token, and every engine that reaches it shares namespace default: ..."
```

Anyone who can reach it can read everything stored, add to it, and steer
exports with forged telemetry. Use it only on a trusted network, as the
real-engine run does on the Docker bridge. What its OTLP receivers make the
service read and hold is bounded all the same: a body over its route's
limit, gzipped or not, is refused with 413, the bodies read at once share a
128 MiB budget, and a body must arrive within 30s (see
[OTLP limits](#otlp-limits)). So an unauthenticated client can't run the
service out of memory with OTLP. It can still keep the budget full, which
delays the engines' telemetry (503, retried): session telemetry loses
nothing, but the engine's cache events are dropped after a minute of it.

### TLS

Engines send their token in the clear over `http://`. Serve the engine
address over TLS, one of two ways:

- **Behind a TLS-terminating proxy** (a load balancer, nginx, Envoy): point
  engines at `https://<host>` (the WebSocket becomes `wss://`), and proxy to
  `-listen`. The proxy must pass WebSocket upgrades (`Upgrade` and
  `Connection` headers, HTTP/1.1 to the service), keep idle connections open
  longer than the engine's 15s ping interval, and pass `Authorization`
  through. Keep `-listen` reachable only from the proxy.
- **`-tls-cert` and `-tls-key`**: the engine address serves HTTPS with that
  certificate and key (PEM). They are read at startup; restart the service to
  rotate them. Engines must trust the certificate's CA.

Either way, `DAGGER_CLOUD_URL` should be the same `https://` URL, so the OTLP
requests, which carry the token too, are encrypted. The debug and metrics
addresses are never served over TLS: they are for local use.

### The debug API

The debug API (`-debug-listen`) lists, exports and imports for every
namespace's engines, and runs garbage collection (`POST /debug/gc`). An
admin token guards it
([zerobull-consulting/dagger#60](https://github.com/zerobull-consulting/dagger/issues/60)):

| Flag | Default | Meaning |
| --- | --- | --- |
| `-debug-listen` | `127.0.0.1:8091` | The debug API's address (empty: off) |
| `-debug-token-file` | none | A file holding the debug token: one line of at least 16 printable ASCII characters, no white space, at most 4KiB of file. SIGHUP reloads it |

- **The token.** With `-debug-token-file`, every request to the debug
  address, `/metrics` and unknown paths included, must carry
  `Authorization: Bearer <token>`. Anything else (no header, another token,
  an engine token, HTTP Basic, two `Authorization` headers) is refused with
  401, a `WWW-Authenticate: Bearer` header and no detail, before the route
  runs: a refused `POST /debug/gc` collects nothing, and a refused export
  sends the engine nothing. The service keeps only the token's SHA-256 hash,
  and compares it in constant time. Generate one with
  `openssl rand -hex 32 > debug-token`.
- **Never an engine token.** A debug token that is also an engine token in the tokens file is
  refused at startup and on reload, and refused per request if a later
  tokens reload adds it.
- **When it is required.** With `-tokens-file`, since an authenticated
  service shouldn't have an unauthenticated side door: the service refuses
  to start (exit 2, naming `-debug-token-file`) with an explicit
  `-debug-listen` and no token, and with the default one turns the debug API
  off, logging `debug API off`, so a service that never used it keeps
  starting. And when `-debug-listen` isn't a loopback IP, such as
  `127.0.0.1` or `[::1]` (`:8091` and `0.0.0.0:8091` are every interface; a
  name, even `localhost`, needs a token too, since the listener resolves it
  again when it binds); it refuses to start without a token. A loopback
  `-debug-listen` without `-tokens-file` may stay open, for local
  development; the service then logs a warning,
  `UNAUTHENTICATED debug API on 127.0.0.1:8091`. In-process, a service with
  engine tokens and no debug token refuses every debug request too. There is
  no flag to expose an open debug API.
- **A file it can't use** (missing, unreadable, empty, malformed, over 4KiB,
  or an engine token) is refused at startup (exit 1), and on SIGHUP keeps
  the token loaded before, logging an error: it never turns the check off.
  Errors never show the file's content.
- **Refusals** are logged with the route they matched (its pattern, such as
  `POST /debug/gc`, or `unmatched`) and the remote address, never the
  request's own path, query or method, where a client could put the token,
  and never the token: at warning level at most once a minute per remote host and
  reason, and 10 times a minute in all (a budget shared with the engine and
  OTLP refusals), then at debug level. `auth_rejected_total{endpoint="debug"}`
  counts them all.
- **Rotation.** Write the new token to the file and send SIGHUP: from then
  on the new one works, and the old one doesn't.

```bash
# The header from a file descriptor, so the token is on no command line.
curl -s -H @<(printf 'Authorization: Bearer %s\n' "$(cat debug-token)") localhost:8091/debug/engines
```

An admin sees every namespace: there is no per-namespace debug access.
`-metrics-listen` stays unauthenticated, and serves only `/metrics`, whose
`scope` labels name the namespaces.

## Retention and garbage collection

[`gc`](gc/) keeps each namespace's storage bounded
([#11](https://github.com/zerobull-consulting/dagger/issues/11)). It expires
index entries by their own expiry, the namespace's max age and its max
size, and deletes what no live entry names any more. **Every delete is the
service's own decision**, from expiry, size or reachability in the index:
nothing an engine reports (`engine.prune`, an offer's `gone`, a failed
`renew`) and no change of tokens ever deletes anything
(`TestGCIgnoresEngineSignals`, `TestGCNamespaceWithoutTokens`). Engine
signals describe one engine's cache, not the stored copy (see
[#11](https://github.com/zerobull-consulting/dagger/issues/11#issuecomment-6561084554)).

It runs every `-gc-interval` (1h, jittered by ±25%, so replicas spread out;
0 runs it only on `POST /debug/gc`), with the S3 stores
(`CACHESERVICE_S3_BUCKET`). It is off on a read-only instance
(`-export=false`), whatever `-gc-interval` says; an explicit nonzero one is
refused at startup (see [Read-only instances](#read-only-instances)). Without limits (`-max-age` and `-max-bytes` are 0
by default, and no namespaces file), it expires only what engines gave an
expiry, and deletes only what nothing names: bundles and blobs of exports
that were never recorded or were superseded, and staged uploads. So an
upgrade applies no age or size limit until one is configured.
`-gc-dry-run`, or `POST /debug/gc?dry-run=1`, logs (`gc dry run: would
delete`) and reports what a run would expire and delete, and changes
nothing (`TestDryRunDeletesNothing`), including the stale claims a real run
would take over (`TestDryRunReportsStaleClaims`). A dry run waits for a real
run of the same service under way, rather than report a state half way
through it (`TestDryRunWaitsForRun`). `POST /debug/gc?dry-run`,
with no value, is a dry run too; a `dry-run` value that isn't a boolean is
refused (400). Neither runs for real. A run that failed somewhere answers
500, with the reports (`TestDebugGC`).

### One run, per namespace

GC relies on the service owning its prefix (see
[Bucket ownership](#bucket-ownership)). Each run, before anything else:

- It reads the ownership marker. Without one of layout 2 (none, another
  version, or one it can't read), the run does nothing: no listing of
  namespaces, no index write, no delete, no abort. It logs why, at error
  level, and counts it (`gc_refused_total{reason}`: `no_marker`,
  `other_layout`, `marker_unreadable`) (`TestNoMarkerDoesNothing`).
- It lists the top level of `<prefix>/` and of `<prefix>/ns/`, and every
  incomplete multipart upload under `<prefix>/`. Anything outside a
  namespace other than the marker (a key at the top, a prefix other than
  `ns/`, a key directly under `ns/`, a name under `ns/` that isn't a valid
  namespace, an upload anywhere but a namespace's blob key) is an
  **ownership violation outside the namespaces**: the run does nothing at
  all (`root_violation`). The old layout's `default/index.json` and
  `blobs/sha256/...` are violations like any other
  (`TestUnexpectedKeyOutsideNamespaces`, `TestMinIOGCOwnership`).
- A failed listing of multipart uploads stops the run too
  (`uploads_unlisted`): without it, an upload of another writer's would go
  unseen (`TestUploadListingFailureDoesNothing`). Before layout 2, the
  namespaces were still collected, all but their uploads.

Namespaces are found under `<prefix>/ns/` only, from the bucket, not the
tokens: namespaces whose tokens were all removed, and namespaces in no
file, are collected too. "No tokens" and "not in the namespaces file" never
mean "delete". In each namespace, in order:

0. **Read the index, list, and check ownership.** The index is read first,
   then every object under `<prefix>/ns/<ns>/` is listed: its sizes are
   the namespace's stored bytes (`gc_stored_bytes`), by kind. Every key, and
   every upload in the namespace, must be of one of the service's shapes
   (package [`layout`](layout)). One that isn't (an **ownership
   violation**: `notes.txt`, `blobs/sha256/<not a digest>`,
   `staging/<stage>/<anything but a digest>`, an upload anywhere but a blob
   key) stops the namespace for the run: nothing in it is deleted,
   aborted, or written, the index included. Other namespaces are still
   collected. GC logs up to 10 of the keys at error level, reports them
   (`unexpected`, `unexpectedCount`), and sets
   `gc_unexpected_keys{scope}` (`TestUnexpectedKeyInNamespace`,
   `TestUnexpectedUploadNeverAborted`). Once the key is gone (removed by
   whoever wrote it: GC never removes it), the next run collects the
   namespace again.

   **A namespace needs an index.** A prefix under `ns/` without one is
   reported (`skipped: no index`) and left alone: its blobs, staged
   uploads and multipart uploads included
   (`TestNamespaceWithoutIndexLeftAlone`, `TestMinIOGCNamespaceWithoutIndex`).
   Before layout 2, its staged and multipart uploads were collected; now
   a tree under `ns/` is the service's to clean only once it has an index.
   A staged upload left by an export that never recorded waits for the
   namespace's first index, or for the lifecycle rule. So does a namespace
   whose first index was written between GC's read of it and its listing
   (`skipped: the index appeared during the run`): the read found none, so
   it can't tell what the new index names; the next run collects it
   (`TestIndexAppearingDuringRunLeftAlone`). Uploads in a
   namespace with no objects at all are left too, and logged; those of no
   known shape are violations there too, for a run of every namespace and
   for `POST /debug/gc` of that one (`TestUploadOnlyNamespaceViolationBothEntryPoints`).
1. **Expire.** One conditional index update (only when something changes):
   roots past their own expiry or their max age, and entries past their
   own expiry, are dropped, with what only they keep alive
   (`metastore.Index.Expire`, the same rules as `DropExpired`). Roots with no
   recording time (indexes before version 3) are stamped now. Cache
   records with no mappings left are kept `metastore.CacheRecordRetention`
   (7 days) after they last changed, then dropped, even when nothing else
   changes (the report's `caches`, `TestEmptyCacheRecordDroppedAlone`). Finished
   deletions older than 4h are dropped from the index.
2. *(The listing is step 0, before the expiry, so a namespace with a
   violation has nothing written.)*
3. **Mark.** What the index names: its entries' bundles and resolutions,
   and every blob those bundles name, through their outputs' chains and
   their records' pending offers (`metastore.BundleBlobs`). Bundles are read
   once and their blob lists kept in memory (at most 65,536). A bundle the
   index names that can't be read stops the namespace's sweep: GC can't
   know its blobs (`TestMissingBundleStopsSweep`); so does one that is
   readable but missing from the run's listing, which comes after the index
   read, since bundles' blob lists are cached across runs
   (`TestMissingBundleStopsSweepWarmCache`). (The index read before the
   listing is what is checked; an entry the expiry's update finds recorded
   since names objects that aren't in the listing, and so aren't
   candidates either.) A namespace whose index can't be read
   (corrupt, or a later version a newer replica wrote); its staged and
   multipart uploads, which don't need the index, are still collected
   (`TestUnreadableIndexStillCollectsUploads`).
4. **Max size**, below.
5. **Sweep.** Bundles, resolutions and blobs nothing marks, older than the
   grace period (`-gc-grace`, 2h), are claimed, deleted and finished (see
   [Safety](#safety)), oldest first, at most `-gc-max-deletes` (5,000) per
   run. What the index named when the run started isn't deleted by this
   run, even if its expiry left nothing naming it: an import sync that read
   the index just before may still be merging it, with GETs of its blobs.
   The next run deletes it (`TestNamespacesApart`). **Known gap
   ([#57](https://github.com/zerobull-consulting/dagger/issues/57)):** the
   hold is the run's own, so another replica's run right after can delete
   them, and a bundle a later export superseded has no hold
   (`TestKnownGapHoldIsPerReplica`). The index stays consistent; an engine
   whose merge was in flight computes those parts again.
6. **Staging**, below.
7. **Incomplete multipart uploads** at the namespace's blob keys older than
   the staging age are aborted: promotions a stopped service left. They are
   listed once per run, for every namespace.

The epoch object and a probe left by a failed delete
(`conditional-write-check/<nonce>`) are the service's and counted as
`other`; they are never deleted. A run of one namespace never deletes
another's data (`TestNamespacesApart`, and the real-engine `gc` scenario,
which checks the run's scope after collecting its own).

### Max age

A root expires `max-age` after an export last recorded it:
`metastore.Root.RecordedAtUnix`, the service's clock when the index update
that wrote its record ran. Times are whole seconds, and a max age with a
fraction of a second counts as the next whole one
(`TestCapExpiryRoundsUp`). The entries only that root keeps alive go with
it; an entry a younger root depends on stays, whenever it was recorded
(`TestExpireMaxAge`).

An export refreshes a root's time **only when it writes the root's record**
(`Index.RecordResolvedAt`, `TestRecordStampsRoots`):

- a new root, with its record: stamped. A root from before version 3, with
  no time yet, is stamped only with its record, or by GC's first run
  (`TestUnstampedRootStampedOnlyWithItsRecord`);
- a later state of the value from its own engine (a later generation, or
  the same with as many replacements or more), or an imported value's
  record that adds parts (see [Imported entries](#imported-entries)):
  refreshed;
- a late export from an older generation, or an engine exporting back an
  import unchanged: not refreshed, and not added when the index has no such
  root. A value whose root the max age expired, still stored as another
  root's dependency, doesn't come back as a root with a fresh time
  (`TestRootAddedOnlyWithItsRecord`). Merges hand stored entries to engines,
  so a refresh by them would keep data alive forever by merging it around.
  The tracker doesn't export unchanged imports anyway; the index doesn't
  rely on it.
- once GC has dropped the value's entry and its cache mapping too, a late
  export from an older generation is compared with the cache's record,
  which outlives its mappings: if it names any value the index has no
  entry for, the whole record is refused (`metastore.ErrStaleGeneration`)
  and nothing is added, so the value doesn't come back with a fresh root
  for another max age
  ([#59](https://github.com/zerobull-consulting/dagger/issues/59),
  `TestLateOlderGenerationDoesNotReviveCollectedRoot`,
  `TestLateOlderGenerationBeforeCollection`). One that names only values
  the index holds is recorded as before, but adds no root: a held value
  with no root had its root expired, whether its mapping is exported or an
  import the stale record takes over with more parts
  (`TestLateOlderGenerationAddsNoRootToHeldValue`). The export ends with its
  roots refused, logged once, and the tracker doesn't export them again
  (`TestStaleGenerationExportNotRetried`), until a newer generation of the
  cache, which the index accepts, reports in: by its engine's hello, or any
  span, event or import of that generation, whether or not it names the
  roots. Then the refused roots still worth exporting are exported again
  once, at the next export tick, from the newest connection of the cache.
  A batch is never exported from a connection of an older generation than
  the tracker's: it waits for the newer generation's hello, as it would for
  an engine not connected (`NoEngineWait`, 10s). With several connections of
  one cache, exports go to the newest generation's, even when an older
  generation's engine connected later
  (`TestOlderGenerationConnectingLaterDoesNotTakeExports`)
  (`TestTrackerStaleRefusalClearedByNewerGeneration`,
  `TestTrackerStaleRefusalClearedRescheduled`, #129), during a drain too
  (`TestTrackerStaleRefusalClearedDrain`), and even when the newer
  generation reports in while the stale export is still in flight
  (`TestTrackerStaleRefusalInFlightDropped`). With several replicas on one
  prefix, the index can learn the newer generation from another replica
  before this one does; the root is exported once the engine connects here,
  even when its spans and events go to the other replica
  (`TestStaleRefusalClearedAcrossReplicas`, `TestStaleGenerationExportNotRetried`). The cache's own generation and
  newer ones still record it: the engine really has it
  (`TestSameAndNewerGenerationRecordAfterCollection`). The record is kept
  `metastore.CacheRecordRetention` (7 days) after its last mapping went,
  wipes included (`TestCacheRecordOutlivesItsMappings`); a late export
  after that is recorded as new.

**Merges are capped at it.** Most stored roots have no expiry (0), and a
merge used to send the index's: every engine kept every imported root until
its own GC removed it. The import path now caps each root's expiry in a
merge at its recording time plus the max age
(`metastore.Index.ApplyMaxAge`, on its own snapshot of the index, never
written back), and leaves out roots past it, even before GC runs
(`TestGCMaxAgeEndToEnd`; the real-engine `gc` scenario checks the expiry
engine GB was sent). Engines then release imported roots on their own.
When a root's capped expiry changes from what a merge sent (the max age
was raised, lowered, set or removed, or an export recorded the root
again), the importer merges its bundle again with it; it compares with what
it sent, not with what the engine reported, so it does so once per change
(`TestMergeAgainWhenCapMovesLater`, `TestMergeAgainWhenCapSet`). An engine
keeps the earlier of the expiries it was sent for a root; GC expires the
stored root on its own either way. **Known gap
([#58](https://github.com/zerobull-consulting/dagger/issues/58)):** a cap
that is already past when it changes is never sent: the root is left out of
merges, and an engine skips a root sent with a past expiry rather than
releasing it. An engine that had the root with no expiry, or a later one,
keeps it until its own GC prunes it, and computes again any part whose blob
GC then deletes (`TestKnownGapPastCapNotSentToEngines`). The
importer's expiry guard is unchanged: a root over an entry that has expired,
or depends on one that has, is not merged.

### Max size

`max-bytes` bounds what a namespace **keeps**: its index, and the bundles,
resolutions and blobs its live entries name, at the sizes the listing
found. Over it, GC expires roots **oldest first**, by recording time, then
service number, until what is left fits: it finds how many with a binary
search over the roots, walking the index about log2(roots) times. Their
entries go as in step 1; their objects, now unreferenced, are deleted once
past the grace period, by a later run (`TestMaxSizeExpiresOldestFirst`). A
root an export recorded again since GC read the index is kept (it is
younger now, or, within the same second, its record changed;
`TestMaxSizeKeepsRootRecordedSameSecond`).

What GC can't delete yet (objects younger than the grace period, staged
uploads, keys it doesn't know) is measured and reported but doesn't make it
expire roots: a burst of uploads shouldn't expire live data. Nor does an
index that alone is at or over the limit: expiring roots can't shrink it
enough, so GC expires none, reports it, and goes on with the rest of the
run; raise `max-bytes` (`TestMaxSizeUnderTheIndexExpiresNothing`). The bucket can
so hold more than `max-bytes` for up to a grace period plus an interval.
`gc_entries_expired_total{reason="size"}` counts what the limit expired.

### Bundles

A bundle no entry names is deleted, with its resolution if no entry names
that either; then their blobs, once nothing names them. That covers
bundles of exports that were never recorded (the service stopped between
the bundle and the index write), and bundles whose records are **all
superseded** by later exports
([#27](https://github.com/zerobull-consulting/dagger/issues/27)): the
index moved each of their entries to a newer bundle, so none names them,
and they are collected like any other. A bundle that still holds any
entry's current record stays, and is still merged whole; #27's merge-order
gap is unchanged for those (see [Import path](#import-path)). The order
keeps the guarantee that the index never names a bundle or resolution that
isn't stored: the index stops naming them first (expiry, or a later
export), and an export can't name them again once GC has claimed them
(below).

### Safety

Objects are content-addressed, so an export can name an object GC found
unreferenced: a blob it found stored (`Has`) before uploading anything, or
a bundle or resolution an earlier export stored. And GC's listing and an
export's checks are both moments, not locks. The index is the one thing
both write with conditional writes, so GC deletes in three steps, each
serialized with exports on the index:

1. **Claim.** In one index update, GC marks what it will delete
   (`metastore.Collection`), after checking against that index, not its
   listing, that nothing names it now and no other run has claimed it
   (`TestClaimRechecksIndex`). An export recorded before the claim is seen;
   one recorded after names a claimed object and is refused
   (`metastore.ErrCollected`).
2. **Delete**, with `DeleteObjects`, 1,000 keys per request, within 10
   minutes of the claim: a run doesn't start a delete after that, and
   cancels one still in flight (`TestDeletesCanceledAtDeadline`). A claim
   is stamped with its own time, not the run's start, so a run that took
   minutes to list doesn't make its claims look stale to other replicas
   (`TestClaimStampedAtClaimTime`); and the deadline counts from that same
   instant, on the monotonic clock, not from after the claim's index
   update returns, so a slow claim can't stretch deletes past the time
   other replicas take it over. A run whose claim took past its deadline
   deletes nothing, and releases its claims
   (`TestDeleteDeadlineFromClaimStamp`). `DeleteObjects` answers per key: keys
   it reports failing are counted apart and reported, and the next batches
   still run (`TestPartialDeleteFailure`).
3. **Finish.** In a second update, GC marks them deleted in a new epoch,
   then publishes the epoch in `<prefix>/ns/<ns>/gc-epoch.json`. Each export
   try reads that object before it checks any blob (one small GET, kept for 10s), and its
   index write is refused if it names an object deleted in a later epoch: it
   checked the object before it was deleted
   (`TestRecordFenced`, `TestExportRacesSweep`).

The epoch object only goes up (a conditional write), and each run
republishes the index's epoch if the object lags it, such as after a failed
publish (`TestWriteEpochMonotonic`, `TestEpochRepublished`). A lagging epoch
is safe anyway: it only makes exports refuse more. A marker pruned while it
lagged can't let an export through either: the deletion finished over 4h
before, and an export that may still record started under 3h ago, so its
checks came after the deletion (`TestStaleEpochAfterPruneIsSafe`).

A refused export is tried again from the start: it finds what is gone, has
the engine upload it again, and is recorded (`export_collected_total`).
Refusals have their own budget of 3 tries, apart from the export's upload
attempts, and when they run out the roots fail as "garbage collection
deleted what the export named", not as failed uploads
(`TestExportCollectedRetriesApart`). An exporter reads the epoch at most
once per 10s and keeps it for later tries: a fence read earlier is still
read before the try's checks, and at worst only refuses more. A refused try
reads a fresh one (`TestExportFenceReadOncePerTTL`). So
the index never names an object that isn't stored, whatever GC does at the
same time. Shown with fake engines by `TestGCRacesExport`: an export held
between its blob promotions and its index write while GC runs. With the
default grace period GC collects nothing of it; with one too short, GC
deletes its blobs, the export's write is refused, and the retry uploads
them again and is stored; the index stays consistent either way.

**The grace period** (2h) covers an export from its first check to its
index write: it waits up to its upload timeout (1h) for the engine, then
promotes and records. So a blob promoted for an export not yet recorded is
younger than it, and never collected (`TestInFlightBlobKept`). Correctness
doesn't rest on it, the fence does; it keeps in-flight exports from being
refused. An export refuses a fence older than 3h, and a finished deletion
stays in the index for 4h, so no export can check an object, see it
deleted, and record it after its marker is gone.

**Replicas.** Every step is a conditional index write or an idempotent
delete, so any number of replicas may run GC at once, with no election
(`TestConcurrentRuns`): a run claims only what no other run has claimed, and
deletes only what it claimed. A run that stops between its claim and its
finish leaves its claims in progress (exports naming them are refused);
once they are 15 minutes old, a later run of any replica takes them over,
deletes and finishes (`TestStaleClaimTakenOver`). That bound is fixed (the
longest deadline a collector takes, 10 minutes, plus 5): a replica
configured with a shorter deadline still waits for one with a longer one
(`TestStalenessIgnoresOwnDeadline`). Claims taken over count
against `-gc-max-deletes` with new ones, stale first, so one run never
deletes more however many runs stopped (`TestStaleClaimsBoundedByMaxDeletes`). Clocks of replicas only
need to agree within minutes.

**Rolling upgrade.** Index version 3 holds the claims; a replica that reads
only version 2 refuses it and stops updating the namespace, rather than
record an export without checking them (see [Metadata store](#metadata-store)).
Version 4 (#59) is refused the same way by a replica that reads only
version 3.

### Staging and multipart uploads

A staged upload is never read as cache (`Has`, every GET and `renew` read
digest keys only), so deleting one is always safe: at worst a promotion
still copying it fails, and its export asks for the blob again. GC reads
the bucket's lifecycle rules once per run (`GetBucketLifecycleConfiguration`):

- a namespace whose `<prefix>/ns/<ns>/staging/` an enabled rule expires by
  age (`Expiration.Days`, filtered by prefix alone; a rule with a `Date`
  expires objects once, not as they age), with a prefix within the staging
  prefix, is left to it (`gc_staging_rule_missing` 0,
  `TestExpiringPrefixes`). A broader rule, over the namespace or the whole
  prefix, doesn't count: it also expires the namespace's live blobs,
  bundles and index by age, which the index still names. GC reports such a
  rule (a warning, and `liveRules` in the run's report) and expires the
  staged uploads itself (`TestBroadLifecycleRule`);
- a namespace without one is reported (`gc_staging_rule_missing` 1, and a
  warning when it has staged uploads), and GC deletes its staged uploads
  older than `-gc-staging-age` itself: 2h, an export's upload timeout (1h)
  plus an hour for its promotions, which take minutes for a large blob the
  service hashes. A staged upload deleted before its promotion is uploaded
  again (`TestStagingKeptThroughPromotion`);
- when the rules can't be read (no `s3:GetLifecycleConfiguration`, or a
  store without them), GC deletes old staged uploads in every namespace,
  and removes `gc_staging_rule_missing` (`TestStagingRuleMetricUnknown`).

Stale incomplete multipart uploads (`ListMultipartUploads`) at a
namespace's blob keys are aborted whatever the rules (`TestStagingAndUploads`,
`TestMinIOGC`). An upload anywhere else is not the service's: it is never
aborted, at any age, and stops collection as any unexpected key does
(`TestUnexpectedUploadNeverAborted`, `TestMinIOGCOwnership`); the bucket's
own abort rule, if it has one, still applies to it. MinIO lists
multipart uploads only for an exact key: a listing by prefix returns none.
For an S3-compatible store (a custom `CACHESERVICE_S3_ENDPOINT`),
`S3Bucket.ListUploads` then lists the bucket's uploads and keeps those
under the prefix, once per run (`FilterAllUploads`). It doesn't on AWS
S3, which lists by prefix. MinIO also drops stale uploads itself after 24h.

**Lifecycle rules.** Keep the staging rules and the multipart rule of
[Lifecycle rule for staging](#lifecycle-rule-for-staging), set with
[`cacheservice lifecycle apply`](#the-lifecycle-command): they cost the
service nothing. Don't add rules for bundles, resolutions, blobs or the
index: whether one is in use depends on the index, not its age, and a rule
by age would delete live entries' data, leaving the index naming missing
objects. GC does those.

### Re-verifying old blobs

GC lists every digest key of each namespace, so a sweep is where a
re-verification of blobs stored before uploads were verified could run
([#53](https://github.com/zerobull-consulting/dagger/issues/53)): re-hash
the marked blobs last modified before a cutoff, once, and act only on the
service's own hash. Not built here: hashing reads every such blob through
the service or a copy, which a run shouldn't do unasked; the one-off job of
[Repairing blobs stored before verification](#repairing-blobs-stored-before-verification)
stays the way to do it.

### Costs

Per namespace and run:

| Step | Requests |
| --- | --- |
| Expire | 1 index GET; 1 conditional PUT when something changed |
| List | 1 `ListObjectsV2` per 1,000 objects |
| Mark | 1 GET per live bundle not read before (kept in memory) |
| Claim, finish | 1 index read and conditional PUT each, only when there is something to delete; 1 GET and conditional PUT of the epoch |
| Epoch | 1 GET of the epoch object, once the namespace has deleted anything; exporters read it at most once per 10s each, and after a refused try |
| Delete | 1 `DeleteObjects` per 1,000 objects |
| Staging | none beyond the listing; 1 `DeleteObjects` per 1,000 deleted |

and, per run: one `ListObjectsV2` of the top level, one
`ListMultipartUploads` per 1,000 uploads under the prefix (plus one more of
the bucket's when the store lists none by prefix, as MinIO), and one
`GetBucketLifecycleConfiguration`. Each index update adds about 100
bytes per claimed object, at most `-gc-max-deletes` (5,000, so about 500 KB)
while a run deletes, and the finished ones stay 4h. Memory: the listing is
held for the namespace's run, about 150 bytes per object.

Measured against the local MinIO (`TestMinIOGCScale`, 2026-11-03), a
namespace of 2,505 objects, 2,500 of them unreferenced: 1.0s, 3 list and 3
delete requests (this is what CI runs, about 5s with the setup);
`CACHESERVICE_TEST_GC_OBJECTS=20000`: 20,005 objects in 9s, 21 list and 20
delete requests, so about 0.45 ms per deleted object, and 14s to store them
first.

### Configuration

| Flag | Default | Meaning |
| --- | --- | --- |
| `-gc-interval` | `1h` | Time between runs, jittered by ±25% (0: only `POST /debug/gc`; a negative interval is refused at startup, rather than taken as 0). GC is off with `-export=false`, which refuses an explicit nonzero value |
| `-gc-dry-run` | false | Log and report what a run would do; change nothing |
| `-gc-grace` | `2h` | How old an unreferenced bundle, resolution or blob must be to be deleted |
| `-gc-staging-age` | `2h` | How old a staged upload, or an incomplete multipart upload, must be to be deleted |
| `-gc-max-deletes` | 5000 | Most bundles, resolutions and blobs one run deletes per namespace |
| `-max-age` | 0 | Default max age of a namespace's roots, such as `720h` (0: none) |
| `-max-bytes` | 0 | Default max size of what a namespace keeps, such as `500GiB` (0: none) |
| `-namespaces-file` | none | Per-namespace limits (see [The namespaces file](#the-namespaces-file)) |

The service's credentials also need `s3:ListBucket`,
`s3:ListBucketMultipartUploads` (without it, GC does nothing:
`uploads_unlisted`), `s3:DeleteObject` on every key under the prefix, and,
to report namespaces without a staging rule,
`s3:GetLifecycleConfiguration` on the bucket.

#### The keys

Keep three credentials, each with only what it needs
([zerobull-consulting/dagger#140](https://github.com/zerobull-consulting/dagger/issues/140)):

| Key | Who holds it | What it needs |
| --- | --- | --- |
| **Read** | pull-request jobs, read-only instances (`-export=false`) | `s3:GetObject` under the prefix, `s3:ListBucket` for it (see [A read-only key](#a-read-only-key)). On Spaces, a limited key with `Read` on the bucket. |
| **Read/Write/Delete** | the writer service, export-capable jobs, GC | the above, plus `s3:PutObject`, `s3:DeleteObject`, `s3:AbortMultipartUpload` and `s3:ListBucketMultipartUploads` under the prefix; `s3:GetLifecycleConfiguration` on the bucket is optional (without it, GC expires every namespace's staged uploads itself, and `lifecycle check` can't run). On Spaces, a limited key with `Read/Write/Delete` on the bucket. |
| **Admin** | an operator, once per bucket and after a change of namespaces | `s3:GetLifecycleConfiguration` and `s3:PutLifecycleConfiguration` on the bucket, and `s3:GetObject` on the ownership marker: what [`lifecycle apply`](#the-lifecycle-command) needs; `lifecycle plan` and `check` need only `s3:GetLifecycleConfiguration`. On Spaces, `apply`, `plan` and `check` need a full-access key: a limited key gets 403. `lifecycle claim` needs none of the lifecycle permissions, only what a writer's startup check needs on a fresh prefix (`s3:ListBucket` and `s3:ListBucketMultipartUploads` for the prefix, and `s3:GetObject` and `s3:PutObject` on the marker, `<prefix>/cacheservice.json`, which it reads before anything else; `s3:GetObject` on each ancestor prefix's marker too, or the [nested-prefix check](#at-startup) skips that ancestor on its 403; any other error reading it refuses), so the Read/Write/Delete key can claim a prefix, and so can an admin key that also has these (on Spaces, a full-access key does). |

The admin key is the most powerful: a lifecycle rule can expire anything in
the bucket. Use it for `lifecycle apply`, and for `plan` and `check` where the writer's
key can't read the rules, interactively, and keep it out
of every CI secret and the service's environment. Neither the service nor
the sidecar action ever writes lifecycle rules: they only read them, once
per GC run (`TestServiceNeverWritesLifecycleSource` checks the sources,
`TestMinIOBinaryNeverWritesLifecycle` a running writer's requests through a
proxy that refuses any lifecycle write). On Spaces, as observed
on 2026-11-07 (sfo3, prefix `cache`), a limited `Read/Write/Delete` key could put,
get and list objects, list multipart uploads, create and abort a multipart
upload, and delete, but could not read the lifecycle configuration
(`GetBucketLifecycleConfiguration` returned 403 `AccessDenied`). GC then logs
"can't read the bucket's lifecycle rules, so it expires every namespace's
staged uploads itself" on each run, the fallback above, which is safe but
costs requests; and `lifecycle check` needs the admin key there. A writer
with such a key also claimed an empty prefix at startup, creating the marker
with `If-None-Match: *`.

## Stopping and draining

There are two ways to get what is pending exported before something stops:

- **The flush**, for an orchestrator stopping one engine while the service
  stays up: `POST /v1/engines/{cacheID}/flush`, then stop the engine, then,
  if the service goes too, stop it with no drain (`-drain-timeout 0`). It is
  per engine cache, bounded by the caller, and takes new work all along (see
  [Flushing one engine cache](#flushing-one-engine-cache)).
- **The drain**, for a shared service restarting under its engines: on
  SIGTERM, every cache, until `-drain-timeout`, taking no new work, and the
  service stops after it. The rest of this section is the drain.

The service runs as a sidecar on an ephemeral CI runner, and the end of the
job is its only chance to store what the job built
([#63](https://github.com/zerobull-consulting/dagger/issues/63)). On the first
SIGTERM or interrupt, it drains (`server.Service.Drain`) before it stops.
A read-only instance (`-export=false`) has nothing to flush: its drain
logs `drain: nothing to export (read-only: -export=false)`, and returns at
once, unless an explicit import admitted before it still runs, which it
waits for, up to its deadline (logged as `drain: waiting for explicit
requests`).

1. **It takes no new work.** A new engine WebSocket gets 503 with
   `Retry-After: 10` before the upgrade, and one upgraded but not yet
   welcomed is closed with 1013 (try again later). An engine is either
   welcomed before the drain starts, and counted in its report (`engines`
   in `drain: done` and `GET /debug/drain`), or refused: the hello is
   checked under the lock the drain starts under
   (`TestDrainWelcomesOrRefusesEveryHello`). Spans and session ends of
   sessions the export path doesn't track are dropped (answered 200, counted
   in `drain_refused_total`). Spans and ends of the sessions it tracks are
   still taken: a session that ended just before the signal, with some of the
   cache spans its carrier counted still on their way, still sends them, and
   dropping them would lose entries (`TestDrainTakesLateSpansOfEndedSession`). The connected engines stay
   connected, since exports run over them. The import sync loops and
   scheduled GC stop; a GC run under way is canceled (GC is idempotent, and
   the next run finishes it), whether scheduled or asked for. `POST
   /debug/gc`, and the explicit export and import hooks, answer 503: the
   drain runs what is pending itself. An explicit export or import admitted
   before the drain started is waited for (see below). Engine events
   (`engine.start`, `engine.prune`) are still taken: a prune makes an entry
   no longer worth exporting.
2. **It flushes.** Every dirty entry is due now, without waiting for it to
   settle (`pending` in its log and `GET /debug/drain`). A session whose end
   isn't seen is no longer tracked, so its later spans are dropped. One that
   ended with some of its counted spans missing is waited for (`sessions`)
   until they are in, or `-export-settle` (at most 2s) passed with none of them. Roots
   waiting for another try (busy, failed, interrupted, or waiting for their
   engine) are due at once too. The drain runs those exports, one at a time
   per engine cache as the scheduler does, and waits for them, their uploads,
   verification and index writes included.
3. **It starts nothing past its deadline.** Each pass checks the deadline,
   and whether the drain was canceled, before it schedules anything, so a
   drain whose deadline passed before it began starts nothing, and a retry
   that comes due as the deadline passes isn't started. What it would have
   started is reported left, not in flight
   (`TestDrainStartsNothingPastItsDeadline`,
   `TestDrainDoesNotStartRetryPastItsDeadline`). Exports started before the
   deadline run on until the shutdown cuts them off.

It ends when nothing is left that it could export, or `-drain-timeout`
(30s) passes. A **linger** follows, of at most `-drain-linger` (15s; see
[The linger](#the-linger)), so that an engine stopped after the drain can
deliver its last cache event. Then a shutdown, **bounded at 5s** as a whole: it cuts off
what is still in flight (exports, import syncs, GC runs, those asked for
with `POST /debug/gc` included) by canceling their context, closes the
engine connections (each gets a close frame, 1001 going away, all at once;
those that don't answer within 2s are dropped; from then on a new engine
gets 503, and one upgraded before is closed with 1001 at its hello, so an
engine reconnecting meanwhile isn't welcomed), shuts down its HTTP
servers, and waits for their requests and for what it cut off to end. So
the whole stop, from the first signal to the exit, takes at most
`-drain-timeout` plus `-drain-linger` plus 5s, even with engines that don't
answer and work that ignores cancellation (GC's finish update and a
promotion's cleanup run on after a cancel, against a store that may not
answer).

- **Exit codes.** 0 when everything ended within the bound without an
  error: a clean stop.
  1 when the shutdown ended in any error: engine connections still closing
  after they were dropped (an error that comes about 2.5s in, before the
  bound; the shutdown still shuts the servers down and waits for what is
  running), or the bound cutting the shutdown off with work still running
  (such as an HTTP request). It logs `stopped uncleanly` with the reason in
  `error=` and what was still running (`exports`, `imports`, `gc`,
  `engines`), says the shutdown's bound passed only when it did, and exits.
  1 too when a signal, or the backstop for a step that overran its
  own bound (`stopped: the shutdown overran its bound`), cut the shutdown
  off: it logs what was still running, and exits without waiting for it. 1 also when an HTTP server
  failed (such as its address in use), which stops the service too, or
  failed during the drain or the shutdown, including as the shutdown ended:
  the shutdown waits, within its bound, for the servers to return before
  their errors are read. A
  process manager, and the tests, can tell a clean stop from a cut-off one
  (`TestMinIOBinaryStopBoundedExitsOne`, `TestMinIOBinaryCleanStopExitsZero`).
- **Signals.** A second signal during the drain ends the drain at once, and
  skips the linger; during the linger, it ends the linger at once
  (`TestMinIOBinarySecondSIGTERMEndsLinger`). The shutdown follows, and
  exits 0 if it ends within its bound without an error. A signal during the shutdown (the
  second with `-drain-timeout 0`, or after the drain and the linger ended)
  exits at once, with 1, logging `signal during the shutdown: exiting at
  once` and what was still running
  (`TestMinIOBinarySignalDuringShutdownExitsAtOnce`).

`-drain-timeout 0` stops without a drain and without a linger, with the same
bounded shutdown. A CI job sets it long, such as `10m`.

| Flag | Default | |
|------|---------|---|
| `-drain-timeout` | `30s` | How long the drain may take (0: no drain, and no linger). A negative one is refused at startup |
| `-drain-linger` | `15s` | After the drain, at most how long the service keeps answering the engines' OTLP requests, so each connected engine whose events come here can deliver `engine.stop` (0: none; see [The linger](#the-linger)). A negative one is refused at startup |

### Flushing one engine cache

`POST /v1/engines/{cacheID}/flush?timeout=<Go duration>`, on the engine
address (`-listen`), exports what is pending for one engine cache now, and
answers once nothing is left or the timeout passed
([zerobull-consulting/dagger#136](https://github.com/zerobull-consulting/dagger/issues/136)).
An orchestrator calls it before it stops a long-lived engine, while the
service is fully up, so the engine still delivers its last events.

```bash
curl -fsS -X POST -u "$ENGINE_TOKEN:" "http://cache:8090/v1/engines/$CACHE_ID/flush?timeout=10s"
# A sidecar, which doesn't know its engine's cache ID: the cache of the
# connections that authenticated with this token.
curl -fsS -X POST -u "sidecar-engine:" "http://localhost:8090/v1/engines/flush?timeout=10s"
```

- **Auth**, as the engine's own requests: the HTTP Basic username is the
  token (401 without one, or with one the tokens file doesn't know), and the
  token's namespace is the one flushed. With `-tokens-file`, only the token
  of the cache's latest connection (its owner, as for its engine events)
  may flush it: another token of the namespace gets 403. A token of
  another namespace sees only its own, where the cache isn't: it gets
  `not_connected`, the same answer as for a cache that doesn't exist. Without
  a tokens file, any token is taken, as on the WebSocket. The cache ID is
  1 to 128 letters, digits, `.`, `_` or `-` (400 otherwise).
- **The token form**, `POST /v1/engines/flush`, flushes the cache of the
  welcomed connections that authenticated with the request's token, in its
  namespace: one in a sidecar. With none, it reports `not_connected`; with
  more than one cache, it answers 409, and the caller flushes each by its
  ID. The report names the cache (`cacheID`).
- **The timeout** is optional: `10s` when the request names none, at most
  `10m` (400 beyond that, or for a negative or unreadable one). The cap is
  fixed, not `-drain-timeout`, since a service stopped with no drain runs
  with `-drain-timeout 0`. `timeout=0` makes nothing due, and only reports
  what is pending.
- **What it does.** Every dirty entry of the cache is due at once, without
  waiting for `-export-settle`, and stays so while the flush runs, so what
  becomes dirty meanwhile is exported too. Roots waiting out a retry's
  backoff (busy, failed, interrupted) are due now, once; a retry after that
  waits its normal backoff. A session whose spans may still come (one that
  hasn't ended, or ended with some of the cache spans its carrier counted
  missing) is waited for until it ended with every span in, or
  `-export-settle` (at most 2s) passed since its last span: so a flush right
  after the client returned waits for its last spans and its carrier, which
  are on their way. Its late spans are taken and exported. A flush starts
  each failing root's retry at once: a caller that loops flushes retries
  them at its own rate. The scheduler runs the exports, one
  at a time per engine cache: the flush starts none of its own. Other
  caches are scheduled as usual.
- **It isn't a drain.** New engines, sessions and spans are taken as usual.
  An engine still running builds can keep a flush busy until its timeout.
- **When it returns:** once nothing of the cache is dirty, waiting for
  spans, in flight or due for a retry before the timeout (`complete`); once
  the timeout passes, or only retries due after it are left (`timeout`; such
  a retry isn't waited for, and is counted `later`); at once, with what is
  left, when the cache's engine isn't connected or disconnects
  (`not_connected`); and when the request is canceled (`canceled`). **It
  never cancels an export**: one in flight when it returns runs on until it
  ends, or the engine stops and interrupts it. A canceled request ends the
  wait, not the exports.
- **Concurrent flushes** of one cache each wait on the cache's state, and
  each reports on its own: what was stored while it ran, and what was left
  when it returned.
- **Refusals:** 409 on a read-only instance (`-export=false`) or with the
  export path off, as the explicit export hook; 503 while draining
  (`ErrDraining`), and once the stop has begun (`ErrStopping`).

The answer is 200 for every outcome, and the body says which; errors are
only for auth, bad requests and refusals. The report:

```json
{
  "outcome": "complete",
  "cacheID": "6e1f...",
  "took": "1.234s",
  "stored": 5,
  "left": {"dirty": 0, "waitingSpans": 0, "inFlight": 0, "retrying": 0, "later": 0}
}
```

- `outcome`: `complete` (nothing left), `timeout`, `not_connected` or
  `canceled`.
- `stored`: the roots stored by exports of the cache that ended during the
  flush.
- `left`, as the drain counts them: `dirty` (worth exporting, not in flight
  or waiting for a retry), `waitingSpans` (sessions still waited
  for), `inFlight`, `retrying` (a retry due before the timeout), `later` (a
  retry due after it).

Each flush logs one `flush: done` line, with the outcome and these counts,
and is counted in `flush_total{outcome}`, `flush_duration_seconds` and
`flush_unexported_roots_total` (see [Metrics](#metrics)). The debug API has
a twin, `POST /debug/engines/{conn}/flush`, keyed by connection.

### The linger

An engine whose `DAGGER_CLOUD_URL` points at the service sends its cache
events here, and its own graceful stop sends the last one, `engine.stop`,
then waits up to 30s (`engineEventShutdownTimeout`) for it to be delivered.
A service already gone answers nothing, so an engine stopped after the
service, as the stop order below requires, used to retry against a closed
port for those 30s, and a `docker stop --time 30` then killed it (exit 137;
[#102](https://github.com/zerobull-consulting/dagger/issues/102)). So after
its drain the service lingers (`server.Service.Linger`):

- **It still answers the engines' OTLP requests** on `-listen`, as during
  the drain: engine events are logged and taken as usual (`engine.stop`
  included, which is logged like any other and deletes nothing), and spans
  of sessions it doesn't track are answered 200 and dropped. The engine
  connections stay open, and new ones are still refused.
- **It starts nothing and stores nothing.** The context the export, import
  and GC paths run on ends as the linger starts, as the shutdown would end
  it: an export the drain's deadline left in flight is cut off then, and
  publishes nothing (`TestMinIOBinaryLingerStoresNothing`).
- **It ends once every engine it waits for sent `engine.stop`**: each engine
  cache with a connection when it starts whose engine events reach this
  service (some event about it arrived since the service started). An
  engine whose events go elsewhere isn't waited for, and with none to wait
  for the linger ends at once. A disconnect ends nothing: an engine closes
  its remote cache connection in the first phase of its stop ("stop remote
  cache integration"), and sends `engine.stop` only later, once its cache
  closed ("close dagql cache", then "stop engine events"). With tokens,
  `engine.stop` is taken only from the cache's own engine's token, as its
  other events are, and only one sent while draining counts.
- **It is bounded.** At `-drain-linger` it ends whatever is left, logging the
  caches whose `engine.stop` didn't come (`linger: no engine.stop`, a
  warning); a second signal ends it at once. `-drain-linger 0` stops right
  after the drain, as before (`TestMinIOBinaryNoLinger`).

The default, 15s, comes from the engine's measured stop: on an engine that
had just run the demo's build, `engine.stop` arrived 0.3s after the engine's
SIGTERM (it disconnected at 0.0s; "close dagql cache" took 0.31s), and its
whole stop took 7.1s, 6.8s of it closing its network providers after the
event was sent. The linger ends as soon as the event arrives, so its bound
only costs time when an engine whose events come here isn't stopped, or its
stop stalls, such as the service restarting under engines that run on. Set
`-drain-linger 0` there.

**An engine not heard from yet.** One whose events come here, but none of
which arrived since the service started (such as after a service restart
under a running engine), isn't waited for: the service can't tell it from
an engine whose events go elsewhere. Its `engine.stop` then finds the
service gone. Known gap, pinned by `TestKnownGapLingerMissesEngineNotSeenYet`
([zerobull-consulting/dagger#124](https://github.com/zerobull-consulting/dagger/issues/124)).
The `engine.stop` of every generation of a cache ID welcomed at the
drain's start is recorded, however many there are; a token's `engine.stop`
for other generations is remembered for at most 8 of them per cache, which
bounds the service's memory
(`TestLingerStoppedGenerationsInSnapshotNotCapped`,
[zerobull-consulting/dagger#126](https://github.com/zerobull-consulting/dagger/issues/126)).
An engine killed during its "stop engine events" phase, which is where it
waits for the service that the linger keeps up, loses its cache on a kept
state volume
([zerobull-consulting/dagger#127](https://github.com/zerobull-consulting/dagger/issues/127)).

It logs `linger: started` with the engines it waits for (`linger: waiting
for engine.stop`, by namespace and cache ID), and `linger: done` with its
outcome: `stopped` (every engine sent `engine.stop`), `nothing` (none to
wait for), `deadline` or `canceled`. `GET /debug/drain` shows it under
`linger`. Tests: `TestLinger*` in [`server`](server), and
`TestMinIOBinaryLinger*`, `TestMinIOBinaryLingersUntilEngineStop` and
`TestMinIOBinaryNoLinger` here.

The drain logs, when it starts, what each engine cache (by cache ID, never
the token) has pending, in flight and waiting for a retry, and when it ends,
the roots stored by cache ID, and what was left unexported by cache ID and
root count (`drain: left unexported`, a warning), with its outcome:
`complete`, `deadline` or `canceled`. `GET /debug/drain` shows the same while
it runs, and the [metrics](#metrics) `draining`, `drain_duration_seconds`,
`drain_stored_roots_total`, `drain_unexported_roots_total` and
`drain_refused_total` measure it.

**Stop order.** Stop the service first, then the engine: the drain's exports
run over the engine's connection, and the engine uploads the layers itself.
An engine stopped first takes its pending exports with it. Stop the engine
once the drain is done (`drain: done` in the service's log), while the
service lingers, then wait for the service to exit: the engine's stop then
delivers its `engine.stop` and takes seconds. The sidecar action does this
(see [Running it as a sidecar](#running-it-as-a-sidecar)).

**Process managers.** The process manager's stop timeout must be longer than
`-drain-timeout` plus `-drain-linger` plus 5s, the linger's and the
shutdown's bounds after the drain, or it can kill the service mid-drain or
mid-shutdown: with the defaults (30s and 15s), give it more than 50s, and
with a `10m` drain, more than `10m20s`. A process manager that sends SIGTERM
again at its own stop timeout ends the linger at once. `docker stop --time`
(10s by default), `docker compose`'s `stop_grace_period` (10s by default) and
Kubernetes' `terminationGracePeriodSeconds` (30s by default) all send SIGKILL
when theirs runs out. Readiness probes and Kubernetes integration are out of
scope here.

**Retries during a drain.** A root that fails or is busy during the drain is
tried again on its normal backoff (2s doubling to 5m), while the deadline
allows; the drain doesn't shorten it. A busy root is one the engine is still
computing, and a failed one most often a store that refused it: trying either
again sooner mostly adds load, and the first retries (2s, 4s, 8s) already fit
a 30s drain. What the drain does shorten is the wait that was under way when
it started: every root waiting for a retry is due at once, since the wait
began before anyone knew the service would stop. A root whose next try is due
after the deadline doesn't keep the drain waiting, and isn't started: the
drain ends, and logs it as left (`retrying`). A try due by the deadline is
waited for, and started if the pass that finds it due begins before the
deadline: the drain checks the deadline on the export path's clock, the
one retries are due on, before every pass.

**Order.** The drain exports in the scheduler's order: by cache ID, then
entry number, in batches of up to 1,000 roots, all started at once. It
doesn't sort by size: an entry's size is known only once the engine answers
`export`, and what a drain spends its time on is the layers S3 lacks, not
how large the entries are. If the deadline turns out to cut exports off, the
better fix is to skip exports whose missing bytes can't finish in time, not
to reorder them.

**An engine that goes away mid-drain.** Its exports are interrupted, as on
any disconnect, and it can't reconnect to a draining service: the drain
doesn't wait for it, and logs its roots as left (`connected=false`). Known
gap, pinned by `TestDrainEngineDisconnectDoesNotHang`
([#70](https://github.com/zerobull-consulting/dagger/issues/70)).

**An explicit export or import admitted just before the drain.** The hooks
are admitted under the drain's lock, so each one is either admitted before
the drain starts, and waited for, or refused with 503. The drain waits for
those admitted, up to its deadline, as for the exports it runs; it doesn't
cancel them. An explicit export admitted while it still waited for its
cache's export slot runs once it has it. While the drain waits, `GET
/debug/drain` counts them in `explicit`. Roots such an export stores while
draining are counted in the report's `stored`. One still running when the
drain ends (at its deadline, or canceled) is counted in the report's
`explicit`, logged as `explicitLeft`, and runs on until it ends or the stop
that follows cuts it off (an export cut off publishes nothing); what it
stores after the drain ended isn't in the report. Pinned by
`TestDrainWaitsForExplicitExport`, `TestDrainWaitsForExplicitImport`,
`TestDrainRunsExplicitExportWaitingForSlot`, the
`TestDrainEndsAtDeadlineWith…` tests and
`TestReadOnlyDrainWaitsForExplicitImport`
([#72](https://github.com/zerobull-consulting/dagger/issues/72), fixed).

**What a crash still loses.** The drain covers a clean stop. A crash, a
SIGKILL (including a process manager's, past its stop timeout), or
`-drain-timeout 0` still loses the exports pending or in flight
([#45](https://github.com/zerobull-consulting/dagger/issues/45)): the tracker
keeps them only in memory. What was stored is never lost, and blobs uploaded
by an export cut off are named by nothing, and GC deletes them.

**A service that doesn't export** (no `CACHESERVICE_S3_BUCKET`) has nothing
to drain: it refuses new engines, and stops at once
(`TestDrainExportOff`), but for an explicit import admitted before the
drain, which it waits for, up to its deadline.

Tests: `TestDrain*`, `TestDrainedRestartStoresPendingExport`,
`TestCloseEnginesBoundedAndOffTheLock` and `TestWaitWaitsForGCRunAskedFor` in
[`server`](server) (fake engine), `TestMinIODrain` (MinIO),
`TestMinIOBinary*` in this directory (the built command, sent SIGTERM), and
the real-engine `drain` scenario (see
[The scenarios](#the-scenarios)).

## Metrics

[`metrics`](metrics/) measures the storage costs that decide when the index
needs another layout
([#46](https://github.com/zerobull-consulting/dagger/issues/46)). The
service serves them in the Prometheus format at `GET /metrics`, on the debug
address and, with `-metrics-listen`, alone on another. The export and import
paths record into them through their metadata store. All are prefixed
`cacheservice_`.

| Metric | Labels | What it shows |
| --- | --- | --- |
| `index_size_bytes`, `index_entries`, `index_roots`, `index_caches` | `scope` | The index's size and contents, as last read or written |
| `index_reads_total`, `index_read_bytes_total`, `index_read_duration_seconds` | `scope`, `caller` | Whole-index reads, including each attempt of an update |
| `index_updates_total` | `scope`, `caller`, `outcome` | Index updates: `ok`, `contention` (gave up with `ErrContention`), `aborted` (the change refused) or `error` |
| `index_update_attempts`, `index_update_duration_seconds` | `scope`, `caller` | Attempts and time per update, across backoffs |
| `index_conflicts_total`, `index_write_bytes_total` | `scope`, `caller` | Conditional writes lost to another writer, and bytes written, lost writes included |
| `bundle_writes_total` | `scope`, `outcome` | Bundles stored: `written`, or `existing` (already stored) |
| `bundle_write_size_bytes` | `scope` | Size of each bundle stored |
| `bundle_reads_total`, `bundle_read_bytes_total` | `scope` | Bundles read from the store |
| `resolution_writes_total`, `resolution_write_size_bytes` | `scope`, `outcome` (writes) | Bundle resolutions stored, one per recorded export plus one per lost index write that changed the numbers: `written`, or `existing` (see [Imported entries](#imported-entries)) |
| `resolution_reads_total`, `resolution_read_bytes_total` | `scope` | Resolutions read from the store |
| `import_bundle_cache_total` | `result` | Bundles a merge needed: `hit` (in memory) or `miss` |
| `import_resolution_cache_total` | `result` | Resolutions a merge needed: `hit` (in memory) or `miss` |
| `import_syncs_total` | `scope`, `outcome` | Import syncs of one engine |
| `import_sync_duration_seconds` | | Time per import sync |
| `export_batches_total` | `scope`, `outcome` | Export batches: `ok`, `error` or `interrupted` |
| `export_batch_duration_seconds`, `export_batch_roots` | | Time and roots per export batch |
| `export_roots_total` | `result` | Roots exported: `stored`, `gone`, `busy`, `refused` or `failed` |
| `export_blobs_total` | `result` | Layer blobs: `uploaded` (and verified), `present` (the store had them), or `rejected` (an upload that failed verification, counted per try) |
| `export_bundles_total` | | Bundles published |
| `export_roots_waiting` | `scope` | Roots due for export and waiting, as the scheduler last saw them: for their engine cache's export in flight to end, or for the next batch |
| `auth_rejected_total` | `endpoint` | Requests refused for a missing or unknown token: `engine` (the WebSocket), `otlp`, `flush` (see [Flushing one engine cache](#flushing-one-engine-cache)), or `debug` (the debug API; see [The debug API](#the-debug-api)) |
| `connections_revoked_total` | `scope` | Connections closed with 4001 because their token left the namespace |
| `engine_events_ignored_total` | `scope`, `kind` | Engine events dropped because they came with a token other than the engine's |
| `engine_ids_refused_total` | `source` | What was refused because its `dagger.io/engine.cache` isn't `<cacheID>/<generation>` with a valid cache ID, or its cache ID or session isn't 1 to 128 letters, digits, `.`, `_` or `-` (see [OTLP limits](#otlp-limits)), by source: `span` (dropped), `event` (dropped), `wiped_cache` (an engine start's wiped cache, ignored; the start is taken) or `hello` (refused) |
| `blob_promotions_total` | `scope`, `outcome` | Uploaded blobs verified and promoted: `promoted`, `existing` (already stored, or another export's copy won), `mismatch` (content not the digest's, or not the declared size), `missing` (reported uploaded, nothing staged) or `error` (the store failed) |
| `blob_hashed_bytes_total` | `scope`, `by` | Bytes hashed to verify uploads: `store` (as it copied them) or `service` (read through the service) |
| `blob_promotion_duration_seconds` | `scope` | Time to verify and promote one blob, slot wait included: it grows with the blob's size (see [Verification and promotion](#verification-and-promotion)) |
| `gc_stored_bytes`, `gc_stored_objects` | `scope`, `kind` | What GC's last listing of the namespace found, by kind: `blob`, `bundle`, `resolution`, `index`, `staging` or `other` |
| `gc_deleted_objects_total`, `gc_deleted_bytes_total` | `scope`, `kind` | What GC deleted: `blob`, `bundle`, `resolution`, `staging`, or `upload` (an incomplete multipart upload aborted; 0 bytes) |
| `gc_runs_total` | `scope`, `outcome` | GC runs of a namespace: `ok`, `error` or `dry_run` |
| `gc_run_duration_seconds` | `scope` | Time of one namespace's GC run |
| `gc_entries_expired_total` | `scope`, `reason` | Index entries GC dropped: `expired` (their own expiry, or the max age) or `size` (the max size) |
| `gc_staging_rule_missing` | `scope` | 1 when no lifecycle rule expires the namespace's staging prefix (GC expires it itself), 0 when one does; absent when the rules can't be read |
| `gc_unexpected_keys` | `scope` | Keys and multipart uploads of no known shape GC's last run found in the namespace, or for `scope="_root"` outside the namespaces (a lower bound there: one listing page per unexpected prefix, and once 10 keys are known, each further prefix counts once, unlisted: `TestUnexpectedPrefixesSampledBounded`). Above 0 is an ownership violation: GC changed nothing in that namespace, or, for `_root`, anywhere (see [Bucket ownership](#bucket-ownership)) |
| `gc_refused_total` | `reason` | GC runs that did nothing at all: `no_marker`, `other_layout`, `marker_unreadable`, `root_violation` (keys of no known shape outside the namespaces), `uploads_unlisted` (the multipart upload listing failed) |
| `export_collected_total` | `scope` | Export tries refused because GC deleted, or was deleting, an object they named; each is tried again |
| `export_enabled` | | 1 when the export path is on, 0 when it is off: a read-only instance (`-export=false`), or no bucket |
| `draining` | | 1 once the service started draining (SIGTERM), 0 before |
| `drain_duration_seconds` | `outcome` | Time of the drain: `complete` (nothing left it could export), `deadline` (`-drain-timeout` passed) or `canceled` (a second signal) |
| `drain_stored_roots_total` | | Roots stored by exports that ended while draining |
| `drain_unexported_roots_total` | | Roots left unexported when the drain ended: pending, in flight, waiting for a retry, or of an engine that went away |
| `engine_version_mismatch_total` | `outcome` | Engine hellos whose version isn't an accept line (see [Engine versions](#engine-versions)): `refused` (closed before the welcome, status 4002) or `allowed` (welcomed by `-allow-engine-version-mismatch`). The version is in the log, not a label |
| `flush_total` | `outcome` | Flushes of one engine cache: `complete`, `timeout`, `not_connected` or `canceled` (see [Flushing one engine cache](#flushing-one-engine-cache)) |
| `flush_duration_seconds` | | Time of each flush |
| `flush_unexported_roots_total` | | Roots a flush left unexported when it ended: dirty, in flight, or waiting for a retry |
| `drain_refused_total` | `kind` | New work refused while draining: `connection` (an engine WebSocket) or `span` (a span or session end of a session the service doesn't track) |
| `otlp_refused_total` | `route`, `reason` | OTLP requests refused for their body (see [OTLP limits](#otlp-limits)), by route (`traces`, `logs`, `metrics`) and reason: `too_large` (413), `busy` (503, the decode budget full), `timeout` (408) or `too_deep` (400, nested past the limit). Nothing of them is recorded |
| `otlp_request_bytes` | `route`, `form` | Size of each OTLP request taken: `received` (as sent, gzipped or not) or `decoded`. Compare with the routes' limits |

`scope` is the namespace (see [Auth and namespaces](#auth-and-namespaces)).
The `gc_stored_*`, `gc_staging_rule_missing` and `gc_unexpected_keys` series
of a namespace gone from the bucket are removed at the next run
(`TestGaugesOfGoneNamespaceRemoved`). No label comes from an engine-supplied
or bucket-supplied string unchecked: `scope` is a validated namespace from
the tokens file or `-scope`, or, for the `gc_` metrics, a valid namespace
name GC listed under `ns/`, or `_root` (which no namespace can be: a
namespace starts with a letter or digit); unexpected keys are counted, never
used as labels; the others are the service's constants (an OTLP `route` is the route that took the request, never its
path or headers).

`caller` is `export` or `import` for the path that read or updated the index,
whether its scheduler, its sync loop or the debug API ran it, `gc` for
garbage collection, and `other` for
the rest (a wiped cache's imports leaving the index). Debug-triggered exports
and imports count in the export and import metrics too. `bundle_reads_total`
counts the read `PutBundle` makes when the bundle is already stored, and
`resolution_reads_total` the same for resolutions. The Go runtime and process collectors are served too.

What to watch:

- **Contention:** `index_conflicts_total` against `index_updates_total`, and
  the `index_update_attempts` distribution. Any `contention` outcome means an
  update was given up.
- **Index cost:** `index_size_bytes` against `index_update_duration_seconds`
  and `index_read_duration_seconds`. Every update rewrites the whole index.
- **Bytes per import sync:** `rate(index_read_bytes_total{caller="import"})`
  over `rate(import_syncs_total)`.
- **Bundle cache:** `import_bundle_cache_total{result="hit"}` against `miss`.
- **Bad uploads:** any `blob_promotions_total{outcome="mismatch"}` is an
  engine that uploaded content that isn't the blob it named (a bug, or a
  compromised engine), in that namespace; the export logs its cache ID and
  the digest. `missing` is one that reported an upload it didn't make.
- **Storage:** `gc_stored_bytes` per namespace against its `max-bytes`;
  `gc_deleted_bytes_total` by kind; any `gc_staging_rule_missing` of 1 is a
  namespace whose staging rule is missing; `gc_runs_total{outcome="error"}`
  is a run that failed somewhere (the log says where).
- **Ownership:** any `gc_unexpected_keys` above 0, or any
  `gc_refused_total`, is GC stopped: something else wrote under the
  service's prefix, or the marker is gone. The log lists up to 10 keys.
  Alert on `gc_refused_total` and on `gc_unexpected_keys{scope="_root"}`
  too, not only per namespace: a run that stops for the whole prefix
  leaves the namespaces' series as the last run left them. A namespace
  skipped for a violation still updates its `gc_stored_*`
  (`TestSkippedNamespaceGaugesCurrent`).
- A rising
  `export_collected_total` means exports keep naming what GC deletes: a
  grace period too short for the exports' duration.
- **OTLP limits:** any `otlp_refused_total{reason="too_large"}` from a
  real engine lost telemetry, and may have cost a session's exports (see
  [OTLP limits](#otlp-limits)); `otlp_request_bytes` shows how close real
  requests come to the limits. `busy` is a budget that filled: a flood, or
  many large requests at once.
- **Promotion cost:** `blob_promotion_duration_seconds` against the blobs'
  sizes, and `blob_hashed_bytes_total{by="service"}`, which is read through
  the service (blobs over 5 GiB, or a store that doesn't hash).

## Image

CI publishes the service as
**`ghcr.io/zerobull-consulting/dagger-remote-cache`**
([zerobull-consulting/dagger#117](https://github.com/zerobull-consulting/dagger/issues/117),
D4 and D6), `linux/amd64` only, from
[`cacheservice-publish.yml`](../../.github/workflows/cacheservice-publish.yml).
The name doesn't depend on this repository, so a later extraction keeps the
pull paths.

- **`:<engine version>`**, such as `:v1.0.0-beta.25`: a **floating** tag,
  one per `accept` line, on the newest verified build that accepts that
  engine. Pull this tag for the engine you run. A version that a newer
  build no longer accepts keeps its tag on the last build that did.
- **`:<commit SHA>`** (the full SHA), or the image's digest: the immutable
  pin, one per verified push to `feature/remote-cache`.
- **No `:latest`**: an image is only right for the engines it accepts.

**What a release is.** Every push to `feature/remote-cache` is a candidate:

1. its image is built **stamped as the manifest's `base`**
   (`--build-arg RELEASE`, from [`compat/engines.txt`](compat/engines.txt)'s
   `base` line, read with `hack/remote-cache/engines.sh base`) and its
   commit (`REVISION`), and smoke-tested, its identity included: it must
   report that base, not `dev`, and that revision;
2. the e2e and the sidecar demo run against **every `accept` line**, a
   matrix of [`remote-cache-tests.yml`](../../.github/workflows/remote-cache-tests.yml)'s
   jobs, the demo with this very image (pull requests run only the first
   line);
3. only once the whole matrix is green, the image is pushed as
   `:<commit SHA>`;
4. then each `:<engine version>` tag moves to the **newest verified
   build**: the newest commit of the branch (first parents, from its head)
   whose `:<commit SHA>` is pushed, one tag per `accept` line of that
   commit's own manifest
   ([`hack/remote-cache/release-tags.sh`](../../hack/remote-cache/release-tags.sh)).
   The job runs one at a time across pushes and always looks from the
   head, so the tags never move backwards, nor onto an unverified build,
   and a red head leaves them on the newest green commit before it. A
   copy that fails is retried; one that still fails leaves only its own tag
   where it was (each tag is correct on its own), fails the job, and is
   moved by re-running it.

A `workflow_dispatch` of the workflow on another branch is a dry run: it
builds, smoke-tests and runs the matrix, and pushes nothing.

**The old name**, `ghcr.io/zerobull-consulting/cacheservice`, is still
published for a transition: `:<commit SHA>` and the moving
`:feature-remote-cache` get the same digests as the new name's commit tag
and head. Its tags are never deleted, so pinned consumers keep working;
move to `dagger-remote-cache` (or to the sidecar action's `version` input).

Pull requests that touch the service build the image as a release is built,
stamped, and smoke-test it, its identity included
([`cacheservice-image.yml`](../../.github/workflows/cacheservice-image.yml),
[`hack/remote-cache/image-smoke.sh`](../../hack/remote-cache/image-smoke.sh)),
and push nothing. To build it from a fresh clone, at the repository root:

```bash
docker build -f cmd/cacheservice/Dockerfile \
  --build-arg RELEASE="$(hack/remote-cache/engines.sh base)" -t dagger-remote-cache .
```

The context is the repository root, since the module is the whole monorepo;
[`Dockerfile.dockerignore`](Dockerfile.dockerignore) keeps it to the
packages the service builds from (BuildKit is needed for that file, and is
Docker's default builder).

The build records the image's revision (`--build-arg REVISION`, which CI
sets to the commit SHA) in the binary, as the `serviceVersion` of the
ownership marker an instance of it creates (see
[Bucket ownership](#bucket-ownership)) and the `revision` of its identity.
`--build-arg RELEASE=<tag>` stamps it as a build of that upstream release,
which must be `compat/engines.txt`'s `base`, or it refuses to start; CI
stamps every image it builds that way. Without it, as in a plain local
build, the service reports its base as `dev` (see
[Engine versions](#engine-versions)). The stamp is the manifest's claim:
`feature/remote-cache` merges upstream main, and the base becomes exact
once the service is its own module, built against that release
(zerobull-consulting/dagger#117, D7). An image of another layout refuses
a prefix this one created, and the other way round: moving between them is
[Changing layouts](#changing-layouts).

It is the static binary on `gcr.io/distroless/static-debian12:nonroot`: CA
certificates and tzdata, no shell, and it runs as UID 65532. The binary is
the entrypoint, so it is PID 1 and gets `docker stop`'s SIGTERM itself (see
[Stopping and draining](#stopping-and-draining) for the stop timeout). It
takes the same flags and `CACHESERVICE_S3_*` variables as the binary, with
the same defaults:

- `-listen` `:8090`: the engine WebSocket and the OTLP receivers. The image
  exposes this port. Its OTLP bodies are bounded (see
  [OTLP limits](#otlp-limits)).
- `-debug-listen` `127.0.0.1:8091`: the container's own loopback, so the
  debug API can't be reached from outside the container, even with `-p`.
  Publishing it takes `-debug-listen :8091`, which isn't loopback, so the
  service then requires `-debug-token-file` (see
  [The debug API](#the-debug-api)); mount the file read-only, readable by
  UID 65532, and bind the `-p` to the host's loopback too:
  `-v "$PWD/debug-token:/run/secrets/debug-token:ro" -p 127.0.0.1:8091:8091
  ... -debug-listen :8091 -debug-token-file /run/secrets/debug-token`.
- `-metrics-listen`: off.

There is no `HEALTHCHECK`: the image has no shell, and the engine reconnects
with backoff. To try it, unauthenticated and with no store (export path off):

```bash
docker run --rm -p 127.0.0.1:8090:8090 ghcr.io/zerobull-consulting/dagger-remote-cache:v1.0.0-beta.25
```

The engine that connects to it is a stock Dagger release whose version is
an `accept` line of the image's manifest (see
[Engine versions](#engine-versions)); the service refuses any other. With
the `:<engine version>` tag, that is the engine of that version. The fork
no longer publishes an engine image of its own.

With a bucket, a pull-request job's instance adds `-export=false` and a
read-only key (see [Read-only instances](#read-only-instances)). Running it
as a sidecar next to an engine in CI is the next section.

## Running it as a sidecar

On an ephemeral CI runner, each job runs the service next to its own engine,
unauthenticated, on the runner's private Docker network, talking to the
bucket directly
([zerobull-consulting/dagger#75](https://github.com/zerobull-consulting/dagger/issues/75)).
There's no endpoint to host. Concurrent jobs are replicas of each other,
which the service is safe for.

On GitHub Actions, use the composite action, which does everything below
([`.github/actions/remote-cache`](../../.github/actions/remote-cache/README.md)):

```yaml
- uses: zerobull-consulting/dagger/.github/actions/remote-cache/start@<commit SHA>
  with:
    role: writer                    # read-only in pull-request jobs
    version: v1.0.0-beta.25         # the engine and the CLI; selects the service image too
    s3-endpoint: ${{ vars.CACHE_S3_ENDPOINT }}
    s3-region: ${{ vars.CACHE_S3_REGION }}
    s3-bucket: ${{ vars.CACHE_S3_BUCKET }}
    s3-prefix: cache
    s3-access-key-id: ${{ secrets.CACHE_S3_ACCESS_KEY_ID }}
    s3-secret-access-key: ${{ secrets.CACHE_S3_SECRET_ACCESS_KEY }}
- run: dagger call build                # any dagger command: start puts a wrapper of the CLI first on PATH
# No stop step: start stops the sidecar, draining its exports, at the end of the job.
# The job's timeout-minutes bounds that stop: set it above the work plus the
# drain timeout (10m) plus 2 minutes. remote-cache/stop stops earlier.
```

Pin the action to a commit of `feature/remote-cache`. Its `version` is an
`accept` line of that commit's [`compat/engines.txt`](compat/engines.txt)
(default: the first, CI's pin), and selects the CLI (installed with
`dagger/dagger-for-github`; the start fails unless it is the engine's
version), the stock engine of that version, pinned by the manifest's digest,
and `dagger-remote-cache:<version>`, the newest verified service that
accepts it (see [Image](#image)). The two images are pulled at the same
time. The start is ready once the service has welcomed
the engine, and fails on a refusal. `service-image` overrides the service
image, to pin one build or test your own.
The [demo](../../hack/remote-cache/demo/README.md) is a small project that
shows what the sidecar carries and what it doesn't; CI runs it through the
action on every pull request (the `sidecar demo` job), so this section, the
action and the demo stay in agreement. Locally, `hack/remote-cache/dev.sh
demo` runs it against MinIO.

Elsewhere, these are the steps the action runs
([`sidecar.sh`](../../.github/actions/remote-cache/sidecar.sh) is the
action's script, and runs outside GitHub too).

**1. The network.** One Docker network for the service and the engine.
The service's port is published on the network's gateway, a host address
only this runner reaches, so the engine (in a container) and the CLI (on the
runner) reach it at the same URL. They must: the engine sends the session's
spans to the CLI's `DAGGER_CLOUD_URL` (see
[ENTRY_DISCOVERY.md](ENTRY_DISCOVERY.md#who-publishes-them-and-where)).

```bash
docker network create remote-cache-net
gw="$(docker network inspect -f '{{(index .IPAM.Config 0).Gateway}}' remote-cache-net)"
```

**2. The service**, by the tag of the engine version it is to run with
(or by commit SHA, to pin one build), with the bucket's `CACHESERVICE_S3_*` variables
passed by name (`-e NAME`, so their values aren't on a command line):

```bash
export CACHESERVICE_S3_ENDPOINT=... CACHESERVICE_S3_REGION=... CACHESERVICE_S3_BUCKET=...
export CACHESERVICE_S3_PREFIX=cache CACHESERVICE_S3_ACCESS_KEY_ID=... CACHESERVICE_S3_SECRET_ACCESS_KEY=...
export CACHESERVICE_S3_PATH_STYLE=true   # MinIO; leave it unset for Spaces or AWS
docker run -d --name remote-cache-service --network remote-cache-net -p "$gw::8090" \
  -e CACHESERVICE_S3_ENDPOINT -e CACHESERVICE_S3_REGION -e CACHESERVICE_S3_BUCKET \
  -e CACHESERVICE_S3_PREFIX -e CACHESERVICE_S3_ACCESS_KEY_ID -e CACHESERVICE_S3_SECRET_ACCESS_KEY \
  -e CACHESERVICE_S3_PATH_STYLE \
  ghcr.io/zerobull-consulting/dagger-remote-cache:v1.0.0-beta.25 \
  -scope default -drain-timeout 10m -drain-linger 15s -gc-interval=0
url="http://$(docker port remote-cache-service 8090/tcp | head -n1)"
```

- **No `-tokens-file`**: the service is unauthenticated, and every engine is
  in the `-scope` namespace. That is fine on a runner's private network, and
  only there: anything that reaches the port can read and add to the
  namespace. Its OTLP bodies are bounded either way (see
  [OTLP limits](#otlp-limits)); `cacheservice_otlp_refused_total` should stay
  at 0 for a sidecar's engines.
- **`-gc-interval=0`** on writers: scheduled GC belongs to a job of its own
  ([zerobull-consulting/dagger#61](https://github.com/zerobull-consulting/dagger/issues/61)).
- **`-drain-timeout 10m`**: the stop's chance to store what the job built
  (see step 4); **`-drain-linger 15s`** (its default) lets the engine's stop
  deliver its last event after the drain.
- **The debug API**, when wanted: `-debug-listen :8091` with
  `-p 127.0.0.1:8091:8091`, on the runner's loopback only, and
  `-debug-token-file` with a token generated for the job and mounted
  read-only: `:8091` isn't loopback inside the container, so the service
  refuses to start without one (see [The debug API](#the-debug-api)). The
  composite action does this for its `debug-port` input.
- It is up once it logs `msg=listening addr=:8090`: it checked the prefix's
  ownership before that, and exits 1 (a store error, such as a prefix it
  doesn't own) or 2 (a flag error) instead. Fail the job if it exits, with
  its log.

**3. The engine and the CLI.** The stock Dagger engine release of that
version, pinned by the digest on its `accept` line of the service's
manifest (see [Engine versions](#engine-versions)), with
the settings `cmd/cacheservice/e2e` starts its engines with:

```bash
echo '{"gc": {"enabled": false}, "registries": {"docker.io": {"mirrors": ["mirror.gcr.io"]}}}' >engine.json
docker run -d --privileged --name remote-cache-engine --network remote-cache-net \
  -v remote-cache-engine-state:/var/lib/dagger -v "$PWD/engine.json:/etc/dagger/engine.json:ro" \
  -e _EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL="$url" \
  -e DAGGER_CLOUD_URL="$url" \
  -e DAGGER_CLOUD_TOKEN=sidecar-engine \
  registry.dagger.io/engine:v1.0.0-beta.25@sha256:39abecfe0eba4c91035dcfc9f72c8a3963ae60481326082b48d78f2230202834 --debug
```

- `DAGGER_CLOUD_TOKEN` is a placeholder (any string but `oidc`): without a
  token the remote cache client stays off. `DAGGER_CLOUD_URL` sends the
  engine's cache events to the service too.
- **The engine's GC is off**: its state ends with the job, and with GC on it
  can prune entries right after their session, before the service exports
  them. That happened in this work's walk-through, on a host whose disk
  looked full: the cacheable step's entry was pruned, and the read-only
  phase missed it. The job's disk bounds the engine instead.
- The engine is ready once the service logs `msg="engine welcomed"`. On
  `msg="engine refused: its version is not accepted"`, the service and the
  engine disagree: the engine would build without the cache. `msg="engine
  connected"` comes before either, and says nothing about the version.

Then the CLI, which must be the engine's version (the engine is pinned by
digest, the CLI only by its release tag; the action installs it with
`dagger/dagger-for-github` and fails the start when `dagger
version` says another). Here it is the engine image's own, which matches,
with the Cloud settings that make the engine publish the session's spans to
the service, and no inherited trace:

```bash
docker cp remote-cache-engine:/usr/local/bin/dagger ./dagger
# As cmd/cacheservice/e2e runs its CLI: no inherited trace, and no OTEL_* or
# DAGGER_* settings of the caller's.
unset $(compgen -e | grep -E '^(TRACEPARENT|TRACESTATE|OTEL_|DAGGER_|_EXPERIMENTAL_DAGGER_)')
env _EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://remote-cache-engine \
  DAGGER_CLOUD_URL="$url" DAGGER_CLOUD_TOKEN=sidecar-cli \
  ./dagger call build
```

**A CLI without `DAGGER_CLOUD_URL` and `DAGGER_CLOUD_TOKEN`, or one that
inherits an unsampled `TRACEPARENT`, exports nothing, silently.** The
service learns entries only from session spans (see
[ENTRY_DISCOVERY.md](ENTRY_DISCOVERY.md)). Some CI runners and sandboxes
export `TRACEPARENT=00-…-00` to every process; the CLI continues that
unsampled trace, records no span, and the service never hears of an entry
(see [Real-engine run](#real-engine-run)). The action puts a `dagger` first
on `PATH` that unsets `TRACEPARENT`, `TRACESTATE`, `OTEL_*`, `DAGGER_*` and
`_EXPERIMENTAL_DAGGER_*`, then sets the three variables, whatever a later step's environment holds. Any other CLI
needs the same.

**4. The stop**, at the end of the job, whatever happened before it
(`if: always()`): SIGTERM the service, which drains; once its drain is done,
stop the engine; then wait for the service to exit. The drain's exports run
over the engine's connection, and the engine uploads the layers itself, so
an engine stopped before the drain is done takes them with it. After the
drain, the service [lingers](#the-linger): it still answers the engine's
events, so the engine's stop delivers its last one (`engine.stop`) and
takes seconds (about 7s, measured), and the service exits once that event
arrived.

```bash
since="$(($(date +%s) - 1))"
timeout 30 docker kill -s TERM remote-cache-service
# Wait for the drain to be done, or the service to have exited, for at most
# -drain-timeout plus 5s. Every docker call is bounded by what is left of
# that (at most 10s, at least 1s), and one that fails or times out prints
# nothing: only docker's "false" means the service has exited, so a failing
# docker never stops the engine mid-drain.
end=$((SECONDS + 605))
left() { t=$((end - SECONDS)); [ "$t" -gt 10 ] && t=10; [ "$t" -lt 1 ] && t=1; echo "$t"; }
while [ "$SECONDS" -lt "$end" ]; do
  log="$(timeout "$(left)" docker logs --since "$since" remote-cache-service 2>&1 || true)"
  grep -q 'msg="drain: done"' <<<"$log" && break
  [ "$(timeout "$(left)" docker inspect -f '{{.State.Running}}' remote-cache-service 2>/dev/null)" = false ] && break
  sleep 0.5
done
timeout 90 docker stop --time 30 remote-cache-engine
# It exits once engine.stop arrived, or 15s after its drain. docker wait
# prints the exit code; nothing means it hadn't exited (or docker failed),
# which is never a clean stop.
code="$(timeout 30 docker wait remote-cache-service 2>/dev/null || true)"
echo "the service's exit code: ${code:-unknown (still running?)}"
[ "$code" = 0 ]
```

Don't shorten the engine's `docker stop --time`: an engine killed before its
"close dagql cache" phase ended leaves its store unclean, which wipes the
engine's cache at its next start on a host that keeps the state volume.
Don't `docker stop` the service again while it lingers either: its SIGTERM
ends the linger at once, and the engine's last event goes undelivered.

- **The bound:** the service's whole stop, from SIGTERM to exit, takes at
  most `-drain-timeout` plus `-drain-linger` (15s) plus 5s (see
  [Stopping and draining](#stopping-and-draining)). The waits above, and the
  CI step's own timeout, must be above that, or the service is cut off
  mid-drain. With `10m`, the action waits up to 625s for the service in all.
  The engine's stop (up to 30s, cut off at 90s) comes after the drain's wait,
  so the step needs `-drain-timeout` plus 2 minutes, 12 for `10m`: the
  action's stop bounds every wait, and the caller sets the step's
  `timeout-minutes`.
- **Exit codes:** 0 for a clean stop; 1 when the bound cut work off, when
  something else left the stop unclean, when a signal arrived during the
  shutdown, or when an HTTP server failed (including one that failed as the
  stop ended: the stop waits, within its bound, for the servers to return
  before it reads their errors); 137 when it was killed (by `docker stop`
  past its `--time`, or out of memory). A shutdown that ends in error is
  logged as `stopped uncleanly`, with the reason in `error=` and what was
  running; the message says `the shutdown's bound passed` only when it did,
  not for an error that comes sooner, such as engine connections still
  closing (about 2.5s in). Two exits log differently: a signal during the
  shutdown (`signal during the shutdown: exiting at once`) and the outer
  backstop for a step that overran its own bound (`stopped: the shutdown
  overran its bound`). The action's stop fails the step on anything but 0, and
  on a service that exited before the stop, with the service's log.
- The drain logs what it stored (`drain: stored`) and its outcome
  (`drain: done outcome=complete`). Most exports are stored before it:
  each entry within about `-export-settle` of its last span, or at its
  session's end.

**Pull-request jobs:** the same, with `-export=false` instead of
`-gc-interval=0` (GC is off anyway, and an explicit nonzero `-gc-interval`
is refused), and a read-only key (see
[A read-only key](#a-read-only-key)): `s3:GetObject` and `s3:ListBucket` on
the prefix. A job that forgets `-export=false` with that key fails at
startup, exit 1, rather than running as a writer. Its drain has nothing to
flush, so the stop is quick. The service merges what is stored as the
engine connects, within a second or two; there's no signal for when that
first merge is done, so a build that starts at once can miss what it hasn't
merged yet
([zerobull-consulting/dagger#103](https://github.com/zerobull-consulting/dagger/issues/103)).

- **DigitalOcean Spaces' `Read` key** was run against the service on
  2026-11-07, after this walk-through (which had no Spaces key); CI's Spaces
  jobs still use a read/write one. What it showed is in
  [A read-only key](#a-read-only-key).

**The prefix is the service's** (see [Bucket ownership](#bucket-ownership)):
nothing else may write under `CACHESERVICE_S3_PREFIX`, including another
service on a prefix nested inside it, or a console folder placeholder. The
first writer on an empty prefix claims it (`<prefix>/cacheservice.json`); a
read-only instance on an empty one starts, logs `no store exists under the
prefix yet`, and imports nothing. Every instance refuses, with exit 1, a
prefix of another layout or one holding keys and no marker. Across a layout
change, wipe the prefix or use a new one (see
[Changing layouts](#changing-layouts)).

**What the remote cache doesn't carry: cache volumes.** It stores cache
entries, the layers of a step's result; the contents of cache volumes
(`withMountedCache`) stay on the engine that wrote them. So a step that
relies on another step's cache-volume state reruns that work on a fresh
runner, even when the step that filled the volume is a cache hit: the hit
means it never ran there, and the volume is empty. The sample
implementation's Android build measured it
([zerobull-consulting/dagger#1](https://github.com/zerobull-consulting/dagger/issues/1),
"What the runs measured"): with the native layer from the remote cache, a
JS-only build on a fresh runner took 14.7–21.8 minutes of Gradle time
against 1.9–2.7 on the warm writer, and 60–67% of what the warm state saved
was CMake tasks recompiling with an empty ccache, the rest mostly Gradle
build-cache misses with an empty Gradle user home, both cache volumes.
Unchanged builds were fully served. **What to do instead:** make that state
an output of a cacheable step (a directory in its container, which the
remote cache carries) rather than a cache volume. The demo's cache-volume
step shows the limit, and CI checks that it reruns.

The service's and the engine's logs name the bucket and the prefix, and the
engine's can hold presigned URLs; the action's stop redacts their
credentials and signatures from the logs it keeps, but treat them as
internal.

## Running it against a local engine

For a local S3-compatible store (MinIO), see
[`hack/remote-cache`](../../hack/remote-cache/README.md).

1. Start the service:

   ```bash
   go run ./cmd/cacheservice -v -dump-dir /tmp/cacheservice-dump
   ```

   That is unauthenticated, with any token in the `default` namespace, and
   its debug API is open on `127.0.0.1:8091` (it logs a warning saying so).
   To check tokens, add `-tokens-file tokens.txt`, with a line
   `<token> <namespace>` for the engine's token, and for the CLI's if it
   sends spans (see [Auth and namespaces](#auth-and-namespaces)). The debug
   API then needs a token of its own: also add
   `-debug-token-file debug-token`, after
   `openssl rand -hex 32 > debug-token`.

2. Build and start a dev engine (see `hack/dev`), or run a stock
   release (see [Engine](../../hack/remote-cache/README.md#engine)). Then
   restart its container with these variables set:

   ```bash
   _EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL=http://<host>:8090
   DAGGER_CLOUD_TOKEN=spike-token
   DAGGER_CLOUD_URL=http://<host>:8090
   ```

   The remote cache connection turns on only when both the URL and
   `DAGGER_CLOUD_TOKEN` are set. `DAGGER_CLOUD_TOKEN=oidc` keeps it off. Any
   other string works as the token, unless the service has a tokens file.

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

   With `-debug-token-file`, add the token to each request:
   `curl -s -H @<(printf 'Authorization: Bearer %s\n' "$(cat debug-token)") localhost:8091/debug/engines`.

   Take entry numbers from the `result` fields of the `cache span` log lines.
   Cache spans arrive only when the CLI itself has Cloud telemetry configured,
   through `DAGGER_CLOUD_TOKEN` or `dagger login`, and has `DAGGER_CLOUD_URL`
   pointing at the service. They don't arrive either when the CLI inherits an
   unsampled `TRACEPARENT` (see [Real-engine run](#real-engine-run)).

## Real-engine run

To run this and the store suites against a real bucket, safely, and for what
each store did, see
[Testing against a real bucket](../../hack/remote-cache/README.md#testing-against-a-real-bucket).

`./hack/remote-cache/dev.sh e2e` runs both paths with real engines, a stock
release by default, or built from this repo with `E2E_BUILD_ENGINE=1`
([#29](https://github.com/zerobull-consulting/dagger/issues/29),
[zerobull-consulting/dagger#112](https://github.com/zerobull-consulting/dagger/issues/112)): engine A
builds a pipeline and the service exports it to MinIO; engine B, with an
empty cache, runs the same pipeline and gets cache hits whose layers come
from MinIO. Then it runs the [scenarios](#the-scenarios) that check what a
first hit doesn't: a repeated export, renewal after the GETs expired, a
pruned import, and a replaced value; and the [resilience](#resilience)
scenarios, which drop a connection, restart an engine and the service, and
make S3 fail, each mid-export. One command builds and starts
everything from empty and checks the outcome. It is a Go test,
[`e2e`](e2e/), that runs the service in-process, and CI runs it on every
pull request. See
[`hack/remote-cache/README.md`](../../hack/remote-cache/README.md#real-engines-devsh-e2e)
for what it does and its settings.

The pipeline is `container.from(alpine:3.22)`, then two `withExec`s that each
write 4MB of random data and a random stamp to `/stamp`, then
`file("/stamp").contents`. After it, B runs one more exec over the same
container that prints the stamps and the checksums of both data files.

### What it showed

From a run on 2026-11-02, in a cloud container (4 CPUs), with the engines'
binaries copied into `registry.dagger.io/engine:v1.0.0-beta.22`, MinIO and the
service on the same host:

| Run | Engine cache | Wall clock |
| --- | --- | --- |
| A: the pipeline | empty; pulls `alpine` from `mirror.gcr.io`, runs both execs | 2.36s |
| B: the same pipeline | empty, plus the 6 entries the service merged from S3 | 0.98s |
| B: one new exec over the pipeline's container | as above | 0.71s |

The pipeline is small, so the times are mostly session setup; the speedup
says little yet about large builds. An earlier run gave 2.33s and 0.92s for
the two pipeline runs.

- **Export.** A's session ended, and within a second the service had
  exported 4 roots and 6 values in one bundle, with 5 blobs uploaded through
  presigned PUTs, and recorded them in the index.
- **Merge.** The service merged that bundle into B right after the export
  was stored (B was already connected): 4 roots retained, none skipped, no
  merge refused. B's cache then had the entries, each with its `fs` and
  `execMeta` parts offered (presigned GETs), not yet on disk.
- **Cache hit.** On B, both `withExec` spans had outcome `hit`, none
  `executed`, and B returned A's stamps byte for byte. The execs did not run
  again.
- **Layers come only when needed.** The pipeline on B was cached down to its
  result string, so it read no layers at all: MinIO saw no blob GET. The extra
  exec needed the container's filesystem. B's engine then fetched the three
  layer blobs (alpine's and both execs') from MinIO through the merged
  presigned GETs, as ranged `GetObject`s (206), and the data files it read had
  A's checksums. B never pulled `alpine` from a registry.
- **No offers.** Every part was already in the merge, so the service sent
  no `offer`. B read within the GETs' hour, so it sent no `renew`; the
  [`renew` scenario](#the-scenarios) lets them expire.
- Nothing in the service broke, and it logged no warning.

### What the fake engine did not show

- **B re-exported what it imported (fixed).** B's session spans name the
  entries it hit, which are the imported rows, and they are retained. So the
  service exported them again, from B: a new bundle with no blobs to upload,
  whose values were recorded as new entries under B's cache ID, with new
  service numbers. That bundle was then merged back into A, which already had
  every value. The extra exec's bundle carried the imported values in its
  closure too, and recorded them again. After the run, the index held 13
  entries: A's 6, and 7 under B, of which only 2 (the new exec and its
  output) were new. Since
  [#35](https://github.com/zerobull-consulting/dagger/issues/35), the index
  records what each merge left in an engine as its imports (see
  [Imported entries](#imported-entries)): the tracker doesn't export an
  unchanged import, and a bundle that names one, as a root or in its closure,
  reuses the stored entry. `dev.sh e2e` now checks that the index ends with
  A's entries and only the verify exec's new ones, each once, and that
  nothing of A's is merged back into A. A run on 2026-11-02 with the fix
  stored two exports, A's build and B's verify exec (5 values, of which 4
  resolved to A's numbers as B's imports), and ended with 7 entries: A's 6
  and the verify exec's container, which depends on A's last exec by its
  original number.
- **The expired-roots path did not run in the first scenario.** Each merged
  bundle's roots were all planned, so none was sent already expired
  (`ExpiresAtUnix: 1`). The `superseded` scenario runs it: see below.
- **An unsampled `TRACEPARENT` hides every entry.** This container exports
  `TRACEPARENT=00-…-00` to every process. The CLI continued that unsampled
  trace, so neither it nor the engine recorded any span: the engine logged
  "session telemetry published to Cloud" and its session span queue counted 0
  spans, and the service saw `/v1/logs` and `/v1/metrics` but never
  `/v1/traces`. The service never learned of an entry, so it exported
  nothing. `dev.sh e2e` unsets `TRACEPARENT` for the CLI. A client in CI that
  inherits one has the same silent miss as a client with no Cloud
  credential (see [ENTRY_DISCOVERY.md](ENTRY_DISCOVERY.md#gaps-and-options)).

### The scenarios

From a run on 2026-11-02, in the same setup, after the first scenario
([`hack/remote-cache/README.md`](../../hack/remote-cache/README.md#scenarios)
has what each does and checks). `renew` and `prune` run against a replica:
a second service on the same bucket and scope, with GETs that last 30s and an
import poll of 2s, which learns of the main service's exports from the index.

**`reexport`
([#30](https://github.com/zerobull-consulting/dagger/issues/30)).** A's 4
stored roots, exported again with every part (`POST
/debug/engines/{conn}/export`), were stored in 0.03s: no blob uploaded, all 4
present, the index unchanged (the same 7 entries and bundles, next number 8),
and no bundle object written. A second pipeline on A over the same `alpine`
uploaded its own 2 blobs (MinIO saw 2 PUTs); `alpine`'s layer was present, in
its bundle, and not uploaded again.

The first real run of this found a bug (fixed): the repeat stored a **new
bundle**, which moved A's entries to it in the index and was merged again
into B. The engine had added a `buildkit/createdat` annotation to the exec
layers' descriptors, which their first export lacked: a layer the engine has
just compressed for an export has no creation time in its snapshot's metadata
yet. The service now drops the annotation before it stores a bundle (see
[Export path](#export-path)), with regression tests against MemStore and
MinIO (`TestRepeatedExportCreatedAtAnnotationSameBundle`,
`TestMinIOExportBuild`); the fake engine annotates re-exported layers as the
engine does.

**`renew` ([#31](https://github.com/zerobull-consulting/dagger/issues/31)).**
The replica merged A's entries into C and D, both empty. 36s after the merge,
with the GETs 30s old, C ran the verify exec: it sent one `renew` with the
whole chain (3 layers, alpine's and both execs'), the replica checked each
blob (3 `HeadObject`s) and answered with 3 fresh GETs 12ms later, and MinIO
served the 3 ranged GETs (206) 1ms after that. MinIO saw no GET before the
renew, and no 403: the engine checks an address's expiry itself, and renews
an expired one without trying it. C's files had A's checksums, and its query
took 1.19s, against 0.61s for B's verify exec with live GETs.

Then exec2's layer was deleted from MinIO (once C's verify exec was stored:
see below) and D ran the verify exec. D read alpine's and exec1's layers
(206), got a 404 for exec2's, and sent `renew` for the chain; the replica
logged `renew of a missing layer` and answered `unavailable`. D then ran
exec2 again, as for a cache miss: the query succeeded in 1.18s, exec1's
files were A's, and exec2's had a new stamp and checksum. D's GETs had not
expired: the replica had merged C's verify export into D after the expiry,
with fresh GETs for the same layers. So the 404, not the expiry, made it
renew.

Two things it found:

- **Stored bundles carried presigned GETs (fixed).** An engine that had
  entries merged into it holds their parts as offers until it pulls them, and
  exports them, addresses included, in its records' pending offers. B's and
  C's verify exports were stored that way, with the GETs the services had
  handed them, and merged into later engines. On the first run of this
  scenario, D read exec1's layers with the hour-long GETs the main service
  had given B, without renewing them, though its own had expired. The service now
  drops every chain's addresses before storing a bundle, and keeps the offers
  and their renewal key (`TestStoredBundlesHoldNoAddresses`, and its MinIO
  variant).
- **The store repairs a deleted blob.** Deleted before C's verify exec was
  exported, exec2's layer came back: the export's `UploadURLs` found it
  missing, and C, which had it on disk, uploaded it again. The scenario now
  deletes it after C's export is stored. This is the export path working as
  designed, and it means a blob lost from the bucket is restored by the next
  export of an engine that has it.

**`prune` ([#32](https://github.com/zerobull-consulting/dagger/issues/32)).**
Engine C pruned its cache with `{ engine { localCache { prune } } }` (no
arguments: every releasable entry; the harness's engines have GC off). Its
disk usage went from 32MB to 0, its `engine.prune` event named 9 entries, and
the replica's importer kept 3 of the 11 values it had merged into C. Over 5
import polls (2s each) and 3 syncs by hand, nothing was merged or offered to
C, and the importer's state for C didn't change. After `docker restart`
(generation 2), the replica merged the 4 bundles into C again, 11 values.

**`superseded`
([#32](https://github.com/zerobull-consulting/dagger/issues/32),
[#27](https://github.com/zerobull-consulting/dagger/issues/27)).** A Dang
module function with `@cache(ttl: "30s")` returns
`container.from(alpine).withExec(...)` writing a random stamp. On A, its first
value was exported as service number 60; once it expired, the same call
replaced the value in place (same engine entry, 1 replacement, a new layer),
and the export kept number 60 and moved it to a newer bundle. The older
bundle still held the first record, among 5 other roots of the module's own
entries. Engine F, empty, then had the scope's 6 bundles merged, in order of their
lowest root: first the newer one (roots 2 and 60), last the older one (5
planned roots; the importer sent its root for the first record already
expired, and the engine merged 47 of its 50 values, skipping the superseded
root and what only it needed). F's call resolved to
the imported entry, and read the **newer** record's layer from MinIO, not the
first's.

So F ended with the newer record. Two things kept #27's outcome from
showing here. The newer bundle's lowest root is `alpine`'s
`Container.from` (2), which it shares, so it was merged first. And the first
record had expired, so even merged first it would not have been kept. The
engine replaces an expired value in place only when no other entry depends
on it (`replaceResultValueInPlaceLocked`); otherwise it computes it into a
new entry, with a new service number, and nothing is superseded. A
superseded record in another root's closure therefore needs that root to be
gone from A but live in the index, as after a prune, and the scenario doesn't
build that. In the index, an entry's expiry doesn't carry to the
entries that depend on it: a `File.contents` over an expired container stays
a root with no expiry, and the plan leaves it out only because a dependency
expired.

Two more things about the module path:

- A function with an explicit cache TTL is evaluated again on every call
  (`core/sdk/dang/v2/entrypoint.go`), so on F its call span says `executed`;
  its body's `withExec` was the hit, on the imported entry.
- The module's own loading (sources, type definitions) is exported too: the
  first call's bundle held 50 values.

**`namespaces`
([#10](https://github.com/zerobull-consulting/dagger/issues/10)).** Its own
service, with a tokens file, and engines NA and NB on tokens of `team-a` and
`team-b`. NA's pipeline (alpine, one exec) was stored in team A's index; a
sync of NB right after merged nothing, and NB ran the exec itself (0 hits, its
own stamp). Neither index mapped the other team's engine; NA's exec layer was
under `team-a/blobs/` only, and alpine's layer under both namespaces. NC, on
NA's token, had NA's bundle merged and hit NA's exec with NA's stamp. After
team B's token was removed and the file reloaded, NB's engine logged
`received close frame: status = StatusCode(4001) and reason = "token
revoked"`, its reconnection was refused with 401, and the service logged
nothing at error level. It adds about 12s to the run (the whole run took
2m06s with the binaries built).

**`verify` ([#23](https://github.com/zerobull-consulting/dagger/issues/23)).**
From a run on 2026-11-02 (7.9s on top of the run, which took 5m58s with the
build). The S3 proxy rewrote every blob A uploaded to other content of the
same length while A ran a new pipeline: 6 PUTs of 2 blobs (the exec's layer,
1.2 MB, and a small one), over the export's 3 tries. MinIO took each (200),
at its staging key. The service rejected all 6 as mismatches, hashed by the
store (`hashedBy=store`: MinIO's checksum of the part copy); neither blob
reached its digest key, the export failed once, and nothing of A's was
published. Once the rewrite was lifted, the tracker's retry asked for both
blobs again, under a new stage; both were uploaded and promoted, and read
back from MinIO they hash to their digests. The export was stored, S3 was
consistent, no staged upload was left in the scope, and B got the pipeline's
exec as a cache hit with A's output in 350ms. The engine is unchanged: it
PUT to the staging URLs as to any other.

### Resilience

[#12](https://github.com/zerobull-consulting/dagger/issues/12)'s cases, and
which layer proves each. Real engines prove what depends on the engine's own
behavior (chains released when a connection ends, reconnect backoff, entry
numbers across a restart, retries of real S3 clients); fake engines, behind
the engine's own `remotecache.Run`, prove what needs exact control (sizes,
timeouts, a restart with state to compare) or would cost minutes.

| Case | Layer | Tests | Why there |
| --- | --- | --- | --- |
| Connection lost mid-upload | both | e2e `disconnect`; `TestConnectionLostMidUpload`, `TestExportHookInterruptedNotFailed`, engine's `TestExportReleasedWhenTheConnectionDrops` | The real engine shows the release and the reconnect; the fake one checks the chains are released before the retry. |
| Engine restart, new generation | real engines (export side) | e2e `engine-restart`; import side: `TestSyncStaleGeneration`, `TestOfferPendingStaleGeneration`, `TestTrackerGenerationFence`, `TestRecordFencesOlderGenerations` | Whether entry numbers survive a restart is the engine's: only a real one shows the cut export runs again under the same numbers. |
| Service restart | both | e2e `service-restart`; `TestServiceRestartMergesPrunedEntriesAgainOnce`, `TestKnownGapServiceRestartForgetsPendingExport`, `TestRestartKeepsState`, `TestMinIOMetastoreRestart` | Real reconnect backoff, and a measured redo; the fake engine pins the pruned-entry redo and the gap of a stop without a drain exactly. |
| Clean stop (drain) | both, and the binary | e2e `drain`; `TestDrain*`, `TestDrainedRestartStoresPendingExport`, `TestMinIODrain`, `TestMinIOBinaryDrainsOnSIGTERM`, `TestMinIOBinarySecondSIGTERMStopsAtOnce` | The real engine shows it stays connected and finishes its upload while the service drains; the fake engine controls the timing (quiet sessions, late spans, deadlines, disconnects); only the built command shows the signal wiring. |
| S3 throttling, 5xx, outage | real engines | e2e `s3-faults`; `TestExportRetriesFailedUploads`, `TestExportGivesUpOnFailingUploads`, `TestExportBlobStoreFailureReleases`, `TestUpdateIndexRetriesOnAFreshIndex` | Who retries what is split between the engine's PUTs and the service's AWS SDK client, which only the real pair shows. |
| Oversized messages | fake engine, and MinIO | `TestImportBundleNearMergeLimit` (and `TestMinIO…`), `TestExportSplitsBundleTooLargeToImport`, `TestExportOneRootBundleTooLargeToImport`, `TestMergeSizeBoundsBuildMerge`; splitting: `TestExportSplitsTooLarge`, `TestSyncTooLarge`, `TestSyncSplitsLargeOffers` | A real bundle near 64 MiB would take minutes to build and move; the limit is a setting, lowered in tests. |
| Slow or stalled uploads | fake engine and unit tests | engine's `TestStalledUploadFails`, `TestStalledHTTP2UploadFails`, `TestSlowUploadIsNotCutOff`, `TestStalledUploadFreesItsSlot`; `TestExportUploadTimeout` | The engine gives up after a minute without progress, with timeouts the unit tests shorten; a real-engine version would add that minute to every run, so there is none. |

**What was fixed.** The merge-size gap of #12's second comment: a stored
bundle could fit in a message while its merge, with a presigned GET per layer
(535 bytes each from MinIO, about 1 KB from S3 with longer keys, more with a
session token), did not, and the importer never sent it. The exporter now
checks each bundle's merge size before it uploads anything, and splits (see
[Export path](#export-path)). `TestImportBundleNearMergeLimit` fails without
the fix. `Service.CloseEngine` is new, to drop one connection.

**Accepted.** After a service restart, every connected engine is merged every
live bundle again, once, pruned entries included: see
[Import path](#import-path). The real run measured it: 8 bundles, 61 values,
merged into B within 187ms of its `hello`.

**Known gaps.** An export pending or in flight when the service stops
without a drain (a crash, SIGKILL, or `-drain-timeout 0`) is lost until a
later session names its entries
([#45](https://github.com/zerobull-consulting/dagger/issues/45)). A clean
stop drains first and stores it (see
[Stopping and draining](#stopping-and-draining)); an engine that disconnects
during the drain can't reconnect, and its exports are left
([#70](https://github.com/zerobull-consulting/dagger/issues/70)). The drain
starting an export past its deadline
([#73](https://github.com/zerobull-consulting/dagger/issues/73)) is fixed:
it starts nothing past it. So is the drain not waiting for an explicit
export or import admitted just before it
([#72](https://github.com/zerobull-consulting/dagger/issues/72)): it waits
for it, up to its deadline.

**Fixed since: bundles of moved imports.** The first run of
`service-restart` found that a bundle holding imports could no longer be
merged once its cache's imports moved
([#47](https://github.com/zerobull-consulting/dagger/issues/47)): C's verify
export from `renew`, after `prune` restarted C. Each export's resolution is
now stored (see [Imported entries](#imported-entries)). `prune` checks that
C's imports moved and that resolving C's bundle through C's current mappings
would fail, and that it merges with its stored resolution; `superseded`'s
engine F, the first started after that, gets C's entries from it;
`service-restart` checks that every bundle B had is merged into it again, C's
included; and the run fails if any service logged a merge that failed with
"is no longer stored". `TestBundleOfMovedImportsMerges` (fake engine, and
MinIO) reproduces it in the service.

From a run on 2026-11-02, in the same setup as above, each scenario's cost
on top of the run (42s together; the whole run took 1m58s with the binaries
built):

| Scenario | Took | What it showed |
| --- | --- | --- |
| `disconnect` | 4.6s | A reconnected 1.5s after the drop, with the same cache and generation. The export on the dropped connection was logged interrupted; nothing was published while it was cut (the index and 8 bundles unchanged); 2s later (`NoEngineWait` in the harness) it ran again on the new connection, uploaded the layers that had been in flight, and was stored. B then got the exec as a hit. |
| `engine-restart` | 13.5s | `docker restart` took 10.9s; A came back with the same cache ID and generation 2. The cut export ran again on generation 2 under the same entry numbers, and its 3 entries were recorded as generation 2's. |
| `s3-faults` | 11.9s | 2 blob PUTs answered 503 `SlowDown` (both in the export's first try), retried by a new export after 1s; 1 bundle PUT answered 500 and 2 index PUTs 503, each retried by the SDK; stored, with no failed export. With S3 down, the export failed once (S3 refused 14 requests, the SDK's retries included), nothing was published, and it was stored on the tracker's next try once S3 was back. |
| `service-restart` | 11.6s | Down 2s; A and B were back 3.5s after the stop. The redo above, of the 8 of B's 11 bundles still live and mergeable (2 had expired; 1 was #47's, and since #47's fix a later run merged all 9 live ones, 62 values, in 174ms, C's included); nothing of A's own merged into A. The export cut by the restart was not retried (`GAP`, #45); A's next session of the pipeline was exported and stored with its layers. |

Every scenario ended with S3 consistent (the index's entries all in stored
bundles; the stored bundles' layers all stored, except the one `renew`
deleted on purpose) and B getting the scenario's pipeline as a cache hit.

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
