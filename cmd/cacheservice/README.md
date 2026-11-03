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
| `-debug-listen` | `POST /debug/gc[?dry-run=1]` | Runs garbage collection now, over every namespace, and answers with what it did (see [Retention and garbage collection](#retention-and-garbage-collection)) |
| `-debug-listen`, and `-metrics-listen` (off) | `GET /metrics` | Prometheus metrics (see [Metrics](#metrics)). `-metrics-listen` serves them alone, for a scraper that shouldn't reach the debug API |

On the engine WebSocket, the service:

- takes the HTTP Basic username as the engine token. With `-tokens-file`, it
  refuses an unknown token with 401 before the upgrade, and the token's
  namespace decides everything the connection exports, imports, is offered
  and renews (see [Auth and namespaces](#auth-and-namespaces)). Without it,
  it accepts any non-empty token, in the `-scope` namespace. It logs only a
  short SHA-256 fingerprint of a token.
- reads `hello`, replies `welcome`, and records the engine's cache identity.
- answers WebSocket pings (the library does this; `-v` logs each one).
- answers `renew` with fresh presigned GETs when the import path is on and
  every layer is stored, and with `{"unavailable": true}` otherwise.
- answers any other request from the engine with an `error` reply.
- logs every envelope in both directions: direction, type, `id`, `re`, message
  size, body size, and a short summary for the known body types.

`-dump-dir DIR` also writes every envelope body and engine event body to a
numbered file in `DIR`.

The debug API has no authentication, and sees and drives every namespace. It
listens on localhost only by default; keep it there.

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
debug := httptest.NewServer(svc.DebugHandler())  // the -debug-listen address
// To stop: cancel ctx, then
svc.CloseEngines()
svc.Wait()
```

`EnableExport` and `EnableImport` take stores directly, such as the in-memory
ones the unit tests use. The stores and the export and import logic are in
[`blobstore`](blobstore/), [`metastore`](metastore/),
[`exporter`](exporter/) and [`importer`](importer/), and garbage collection
in [`gc`](gc/). What the service measures is in [`metrics`](metrics/).

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
<prefix>/<ns>/blobs/<alg>/<hex>                    a blob, verified: its digest key
<prefix>/<ns>/staging/<stage>/<alg>/<hex>          an upload, not yet verified
```

such as `cache/team-a/blobs/sha256/4f1e...`. A **stage** is 128 random bits
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
  (`TestMinIOPromoteManyParts`, with the limits lowered to 5 MiB).
- So is every blob with `CACHESERVICE_S3_HASH_IN_SERVICE=true`, for a store
  whose copy checksums can't be trusted, and every blob once a store returns
  a part copy without a checksum: the service then hashes from that point
  on (`TestMinIOPromoteStoreWithoutChecksums`).

**Why a multipart copy.** MinIO ignores `If-None-Match` on `CopyObject` (a
second conditional copy overwrote the key), but honors it on
`CompleteMultipartUpload` (412), and honors `CopySourceIfMatch`
(`TestMinIOConditionalPromotion`). AWS S3 documents both conditions on
those calls too. Correctness doesn't
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
uploads older than an export's upload timeout itself, and it aborts stale
multipart uploads in every namespace (see
[Retention and garbage collection](#retention-and-garbage-collection)). One
day is safe for the rule: an export's uploads and promotions finish within
its upload timeout (1h).

S3 lifecycle filters match a key prefix, not a wildcard segment, so one rule
can't match `<prefix>/*/staging/`. The options, and the choice:

- **A tag set on staging objects** would need the engine to send
  `x-amz-tagging` with its PUT: the presigned URL signs it as a header, and a
  bare PUT then fails (tried against MinIO: 400). Ruled out, since the engine
  doesn't change.
- **Staging outside the namespace** (`<prefix>/staging/<ns>/...`) would take
  one rule, but #23's comment from #52 settles staging under the namespace,
  so that promotion never crosses a namespace's prefix.
- **One rule per namespace**, on `<prefix>/<ns>/staging/`: the choice. A
  namespace is added in the tokens file, so its rule can be generated from
  the same file (below). A namespace without a rule is covered by garbage
  collection, which deletes its abandoned uploads itself and reports the
  namespace ([#11](https://github.com/zerobull-consulting/dagger/issues/11)).
  AWS allows 1,000 rules per bucket.

Plus one rule for the whole prefix that aborts incomplete multipart uploads
(a promotion aborts its own, but a service killed mid-copy leaves its parts).

On AWS, with the service's prefix `cache` and namespaces `team-a` and
`team-b`:

```json
{
  "Rules": [
    {"ID": "cacheservice-staging-team-a", "Status": "Enabled",
     "Filter": {"Prefix": "cache/team-a/staging/"}, "Expiration": {"Days": 1}},
    {"ID": "cacheservice-staging-team-b", "Status": "Enabled",
     "Filter": {"Prefix": "cache/team-b/staging/"}, "Expiration": {"Days": 1}},
    {"ID": "cacheservice-incomplete-copies", "Status": "Enabled",
     "Filter": {"Prefix": "cache/"}, "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}
  ]
}
```

```bash
aws s3api put-bucket-lifecycle-configuration --bucket dagger-cache --lifecycle-configuration file://lifecycle.json
```

That call replaces the bucket's whole lifecycle configuration: merge these
rules into any the bucket has. The rules can be generated from the tokens
file (a namespace's rule is the same whichever of its tokens names it):

```bash
awk '!/^#/ && NF >= 2 {print $2}' tokens | sort -u |
  jq -R --arg p cache '{ID: ("cacheservice-staging-" + .), Status: "Enabled",
    Filter: {Prefix: ($p + "/" + . + "/staging/")}, Expiration: {Days: 1}}' |
  jq -s --arg p cache '{Rules: (. + [{ID: "cacheservice-incomplete-copies", Status: "Enabled",
    Filter: {Prefix: ($p + "/")}, AbortIncompleteMultipartUpload: {DaysAfterInitiation: 1}}])}' > lifecycle.json
```

Without a tokens file, the one namespace is `-scope` (`default`):
`cache/default/staging/`.

On MinIO, with `mc` (the alias `local` and bucket `dagger-cache` of
`hack/remote-cache`):

```bash
mc ilm rule add --prefix cache/team-a/staging/ --expire-days 1 local/dagger-cache
```

once per namespace. MinIO drops incomplete multipart uploads on its own
(after 24 hours, its `api stale_uploads_expiry`).

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

`S3ConfigFromEnv` reads endpoint, region, bucket, prefix, path-style
addressing, expiries, static credentials,
`CACHESERVICE_S3_MAX_PROMOTIONS` (8) and `CACHESERVICE_S3_HASH_IN_SERVICE`
(false) from `CACHESERVICE_S3_*`. Without static credentials it uses the AWS
SDK's default chain. The service's credentials need `s3:GetObject` and
`s3:DeleteObject` on staging keys (a copy reads its source), `s3:PutObject`
and `s3:AbortMultipartUpload` on digest keys (the multipart copy), besides
what the metadata store needs, and what garbage collection needs (see
[Retention and garbage collection](#retention-and-garbage-collection)).

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
- **resolutions**, at `<prefix>/<scope>/resolutions/<id>.json`: one per
  export, the service number each of the bundle's values resolved to when
  the export was recorded, which merges of that bundle send (see
  [Imported entries](#imported-entries)). Content-addressed and written once,
  like bundles.
- **an index**, at `<prefix>/<scope>/index.json`: every stored entry (the
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
its blobs are under it too, at `<prefix>/<scope>/blobs/`. The scope `blobs` is
refused, since it would clash with the blob store's own keys under the same
prefix (`<prefix>/blobs/`, where blobs were kept before namespaces).

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
  exported mapping names an entry from that cache and engine number, and
  each entry is its source's mapping. Each imported mapping names a stored
  entry from another cache, on an engine entry the cache didn't export, and
  a stored entry is imported at most once per cache.

**Imported entries.** An engine that had a stored entry merged into it holds
the value on an entry of its own, and its sessions name that entry when they
hit it. So, after each merge, the importer records the engine entry each
stored entry landed on as the cache's import (`Index.RecordImports`):

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
  entries stay. `engine.prune` does not drop imports: a prune drops
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
`CACHESERVICE_S3_BUCKET` is set; the other `CACHESERVICE_S3_*` settings are as
for the blob store. At startup the service checks the bucket's conditional
writes (`VerifyConditionalWrites`) and refuses to start if they fail.
Exported entries go into the scope of the engine's namespace: its token's,
with `-tokens-file`, or `-scope` (`default`) without (see
[Auth and namespaces](#auth-and-namespaces)). Each namespace has its own
tracker and exporter, and its blobs, bundles and index under its own prefix.

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
out, and so are entries merged into the engine by the import path that still
have the replacement count and no part beyond what the merge left: they are
the stored values. So are entries whose expiry or retention has lapsed, and
entries refused for the same value. An import that changed is exported, and
`Record` keeps the stored entry's number for it (see
[Imported entries](#imported-entries)). The export goes to the engine connected with the
session's cache ID. If none is, the export waits and is tried again.

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
   GC's epoch (`<prefix>/<ns>/gc-epoch.json`, one small GET) before it
   checks any blob, and its index write is refused if it names a blob,
   bundle or resolution GC is deleting, or deleted after that read: it
   checked the object before it was deleted. The export is then tried again
   from step 1, and asks for what is gone (`export_collected_total`; see
   [Retention and garbage collection](#retention-and-garbage-collection)).
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

**Limits.** The engine works on 4 exports at once and queues the rest, so the
service sends at most 4 at a time to each engine. The engine uploads 8 blobs at
once across its exports; the service waits up to an hour for `uploaded`.
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
or digit, and not `blobs`. Many tokens may share a namespace. Generate tokens
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
- It is the one list of namespaces an operator maintains: the staging
  lifecycle rules can be generated from it (below and
  [Lifecycle rule for staging](#lifecycle-rule-for-staging)), as they can
  from the tokens file:

```bash
awk '!/^#/ && NF >= 1 && $1 != "*" {print $1}' namespaces | sort -u |
  jq -R --arg p cache '{ID: ("cacheservice-staging-" + .), Status: "Enabled",
    Filter: {Prefix: ($p + "/" + . + "/staging/")}, Expiration: {Days: 1}}' | jq -s .
```

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
(`<prefix>/<ns>/staging/<stage>/...`), and a promotion copies within one
namespace's prefix, so verification never crosses tenants
(`TestVerifyNamespacesApart`).

Before #23, sharing a prefix would have let one tenant's bad or malicious
same-size upload break a layer for every tenant, with `Has` keeping the
others from ever uploading the right one. Now that only verified blobs are
promoted ([#23](https://github.com/zerobull-consulting/dagger/issues/23)),
that no longer holds, so blobs could be shared per group of namespaces that
accept each other's uploads: the promotion target would be a group's prefix
outside `<prefix>/<ns>/` (say `<prefix>/shared/<group>/blobs/`), and `Has`,
GETs and `renew` would read it. Not built: sharing would still tell a tenant
which digests the others in its group hold (an upload skipped is a blob
someone stored), and GC would have to count references across the group's
namespaces ([#11](https://github.com/zerobull-consulting/dagger/issues/11)).

Without a tokens file, the one namespace is `-scope`, so blobs are at
`<prefix>/<scope>/blobs/`. Blobs stored by earlier versions, at
`<prefix>/blobs/`, are no longer read: their entries' layers are uploaded
again on the next export, and an import of an entry whose layers are only at
the old keys finds them missing, as for a deleted blob.

### Unauthenticated mode

Without `-tokens-file`, the service takes any non-empty token on the
WebSocket and any OTLP request, and puts every engine in the `-scope`
namespace, as before namespaces existed. It logs a warning at startup:

```text
level=WARN msg="UNAUTHENTICATED: no -tokens-file, so the service takes any token, and every engine that reaches it shares namespace default: ..."
```

Anyone who can reach it can read everything stored, add to it, and steer
exports with forged telemetry. Use it only on a trusted network, as the
real-engine run does on the Docker bridge.

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

The debug API (`-debug-listen`) is **not authenticated**, and lists, exports
and imports for every namespace's engines. It listens on `127.0.0.1` by
default; never expose it. `-metrics-listen` serves only `/metrics`, whose
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
(`CACHESERVICE_S3_BUCKET`). Without limits (`-max-age` and `-max-bytes` are 0
by default, and no namespaces file), it expires only what engines gave an
expiry, and deletes only what nothing names: bundles and blobs of exports
that were never recorded or were superseded, and staged uploads. So an
upgrade applies no age or size limit until one is configured.
`-gc-dry-run`, or `POST /debug/gc?dry-run=1`, logs (`gc dry run: would
delete`) and reports what a run would expire and delete, and changes
nothing (`TestDryRunDeletesNothing`). `POST /debug/gc` refuses a `dry-run`
value that isn't a boolean (400) rather than run for real, and answers 500,
with the reports, when a run failed somewhere (`TestDebugGC`).

### One run, per namespace

GC lists the top level of `<prefix>/`, and the incomplete multipart
uploads under it: every valid namespace name with objects or uploads is
collected, including namespaces whose tokens were all removed, namespaces
in no file, and a namespace whose only leftover is an upload
(`TestNamespaceOfOnlyAnUpload`). `<prefix>/blobs/` (where blobs were kept before
namespaces) and anything that isn't a valid namespace name are skipped. In
each namespace, in order:

1. **Expire.** One conditional index update (only when something changes):
   roots past their own expiry or their max age, and entries past their
   own expiry, are dropped, with what only they keep alive
   (`metastore.Index.Expire`, the same rules as `DropExpired`). Roots with no
   recording time (indexes before version 3) are stamped now. Finished
   deletions older than 4h are dropped from the index.
2. **List** every object under `<prefix>/<ns>/`: its sizes are the
   namespace's stored bytes (`gc_stored_bytes`), by kind.
3. **Mark.** What the index names: its entries' bundles and resolutions,
   and every blob those bundles name, through their outputs' chains and
   their records' pending offers (`metastore.BundleBlobs`). Bundles are read
   once and their blob lists kept in memory (at most 65,536). A bundle the
   index names that can't be read stops the namespace's sweep: GC can't
   know its blobs (`TestMissingBundleStopsSweep`). A namespace with no index
   is not swept either: GC can't tell what is in use
   (`TestLeavesWhatItDoesNotKnow`).
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
7. **Incomplete multipart uploads** under `<prefix>/<ns>/` older than the
   staging age are aborted: promotions a stopped service left. They are
   listed once per run, for every namespace.

Keys GC doesn't know (`conditional-write-check/`, anything else) are
counted as `other` and never deleted. A run of one namespace never deletes
another's data (`TestNamespacesApart`, and the real-engine `gc` scenario,
which checks the run's scope after collecting its own).

### Max age

A root expires `max-age` after an export last recorded it:
`metastore.Root.RecordedAtUnix`, the service's clock when the index update
that wrote its record ran. The entries only that root keeps alive go with
it; an entry a younger root depends on stays, whenever it was recorded
(`TestExpireMaxAge`).

An export refreshes a root's time **only when it writes the root's record**
(`Index.RecordResolvedAt`, `TestRecordStampsRoots`):

- a new root, or a root with no time yet: stamped;
- a later state of the value from its own engine (a later generation, or
  the same with as many replacements or more), or an imported value's
  record that adds parts (see [Imported entries](#imported-entries)):
  refreshed;
- a late export from an older generation, or an engine exporting back an
  import unchanged: not refreshed. Merges hand stored entries to engines,
  so a refresh by them would keep data alive forever by merging it around.
  The tracker doesn't export unchanged imports anyway; the index doesn't
  rely on it.

**Merges are capped at it.** Most stored roots have no expiry (0), and a
merge used to send the index's: every engine kept every imported root until
its own GC removed it. The import path now caps each root's expiry in a
merge at its recording time plus the max age
(`metastore.Index.ApplyMaxAge`, on its own snapshot of the index, never
written back), and leaves out roots past it, even before GC runs
(`TestGCMaxAgeEndToEnd`; the real-engine `gc` scenario checks the expiry
engine GB was sent). Engines then release imported roots on their own. The
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
expire roots: a burst of uploads shouldn't expire live data. The bucket can
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
   minutes of the claim (a run doesn't start a delete after that).
3. **Finish.** In a second update, GC marks them deleted in a new epoch,
   then publishes the epoch in `<prefix>/<ns>/gc-epoch.json`. Each export
   try reads that object before it checks any blob (one small GET), and its
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
the engine upload it again, and is recorded (`export_collected_total`). So
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
deletes and finishes (`TestStaleClaimTakenOver`). Claims taken over count
against `-gc-max-deletes` with new ones, stale first, so one run never
deletes more however many runs stopped (`TestStaleClaimsBoundedByMaxDeletes`). Clocks of replicas only
need to agree within minutes.

**Rolling upgrade.** Index version 3 holds the claims; a replica that reads
only version 2 refuses it and stops updating the namespace, rather than
record an export without checking them (see [Metadata store](#metadata-store)).

### Staging and multipart uploads

A staged upload is never read as cache (`Has`, every GET and `renew` read
digest keys only), so deleting one is always safe: at worst a promotion
still copying it fails, and its export asks for the blob again. GC reads
the bucket's lifecycle rules once per run (`GetBucketLifecycleConfiguration`):

- a namespace whose `<prefix>/<ns>/staging/` an enabled rule expires by
  age (`Expiration.Days`, filtered by prefix alone; a rule with a `Date`
  expires objects once, not as they age) is left to it
  (`gc_staging_rule_missing` 0, `TestExpiringPrefixes`);
- a namespace without one is reported (`gc_staging_rule_missing` 1, and a
  warning when it has staged uploads), and GC deletes its staged uploads
  older than `-gc-staging-age` (1h, an export's upload timeout) itself;
- when the rules can't be read (no `s3:GetLifecycleConfiguration`, or a
  store without them), GC deletes old staged uploads in every namespace,
  and removes `gc_staging_rule_missing` (`TestStagingRuleMetricUnknown`).

Stale incomplete multipart uploads (`ListMultipartUploads`) are aborted
whatever the rules (`TestStagingAndUploads`, `TestMinIOGC`). MinIO lists
multipart uploads only for an exact key: a listing by prefix returns none.
`S3Bucket.ListUploads` then lists the bucket's uploads and keeps those
under the prefix; MinIO also drops them itself after 24h.

**Lifecycle rules.** Keep the staging rules and the multipart rule of
[Lifecycle rule for staging](#lifecycle-rule-for-staging): they cost the
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
| Epoch | 1 GET of the epoch object, once the namespace has deleted anything |
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
| `-gc-interval` | `1h` | Time between runs, jittered by ±25% (0: only `POST /debug/gc`) |
| `-gc-dry-run` | false | Log and report what a run would do; change nothing |
| `-gc-grace` | `2h` | How old an unreferenced bundle, resolution or blob must be to be deleted |
| `-gc-staging-age` | `1h` | How old a staged upload, or an incomplete multipart upload, must be to be deleted |
| `-gc-max-deletes` | 5000 | Most bundles, resolutions and blobs one run deletes per namespace |
| `-max-age` | 0 | Default max age of a namespace's roots, such as `720h` (0: none) |
| `-max-bytes` | 0 | Default max size of what a namespace keeps, such as `500GiB` (0: none) |
| `-namespaces-file` | none | Per-namespace limits (see [The namespaces file](#the-namespaces-file)) |

The service's credentials also need `s3:ListBucket`,
`s3:ListBucketMultipartUploads`, `s3:DeleteObject` on every key under the
prefix, and, to report namespaces without a staging rule,
`s3:GetLifecycleConfiguration` on the bucket.

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
| `auth_rejected_total` | `endpoint` | Requests refused for a missing or unknown token: `engine` (the WebSocket) or `otlp` |
| `connections_revoked_total` | `scope` | Connections closed with 4001 because their token left the namespace |
| `engine_events_ignored_total` | `scope`, `kind` | Engine events dropped because they came with a token other than the engine's |
| `blob_promotions_total` | `scope`, `outcome` | Uploaded blobs verified and promoted: `promoted`, `existing` (already stored, or another export's copy won), `mismatch` (content not the digest's, or not the declared size), `missing` (reported uploaded, nothing staged) or `error` (the store failed) |
| `blob_hashed_bytes_total` | `scope`, `by` | Bytes hashed to verify uploads: `store` (as it copied them) or `service` (read through the service) |
| `blob_promotion_duration_seconds` | `scope` | Time to verify and promote one blob, slot wait included: it grows with the blob's size (see [Verification and promotion](#verification-and-promotion)) |
| `gc_stored_bytes`, `gc_stored_objects` | `scope`, `kind` | What GC's last listing of the namespace found, by kind: `blob`, `bundle`, `resolution`, `index`, `staging` or `other` |
| `gc_deleted_objects_total`, `gc_deleted_bytes_total` | `scope`, `kind` | What GC deleted: `blob`, `bundle`, `resolution`, `staging`, or `upload` (an incomplete multipart upload aborted; 0 bytes) |
| `gc_runs_total` | `scope`, `outcome` | GC runs of a namespace: `ok`, `error` or `dry_run` |
| `gc_run_duration_seconds` | `scope` | Time of one namespace's GC run |
| `gc_entries_expired_total` | `scope`, `reason` | Index entries GC dropped: `expired` (their own expiry, or the max age) or `size` (the max size) |
| `gc_staging_rule_missing` | `scope` | 1 when no lifecycle rule expires the namespace's staging prefix (GC expires it itself), 0 when one does; absent when the rules can't be read |
| `export_collected_total` | `scope` | Export tries refused because GC deleted, or was deleting, an object they named; each is tried again |

`scope` is the namespace (see [Auth and namespaces](#auth-and-namespaces)).
No label comes from an engine-supplied string: `scope` is a validated
namespace from the tokens file or `-scope`, or, for the `gc_` metrics, a
valid namespace name GC listed in the bucket; the others are the service's
constants.

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
  is a run that failed somewhere (the log says where). A rising
  `export_collected_total` means exports keep naming what GC deletes: a
  grace period too short for the exports' duration.
- **Promotion cost:** `blob_promotion_duration_seconds` against the blobs'
  sizes, and `blob_hashed_bytes_total{by="service"}`, which is read through
  the service (blobs over 5 GiB, or a store that doesn't hash).

## Running it against a local engine

For a local S3-compatible store (MinIO), see
[`hack/remote-cache`](../../hack/remote-cache/README.md).

1. Start the service:

   ```bash
   go run ./cmd/cacheservice -v -dump-dir /tmp/cacheservice-dump
   ```

   That is unauthenticated, with any token in the `default` namespace. To
   check tokens, add `-tokens-file tokens.txt`, with a line
   `<token> <namespace>` for the engine's token, and for the CLI's if it
   sends spans (see [Auth and namespaces](#auth-and-namespaces)).

2. Build and start a dev engine (see `hack/dev`). Then restart its container
   with these variables set:

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

   Take entry numbers from the `result` fields of the `cache span` log lines.
   Cache spans arrive only when the CLI itself has Cloud telemetry configured,
   through `DAGGER_CLOUD_TOKEN` or `dagger login`, and has `DAGGER_CLOUD_URL`
   pointing at the service. They don't arrive either when the CLI inherits an
   unsampled `TRACEPARENT` (see [Real-engine run](#real-engine-run)).

## Real-engine run

`./hack/remote-cache/dev.sh e2e` runs both paths with real engines built
from this repo
([#29](https://github.com/zerobull-consulting/dagger/issues/29)): engine A
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
| Service restart | both | e2e `service-restart`; `TestServiceRestartMergesPrunedEntriesAgainOnce`, `TestKnownGapServiceRestartForgetsPendingExport`, `TestRestartKeepsState`, `TestMinIOMetastoreRestart` | Real reconnect backoff, and a measured redo; the fake engine pins the pruned-entry redo and the gap exactly. |
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

**Known gaps.** An export pending or in flight when the service stops is lost
until a later session names its entries
([#45](https://github.com/zerobull-consulting/dagger/issues/45)).

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
