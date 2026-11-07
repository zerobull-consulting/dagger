# Remote cache dev environment: MinIO

A local S3-compatible store for developing and testing the remote cache
service (`cmd/cacheservice`), so nothing needs an AWS account
([#2](https://github.com/zerobull-consulting/dagger/issues/2), under
[#1](https://github.com/zerobull-consulting/dagger/issues/1)).

Work items on the remote cache are run from prompts generated from
[`PROMPT_TEMPLATE.md`](PROMPT_TEMPLATE.md), which also holds the working
agreement for review and merge.

## Quick start

```bash
./hack/remote-cache/dev.sh up
```

This starts MinIO, waits until it is healthy, creates the bucket and prints
the settings. Data persists in the `dagger-remote-cache_minio-data` volume.

| Command | What it does |
| --- | --- |
| `dev.sh up` | Start MinIO, create the bucket, print the settings. Safe to run again. |
| `dev.sh down` | Stop MinIO and keep the data. |
| `dev.sh reset` | Stop MinIO and delete the data. |
| `dev.sh status` | Show the containers. |
| `dev.sh env` | Print the settings as `export` lines: `eval "$(./hack/remote-cache/dev.sh env)"`. |
| `dev.sh test` | Start MinIO and run the Go tests against it, or any bucket with `CACHESERVICE_TEST_EXTERNAL=1` ([Testing against a real bucket](#testing-against-a-real-bucket)). |
| `dev.sh e2e` | Start MinIO and run the remote cache with real engines, and its scenarios (`cmd/cacheservice/e2e`). See [Real engines](#real-engines-devsh-e2e). |
| `dev.sh e2e-down` | Remove the engine containers and volumes a run with `E2E_KEEP=1` left. |
| `dev.sh demo` | Start MinIO where containers reach it and run the sidecar demo through both phases, writer then read-only. See [`demo/`](demo/README.md). |
| `dev.sh demo-store` | Start MinIO where containers reach it, with the demo's read-only user, and print its settings as `key=value` lines (CI's `sidecar demo` job uses them). |

It needs Docker with Compose v2.

## Settings

| Setting | Value | Override with |
| --- | --- | --- |
| Listen address | `127.0.0.1` (loopback only) | `MINIO_BIND_ADDR` |
| S3 endpoint | `http://localhost:9000` | `MINIO_API_PORT` |
| Console | `http://localhost:9001` | `MINIO_CONSOLE_PORT` |
| Region | `us-east-1` | `MINIO_REGION` |
| Bucket | `dagger-cache` | `MINIO_BUCKET` |
| Access key | `dagger-dev` | `MINIO_ROOT_USER` |
| Secret key | `dagger-dev-secret` | `MINIO_ROOT_PASSWORD` |
| Addressing | path-style (`http://host:9000/<bucket>/<key>`) | none |

The credentials are MinIO's root user, and they are committed in this repo.
That's why MinIO listens on loopback unless you set `MINIO_BIND_ADDR`. Never
set it to `0.0.0.0` on a machine others can reach.

Set the same overrides for every `dev.sh` command, since `env` and `test`
print and use them.

`dev.sh env` prints the settings as the `CACHESERVICE_S3_*` variables the
tests read: `ENDPOINT`, `REGION`, `BUCKET`, `ACCESS_KEY_ID`,
`SECRET_ACCESS_KEY` and `PATH_STYLE`. The blob store
(`cmd/cacheservice/blobstore`, `S3ConfigFromEnv`) reads the same names, plus
`PREFIX`, `PUT_EXPIRY`, `GET_EXPIRY`, `MAX_PROMOTIONS` and
`HASH_IN_SERVICE`.

The service owns everything under its prefix (the whole bucket when
`PREFIX` is empty), and refuses at startup a prefix that holds anything but
its own store (see
[Bucket ownership](../../cmd/cacheservice/README.md#bucket-ownership)). Run
a service against the dev bucket under a prefix of its own, such as
`CACHESERVICE_S3_PREFIX=dev`, and wipe it (`mc rm --recursive --force
local/dagger-cache/dev/`) to start over, or after a change of layout.

The dev bucket has no lifecycle rule: staged uploads that were never
promoted (an export cut short) stay until `dev.sh reset`, and are never read.
To try the rule the service's README recommends
([lifecycle rule for staging](../../cmd/cacheservice/README.md#lifecycle-rule-for-staging)),
once per namespace:

```bash
docker compose -f hack/remote-cache/compose.yaml exec minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ilm rule add --prefix <prefix>/ns/<namespace>/staging/ --expire-days 1 local/dagger-cache'
```

Configure an AWS SDK client for MinIO with a base endpoint, a static
credential, path-style addressing and checksums only when required. The SDK's
default checksums add headers to presigned PUTs, and the engine doesn't send
them:

```go
s3.New(s3.Options{
	BaseEndpoint:               aws.String("http://localhost:9000"),
	Region:                     "us-east-1",
	Credentials:                credentials.NewStaticCredentialsProvider("dagger-dev", "dagger-dev-secret", ""),
	UsePathStyle:               true,
	RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
})
```

## Image

`pgsty/silo:RELEASE.2026-10-26T00-00-00Z`, the community fork of MinIO that
pgsty maintains. It used to ship as `pgsty/minio`, which stopped at
`RELEASE.2026-08-04T00-00-00Z`; new releases ship only as `pgsty/silo`. The
tag was checked on Docker Hub on 2026-10-31, and it was the newest release
tag. Use the plain tags, not the `-distroless` ones: the plain image ships
`mc` and `curl`, so the bucket bootstrap and the health check use the same
image and there is only one tag to pin. Silo still accepts MinIO's `server`
command and `MINIO_*` settings. To update it, change the `x-minio-image`
anchor in `compose.yaml` and this section.

## Tests

`dev.sh test` runs every `TestMinIO*` test under `cmd/cacheservice`. Each test
skips unless `CACHESERVICE_S3_ENDPOINT` is set.

`cmd/cacheservice/server/minio_test.go` checks what the storage work relies on,
through the raw SDK:

- `TestMinIOPresignedPutGet`: a blob keyed by its digest goes up through a
  presigned PUT, sent the way the engine sends it (`Content-Length` and no
  other headers), and comes back through a presigned GET. `HeadObject` tells
  whether it exists.
- `TestMinIOPresignedPutSizeBound`: a presigned PUT signed for a size refuses a
  body of another size.
- `TestMinIOPresignedGetExpires`: an expired presigned GET is refused with 403.
- `TestMinIOConditionalWrites`: MinIO honors `If-None-Match: *` and `If-Match`
  on PUT, which atomic index updates
  ([#6](https://github.com/zerobull-consulting/dagger/issues/6)) need.

`cmd/cacheservice/blobstore/minio_test.go` checks the blob store itself:

- `TestMinIOStorePutGet`: a blob uploaded through the store's presigned PUT
  lands at a staging key, and only once promoted at its digest key; then it
  comes back through its presigned GET, and `Has` sees it.
- `TestMinIOStoreUploadURLsSkipsExisting`: `UploadURLs` asks only for blobs
  the bucket doesn't have at their digest keys, so a promoted blob is not
  requested again, and one only staged is.
- `TestMinIOStorePutSizeBound`: the store's PUTs refuse bodies of another
  size.
- `TestMinIOStoreExpiry`: the store's PUTs and GETs stop working at their
  configured expiry.
- `TestMinIOStoreHasBadCredentials`: `Has` reports bad credentials as an
  error, not as a missing blob.

`cmd/cacheservice/blobstore/minio_promote_test.go` checks the verification of
uploads ([#23](https://github.com/zerobull-consulting/dagger/issues/23); see
[`cmd/cacheservice/README.md`](../../cmd/cacheservice/README.md#verification-and-promotion)):

- `TestMinIOStoreMisdeclaredDigestNeverStored`: a same-size body with the
  wrong content is taken by S3 at its staging key, but its promotion is a
  mismatch: it never appears at the digest key, the staged object is
  dropped, the blob is requested again, and the right upload is promoted.
  Both ways of hashing. It replaced the known-gap test
  `TestMinIOStoreKnownGapMisdeclaredDigest`.
- `TestMinIOStoreHashesCopies`: MinIO computes a full-object SHA-256 on
  `CopyObject` and on a one-part `UploadPartCopy` with `ChecksumAlgorithm
  SHA256`, from an object uploaded with no checksum.
- `TestMinIOConditionalPromotion`: MinIO ignores `If-None-Match` on
  `CopyObject`, honors it on `CompleteMultipartUpload`, and honors
  `CopySourceIfMatch`; two promotions of one blob racing from two stages end
  with one promoted, the other existing, and the blob correct.
- `TestMinIOPromoteManyParts`: a blob over the part-copy limit (lowered to
  5 MiB) is hashed by the service and copied in parts; a mismatch is caught
  the same way.
- `TestMinIOPromoteStoreWithoutChecksums`: a store that doesn't hash on
  copy has its blobs hashed by the service.
- `TestMinIOPromoteLatency`: logs the time to promotion of blobs of 64 KiB
  to 64 MiB, both ways of hashing (about 3s). `CACHESERVICE_TEST_PROMOTE_MB`
  adds a size in MiB: 1024 adds about 50s and 4 GiB of writes.
- `TestMinIOStoreKnownGapUnverifiedOldObjects`: pins a known gap
  ([#53](https://github.com/zerobull-consulting/dagger/issues/53)). An
  object stored at a digest key before uploads were verified is trusted as it
  is.

`cmd/cacheservice/metastore/minio_test.go` checks the metadata store:

- `TestMinIOMetastoreConditionalWrites`: the store's own startup check,
  `VerifyConditionalWrites`, passes against MinIO.
- `TestMinIOMetastoreRecord`: a recorded bundle and the index land at their
  documented keys and read back; recording the same bundle again hands out no
  new numbers.
- `TestMinIOMetastoreConcurrentWriters`: several stores, each with its own S3
  client, record into one scope at once. No update is lost and no service
  number is handed out twice.
- `TestMinIOMetastoreRestart`: a new store reads what the old one wrote and
  numbers new entries after the old ones.

`cmd/cacheservice/server/minio_export_test.go` checks the export path end to end,
with a fake engine cache served through the engine's own `remotecache.Run`:

- `TestMinIOExportBuild`: after a build's session ends, the engine has
  uploaded its layers to their keys through presigned PUTs, and its bundle and
  the index are in the bucket. Exporting it again uploads nothing, stores the
  same bundle and hands out no new numbers.
- `TestMinIOExportSharedLayer`: a second build that shares the base layer
  uploads only its own.
- `TestMinIOEnableS3`: `server.EnableS3`, what `cacheservice` runs at
  startup, turns on the export and import paths against MinIO, and refuses a
  missing bucket.

`cmd/cacheservice/server/minio_import_test.go` checks the import path the same
way: a fresh engine has the build merged and pulls its layers from MinIO
(`TestMinIOImportFreshEngine`), renews expired GETs (`TestMinIOImportRenew`),
doesn't export its imports back (`TestMinIOImportsNotReexported`), and stored
bundles hold no GETs (`TestMinIOStoredBundlesHoldNoAddresses`).
`TestMinIOImportBundleNearMergeLimit` exports a bundle whose merge, with
MinIO's presigned GETs, is just under the import path's limit, just over it,
and over it only once the GETs are added, and checks that each is stored
(split when over) and imported
([#12](https://github.com/zerobull-consulting/dagger/issues/12)).

Each test writes under `test/<run>/` in the bucket and deletes it afterwards.
The `Remote cache tests` workflow runs them on every pull request. They need
`CACHESERVICE_S3_PREFIX`, which `dev.sh test` and `dev.sh e2e` set to `test`
for MinIO, but `dev.sh env` doesn't, since it's also the service's settings.
Set it yourself to run `go test` by hand.

## Testing against a real bucket

Both layers run against any S3-compatible bucket, under a prefix of their own.
With `CACHESERVICE_TEST_EXTERNAL=1`, `dev.sh test` and `dev.sh e2e` don't start
MinIO, and use the bucket in the `CACHESERVICE_S3_*` variables, refusing to run
without an endpoint, bucket and prefix, since the tests would otherwise skip and
pass:

```bash
# Encrypted values, with dotenvx, from cmd/cacheservice/.env:
cd cmd/cacheservice
npx @dotenvx/dotenvx run -- bash -c '
  export CACHESERVICE_TEST_EXTERNAL=1 CACHESERVICE_TEST_STORE=spaces
  cd ../.. && ./hack/remote-cache/dev.sh test && ./hack/remote-cache/dev.sh e2e'
```

| Variable | Meaning |
| --- | --- |
| `CACHESERVICE_S3_ENDPOINT`, `_REGION`, `_BUCKET`, `_ACCESS_KEY_ID`, `_SECRET_ACCESS_KEY` | The bucket. Without the keys, the SDK's default chain is used; the e2e's proxy, when it signs again, uses what the S3 client resolved. |
| `CACHESERVICE_S3_PREFIX` | Required: everything happens under it. Empty or `/` is refused. |
| `CACHESERVICE_S3_PATH_STYLE` | `true` or `false`; set it. Unset, the `blobstore`, `gc` and `metastore` suites use virtual-hosted addressing, and `server` and the e2e path-style. |
| `CACHESERVICE_TEST_EXTERNAL` | `1` uses that bucket instead of starting MinIO. |
| `CACHESERVICE_TEST_STORE` | `minio` (default) or `spaces`: what the tests expect where stores differ ([below](#what-each-store-does)). |
| `E2E_S3_RESIGN` | `1` makes the e2e's proxy sign requests again for any store, to run that path against MinIO. |
| `CACHESERVICE_TEST_DISPOSABLE_BUCKET` | `1` lets `TestMinIOGC` replace the bucket's lifecycle configuration. Leave it unset on a shared bucket. |

### What keeps a shared bucket safe

The bucket doesn't have to be disposable. `cmd/cacheservice/internal/s3test`
enforces this in test setup:

- A run refuses an empty or `/` prefix.
- Every key a test writes, deletes or aborts is under `<prefix>/<run>/` (the
  e2e: `<prefix>/e2e/<run>/`). Every S3 client of the tests, and the e2e's
  proxy, goes through a guard (`s3test.Guard`) that refuses any other write,
  delete or abort before it reaches the store, and the test then fails. It
  reads the keys of bulk deletes too, from a body bounded by
  `s3test.MaxBulkDeleteBytes`.
  Presigned PUTs a test sends itself are checked by `s3test.CheckURL`. In the
  e2e, the ones the service hands real engines go through the proxy, and its
  guard. The ones it hands a fake engine go through `remotecache`'s own HTTP
  client, past the guard: their keys come from a store configured with the
  test's prefix, which is what keeps them in it.
- Nothing changes the bucket's configuration. The guard refuses every
  bucket-level change. The one exception is the lifecycle configuration with
  `CACHESERVICE_TEST_DISPOSABLE_BUCKET=1`, since
  `PutBucketLifecycleConfiguration` replaces the whole bucket's. Without it,
  `TestMinIOGC` skips that part, and expects no rule. Reads are not limited:
  they change nothing.
- Each test deletes its objects, with their versions and delete markers on a
  versioned bucket, and aborts its uploads when it ends, failed or not, and
  fails if any are left. The key needs to be allowed to list versions.

A process that is killed (`go test`'s `-timeout`) can't clean up: its objects
stay under `<prefix>/<run>/`, whose name starts with the run's UTC time.

`TestGuardWithSDK` and `TestGuardBucketConfiguration` check the guard with the
requests the SDK really makes, in both ways of addressing the bucket, and
`TestMinIODeletePrefix` the cleanup, against a real store. `TestS3ProxyGuard`
checks the proxy's, and an e2e run fails if its proxy or its direct client
refused a request.

### The e2e's proxy on an HTTPS store

Engines and services reach the store through the run's recording proxy, and
most scenarios read its record or inject faults, so bypassing it would leave
little of the e2e. A store checks a signature against the `Host` it receives,
and URLs are signed for the proxy's address. MinIO takes that, but a hosted
store needs its own name. So for an HTTPS or virtual-hosted endpoint the proxy
(`e2e/resign.go`) checks the signature it received, then signs again for the
store with the same signed headers, time and expiry. A tampered URL or the
wrong body size gets 403 from the proxy, and the store itself still enforces
expiry. Services always address the proxy path-style, and it forwards in the
addressing `CACHESERVICE_S3_PATH_STYLE` says. Every scenario runs, faults
included. `TestMinIOProxySignsAgain` checks this against the real store, and
`TestS3ProxyForwardsToTheStore` the host and path it sends.

What this doesn't cover: in the e2e, the store checks the proxy's signature
rather than the service's, and engines talk plain HTTP to the proxy. The store
suites cover those directly, with the service's own presigned URLs and an
engine-style PUT over HTTPS.

### What each store does

These are the S3 calls the service makes. Layout version 2 moved every key
under `ns/<namespace>/`, but made no call behave differently, so every row
holds as it was measured.

The checks of
[zerobull-consulting/dagger#55](https://github.com/zerobull-consulting/dagger/issues/55),
from runs against a dedicated Spaces bucket on 2026-11-03. The tests pin these
(`s3test.StoreBehavior`), so a store that changes fails them.

| Check | MinIO | DigitalOcean Spaces | What the service does on Spaces |
| --- | --- | --- | --- |
| `If-None-Match: *` on `PutObject` | Honored | Honored | Relies on it for the index. |
| `If-Match` on `PutObject` | Honored, quoted or not | **Honored only unquoted**; a quoted current ETag gets 412. A stale one gets 412 either way. 8 racing writers lost no update in 80. | Sends it unquoted, on every store. |
| Store-side checksum on a one-part `UploadPartCopy` | Full SHA-256 | **None returned** | Detects it on the first copy and hashes in the service from then on, so blob bytes pass through the service. |
| `If-None-Match: *` on `CompleteMultipartUpload` | Honored (412) | **Ignored**: overwrites | Accepts it: only verified copies complete, so racing exports write the same bytes. |
| `CopySourceIfMatch` on copies | Honored (412) | **Ignored** | Compares each copied part's ETag with the MD5 it took while hashing. |
| `If-Match` on `GetObject` | Honored | Honored | Relies on it while hashing. |
| Presigned PUT with a signed `Content-Length`, to a staging key | Works; other sizes refused | Works; other sizes refused | None needed. |
| Presigned expiry | 403 | 403 | None needed. |
| Lifecycle rules | Expiry accepted; **`AbortIncompleteMultipartUpload` refused (400)** | Both accepted with a full-access key; **403 with a more limited key** | Whether they take effect isn't checked: rules run on a scale of days. |
| A blob over 5 GiB | Not run | Not run | The multipart path ran with its limits lowered to 5 MiB. |
| Path-style and virtual-hosted addressing | Path-style | Both work | None needed. |
| Region | Pinned in `compose.yaml` | The endpoint's region signs correctly | None needed. |
| The e2e with real engines | Passes | Passes, every scenario, virtual-hosted, faults included | None needed. |

`CACHESERVICE_S3_HASH_IN_SERVICE` needn't be set on Spaces: the fallback is
automatic. AWS S3 is still open in zerobull-consulting/dagger#55.

### In CI

`Remote cache tests against Spaces`
([`remote-cache-spaces.yml`](../../.github/workflows/remote-cache-spaces.yml))
has two jobs, `S3 tests against Spaces` and `real engines (e2e) against
Spaces`. It runs on pushes to `feature/remote-cache` and by hand
(`workflow_dispatch`) on any branch, not on pull requests. Its jobs are
skipped unless these repository secrets are set: `SPACES_ENDPOINT`, `SPACES_REGION`, `SPACES_BUCKET`,
`SPACES_PREFIX`, `SPACES_ACCESS_KEY_ID` and `SPACES_SECRET_ACCESS_KEY`. An
optional `SPACES_DISPOSABLE_BUCKET`, set to `1`, runs the lifecycle part on a
bucket dedicated to the tests. Concurrent runs with it replace each other's
lifecycle rule, so leave it off if pull requests overlap.

**Use a key for a dedicated test bucket, never a production one.** Pull requests
from forks don't get these secrets, but anyone with write access can read them
through a workflow on a branch of this repository, and a pull request's own code
runs with them. To put a review in front of them, keep them in a
[GitHub environment](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment)
with required reviewers instead.

## Pointing an engine at the service and MinIO

The engine reaches only the cache service. MinIO's address reaches it inside
the presigned URLs the service hands out. **Those URLs are signed for one
host, and the engine must reach MinIO at exactly that host.** An engine in a
container can't reach the host's loopback, so either:

- publish MinIO on the Docker bridge gateway, which containers on this host
  can reach and other machines can't:

  ```bash
  MINIO_BIND_ADDR=172.17.0.1 ./hack/remote-cache/dev.sh up
  ```

  Find the gateway with
  `docker network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}'`.
  `dev.sh env` then prints `http://172.17.0.1:9000` as the endpoint, and
  `host.docker.internal` (with `--add-host=host.docker.internal:host-gateway`)
  reaches the same address. Pass the same `MINIO_BIND_ADDR` to every `dev.sh`
  command;
- or run the engine with `--network host`, and keep the loopback default.

Both were checked from a container on 2026-10-31.

The service reads the `CACHESERVICE_S3_*` variables that `dev.sh env`
prints. To run it against a dev engine by hand, see
[`cmd/cacheservice/README.md`](../../cmd/cacheservice/README.md#running-it-against-a-local-engine);
`dev.sh e2e` below does it all in one command.

`dev.sh` starts and stops MinIO only; it never runs the service, so it has
no stop path to drain. Stop a service you run by hand with SIGTERM (Ctrl-C),
which drains its exports first (see
[Stopping and draining](../../cmd/cacheservice/README.md#stopping-and-draining)),
before stopping the engine.

## Engine

Run a stock Dagger engine release: its remote cache client is the one this
branch carries, so the fork builds no engine image of its own
([zerobull-consulting/dagger#114](https://github.com/zerobull-consulting/dagger/issues/114)).
The image the fork used to publish, `ghcr.io/zerobull-consulting/dagger-engine`,
is no longer built; its tags stay where they are, but nothing new is pushed.

**The supported engines are the versions in the service's manifest,
[`cmd/cacheservice/compat/engines.txt`](../../cmd/cacheservice/compat/engines.txt),**
and the service enforces it
([zerobull-consulting/dagger#117](https://github.com/zerobull-consulting/dagger/issues/117)).
Each `accept` line names an engine version, its stock image's digest and
the CI run of the e2e and the sidecar demo against that image that verified
it; a version goes on the list only after such a run is green, never from
reading the code. The service refuses, before its welcome, every engine
whose hello names any other version: an exact string match, no ranges, so
a dev engine (`-dev-<commit>`, `+dirty`) is refused too. A `reject` line
names a version that was tried, and the issue saying why. See
[the cache service's README](../../cmd/cacheservice/README.md#engine-versions)
for the refusal, the escape hatch for development and the service's
identity. There is no minimum release: a version is supported only while
it is an `accept` line.

**Pick the service by the engine's version.** Each release build of the
service is published as
`ghcr.io/zerobull-consulting/dagger-remote-cache:<commit SHA>`, and
`:<engine version>` is a floating tag on the newest build that accepts that
engine, moved only once that build passed the e2e and the sidecar demo
against every one of its `accept` lines
([Image](../../cmd/cacheservice/README.md#image)). So for an engine
`v1.0.0-beta.25`, run `dagger-remote-cache:v1.0.0-beta.25`. The sidecar
action's `version` input, and the demo's `VERSION`, pick both images this
way.

What the engine releases share, for deciding which version to try next:
the client and its protocol (`engine/remotecache`), whose hello carries the
engine's version but no protocol version, and `cmd/engine/remotecache.go`
are byte-for-byte the same in every release from `v1.0.0-beta.20` to
`v1.0.0-beta.25` and on this branch, apart from tests. What the client
exports and merges, the engine's cache (`engine/server/remote_cache*.go`,
`dagql/cache*.go`), is the same in `v1.0.0-beta.24`, `v1.0.0-beta.25` and
this branch (bundle version 3 in all of them; the service keeps its own
copy, `metastore.BundleVersion`, pinned to `dagql`'s capture by a test).
`v1.0.0-beta.23` has a different `dagql/cache.go`, `dagql/cache_egraph.go`
and persistence code. That's advice on what to try, not a verification: a
green run is.

CI's pull requests run the manifest's first accept line (`STOCK_ENGINE`,
read with [`engines.sh pin`](engines.sh) in each job of
[`remote-cache-tests.yml`](../../.github/workflows/remote-cache-tests.yml)),
and warn when this branch's client differs from it: the job compares
`engine/remotecache`, `cmd/engine/remotecache.go`,
`engine/server/remote_cache*.go` and the bundle code in `dagql` with the
release's, ignoring only blank lines. The wider check, over all of
`dagql/cache*.go`, against an upstream release tag:

```bash
git fetch --depth=1 https://github.com/dagger/dagger "refs/tags/<tag>:refs/tags/<tag>"
git diff --stat <tag> HEAD -- engine/remotecache cmd/engine/remotecache.go \
  'engine/server/remote_cache*.go' 'dagql/cache*.go' ':!**/*_test.go'
```

The same client shows only `dagql/cache_transfer_types.go`, with that
constant.

**Verifying a new version.** The release check does this every hour for
each new upstream release ([Release check](#release-check)). By hand: run
the workflow with
`candidate-version` and `candidate-digest` (the image index digest:
`docker buildx imagetools inspect --format '{{.Manifest.Digest}}'
registry.dagger.io/engine:<version>`). Its e2e and sidecar jobs add the
candidate as the first accept line, verified by that run
(`engines.sh candidate`), so the run's service accepts it and the jobs run
it. Once the run is green, commit the line the run printed as a warning,
`accept <version> <digest> verified: <run URL>`. If it fails, commit a
`reject <version> <issue>` line with an issue explaining why. The manifest
holds at most 100 `accept` lines; its parser and `TestPinsMatchManifest`
fail the unit tests on anything else, including a digest-pinned engine in
these docs, scripts or workflows that isn't an accept line. Once the line
is on `feature/remote-cache`, its push's release run verifies it again,
with the whole accept list, and moves the version's
`dagger-remote-cache:<version>` tag to that build.

Run it as the engine container, pinned by digest (this is an accept line), with
the service's URL, the settings `cmd/cacheservice/e2e` starts its engines
with (`cmd/cacheservice/e2e/engine.go`):

```bash
docker run -d --name dagger-engine-rc --privileged \
  -e _EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL=https://<service> \
  -e DAGGER_CLOUD_URL=https://<service> \
  -e DAGGER_CLOUD_TOKEN=<engine token> \
  -v dagger-engine-rc:/var/lib/dagger \
  registry.dagger.io/engine:v1.0.0-beta.25@sha256:39abecfe0eba4c91035dcfc9f72c8a3963ae60481326082b48d78f2230202834
```

`DAGGER_CLOUD_TOKEN` must be set (any string but `oidc`), or the client stays
off. On a host with little free disk, also mount `{"gc":{"enabled":false}}`
at `/etc/dagger/engine.json`, or the engine can prune entries before the
service exports them (see
[Running it as a sidecar](../../cmd/cacheservice/README.md#running-it-as-a-sidecar)).

Use the image's own CLI, which matches the engine:

```bash
docker cp dagger-engine-rc:/usr/local/bin/dagger ./dagger
```

That is a Linux binary. On another host, such as macOS, install the same
release's CLI instead:

```bash
curl -fsSL https://dl.dagger.io/dagger/install.sh | DAGGER_VERSION=v1.0.0-beta.25 BIN_DIR=. sh
```

Then run it against that engine, with `DAGGER_CLOUD_TOKEN` and
`DAGGER_CLOUD_URL` set for the CLI too, and `TRACEPARENT` unset: the engine
sends the session's spans to the CLI's `DAGGER_CLOUD_URL`, and a CLI that
doesn't send them there, or that runs under an inherited, unsampled
`TRACEPARENT` (some CI and sandboxes export one), leaves the service with
nothing to export, quietly (see
[Running it against a local engine](../../cmd/cacheservice/README.md#running-it-against-a-local-engine)):

```bash
env -u TRACEPARENT -u TRACESTATE \
  _EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://dagger-engine-rc \
  DAGGER_CLOUD_URL=https://<service> DAGGER_CLOUD_TOKEN=<CLI token> \
  ./dagger call ...
```

Without `_EXPERIMENTAL_DAGGER_RUNNER_HOST`, the CLI provisions its own
engine, which gets the CLI's token but not the service's URL, so it never
connects to the service: run the engine yourself, as above.

To work on the client itself, `dev.sh e2e` builds the engine from this
checkout (below); nothing else here does.

## Real engines: `dev.sh e2e`

```bash
./hack/remote-cache/dev.sh e2e
```

One command runs the remote cache end to end with real engines
([#29](https://github.com/zerobull-consulting/dagger/issues/29)), then
the [scenarios](#scenarios) that check the paths beyond a first cache hit.
The engines are a stock Dagger release, the one CI's sidecar demo runs
([zerobull-consulting/dagger#112](https://github.com/zerobull-consulting/dagger/issues/112)):
their remote cache client is the one users run. The service is built from
this checkout, in the test process.
It is a Go test, `TestRealEngines` in
[`cmd/cacheservice/e2e`](../../cmd/cacheservice/e2e/); `dev.sh e2e` starts
MinIO and runs it with `CACHESERVICE_E2E=1`, which it skips without. The
`Remote cache tests` workflow runs it on every pull request.

Each run starts from nothing without deleting anything: it works under its
own prefix in the bucket (`test/e2e/<run>/`), and gives each engine a new, empty
cache volume. It:

1. builds the run's engine image on the release image `E2E_ENGINE_IMAGE`
   (`registry.dagger.io/engine:v1.0.0-beta.25`, pinned by digest), adding only
   `{"gc":{"enabled":false}}` and a `mirror.gcr.io` mirror for `docker.io`
   in `/etc/dagger/engine.json`, and the CA bundle of `E2E_CA_FILE` if set;
   and copies the `dagger` CLI out of it (`docker create`, then `docker cp`
   of `/usr/local/bin/dagger`), so the CLI matches the engine. With
   `E2E_BUILD_ENGINE=1` it first builds `dagger-engine`, `dagger-init-session`
   and the CLI from this repo, and copies them over the release image too: for
   work on the engine's client itself (see [Engine
   builds](#engine-builds-e2e_build_engine1));
2. starts a recording proxy to MinIO, and the cache service, both **in the
   test process**, on the Docker bridge gateway with free ports. The service
   (`server.Service`) is configured with the proxy as its S3 endpoint, so
   the presigned URLs it hands out point at the proxy, and every request an
   engine makes to the store is recorded, with its time and status. MinIO
   itself can stay on loopback. The main service has no tokens file: it is
   unauthenticated, so only the bridge reaches it, and every engine is in the
   run's scope (`e2e`), whose blobs are at `test/e2e/<run>/ns/e2e/blobs/`.
   The run's prefix is new, so the service's startup check finds it empty
   and claims it (`test/e2e/<run>/cacheservice.json`); every check of the
   store (`Env.CheckConsistent`) also checks that the marker is there and
   that every key under the prefix is of the service's shapes;
3. starts engines A and B in containers, each with
   `_EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL`, `DAGGER_CLOUD_TOKEN` and
   `DAGGER_CLOUD_URL` pointing at the service;
4. runs a pipeline on A with the CLI (whose `DAGGER_CLOUD_URL` and token point
   at the service too): `alpine`, then two `withExec`s that each write 4MB of
   random data and a random stamp, then reads the stamps;
5. waits until the service has stored A's entries, and merged them into B;
6. runs the same pipeline on B, then one more exec on B that checksums both
   layer files;
7. waits until the service has stored B's new entries and merged them into
   A, and reads the index again.

The test reads the service's state through its Go API (`Engines`,
`Entries`, `Imports`, `Export`, `Import`), the index and bundles through
`metastore`, what the service logged through a recording `slog` handler, and
what the engines read and uploaded from the proxy's record.

It passes when B's stamps are A's (the execs didn't run again), B's
`withExec` spans are hits, the layer files on B have A's checksums, B's
engine read every layer of A's chain from the store, and the index holds A's
entries, unchanged, plus only the entries B's verify exec built, each once:
none of B's entries is one it imported, every service number handed out is
an entry, and no merge into A carries A's own entries back
([#35](https://github.com/zerobull-consulting/dagger/issues/35)). It logs the
wall-clock time of each run. What a run showed is in
[`cmd/cacheservice/README.md`](../../cmd/cacheservice/README.md#real-engine-run).

### Scenarios

After that first part, on the same service and engines, it runs the
scenarios as subtests, in this order. A scenario that needs other service
settings runs against a **replica**: a second service on the same bucket
prefix and scope, as another replica of the service would be. It learns of
the main service's exports by polling the index.

| Scenario | Issue | What it does | Passes when |
| --- | --- | --- | --- |
| `reexport` | [#30](https://github.com/zerobull-consulting/dagger/issues/30) | Exports A's stored entries again with `Service.Export`, with every root and part the tracker knows of. Then runs a second pipeline on A over the same `alpine` (one exec writing 2MB). | The repeat stores every root, uploads no blob, finds every blob present, leaves the index as it was (numbers and bundles) and writes no bundle object. The second pipeline uploads only its own layers: `alpine`'s is in its bundle, and not among the blob PUTs. |
| `renew` | [#31](https://github.com/zerobull-consulting/dagger/issues/31) | Starts the replica, with GETs that last `E2E_GET_EXPIRY` seconds and an import poll of `E2E_IMPORT_POLL` seconds, and engines C and D on it, empty. Once both have A's entries merged, waits for the GETs to expire, and runs the verify exec on C. Then deletes the last exec's layer from MinIO, and runs it on D. | C sends `renew`, the replica answers with fresh GETs, the store serves C every layer of A's chain, with no blob GET before the renew and none refused, and C's files are A's. D's `renew` of the chain with the deleted layer answers `unavailable`; D's query still succeeds, with exec1's files A's and exec2's new (it ran again). |
| `prune` | [#32](https://github.com/zerobull-consulting/dagger/issues/32) | Prunes C's cache with `{ engine { localCache { prune } } }` (with no arguments, it drops every releasable entry), waits 5 import polls, syncs C 3 times by hand (`Service.Import`), then restarts C on the same volume. Starts the replica and C if `renew` didn't run. | The prune reaches the replica (`engine.prune`) and the importer forgets the pruned values; nothing is merged or offered to C over the polls and syncs; after the restart (a new generation), the replica merges the entries into C again. [#47](https://github.com/zerobull-consulting/dagger/issues/47): C's bundles that hold entries it imported (its `renew` verify exec) still build a merge once those imports moved to C's new entries, though resolving them through C's current mappings would fail; and no merge so far failed with "is no longer stored". |
| `namespaces` | [#10](https://github.com/zerobull-consulting/dagger/issues/10) | Starts its own service, with a tokens file (`ServiceOptions.TokensFile`) of two tokens in namespaces `team-a` and `team-b`, and engines NA and NB on them, empty (`StartEnginesWithTokens`; each runs its CLI with its engine's token, since session spans come under the CLI's). Runs a pipeline of its own on NA (one exec writing 1MB, with a nonce), then on NB once NA's entries are stored. Starts engine NC on NA's token, waits for NA's entries to be merged into it, and runs the pipeline on NC. Then removes team B's token from the file and reloads it (`Service.ReloadTokens`, as on `SIGHUP`). | Each engine is in its token's namespace. Nothing is merged into NB, whose exec runs itself (no hit, its own stamp); neither namespace's index maps the other's engine; NA's exec layer is stored under `ns/team-a/blobs/` only, and `alpine`'s layer under both namespaces' prefixes. NC gets only NA's bundles, and hits NA's exec with NA's stamp. After the reload, NB's connection is closed (logged revoked), NB's engine logs the close with status 4001, its reconnection is refused (401), the service lists NA and NC only, and it logged nothing at error level. |
| `verify` | [#23](https://github.com/zerobull-consulting/dagger/issues/23) | Rewrites every blob A uploads to other content of the same length in the S3 proxy (`SameSizeCorruption`, on `/staging/` PUTs), runs a new pipeline on A (an exec writing 1MB), and once its export has failed, lifts the rewrite. | S3 took each rewritten upload, and the service rejected each as a mismatch; none reached its digest key; nothing of A's was published while the uploads were corrupted. Once lifted, the tracker's retry asked for each rejected blob again and stored the right content (read back from MinIO and hashed), the export was stored, S3 is consistent, and B gets the pipeline's exec as a cache hit with A's output. |
| `gc` | [#11](https://github.com/zerobull-consulting/dagger/issues/11) | Starts its own service with a tokens file (two tokens in namespace `gc`), a namespaces file giving `gc` a max age of 10s (`ServiceOptions.NamespacesFile`), and a GC grace period of 5s (`GCGrace`); GC runs only when the scenario asks (`Service.CollectGarbage`). Runs a pipeline of its own on engine GA (one exec writing 1MB, with a nonce), waits past the max age and the grace period, and runs GC twice. Then starts engine GB, empty, raises the max age to 10m and reloads the file (`Service.ReloadLimits`, as on `SIGHUP`), and runs a build on top of the first on GA, then on GB. | GC expired GA's entries (the index has none) and deleted their bundles, resolutions and every layer blob from MinIO; the run's scope still has A's chain; nothing is merged into GB. The build on top uploaded every deleted layer again and was stored; GB got it merged with each root's expiry capped at its recording time plus the max age, and hit it with GA's stamp. S3 is consistent throughout (`Env.CheckConsistentIn`), in namespace `gc` and in the others GC also collected: the run's scope and the `namespaces` scenario's. It adds about 25s to the run, most of it waiting out the max age and grace period. |
| `superseded` | [#32](https://github.com/zerobull-consulting/dagger/issues/32), [#27](https://github.com/zerobull-consulting/dagger/issues/27) | Writes a Dang module (`ttlmod` in the run's directory) whose function `stamp2` has a cache TTL of `E2E_TTL` seconds, runs it on A, waits for it to expire, and runs it again. Then starts engine F, empty, waits until both bundles are merged into it, and reads the value on it. | A replaced the value in place, the index kept its service number and moved it to a newer bundle, and F's call resolved to the imported entry (its `withExec` hit; a function with a TTL is evaluated on every call). It reports which record F got by the layer it read: the newer one passes; the older one is logged as `GAP` (#27), without failing; neither fails. F, the first engine started after `prune` moved C's imports, gets C's bundles holding them merged, with their roots and the imported entries (#47). |
| `parts` | [zerobull-consulting/dagger#133](https://github.com/zerobull-consulting/dagger/issues/133) | Three sessions on A, with a nonce. One: exec V, which writes the nonce to its stdout (its `execMeta` part) and to `/v` (`fs`). Engine F1, empty, then imports V. Two: exec W over V. Three: exec Y over X, alpine with a variable set, which the engine doesn't retain. Engine F2, empty, then imports. F1 and F2 read V; F2 reads X's variable, then Y's stdout. | A stored V as a root with its parts, and W's bundle holds V's record with fewer (the engine exports only the roots' outputs), and the index kept V's first record and parts all the same; before #133's fix, V's entry moved to W's bundle with `fs` only. F1 and F2 read A's V (stdout and file) as a cache hit, with no exec run. The store holds X only in Y's bundle, with no parts, and F2 still reads X's variable as A set it: a partial import doesn't fail. F2 hits Y with A's stdout. |
| `streaming` | [zerobull-consulting/dagger#134](https://github.com/zerobull-consulting/dagger/issues/134) | A runs one session with a nonce: an exec, then `sleep 40` over it. | The first exec's entry is stored while the session still runs (the sleep), within `-export-settle` plus one scheduler tick, plus the export itself, of the last span naming it; then A's retained entries are all stored once the session ends. |
| `readonly` | [#65](https://github.com/zerobull-consulting/dagger/issues/65) | Starts a second service with `-export=false` (`ServiceOptions.ReadOnly`), on an S3 proxy of its own and, on MinIO, with the read-only user `dev.sh` creates, and engine R, empty, on it. Waits until A's roots are merged into R, runs A's pipeline on R, then a pipeline of R's own, and waits past `-export-max-delay` (13s), by which a writer would have exported it. | R's `withExec` calls hit, with A's stamps; no request but GET and HEAD reached the service's proxy (the other services, writers still running, use the run's); the index has no mapping of R's cache and no entry from it; the service tracks nothing (`ErrReadOnly`) and runs no GC (`ErrGCReadOnly`); `CheckConsistent` passes. |

The resilience scenarios ([#12](https://github.com/zerobull-consulting/dagger/issues/12))
run last, since they break things on purpose. Each ends with two checks:
S3 is consistent (read from MinIO directly: the index names no bundle that
isn't stored, and no stored bundle names a layer blob that isn't, besides
the one `renew` deleted), and the engines work: B runs the pipeline A built
during the fault, and gets its exec as a cache hit with A's output. They
inject faults with the S3 proxy (`Fault`: an S3 error for some requests, or
a stall until removed), `Service.CloseEngine`, `Engine.RestartAsync`,
`Env.RestartService` and `Service.DrainFor`.

| Scenario | What it does | Passes when |
| --- | --- | --- |
| `disconnect` | Stalls blob PUTs (to staging keys) in the S3 proxy, runs a new pipeline on A (an exec writing 1MB), and once its export's upload is stalled, drops A's WebSocket (`Service.CloseEngine`, no close frame). Then ends the stall. | A reconnects (same cache and generation, new connection); the export on the dropped connection is logged interrupted, not failed; nothing of it was published while it was cut; it runs again on the new connection and is stored, with the layer that was in flight. |
| `engine-restart` | As `disconnect`, but restarts A's container (`docker restart`) while the upload is stalled. | A restarted cleanly (same cache ID, generation one higher); the cut export was interrupted, not failed, and published nothing; it ran again on the new generation, under the same entry numbers, and was stored as that generation's, with the layer that was in flight. |
| `s3-faults` | First injects, for one export of a new pipeline on A, 503 `SlowDown` for 2 blob PUTs (the engine's, to staging keys), 500 `InternalError` for 1 bundle PUT and 503 `SlowDown` for 2 index PUTs (the service's). Then answers every S3 request with 503 while A runs another pipeline, until its export has failed, and lifts it. | Each faulted request was made again and succeeded: the blob PUTs by the export path, which retries the export with backoff (1s, then 2s) under a new stage (so at another key: the same blob's), the bundle and index writes by the service's S3 client. The first export is stored, and none failed. While S3 was down, the export failed and published nothing (the index and bundles as before); once back, it was stored. |
| `service-restart` | Stalls blob PUTs, runs a new pipeline on A, and once its upload is stalled, stops the main service and starts a new one on the same address and scope 2s later (`Env.RestartService`). Then runs the pipeline on A again. | A and B reconnect with their backoff; the new service merges into B every bundle B had that is still live and mergeable, once (none more over 3 syncs by hand), and nothing of A's own into A; A's next session is exported and stored. The export cut by the restart, a stop without a drain, is not tried again by the new service: logged as `GAP` ([#45](https://github.com/zerobull-consulting/dagger/issues/45)), without failing (`drain` covers the clean stop). Bundles B had that expired since aren't expected; every other one must build a merge, and C's bundles holding its moved imports must be merged into B again ([#47](https://github.com/zerobull-consulting/dagger/issues/47)). Last, no merge in any service of the run failed with "is no longer stored". |
| `drain` | Stalls blob PUTs, runs a new pipeline on A, and once its upload is stalled, drains the main service (`Service.DrainFor`, as SIGTERM does, with a 2m deadline) and ends the stall once the drain sees the export in flight. Then stops the service, starts a new one on the same address 2s later, and attaches A and B to it ([#63](https://github.com/zerobull-consulting/dagger/issues/63)). | While the drain waited, A stayed connected and nothing of the export was published; the drain completed, with A's roots stored and none left, and the export was stored before it returned, with the layer that was in flight. The new service merges that export into B without any session naming its entries again, and B gets the exec as a cache hit with A's output: the `GAP` of `service-restart` doesn't apply to a drained stop. S3 is consistent while draining, after it, and after the restart. A real engine sends its session's end at once, so the stop comes during the export, not during a quiet period; the in-process and binary tests cover the quiet period. |
| `otlp-sizes` | Runs last, and starts nothing: reads what every service of the run logged of the OTLP requests it took and refused ([zerobull-consulting/dagger#77](https://github.com/zerobull-consulting/dagger/issues/77)). | No real-engine OTLP request was refused, and the largest on each route, as received and decoded, and the most of the decode budget in use at once, are at most a tenth of their limit (`otlpMargin`). It logs them, so the limits can be checked against what engines send (see [OTLP limits](../../cmd/cacheservice/README.md#otlp-limits)). |

They add about 42s to the run: see
[`cmd/cacheservice/README.md`](../../cmd/cacheservice/README.md#resilience)
for what each costs, and for the cases of #12 tested only with fake engines
(a stalled upload, which the engine gives up on after a minute without
progress, and oversized bundles).

Each check is logged as `ok` or `FAIL`; a failed check fails its scenario
without stopping it. Run some scenarios only with
`E2E_SCENARIOS="renew prune" ./hack/remote-cache/dev.sh e2e` (the names:
`reexport`, `renew`, `prune`, `superseded`, `parts`, `streaming`, `readonly`, `namespaces`, `verify`, `gc`, `disconnect`,
`engine-restart`, `s3-faults`, `service-restart`, `drain`), or pass `go
test` flags after `e2e`, such as `-run 'TestRealEngines/^renew$'`. The first
part always runs.

Each run's files are in `E2E_DIR/runs/<run>/`: each service's log
(`main.log`, `replica.log`, `namespaces.log`, `gc.log`, `main-2.log` and
`main-drained.log` after the restarts), the `namespaces` scenario's
tokens file (`tokens`), the `gc` scenario's tokens and namespaces files
(`gc-tokens`, `gc-namespaces`), each CLI call's output and log, the index and
importer state at each step, and, when the run failed, each engine's log.
When the test ends it removes the engines, their volumes and the run's
objects in the bucket; with `E2E_KEEP=1` it leaves them, and `dev.sh
e2e-down` removes the engines and volumes later.

| Setting | Default | Meaning |
| --- | --- | --- |
| `E2E_DIR` | `$TMPDIR/dagger-remote-cache-e2e` | The image's build context (`image/`), the CLI copied out of each engine image (`cli/<image ID>/`; another image's copy is removed once unused for a day), the binaries `E2E_BUILD_ENGINE=1` builds (`bin/`), and each run's files (`runs/`) |
| `E2E_ENGINE_IMAGE` | The manifest's first accept line, as CI's `STOCK_ENGINE`: `registry.dagger.io/engine:v1.0.0-beta.25`, pinned by digest (`DefaultEngineImage`) | The release image the run's engine image is built on. A stock image whose version the service doesn't accept fails the run at once, saying so |
| `E2E_BUILD_ENGINE` | unset | `1` builds the engine, `dagger-init-session` and the CLI from this repo and copies them over `E2E_ENGINE_IMAGE` |
| `E2E_SKIP_BUILD` | unset | With `E2E_BUILD_ENGINE=1`, `1` reuses the binaries in `E2E_DIR/bin`; without it, the run fails |
| `E2E_IMAGE` | `dagger-remote-cache-e2e-engine` | The run's engine image's tag. Engines start from the image's ID, so a run building the same tag meanwhile doesn't change a running one's engine |
| `E2E_CA_FILE` | unset | A CA bundle to add to the engine image, for hosts behind a TLS-intercepting proxy |
| `E2E_TIMEOUT` | `180` | Seconds to wait for each export, and each merge |
| `E2E_SCENARIOS` | all | The scenarios to run after the first part, which always runs |
| `E2E_GET_EXPIRY` | `5` | Seconds the replica's presigned GETs last (`renew`) |
| `E2E_IMPORT_POLL` | `1` | Seconds between the replica's import polls (`prune`) |
| `E2E_TTL` | `10` | The cache TTL of the `superseded` scenario's function, in seconds. The value has to reach engine F and be read there before it expires; the scenario fails asking for a longer one if it doesn't |
| `E2E_KEEP` | unset | `1` leaves the engines and the run's objects in the bucket |

`E2E_BASE_IMAGE`, the image built binaries used to be copied over, is now
`E2E_ENGINE_IMAGE`; a run with it set fails, saying so, rather than testing
another engine than asked.

### Engine builds: `E2E_BUILD_ENGINE=1`

The default run tests the stock engine's client, not this branch's. Where
they differ (this branch merges upstream main, whose client can be ahead of
the release), the `real engines (e2e)` job and the `sidecar demo` job both
warn, from one script
([`compare-client.sh`](compare-client.sh), which takes `STOCK_ENGINE` and
`STOCK_ENGINE_TAG` from the environment): `the engine's side of the remote cache differs from v1.0.0-beta.25's`,
with the files (the client, its adapter in `engine/server`, and the `dagql`
bundle code). The step fails only on a pin that contradicts itself: a
`STOCK_ENGINE_TAG` that isn't `STOCK_ENGINE`'s tag, or a digest the registry
doesn't give for that tag. To test this branch's client, run:

```bash
E2E_BUILD_ENGINE=1 ./hack/remote-cache/dev.sh e2e
```

It builds the three binaries with `CGO_ENABLED=0` (the engine with the
`dfexcludepatterns,dfparents` tags) and copies them over `E2E_ENGINE_IMAGE`,
whose other files (`dagger-init`, `crun`, `tini`, the QEMU helpers) stay the
release's. Each run logs the CLI's `dagger version` and every engine's
version from its hello (`engine A connected to main: engine version
"v1.0.0-beta.25"`), so a log says which engine it tested.

An engine built this way reports a development version, which no accept
line names, so the run's services welcome it with
`-allow-engine-version-mismatch` (`AllowEngineVersionMismatch`), and the run
prints a `::warning::` saying so: CI's job output shows it as an annotation.

The pin is the manifest's first accept line: `DefaultEngineImage` in
`cmd/cacheservice/e2e/env.go` is read from it, and so is CI's
`STOCK_ENGINE` (`engines.sh pin`), which CI passes in as `E2E_ENGINE_IMAGE`.
`TestDefaultEngineImageMatchesCI` (in the unit job) fails when the two
disagree. To move the pin, reorder the manifest's accept lines.

Notes:

- It needs Go, and Docker with Compose v2, on Linux: the test listens on the
  Docker bridge gateway, which must be one of the host's own addresses. The
  engines run `--privileged`.
- The CLI runs with a clean environment: no `TRACEPARENT`, and no `OTEL_*`
  or `DAGGER_*` settings of the caller. Some CI runners and sandboxes export an unsampled `TRACEPARENT`; the
  CLI then continues that unsampled trace, the engine records no session
  spans, and the service never hears of any entry. Watch for this when
  pointing your own CLI at the service.
- Where Docker Hub rate-limits pulls, pull the MinIO image through
  `mirror.gcr.io` and tag it as the name in `compose.yaml`. The engines pull
  `alpine` through `mirror.gcr.io` already.

## The fork's paths

The fork changes upstream only where the service needs it, so merges from
upstream don't conflict and the service can move to a project of its own
([zerobull-consulting/dagger#117](https://github.com/zerobull-consulting/dagger/issues/117),
D7). `check-fork-paths.sh` enforces this: it diffs `HEAD` against the
upstream commit the fork last merged and fails on any changed path
`fork-paths.txt` doesn't allow, listing each one and how to fix it.

```sh
hack/remote-cache/check-fork-paths.sh       # needs network access to github.com/dagger/dagger
hack/remote-cache/check-fork-paths_test.sh  # the check's own tests, offline
```

Both need git, Go (for `go mod edit -json`), jq and coreutils' `timeout`.

**The base** is the merge base of `HEAD` with upstream `main`, which the fork
tracks. Upstream tags releases on `main`, so after a merge from a release tag
the base is that tag's commit (or a newer `main` commit). The script fetches
upstream shallowly and deepens both histories in bounded steps until it finds
the base, and prints the commit it picked. If `HEAD` contains an upstream
`v*` tag from off `main`, there's no single base, and the check stops: set
`FORK_BASE` to the full id of the commit to measure against (in CI, the
`FORK_PATHS_BASE` repository variable; clear it once upstream `main` is
merged again). A release tag isn't the base
on its own: the fork tracks `main`, so diffing against a tag would count
every upstream change since that tag as the fork's.

**The allowlist**, `fork-paths.txt`, has one entry per line:
`<rule> <path> <reason>`. A path is a file, or a directory ending in `/`; no
globs. The rules:

- `any`: the fork may add, change or delete it. The service
  (`cmd/cacheservice/`), its tooling (`hack/remote-cache/`), the sidecar
  action, and the fork's own workflows, each named.
- `deleted`: the fork deletes this upstream file and does nothing else to it
  (`.github/workflows/changelog.yml`).
- `go-mod`: the fork only adds requirements to this `go.mod`; upstream's keep
  their versions and every other directive is unchanged.
- `add-lines`: the fork only adds lines (`go.sum`, `.gitignore`).

Every entry needs a reason, and an entry no changed path needs fails the
check, so the list stays small. To change it, edit `fork-paths.txt` in the
pull request that needs it and say why there. Prefer moving the change under
the service's paths, or upstreaming it, to a new entry.

The `fork paths` job of `.github/workflows/remote-cache-fork-paths.yml` runs
the tests and the check on every pull request into `feature/remote-cache`
and every push to it. On a pull request it runs the base branch's checker,
so a pull request can't weaken the check that measures it; a change to the
checker takes effect once merged. The allowlist is the pull request's, since
that's where an entry is added, and the job flags any change to it with a
warning for the reviewer.

## Automation tokens

The release check and the nightly upstream sync
([zerobull-consulting/dagger#117](https://github.com/zerobull-consulting/dagger/issues/117),
D5, steps 3 and 4) open pull requests that need CI. GitHub starts no
workflows for pushes or pull requests made with a workflow's own
`GITHUB_TOKEN`, so they use fine-grained personal access tokens instead.
The release check ([Release check](#release-check)) uses them; the nightly
sync is still to be built. Each token is an environment secret:

| Token | Permissions | Environment | Secret | Used by |
|---|---|---|---|---|
| `dagger-cache-bot-pr` | Contents: write, Pull requests: write | `cache-bot-pr` | `CACHE_BOT_PR_TOKEN` | the release check's `accept` and `reject` PRs |
| `dagger-cache-bot-sync` | Contents: write, Pull requests: write, Workflows: write | `cache-bot-sync` | `CACHE_BOT_SYNC_TOKEN` | the nightly sync, and the release check's rebuild on a new upstream tag |

- **Scope.** Both tokens are owned by `zerobull-consulting` and reach this
  repository only.
- **Why two.** Merges from upstream regularly change `.github/workflows/`,
  and GitHub refuses that push without Workflows permission. The
  accept-line PR changes only `cmd/cacheservice/compat/engines.txt`, so its
  token can't touch CI.
- **Expiry.** Each token expires 90 days after it's created; the token's
  settings page shows the date. Rotate both together before the earlier
  expiry: regenerate each one with the same permissions, then update its
  secret.
- **Environments.** Neither has reviewers or a wait timer, and both deploy
  only from `feature/remote-cache`, the default branch. Scheduled runs,
  and dispatches on `feature/remote-cache`, get the secrets. A dispatch on
  any other ref doesn't, and neither does a `pull_request` run, whose ref is
  the pull request's. A job reads a token with
  `environment: cache-bot-pr` (or `cache-bot-sync`).
- **Never from `pull_request_target` or `workflow_run`.** Those run on the
  default branch's ref, so the branch rule would let them in, while they can
  run a pull request's code. Request these environments only from
  `schedule` and `workflow_dispatch` jobs.

**Neither token merges anything:**
- The release check's `auto-merge` job enables squash auto-merge on the
  accept-line PR with `GITHUB_TOKEN`, and only while the repository
  variable `RELEASE_CHECK_AUTO_MERGE` is `true`. GitHub then merges it once
  the required checks pass.
- The `accept line only` check fails any `release-check/accept/*` PR that
  changes more than one added accept line, and turns its auto-merge off.
  Set `RELEASE_CHECK_AUTO_MERGE` only once that check is in the ruleset's
  required checks: the five required before it don't stop a PR changing
  more than the manifest.
- That merge starts no workflows, so the release check's next run
  dispatches `cacheservice-publish.yml` on `feature/remote-cache` itself,
  with `GITHUB_TOKEN` and `actions: write` (`workflow_dispatch` is the
  exception that does start a run). That moves the floating
  `:<engineVersion>` tags.
- The tokens act as their owner, who can bypass the ruleset. A merge made
  with one might not wait for the checks, so they are only used to push and
  to open PRs. Removing that risk is
  [zerobull-consulting/dagger#152](https://github.com/zerobull-consulting/dagger/issues/152).

**Merges from upstream use a merge commit, never a squash.** That's the
nightly sync PR, and the release check's rebuild PR (outcome B), which
always merges the tag: the check doesn't rebuild on a tag the branch
already contains.
Either has to keep the upstream commit as a parent, or the next sync
conflicts again and the `fork paths` base stays old (see "The fork's
paths"). For this, the repository allows merge commits and the ruleset
doesn't require linear history. Squash stays the method for every other
pull request.

Other settings these flows rely on:
- **Allow auto-merge** is on.
- **Allow GitHub Actions to create and approve pull requests** is off. The
  tokens open every PR.
- Issues the flows file (`compat-break`, upstream drift) use `GITHUB_TOKEN`
  with `issues: write`.

**Checking a token** on your own machine, without it reaching your shell
history (bash; in zsh, `read -rs "GH_TOKEN?token: "`):

```sh
read -rsp 'token: ' GH_TOKEN && echo && export GH_TOKEN
gh api repos/zerobull-consulting/dagger --jq .full_name       # zerobull-consulting/dagger
gh api repos/zerobull-consulting/mcp-github-readonly --jq .full_name  # HTTP 404
unset GH_TOKEN
```

The 404 shows the token reaches no other repository, but only because
`mcp-github-readonly` is private: a fine-grained token can read any public
repository. Probe a private repository of the org.

Skip `--jq .permissions`: for a fine-grained token it shows the owner's
role on the repository, not the token's permissions.

## Release check

`.github/workflows/remote-cache-release-check.yml` verifies each new
upstream Dagger release against the cache service, once per release
([zerobull-consulting/dagger#117](https://github.com/zerobull-consulting/dagger/issues/117),
D5). `release-check.sh` does the work and documents each step; the workflow
only decides when it runs and with which token.

**Each hour** (minute 17, and by hand):

1. **Publish what nothing published.** If `feature/remote-cache`'s head has
   no `cacheservice-publish.yml` run (an auto-merge by `GITHUB_TOKEN`
   starts none) and is older than 10 minutes, it dispatches one, which
   moves the floating `dagger-remote-cache:<version>` tags.
2. **Poll.** `git ls-remote --tags https://github.com/dagger/dagger` for
   `v1.*`; a tag must match `^v1\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$` and be
   a version `engines.sh` takes. A release is **new** when it is newer than
   every version the manifest names (`base`, `accept` and `reject` lines):
   older releases (`v1.0.0-beta.2` to `beta.23`) are never tried. The oldest
   new release is handled first, and only one at a time:
   - if a release-check PR for it is open, the run waits for it (and turns
     auto-merge on for an accept PR once the gate is on);
   - if its image isn't on `registry.dagger.io` yet, it's skipped this hour,
     and the next one is tried. Only a multi-platform index digest counts;
     a registry error fails the run rather than skipping.
3. **Verify (A).** It dispatches `remote-cache-tests.yml` on
   `feature/remote-cache` with `candidate-version` and `candidate-digest`,
   and waits for it (at most 150 minutes): the e2e and the sidecar demo of
   the service as it is, against that stock engine. With the tag merged into
   a scratch copy of the branch, it runs `TestBundleVersionMatchesEngineCapture`.
   It writes the advisory diff of the engine's side of the remote cache from
   the manifest's `base` to the release, and whether the bundle or protocol
   version changed. **The run is the gate**; the diff only informs.
   A run that is cancelled, or that the check can't read, is an error: it
   never leads to a reject line.
4. **The outcome:**
   - **A, both green:** a PR from `release-check/accept/<version>`, as
     `CACHE_BOT_PR_TOKEN`, adding only
     `accept <version> <digest> verified: <run>` as the first accept line,
     so the PR's own CI runs that engine too. With the gate on, it
     auto-merges (squash).
   - **B, otherwise:** `release-check/rebuild/<version>`, as
     `CACHE_BOT_SYNC_TOKEN`: the tag merged with a merge commit, the `base`
     line moved. That branch is verified the same way (a dispatch on it),
     and if green, gets the accept line, citing that run, and a PR. If the
     bundle or protocol version changed, the accept list resets to that one
     version. **A human merges it, with a merge commit.** No rebuild is
     tried if the merge conflicts, or if the branch already contains the
     tag: that rebuild would be the code A ran.
   - **C, the rebuild red or not tried:** an issue titled
     `compat-break: engine <version> fails against the cache service`,
     labeled `compat-break` (with `GITHUB_TOKEN`), with both runs' failing
     jobs and the advisory diff; then `release-check/reject/<version>`, as
     `CACHE_BOT_PR_TOKEN`, adding `reject <version> <repo>#<issue>`. A human
     merges it.

**Once per release.** A release with an open release-check PR is waited
on; nothing newer is tried meanwhile. A release whose `compat-break` issue
is open but whose reject branch doesn't exist (a run that stopped between
them) gets its reject PR, without a second issue. Once a line for it is
merged, it's no longer new. A `release-check/*/<version>` branch with no
open PR (a PR closed by hand, or a run that died) **fails the run** with
the branch named: delete it to try that release again, or add its line by
hand. To verify a rejected release again, remove its `reject` line.

**Reading its PRs and issues.**
- **Accept** (`remote cache: accept engine <v> (release check)`): one line,
  the verifying run, the bundle test, the advisory diff. Squash it, titled
  as the description says.
- **Rebuild** (`remote cache: build on engine <v> (release check)`): the
  merge, the base move, the line, the run of the service as it is that
  failed and the run of the rebuild that passed. Merge it with
  `gh pr merge --merge`, never a squash: upstream's tag must stay a parent,
  or `fork paths` keeps the old base.
- **Reject** (`remote cache: reject engine <v> (release check)`) and its
  `compat-break` issue: the failing jobs of both runs, and the diff.

**The auto-merge gate.** `RELEASE_CHECK_AUTO_MERGE` (a repository
variable) stays unset until `accept line only` is a required check. Then:
1. `accept line only` has reported on a PR (it runs on every PR; any PR
   that isn't from `release-check/accept/*` passes it);
2. add it to the ruleset's required checks;
3. set `RELEASE_CHECK_AUTO_MERGE` to `true`.

The check, `.github/workflows/remote-cache-accept-line.yml`, runs the base
branch's copy of `accept-line-only.sh`, and fails an accept PR unless, from
its merge base, it changes only `cmd/cacheservice/compat/engines.txt`, by
one added line (no mode change, no rename), that line is
`accept <the branch's version> <digest> verified: <run URL>`, it is the
first accept line, and `engines.sh` takes the result. On failure it turns
the PR's auto-merge off. The release check's `auto-merge` job checks the
same before enabling it, and only on PRs from this repository's own
branches.

**Running it by hand.** Actions, "Remote cache release check", Run
workflow. Off `feature/remote-cache` it is always a dry run, which can't
reach the bots' environments: it polls, prints the plan, and prints each
push, PR and issue the outcome would make, after a real, local merge. On
`feature/remote-cache`, `dry-run` (default on) does the same. In a dry run:
- `verify` also runs the real verification (about an hour); it changes no
  file in the repository;
- `version` checks a chosen release, listed or not. For an accepted one,
  `engines.sh candidate` moves its own line first, with its digest, so the
  run runs it.

Locally, with `gh` logged in and Docker:

```sh
hack/remote-cache/release-check.sh --dry-run plan
hack/remote-cache/release-check_test.sh      # offline, fake gh, docker and go
hack/remote-cache/accept-line-only_test.sh   # offline
```

**What it never does.** It never merges, never pushes to
`feature/remote-cache`, and never gives a bot token to anything but a push
and the PR it opens (through git's environment, never its arguments).
Tags and digests from upstream and the registry are checked against strict
patterns before they reach a branch name, a file or a command, and every
call is bounded. The manifest is edited only through `engines.sh`
(`add-accept`, `add-reject`, `set-base`, `reset-accept`), which writes
nothing the service's parser would refuse (`TestEnginesScriptEditsParse`).
