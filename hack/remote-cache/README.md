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

`Remote cache tests` has two optional jobs, `S3 tests against Spaces` and
`real engines (e2e) against Spaces`. They are skipped unless these repository
secrets are set: `SPACES_ENDPOINT`, `SPACES_REGION`, `SPACES_BUCKET`,
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

## Engine image

The stock engine has no remote cache client. CI publishes one that has it, as
`ghcr.io/zerobull-consulting/dagger-engine`
([`engine-image.yml`](../../.github/workflows/engine-image.yml)),
`linux/amd64` only, with the same tags as the
[service's image](../../cmd/cacheservice/README.md#image):

- `:<commit SHA>` (the full SHA) for each push to `feature/remote-cache`;
- `:feature-remote-cache`, which moves to the newest of those.

It is the release engine image the fork last merged from main
(`registry.dagger.io/engine:v1.0.0-beta.24`, pinned by digest in
[`engine.Dockerfile`](engine.Dockerfile)), with four binaries built from this
repo copied over it: `dagger-engine`, `dagger-init`, `dagger-init-session`
and the `dagger` CLI. Everything else (crun, CNI plugins, qemu, tini, the SDK
manifests, `engine.json` with GC on) is the release's. The binaries report
the repo's next version, `v1.0.0-beta.25`, with the commit. When a merge from
main brings a new release, move `BASE_IMAGE` to it.

Run it as the engine container, with the service's URL:

```bash
docker run -d --name dagger-engine-rc --privileged \
  -e _EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL=https://<service> \
  -e DAGGER_CLOUD_URL=https://<service> \
  -e DAGGER_CLOUD_TOKEN=<engine token> \
  -v dagger-engine-rc:/var/lib/dagger \
  ghcr.io/zerobull-consulting/dagger-engine:feature-remote-cache
```

Then point the CLI at it, with `DAGGER_CLOUD_TOKEN` and `DAGGER_CLOUD_URL`
set for the CLI too, and `TRACEPARENT` unset (without them, the service never
learns the entries: see
[Running it against a local engine](../../cmd/cacheservice/README.md#running-it-against-a-local-engine)):

```bash
export _EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://dagger-engine-rc
```

Use the image's own CLI, which matches the engine:

```bash
docker run --rm --entrypoint cat ghcr.io/zerobull-consulting/dagger-engine:feature-remote-cache /usr/local/bin/dagger > dagger && chmod +x dagger
```

That CLI is stamped with its image's SHA tag as its default engine, so
without `_EXPERIMENTAL_DAGGER_RUNNER_HOST` it provisions exactly that engine
itself. A provisioned engine gets the CLI's `DAGGER_CLOUD_TOKEN` but not the
service's URL, so it doesn't connect to the service: run the engine yourself,
as above, to use the remote cache.

To build it from a fresh clone, at the repository root:

```bash
docker build -f hack/remote-cache/engine.Dockerfile \
  --build-arg ENGINE_IMAGE=dagger-engine -t dagger-engine .
```

`ENGINE_IMAGE` is the CLI's default engine: keep it the same as `-t`, so the
CLI copied out of the image provisions the local image (the CLI uses a local
image when there is one, and pulls only when there isn't). Without it, the
CLI's default is `registry.dagger.io/engine:v1.0.0-beta.25`, which doesn't
exist, and it needs `_EXPERIMENTAL_DAGGER_RUNNER_HOST`.
`--build-arg REVISION=<sha>` stamps the commit. CI passes both.
[`engine.Dockerfile.dockerignore`](engine.Dockerfile.dockerignore) keeps the
context to the packages the binaries build from.

[`engine-image-smoke.sh`](engine-image-smoke.sh) is CI's check, and runs the
same locally: the CLI's version, commit and default engine; the engine
connecting to the published service image (no store); and a container run
through it.

```bash
RUNNER_HOST=image://dagger-engine ./hack/remote-cache/engine-image-smoke.sh dagger-engine
```

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
| `readonly` | [#65](https://github.com/zerobull-consulting/dagger/issues/65) | Starts a second service with `-export=false` (`ServiceOptions.ReadOnly`), on an S3 proxy of its own and, on MinIO, with the read-only user `dev.sh` creates, and engine R, empty, on it. Waits until A's roots are merged into R, runs A's pipeline on R, then a pipeline of R's own, and waits past the quiet period and end grace (15s). | R's `withExec` calls hit, with A's stamps; no request but GET and HEAD reached the service's proxy (the other services, writers still running, use the run's); the index has no mapping of R's cache and no entry from it; the service tracks nothing (`ErrReadOnly`) and runs no GC (`ErrGCReadOnly`); `CheckConsistent` passes. |

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
`reexport`, `renew`, `prune`, `superseded`, `readonly`, `namespaces`, `verify`, `gc`, `disconnect`,
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
| `E2E_DIR` | `$TMPDIR/dagger-remote-cache-e2e` | The image's build context (`image/`), the CLI copied out of each engine image (`cli/<image ID>/`), the binaries `E2E_BUILD_ENGINE=1` builds (`bin/`), and each run's files (`runs/`) |
| `E2E_ENGINE_IMAGE` | `registry.dagger.io/engine:v1.0.0-beta.25@sha256:39abecfe…`, the workflow's `STOCK_ENGINE` | The release image the run's engine image is built on |
| `E2E_BUILD_ENGINE` | unset | `1` builds the engine, `dagger-init-session` and the CLI from this repo and copies them over `E2E_ENGINE_IMAGE` |
| `E2E_SKIP_BUILD` | unset | With `E2E_BUILD_ENGINE=1`, `1` reuses the binaries in `E2E_DIR/bin`; without it, the run fails |
| `E2E_IMAGE` | `dagger-remote-cache-e2e-engine` | The run's engine image's tag |
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
warn: `the remote cache client differs from v1.0.0-beta.25's`, with the
files. To test this branch's client, run:

```bash
E2E_BUILD_ENGINE=1 ./hack/remote-cache/dev.sh e2e
```

It builds the three binaries with `CGO_ENABLED=0` (the engine with the
`dfexcludepatterns,dfparents` tags) and copies them over `E2E_ENGINE_IMAGE`,
whose other files (`dagger-init`, `crun`, `tini`, the QEMU helpers) stay the
release's. Each run logs the CLI's `dagger version` and every engine's
version from its hello (`engine A connected to main: engine version
"v1.0.0-beta.25"`), so a log says which engine it tested.

The pin is `DefaultEngineImage` in `cmd/cacheservice/e2e/env.go` and
`STOCK_ENGINE` in the workflow's top-level `env`; CI passes `STOCK_ENGINE` in
as `E2E_ENGINE_IMAGE`, and `TestDefaultEngineImageIsCIs` (in the unit job)
fails when they differ. Update them together, with `STOCK_ENGINE_TAG`.

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
