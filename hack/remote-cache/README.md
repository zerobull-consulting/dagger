# Remote cache dev environment: MinIO

A local S3-compatible store for developing and testing the remote cache
service (`cmd/cacheservice`), so nothing needs an AWS account
([#2](https://github.com/zerobull-consulting/dagger/issues/2), under
[#1](https://github.com/zerobull-consulting/dagger/issues/1)).

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
| `dev.sh test` | Start MinIO and run the Go tests against it. |
| `dev.sh e2e` | Run the remote cache with real engines, from empty, and its scenarios. **Deletes MinIO's data first.** See [Real engines](#real-engines-devsh-e2e). |
| `dev.sh e2e-down` | Stop the services and engines `dev.sh e2e` left running. |

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
`PREFIX`, `PUT_EXPIRY` and `GET_EXPIRY`.

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

`cmd/cacheservice/minio_test.go` checks what the storage work relies on,
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
  comes back through its presigned GET, and `Has` sees it only after the
  upload.
- `TestMinIOStoreUploadURLsSkipsExisting`: `UploadURLs` asks only for blobs
  the bucket doesn't have, so an uploaded blob is not requested again.
- `TestMinIOStorePutSizeBound`: the store's PUTs refuse bodies of another
  size.
- `TestMinIOStoreExpiry`: the store's PUTs and GETs stop working at their
  configured expiry.
- `TestMinIOStoreHasBadCredentials`: `Has` reports bad credentials as an
  error, not as a missing blob.
- `TestMinIOStoreKnownGapMisdeclaredDigest`: pins a known gap. A same-size
  body with the wrong content is stored under the digest and never requested
  again. It should fail once uploads are verified; see
  [`cmd/cacheservice/README.md`](../../cmd/cacheservice/README.md#blob-store).

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

`cmd/cacheservice/minio_export_test.go` checks the export path end to end,
with a fake engine cache served through the engine's own `remotecache.Run`:

- `TestMinIOExportBuild`: after a build's session ends, the engine has
  uploaded its layers to their keys through presigned PUTs, and its bundle and
  the index are in the bucket. Exporting it again uploads nothing, stores the
  same bundle and hands out no new numbers.
- `TestMinIOExportSharedLayer`: a second build that shares the base layer
  uploads only its own.

Each run writes under its own `test/` prefix in the bucket and deletes it
afterwards. The `Remote cache tests` workflow runs them on every pull request.

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

## Real engines: `dev.sh e2e`

```bash
./hack/remote-cache/dev.sh e2e
```

One command runs the remote cache end to end with engines built from this
repo ([#29](https://github.com/zerobull-consulting/dagger/issues/29)), then
the [scenarios](#scenarios) that check the paths beyond a first cache hit. It
starts from nothing: it **deletes MinIO's data** (`dev.sh reset`), and gives
engines A and B new, empty cache volumes. Then it:

1. builds `dagger-engine`, `dagger-init-session`, the `dagger` CLI and
   `cacheservice` from this repo, and an engine image that copies them over
   the release image `E2E_BASE_IMAGE` (`registry.dagger.io/engine:v1.0.0-beta.22`),
   with `{"gc":{"enabled":false}}` and a `mirror.gcr.io` mirror for
   `docker.io` in `/etc/dagger/engine.json`;
2. starts MinIO on the Docker bridge gateway (see
   [above](#pointing-an-engine-at-the-service-and-minio)), and
   `cacheservice` on the host with `dev.sh env`'s settings, listening on
   the gateway's port 8090 only (it accepts any engine token and any OTLP
   request, so it isn't exposed beyond this host), with its debug API on
   `127.0.0.1:8091`;
3. starts engines A and B in containers, each with
   `_EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL`, `DAGGER_CLOUD_TOKEN` and
   `DAGGER_CLOUD_URL` pointing at the service on the gateway;
4. runs a pipeline on A with the CLI (whose `DAGGER_CLOUD_URL` and token point
   at the service too): `alpine`, then two `withExec`s that each write 4MB of
   random data and a random stamp, then reads the stamps;
5. waits until the service has stored A's entries (`GET
   /debug/engines/{conn}/entries`, and `index.json` in MinIO), and merged
   them into B (`GET /debug/engines/{conn}/imports`);
6. runs the same pipeline on B, then one more exec on B that checksums both
   layer files, while tracing MinIO's requests (`mc admin trace`);
7. waits until the service has stored B's new entries and merged them into
   A, and reads the index again (`index-final.json`).

It passes when B's stamps are A's (the execs didn't run again), B's
`withExec` spans are hits, the layer files on B have A's checksums, MinIO
served B's engine the layer blobs, and the index holds A's entries,
unchanged, plus only the entries B's verify exec built, each once: none of
B's entries is one it imported, every service number handed out is an entry,
and no merge into A carries A's own entries back
([#35](https://github.com/zerobull-consulting/dagger/issues/35)). It prints the wall-clock time of each run.
What a run showed is in
[`cmd/cacheservice/README.md`](../../cmd/cacheservice/README.md#real-engine-run).

### Scenarios

After that first scenario, on the same MinIO, service and engines, it runs
the scenarios in `E2E_SCENARIOS`, in this order (all of them by default). A
scenario that needs other service settings runs against a **replica**: a
second `cacheservice` on the same bucket and scope, on port 8092 (debug API
on `127.0.0.1:8093`, log in `replica.log`), as another replica of the service
would be. It learns of the main service's exports by polling the index, so
nothing needs restarting, and the one-command run stays one command.

| Scenario | Issue | What it does | Passes when |
| --- | --- | --- | --- |
| `reexport` | [#30](https://github.com/zerobull-consulting/dagger/issues/30) | Exports A's stored entries again with `POST /debug/engines/{conn}/export`, with every root and part the tracker knows of. Then runs a second pipeline on A over the same `alpine` (one exec writing 2MB). | The repeat stores every root, uploads no blob, finds every blob present, leaves the index as it was (numbers and bundles) and writes no bundle object. The second pipeline uploads only its own layers: `alpine`'s is in its bundle, and not among MinIO's PUTs. |
| `renew` | [#31](https://github.com/zerobull-consulting/dagger/issues/31) | Starts the replica, with GETs that last `E2E_GET_EXPIRY` seconds and an import poll of `E2E_IMPORT_POLL` seconds, and engines C and D on it, empty. Once both have A's entries merged, waits for the GETs to expire, and runs the verify exec on C. Then deletes the last exec's layer from MinIO, and runs it on D. | C sends `renew`, the replica answers with fresh GETs, MinIO serves C every layer of A's chain, and C's files are A's. D's `renew` of the chain with the deleted layer answers `unavailable`; D's query still succeeds, with exec1's files A's and exec2's new (it ran again). |
| `prune` | [#32](https://github.com/zerobull-consulting/dagger/issues/32) | Prunes C's cache with `{ engine { localCache { prune } } }` (with no arguments, it drops every releasable entry), waits 5 import polls, syncs C 3 times by hand (`POST /debug/engines/{conn}/import`), then restarts C on the same volume. Starts the replica and C if `renew` didn't run. | The prune reaches the replica (`engine.prune`) and the importer forgets the pruned values; nothing is merged or offered to C over the polls and syncs; after the restart (a new generation), the replica merges the entries into C again. |
| `superseded` | [#32](https://github.com/zerobull-consulting/dagger/issues/32), [#27](https://github.com/zerobull-consulting/dagger/issues/27) | Writes a Dang module (`E2E_DIR/ttlmod`) whose function `stamp2` has a cache TTL of `E2E_TTL` seconds, runs it on A, waits for it to expire, and runs it again. Then starts engine F, empty, and reads the value on it, tracing MinIO. | A replaced the value in place, the index kept its service number and moved it to a newer bundle, and F's call resolved to the imported entry (its `withExec` hit; a function with a TTL is evaluated on every call). It reports which record F got by the layer it read: the newer one passes; the older one is printed as `GAP` (#27), without failing; neither fails. |

Each scenario prints its checks as it ends (`ok`, `FAIL`), and the run exits
non-zero if any failed. `E2E_SCENARIOS=renew ./hack/remote-cache/dev.sh e2e`
runs the first scenario and `renew` only. The evidence is in `E2E_DIR`, by
scenario prefix: `reexport-*` (the request, its result, the index and bundle
listing before and after, MinIO's trace of the second pipeline), `renew-*`
(the importer's state, the renew envelopes from `replica.log`, each engine's
MinIO trace and blob GETs), `prune-*` (the importer's state before, after the
prune, after the polls and after the restart, the prune event, the hand
syncs) and `superseded-*` (each export's index and bundle, F's imports, its
span and MinIO trace).

Everything stays up afterwards for poking at (`curl
localhost:8091/debug/engines`, the MinIO console); `dev.sh e2e-down` stops
the engines, the service and the replica, and `dev.sh down` stops MinIO. Logs, query
results, the index before and after, the importer's state and MinIO's trace
are in `E2E_DIR`.

| Setting | Default | Meaning |
| --- | --- | --- |
| `E2E_DIR` | `$TMPDIR/dagger-remote-cache-e2e` | Binaries, logs and results |
| `E2E_SKIP_BUILD` | unset | `1` reuses the binaries in `E2E_DIR/bin` |
| `E2E_BASE_IMAGE` | `registry.dagger.io/engine:v1.0.0-beta.22` | The release image the engine binaries are copied over |
| `E2E_CA_FILE` | unset | A CA bundle to add to the engine image, for hosts behind a TLS-intercepting proxy |
| `E2E_TIMEOUT` | `180` | Seconds to wait for the export, and for the merge |
| `CACHESERVICE_PORT`, `CACHESERVICE_DEBUG_PORT` | `8090`, `8091` | The service's ports |
| `REPLICA_PORT`, `REPLICA_DEBUG_PORT` | `8092`, `8093` | The replica's ports |
| `E2E_SCENARIOS` | `reexport renew prune superseded` | The scenarios to run after the first, which always runs |
| `E2E_GET_EXPIRY` | `30` | Seconds the replica's presigned GETs last (`renew`) |
| `E2E_IMPORT_POLL` | `2` | Seconds between the replica's import polls (`prune`) |
| `E2E_TTL` | `30` | The cache TTL of the `superseded` scenario's function, in seconds |

Notes:

- It needs Go, Docker with Compose v2, `jq` and `curl`. The engines run
  `--privileged`.
- The CLI runs with `TRACEPARENT` (and the `OTEL_EXPORTER_OTLP_*` endpoints)
  unset. Some CI runners and sandboxes export an unsampled `TRACEPARENT`; the
  CLI then continues that unsampled trace, the engine records no session
  spans, and the service never hears of any entry. Watch for this when
  pointing your own CLI at the service.
- Where Docker Hub rate-limits pulls, pull the MinIO image through
  `mirror.gcr.io` and tag it as the name in `compose.yaml`. The engines pull
  `alpine` through `mirror.gcr.io` already.
