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

It needs Docker with Compose v2.

## Settings

| Setting | Value | Override with |
| --- | --- | --- |
| S3 endpoint | `http://localhost:9000` | `MINIO_API_PORT` |
| Console | `http://localhost:9001` | `MINIO_CONSOLE_PORT` |
| Region | `us-east-1` | `MINIO_REGION` |
| Bucket | `dagger-cache` | `MINIO_BUCKET` |
| Access key | `dagger-dev` | `MINIO_ROOT_USER` |
| Secret key | `dagger-dev-secret` | `MINIO_ROOT_PASSWORD` |
| Addressing | path-style (`http://host:9000/<bucket>/<key>`) | none |

The credentials are MinIO's root user. Use them only for local development.

Set the same overrides for every `dev.sh` command, since `env` and `test`
print and use them.

`dev.sh env` prints the settings as the `CACHESERVICE_S3_*` variables the
tests read: `ENDPOINT`, `REGION`, `BUCKET`, `ACCESS_KEY_ID`,
`SECRET_ACCESS_KEY` and `PATH_STYLE`. The blob store
([#5](https://github.com/zerobull-consulting/dagger/issues/5)) is expected to
read the same names.

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

`pgsty/minio:RELEASE.2026-08-04T00-00-00Z`, a community build of MinIO. The
tag was checked on Docker Hub on 2026-10-31, and it was the newest release
tag. The image ships `mc`, so the bucket bootstrap uses the same image and
there is only one tag to pin. To update it, change the `x-minio-image` anchor
in `compose.yaml` and this section.

## Tests

`cmd/cacheservice/minio_test.go` runs against this environment. Each test
skips unless `CACHESERVICE_S3_ENDPOINT` is set. They check what the storage
work relies on:

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

Each run writes under its own `test/` prefix in the bucket and deletes it
afterwards. The `Remote cache tests` workflow runs them on every pull request.

## Pointing an engine at the service and MinIO

The engine reaches only the cache service. MinIO's address reaches it inside
the presigned URLs the service hands out. **Those URLs are signed for one
host, and the engine must reach MinIO at exactly that host.** An engine in a
container can't reach `localhost:9000`, so give the service an endpoint that
works from inside the engine's container too:

- the Docker bridge gateway, usually `http://172.17.0.1:9000`
  (`docker network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}'`);
- `http://host.docker.internal:9000`, with the engine started with
  `--add-host=host.docker.internal:host-gateway`, and that name also resolving
  where the service runs;
- or run the engine with `--network host`, and keep `localhost`.

Both of the first two were checked from a container on 2026-10-31.

Then start the service with the S3 settings and point the engine at the
service. See [`cmd/cacheservice/README.md`](../../cmd/cacheservice/README.md)
for running the service against a dev engine:

```bash
./hack/remote-cache/dev.sh up
eval "$(./hack/remote-cache/dev.sh env)"
export CACHESERVICE_S3_ENDPOINT=http://172.17.0.1:9000
go run ./cmd/cacheservice -v
```

and on the engine:

```bash
_EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL=http://<host>:8090
DAGGER_CLOUD_TOKEN=<any token>
DAGGER_CLOUD_URL=http://<host>:8090
```

Until the blob store (#5) lands, the service reads none of the S3 settings,
and its `PUT /blobs/{digest}` stand-in still stores nothing.
