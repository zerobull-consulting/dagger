# e2e: the remote cache with real engines

`TestRealEngines` runs the remote cache end to end: engines built from this
repo, in Docker containers, get cache hits from what another engine stored
in MinIO. It skips unless `CACHESERVICE_E2E=1`; run it with:

```bash
./hack/remote-cache/dev.sh e2e
```

What it does, its scenarios and its settings are in
[`hack/remote-cache/README.md`](../../../hack/remote-cache/README.md#real-engines-devsh-e2e).

To run it against a real bucket, its safety guarantees, its variables and what
each store did, see
[Testing against a real bucket](../../../hack/remote-cache/README.md#testing-against-a-real-bucket).
A run needs `CACHESERVICE_S3_PREFIX`, works under `<prefix>/e2e/<run>/`, and
fails if the proxy or its direct client refused a request: one that would
change something outside it, or a bulk delete whose body it couldn't read
or that is over `s3test.MaxBulkDeleteBytes`.
Reads outside it are let through: they change nothing. That
prefix is new for each run, so the first service's startup check finds it
empty and claims it with the ownership marker (see
[Bucket ownership](../README.md#bucket-ownership)); `Env.CheckConsistent`
checks the marker is there and that every key under the prefix is of the
service's shapes (`Env.CheckOwned`). Keys are laid out as package
[`layout`](../layout/) has them: `Env.BlobKeyIn` and the rest build them
from it.

## The pieces

| Piece | Where it runs | How the test drives it |
| --- | --- | --- |
| Cache service (`Service`, `StartService`) | In the test process, on the Docker bridge gateway | `server.Service`'s Go API: `Engines`, `Entries`, `Imports`, `Export`, `Import`. A recording `slog` handler (`LogRecorder`) keeps what it logs: cache spans, envelopes, engine events, exports and merges. |
| S3 proxy (`S3Proxy`) | In the test process, on the gateway | The services' S3 endpoint, so presigned URLs point at it too. It records every request (method, key, query, time, status) instead of tracing MinIO, and injects faults (`Fault`): S3 errors or stalls for the requests they match, or a rewrite of their body before MinIO gets it. An engine's blob requests (`S3Request.IsBlob`) are its uploads, PUTs to staging keys (`ns/<ns>/staging/<stage>/<alg>/<hex>`), and its downloads, GETs of digest keys (`ns/<ns>/blobs/<alg>/<hex>`); the service's own (HEADs, and a promotion's copies and reads) are not. |
| MinIO, or any bucket | `hack/remote-cache/compose.yaml`, on loopback; or the bucket in `CACHESERVICE_S3_*` with `CACHESERVICE_TEST_EXTERNAL=1` | Through the proxy for the services and engines; directly (`Env.S3`, `Env.Meta`) to read the index and bundles and delete blobs, so the test's own reads aren't recorded as the engines'. On an HTTPS or virtual-hosted endpoint the proxy checks each signature and signs again for the store (`Upstream`). |
| Engines (`Engine`, `StartEngines`, `StartEnginesWithTokens`) | Docker containers from an image built for the run | The `docker` CLI: start, restart, remove, logs (`Engine.Logs`). Each authenticates with `Engine.Token`: its host name, or the token it was started with. |
| Client (`Engine.CLI`, `Engine.Query`) | The `dagger` CLI built for the run | With a clean environment. The CLI's Cloud settings come from the environment, which is process-wide, so it runs as its own process rather than as the engine client in this one. Its token is `e2e-cli`, or its engine's when the engine was started with a token: session spans reach the service under the CLI's token, which must be in the engine's namespace. |

Each run has its own bucket prefix, scope, engine names and volumes, so runs
don't need MinIO reset and don't see each other's entries. Services are
unauthenticated, with every engine in the run's scope, unless started with
`ServiceOptions.TokensFile`; `Service.ReloadTokens` then reads the file again,
as `SIGHUP` does for the command. `Env.BlobKey` is a blob's key in the run's
scope, and `Env.BlobKeyIn`, `Env.IndexOf` and `Env.ChainLayersIn` read another
namespace's.

## Adding a scenario

Write a method on `state` that takes a `*testing.T`, and add it with
`env.Run` in `TestRealEngines`. It can reuse what the first part left (A, B,
`indexAfterA`, `layersA`) and start its own engines or services. Mark the
proxy and the service's log before the step, read what came after, and
report each check with `Check`. To break something on purpose, use
`Env.DeleteBlob`, a short `ServiceOptions.GetExpiry`, `Engine.Restart` or
`Engine.RestartAsync`, `Service.CloseEngine` (drops one engine's
connection), `Env.RestartService` (a new service on the same address, scope
and options; `Engine.Attach` the engines to it; it stops the old one at once,
as a crash would), `Service.DrainFor` (a clean stop's drain, as SIGTERM does,
before `Stop` or `Env.RestartService`; `Service.DrainStatus` shows it while it
runs), or a `Fault` in the S3
proxy (`Proxy.Inject`: an S3 error such as 503 `SlowDown` for the requests
it matches, up to a count, a stall until `Proxy.Remove`, or a `Rewrite` of
their body, such as `SameSizeCorruption`; requests it answered or rewrote
are recorded with its name). After breaking something,
`Env.CheckConsistent` checks the store from MinIO directly: no index entry
names a missing bundle, and no stored bundle a missing layer.
`Env.StoredDigest` hashes what a digest key holds, and `Env.StagedKeys`
lists the scope's staged uploads.
`ServiceOptions.ReadOnly` starts a service as `-export=false` does (#65);
`ServiceOptions.Proxy`, with `Env.NewProxy`, gives a service an S3 proxy of
its own, whose record is then that service's and its engines' alone; and
`ServiceOptions.AccessKey`, with `Env.ReadOnlyKey`, runs it with the store's
read-only user.

Engine uploads go to staging keys, under a stage of each export try's own,
and reach the digest key only once the service has verified them (#23): to
match an engine's upload of a blob in a fault, match `/staging/` PUTs, and
compare a retried upload by its digest (`blobDigest`), not its key.

The resilience scenarios (#12) are in `resilience_test.go`; they run last,
since `service-restart` replaces the main service. `drain` (#63), in
`drain_test.go`, runs after them, and replaces the main service once more. `otlp-sizes`
(#77), in `otlp_test.go`, runs after everything: it reads every service's
log for the OTLP requests taken, so a scenario added after it isn't
counted. `namespaces` (#10), in
`namespaces_test.go`, runs its own service with a tokens file, and its own
engines. `verify` (#23), in `verify_test.go`, runs after `namespaces` and
before the resilience scenarios. `gc` (#11), in `gc_test.go`, runs after
`verify`, on its own service, namespace (`gc`) and engines, so it deletes
nothing the other scenarios use.

Every service has garbage collection on, run only when a scenario asks
(`Service.CollectGarbage`): `ServiceOptions.NamespacesFile` gives
namespaces their limits (`Service.ReloadLimits` reads it again, as
`SIGHUP` does), and `ServiceOptions.GCGrace` shortens the grace period.
`Env.CheckConsistentIn` checks another namespace than the run's scope,
and `Env.IndexOf` and `Env.BundleIDsIn` read one.
