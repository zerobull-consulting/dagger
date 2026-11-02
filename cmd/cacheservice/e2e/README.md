# e2e: the remote cache with real engines

`TestRealEngines` runs the remote cache end to end: engines built from this
repo, in Docker containers, get cache hits from what another engine stored
in MinIO. It skips unless `CACHESERVICE_E2E=1`; run it with:

```bash
./hack/remote-cache/dev.sh e2e
```

What it does, its scenarios and its settings are in
[`hack/remote-cache/README.md`](../../../hack/remote-cache/README.md#real-engines-devsh-e2e).

## The pieces

| Piece | Where it runs | How the test drives it |
| --- | --- | --- |
| Cache service (`Service`, `StartService`) | In the test process, on the Docker bridge gateway | `server.Service`'s Go API: `Engines`, `Entries`, `Imports`, `Export`, `Import`. A recording `slog` handler (`LogRecorder`) keeps what it logs: cache spans, envelopes, engine events, exports and merges. |
| S3 proxy (`S3Proxy`) | In the test process, on the gateway | The services' S3 endpoint, so presigned URLs point at it too. It records every request (method, key, time, status) instead of tracing MinIO. |
| MinIO | `hack/remote-cache/compose.yaml`, on loopback | Through the proxy for the services and engines; directly (`Env.S3`, `Env.Meta`) to read the index and bundles and delete blobs, so the test's own reads aren't recorded as the engines'. |
| Engines (`Engine`, `StartEngines`) | Docker containers from an image built for the run | The `docker` CLI: start, restart, remove. |
| Client (`Engine.CLI`, `Engine.Query`) | The `dagger` CLI built for the run | With a clean environment. The CLI's Cloud settings come from the environment, which is process-wide, so it runs as its own process rather than as the engine client in this one. |

Each run has its own bucket prefix, scope, engine names and volumes, so runs
don't need MinIO reset and don't see each other's entries.

## Adding a scenario

Write a method on `state` that takes a `*testing.T`, and add it with
`env.Run` in `TestRealEngines`. It can reuse what the first part left (A, B,
`indexAfterA`, `layersA`) and start its own engines or services. Mark the
proxy and the service's log before the step, read what came after, and
report each check with `Check`. To break something on purpose, use
`Env.DeleteBlob`, a short `ServiceOptions.GetExpiry`, or `Engine.Restart`.
