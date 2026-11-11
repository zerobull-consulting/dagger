# The remote cache sidecar demo

A small project that shows what a
[remote cache sidecar](../../../cmd/cacheservice/README.md#running-it-as-a-sidecar)
carries from one engine to a fresh one, and what it doesn't. CI runs it
through the [action](../../../.github/actions/remote-cache/README.md)
on every pull request (the `sidecar demo` job), so the docs, the action and
the demo stay in agreement.

It is two `dagger` shell pipelines in [`demo.sh`](demo.sh), with no module,
so the engine image's own CLI runs it as it is:

| Step | What it does | On the writer's engine | On a fresh engine with a sidecar |
| --- | --- | --- | --- |
| `build`, the cacheable step | Writes 4 MiB of random bytes and a stamp with a random ID, copies them into a cache volume (`/deps`), sleeps 20s | Runs: about 20s | **A cache hit**: back in about a second, with the writer's stamp, 0 executed |
| `app`, the cache-volume step | Builds on `build`'s container, with a change of its own each run; reuses what `build` left in the cache volume, or redoes that work (10s) | Warm: the cache volume has `build`'s copy | **Cold, and reruns**: `build` was a hit, so it never ran here and the cache volume is empty |

The second row is the limit the README describes: cache volumes stay local.
To carry such state, make it an output of a cacheable step instead.

## Run it locally

```bash
./hack/remote-cache/dev.sh demo
```

It starts MinIO on the Docker bridge gateway, where containers reach it
(with a read-only user of the demo's own, for its prefix `demo/`), then
runs [`run.sh`](run.sh):

1. **writer phase:** a writer sidecar, the demo, then the stop, which
   flushes;
2. **read-only phase:** a fresh engine and a read-only sidecar
   (`-export=false`, with the read-only user), and the demo again.

[`check.sh`](check.sh) checks each phase from the service's log, counting
`Container.withExec` cache spans (hit or executed), as `cmd/cacheservice/e2e`
does, never wall time:

- writer: `build` executes and prints a stamp, `app` is warm; after the
  stop, exports were stored and its flush left nothing it could export;
- read-only: the service is read-only and merged the writer's entries; the
  engine's cache isn't the writer's; `build` is a hit with 0 executed and the
  writer's stamp; `app` is cold and executes; nothing is exported.

`VERSION` picks both images, as the sidecar action's `version` input does:
the stock engine of that version, pinned by the digest in the service's
engine manifest,
[`cmd/cacheservice/compat/engines.txt`](../../../cmd/cacheservice/compat/engines.txt),
and the published service image that accepts it,
`ghcr.io/zerobull-consulting/dagger-remote-cache:<version>`. Its default is
the manifest's first accept line, the release CI runs. The two can't
disagree; `SERVICE_IMAGE` overrides the service (such as an image built
from a checkout), and the start then fails, with the service's reason, if
that service refuses the engine
([zerobull-consulting/dagger#146](https://github.com/zerobull-consulting/dagger/issues/146)).
Until a service release logs `import sync: first done`, set `SERVICE_IMAGE`
to a build of this checkout: the read-only check fails without that line
([zerobull-consulting/dagger#103](https://github.com/zerobull-consulting/dagger/issues/103)).
`ENGINE_IMAGE` is gone: `run.sh` refuses it. On a host behind a
TLS-intercepting proxy, `CA_FILE=<bundle>` gives the engine its CA bundle.
Each run uses a new prefix, `demo/<run>/`, and leaves its objects there;
`dev.sh reset` deletes MinIO's data. Logs go to
`$TMPDIR/remote-cache-demo/<run>/` (`DEMO_DIR`).

To run the steps by hand against a sidecar of your own, with its `dagger`
first on `PATH` (or in `DAGGER`):

```bash
./hack/remote-cache/demo/demo.sh          # both steps
./hack/remote-cache/demo/demo.sh build    # the cacheable step only
```
