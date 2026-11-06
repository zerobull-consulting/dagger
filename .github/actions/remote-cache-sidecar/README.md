# remote-cache-sidecar

Runs the remote cache service (`cmd/cacheservice`) and an engine with its
client side by side on a GitHub Actions runner, and sets up the `dagger` CLI
for the job's later steps. A second action, [`stop`](stop/action.yml), stops
them at the end of the job, after the service drains its exports. What each
step does, and how to run the same outside GitHub, is in
[Running it as a sidecar](../../../cmd/cacheservice/README.md#running-it-as-a-sidecar).

```yaml
jobs:
  build:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v5
      - uses: zerobull-consulting/dagger/.github/actions/remote-cache-sidecar@<commit SHA>
        with:
          # A job on the default branch. A pull-request job's is the same,
          # with role: read-only and a read-only key.
          role: writer
          service-image: ghcr.io/zerobull-consulting/cacheservice:<commit SHA>
          engine-image: ghcr.io/zerobull-consulting/dagger-engine:<commit SHA>
          s3-endpoint: ${{ vars.CACHE_S3_ENDPOINT }}
          s3-region: ${{ vars.CACHE_S3_REGION }}
          s3-bucket: ${{ vars.CACHE_S3_BUCKET }}
          s3-prefix: cache
          s3-access-key-id: ${{ secrets.CACHE_S3_ACCESS_KEY_ID }}
          s3-secret-access-key: ${{ secrets.CACHE_S3_SECRET_ACCESS_KEY }}
      - run: dagger call build
      - if: always()
        # Above drain-timeout (10m) plus 5s.
        timeout-minutes: 12
        uses: zerobull-consulting/dagger/.github/actions/remote-cache-sidecar/stop@<commit SHA>
```

Pin both actions and both images to the same commit of
`feature/remote-cache`: the action passes flags that commit's service
takes, and the service and engine speak that commit's protocol.

## Inputs

| Input | Required | Default | What it is |
| --- | --- | --- | --- |
| `role` | yes | | `writer`: exports what the job builds, with `-gc-interval=0` (scheduled GC is a job of its own, [zerobull-consulting/dagger#61](https://github.com/zerobull-consulting/dagger/issues/61)). `read-only`: `-export=false`, never writes to or deletes from the bucket; give it a [read-only key](../../../cmd/cacheservice/README.md#a-read-only-key). |
| `service-image` | yes | | The cache service image, by SHA tag. |
| `engine-image` | yes | | The engine image with the remote cache client, by the same SHA. Its own `dagger` CLI is the one set up. |
| `s3-endpoint`, `s3-region`, `s3-bucket` | yes | | The store. |
| `s3-access-key-id`, `s3-secret-access-key` | yes | | The key, from the caller's secrets. Never defaulted. |
| `s3-prefix` | no | empty: the whole bucket | The prefix the service [owns](../../../cmd/cacheservice/README.md#bucket-ownership): nothing else may write under it. Segments of letters, digits, `.`, `_` and `-`, separated by `/`, none of them `.` or `..`, with no leading or trailing `/`. |
| `s3-path-style` | no | the service's | `true` for path-style addressing, such as MinIO's, or `false`. |
| `namespace` | no | `default` | The namespace entries are stored in and imported from (`-scope`). Writers and readers that should share a cache use the same one. |
| `drain-timeout` | no | `10m` | How long the stop lets the service finish its exports (`-drain-timeout`): hours, minutes and seconds, such as `10m` or `1h30m`. |
| `name` | no | `remote-cache` | Prefix of the containers (`<name>-service`, `<name>-engine`), the engine's state volume and the network. Set another one to run a second sidecar in the same job after the first is stopped, and pass the same to `stop`. |
| `debug-port` | no | not published | Publishes the service's unauthenticated debug API on the runner's `127.0.0.1:<port>`. |
| `engine-config` | no | [`engine.json`](engine.json) | The engine's `engine.json`. The default turns the engine's GC off (see below) and pulls `docker.io` images through `mirror.gcr.io`, as the release's does. |
| `verbose` | no | `false` | `true`: the service logs every cache span (`-v`); CI's demo counts them. |

Every input that becomes a container name, a namespace, a path or a flag is
validated before anything is created ([`sidecar_test.sh`](sidecar_test.sh)
tests it). The S3 settings reach the service as
environment variables passed by name, never on a command line, and the key
is masked in the job's log.

**The engine's GC is off by default**, as `cmd/cacheservice/e2e` runs its
engines: the engine's state ends with the job, and with GC on it can prune
entries right after their session, before the service exports them. That
happened on a host whose disk looked full. On a runner whose disk a build
fills, pass an `engine-config` with GC on.

## Outputs

| Output | What it is |
| --- | --- |
| `dagger` | The CLI set up for the sidecar. It is also first on `PATH` for later steps. |
| `runner-host` | The engine, as `_EXPERIMENTAL_DAGGER_RUNNER_HOST` (`docker-container://<name>-engine`). |
| `service-url` | The service, as the engine and the CLI reach it: on the sidecar network's gateway, which only this runner reaches. |
| `service-container`, `engine-container` | The containers. |
| `logs` | Where `stop` puts the service's and the engine's logs (`service.log`, `engine.log`). |

It also sets `_EXPERIMENTAL_DAGGER_RUNNER_HOST`, `DAGGER_CLOUD_URL` and
`DAGGER_CLOUD_TOKEN` (a placeholder) in the job's environment, for a CLI of
the job's own.

**The CLI it sets up** is a wrapper around the engine image's own CLI. It
unsets `TRACEPARENT`, `TRACESTATE` and `OTEL_*`, and sets the engine and the
service's URL, whatever a later step's environment holds. A CLI that
continues an inherited, unsampled `TRACEPARENT`, or lacks the Cloud
settings, sends the service no session spans, and nothing is exported:
silently (see
[Running it as a sidecar](../../../cmd/cacheservice/README.md#running-it-as-a-sidecar)).
It also fails at once, with the service's log, if the service isn't running.
Another CLI (such as one an action installs) gets the variables, but not
the `TRACEPARENT` guard.

## Failing loudly

- **Start:** the step fails, with the service's log, when the service exits
  at startup (exit 1: a store error, such as a prefix it doesn't own or a
  read-only key on a writer; exit 2: a flag error) or doesn't listen within
  120s, and, with the engine's log, when the engine exits or doesn't connect
  within 120s. Pulls are bounded at 15 minutes each.
- **During the job:** every `dagger` run through the wrapper fails if the
  service has exited.
- **Stop:** fails when the service exited before the stop, or its stop
  wasn't clean.

## stop

```yaml
- if: always()
  timeout-minutes: 12
  uses: zerobull-consulting/dagger/.github/actions/remote-cache-sidecar/stop@<commit SHA>
  with:
    name: remote-cache   # the same name as the start's
```

Composite actions have no `post` step, so the stop is an action of its own,
run with `if: always()`. A JavaScript action with `post` would run the stop
by itself, but it would be a second implementation of the same Docker
steps, in another language, that a local run can't share; this way the
action, the stop, the demo's local run and the README all run one script,
[`sidecar.sh`](sidecar.sh).

It stops the service first, with `docker stop --time` at `drain-timeout`
plus 10s: the service's whole stop takes at most `drain-timeout` plus 5s
(see [Stopping and draining](../../../cmd/cacheservice/README.md#stopping-and-draining)).
Then it stops the engine (within 30s: the engine's own stop waits to send
its last cache event to the service, which is gone by then, so it is
usually killed at 30s; its state goes with the job), keeps both logs (with presigned URLs' credentials
and signatures redacted), and removes the containers, the engine's state
volume and the network. Give the step a `timeout-minutes` above
`drain-timeout` plus 5s, with room for the engine's 30s: 12 for `10m`.

**Exit codes**, the service's:

| Code | Meaning | The step |
| --- | --- | --- |
| 0 | A clean stop: everything ended within the bound. | passes |
| 1 | The bound cut the shutdown off with work still running (logged as `the shutdown's bound passed with work still running`, with what was running), a signal arrived during the shutdown, or an HTTP server failed. | fails |
| 137 | Still stopping at `docker stop`'s `--time`: killed. What it was exporting is lost. | fails |
| any | The service had exited before the stop. | fails |

Known gaps in the service's exit code and log:
[zerobull-consulting/dagger#80](https://github.com/zerobull-consulting/dagger/issues/80)
and
[zerobull-consulting/dagger#81](https://github.com/zerobull-consulting/dagger/issues/81).

The stop's step summary has the exit code, and its log the drain's lines
(`drain: stored`, `drain: done outcome=...`). The logs name the bucket and
the prefix: upload them, as CI does on failure, only where that's fine.

## Tested by

The `sidecar demo` job of
[`remote-cache-tests.yml`](../../workflows/remote-cache-tests.yml) runs the
[demo](../../../hack/remote-cache/demo/README.md) through both actions on
every pull request, against MinIO: a writer phase, then a read-only phase on
a fresh engine with a read-only key. Locally, `hack/remote-cache/dev.sh demo`
runs the same with `sidecar.sh`.
