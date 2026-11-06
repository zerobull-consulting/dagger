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
          engine-image: registry.dagger.io/engine:v1.0.0-beta.25@sha256:39abecfe0eba4c91035dcfc9f72c8a3963ae60481326082b48d78f2230202834
          s3-endpoint: ${{ vars.CACHE_S3_ENDPOINT }}
          s3-region: ${{ vars.CACHE_S3_REGION }}
          s3-bucket: ${{ vars.CACHE_S3_BUCKET }}
          s3-prefix: cache
          s3-access-key-id: ${{ secrets.CACHE_S3_ACCESS_KEY_ID }}
          s3-secret-access-key: ${{ secrets.CACHE_S3_SECRET_ACCESS_KEY }}
      - run: dagger call build
      - if: always()
        # Above drain-timeout (10m) plus the engine's stop and the linger.
        timeout-minutes: 12
        uses: zerobull-consulting/dagger/.github/actions/remote-cache-sidecar/stop@<commit SHA>
```

Pin both actions and the service's image to the same commit of
`feature/remote-cache`: the action passes flags that commit's service
takes. The engine is a stock Dagger release, pinned by digest: the example
is the release CI's `sidecar demo` job runs (`STOCK_ENGINE` in
[`remote-cache-tests.yml`](../../workflows/remote-cache-tests.yml)). The
minimum release with a compatible client is `v1.0.0-beta.24`; how that was
checked is in
[Engine](../../../hack/remote-cache/README.md#engine). Nothing checks the
engine's version yet: under the lockstep policy
([zerobull-consulting/dagger#117](https://github.com/zerobull-consulting/dagger/issues/117))
the engine will be the release the service was built from, and one version
input will replace both image inputs. The fork no longer publishes an engine
image of its own.

## Inputs

| Input | Required | Default | What it is |
| --- | --- | --- | --- |
| `role` | yes | | `writer`: exports what the job builds, with `-gc-interval=0` (scheduled GC is a job of its own, [zerobull-consulting/dagger#61](https://github.com/zerobull-consulting/dagger/issues/61)). `read-only`: `-export=false`, never writes to or deletes from the bucket; give it a [read-only key](../../../cmd/cacheservice/README.md#a-read-only-key). |
| `service-image` | yes | | The cache service image, by SHA tag. |
| `engine-image` | yes | | A stock Dagger engine release, `v1.0.0-beta.24` or later, pinned by digest (`registry.dagger.io/engine:<release>@sha256:<digest>`). Its own `dagger` CLI is the one set up. |
| `s3-endpoint`, `s3-region`, `s3-bucket` | yes | | The store. |
| `s3-access-key-id`, `s3-secret-access-key` | yes | | The key, from the caller's secrets. Never defaulted. |
| `s3-prefix` | no | empty: the whole bucket | The prefix the service [owns](../../../cmd/cacheservice/README.md#bucket-ownership): nothing else may write under it. Segments of letters, digits, `.`, `_` and `-`, separated by `/`, none of them `.` or `..`, with no leading or trailing `/`. |
| `s3-path-style` | no | the service's | `true` for path-style addressing, such as MinIO's, or `false`. |
| `namespace` | no | `default` | The namespace entries are stored in and imported from (`-scope`). Writers and readers that should share a cache use the same one. |
| `drain-timeout` | no | `10m` | How long the stop lets the service finish its exports (`-drain-timeout`): hours, minutes and seconds, such as `10m` or `1h30m`. |
| `name` | no | `remote-cache` | Prefix of the containers (`<name>-service`, `<name>-engine`), the engine's state volume and the network. Set another one to run a second sidecar in the same job after the first is stopped, and pass the same to `stop`. |
| `debug-port` | no | not published | Publishes the service's debug API on the runner's `127.0.0.1:<port>`, behind a token generated for this start (see [The debug API](#the-debug-api)). |
| `engine-config` | no | [`engine.json`](engine.json) | The engine's `engine.json`. The default turns the engine's GC off (see below) and pulls `docker.io` images through `mirror.gcr.io`, as the release's does. |
| `verbose` | no | `false` | `true`: the service logs every cache span (`-v`); CI's demo counts them. |

The start refuses a container, volume or network of its names that is
already there, before it creates anything, and the stop removes only what
the start created. Every input that becomes a container name, a namespace,
a path or a flag is validated before anything is created ([`sidecar_test.sh`](sidecar_test.sh)
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
| `debug-token-file` | With `debug-port`: the file holding the debug API's token; empty otherwise. |

It also sets `_EXPERIMENTAL_DAGGER_RUNNER_HOST`, `DAGGER_CLOUD_URL` and
`DAGGER_CLOUD_TOKEN` (a placeholder) in the job's environment, for a CLI of
the job's own.

**The CLI it sets up** is a wrapper around the engine image's own CLI. It
unsets `TRACEPARENT`, `TRACESTATE`, `OTEL_*`, `DAGGER_*` and
`_EXPERIMENTAL_DAGGER_*` (as the e2e harness runs its CLI: a `DAGGER_ENGINE`
would pick another engine), then sets the engine and the service's URL,
whatever a later step's environment holds. A CLI that
continues an inherited, unsampled `TRACEPARENT`, or lacks the Cloud
settings, sends the service no session spans, and nothing is exported:
silently (see
[Running it as a sidecar](../../../cmd/cacheservice/README.md#running-it-as-a-sidecar)).
It also fails at once, with the service's log, if the service isn't running.
Another CLI (such as one an action installs) gets the variables, but not
the `TRACEPARENT` guard.

## The debug API

With `debug-port`, the service's debug API (`-debug-listen :8091`) is
published on the runner's `127.0.0.1:<debug-port>` only, and requires a
token (`-debug-token-file`; see
[The debug API](../../../cmd/cacheservice/README.md#the-debug-api)): the
start generates one for this sidecar, from `/dev/urandom`, masks it in the
job's log, and writes it to a file in a directory only the runner's user
can read, mounted read-only into the service's container. The token never
appears on a command line, in the container's environment, or in an output:
the `debug-token-file` output is the file's path. The stop removes the file.

```yaml
- name: Ask the service which engines are connected
  env:
    PORT: "8091"
    TOKEN_FILE: ${{ steps.sidecar.outputs.debug-token-file }}
  # curl reads the header from a file descriptor, so the token is on no
  # command line (printf is a bash builtin).
  run: curl -fsS -H @<(printf 'Authorization: Bearer %s\n' "$(cat "$TOKEN_FILE")") "http://127.0.0.1:$PORT/debug/engines"
```

A request without the token, or with another, gets 401. Before this,
`debug-port` published an unauthenticated API; a step that `curl`s it now
needs the header.

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

It stops the service first: a SIGTERM, then a wait for `drain: done` in its
log, for up to `drain-timeout` plus 5s. Then the engine, with `docker stop
--time 30`: the service, done draining, still answers the engine's events
for up to 15s (the `-drain-linger` the start passes), so the engine's own stop
delivers its last cache event (`engine.stop`) and ends in seconds, cleanly
([zerobull-consulting/dagger#102](https://github.com/zerobull-consulting/dagger/issues/102)).
Then it waits for the service to exit, which it does once that event
arrived, up to `drain-timeout` plus 25s from the SIGTERM in all: the
service's whole stop takes at most `drain-timeout` plus its linger plus 5s
(see [Stopping and draining](../../../cmd/cacheservice/README.md#stopping-and-draining)).
A service still running then is stopped, and fails the step. A `docker
inspect` that fails during a wait is never taken for the service's exit:
the engine isn't stopped before the drain is done. Every wait is bounded,
and a wait that runs out doesn't skip the rest: the stop keeps both
logs (with presigned URLs' credentials and signatures redacted), and
removes the containers, the engine's state volume and the network. With
nothing to drain, the stop takes about 10s, most of it the engine's.

Give the step a `timeout-minutes` above `drain-timeout` plus 25s, with room
for the engine's 30s: 12 for `10m`. The start passes the service
`-drain-linger`, which a service image from before the linger doesn't take:
pin the images to the action's commit, as above.

**Exit codes**, the service's:

| Code | Meaning | The step |
| --- | --- | --- |
| 0 | A clean stop: everything ended within the bound. | passes |
| 1 | The bound cut the shutdown off with work still running (logged as `the shutdown's bound passed with work still running`, with what was running), a signal arrived during the shutdown, or an HTTP server failed. | fails |
| 137 | Killed before its stop ended, not by the stop (such as out of memory). What it was exporting is lost. | fails |
| any | Still running past the stop's bound, `drain-timeout` plus 25s: the stop stopped it. | fails |
| any | The service had exited before the stop. | fails |

Known gaps in the service's exit code and log:
[zerobull-consulting/dagger#80](https://github.com/zerobull-consulting/dagger/issues/80)
and
[zerobull-consulting/dagger#81](https://github.com/zerobull-consulting/dagger/issues/81).
Of the action: a read-only start doesn't wait for the service's first merge
([zerobull-consulting/dagger#103](https://github.com/zerobull-consulting/dagger/issues/103)),
and the leads in
[zerobull-consulting/dagger#104](https://github.com/zerobull-consulting/dagger/issues/104).

The stop's step summary has the exit code, and its log the drain's and the
linger's lines (`drain: stored`, `drain: done outcome=...`, `linger: done
outcome=...`). The logs name the bucket and
the prefix: upload them, as CI does on failure, only where that's fine.

## Tested by

The `sidecar demo` job of
[`remote-cache-tests.yml`](../../workflows/remote-cache-tests.yml) runs the
[demo](../../../hack/remote-cache/demo/README.md) through both actions on
every pull request, against MinIO: a writer phase, with `debug-port` (it
checks that the debug API refuses a request without the token and answers
one with it), then a read-only phase on a fresh engine with a read-only key. Locally, `hack/remote-cache/dev.sh demo`
runs the same with `sidecar.sh`.

The job builds the service's image from the pull request, but runs the stock
Dagger engine release in `STOCK_ENGINE`, not an engine built from this
branch: the engine's remote cache client comes from upstream, and a step of
the job warns when this branch's client differs from that release's. Which engine to
pin is settled by the lockstep policy, a vX service for vX engines
([zerobull-consulting/dagger#117](https://github.com/zerobull-consulting/dagger/issues/117)).
