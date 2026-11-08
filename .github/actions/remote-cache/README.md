# remote-cache: start and stop

Two actions manage the remote cache service (`cmd/cacheservice`) and a stock
engine of one accepted version, side by side on a GitHub Actions runner:
`start` starts them (and stops them at the end of the job), and `stop` stops
them earlier.

- **`start`** installs the `dagger` CLI with
  [`dagger/dagger-for-github`](https://github.com/dagger/dagger-for-github),
  starts the service and the engine, and puts a wrapper of the CLI first on
  `PATH` for the job's later steps. At the end of the job, whatever
  happened, it stops them by itself, after the service drains its exports.
- [**`stop`**](stop/action.yml) stops them earlier, when a step needs the
  drain done (the export stored, the next phase's fresh engine), or with a
  timeout of its own.

What each step does, and how to run the same outside GitHub, is in
[Running it as a sidecar](../../../cmd/cacheservice/README.md#running-it-as-a-sidecar).
(These replace `remote-cache-sidecar`, which is gone: switch the `uses:` path,
and give `version` where you gave `engine-image`.)

```yaml
jobs:
  build:
    runs-on: ubuntu-24.04
    # Bounds the stop at the end of the job too: set it above the job's work
    # plus drain-timeout (10m) plus 2 minutes (see "The stop at the end of the job").
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v5
      - uses: zerobull-consulting/dagger/.github/actions/remote-cache/start@<commit SHA>
        with:
          # A job on the default branch. A pull-request job's is the same,
          # with role: read-only and a read-only key.
          role: writer
          version: v1.0.0-beta.25   # the engine and the CLI; optional, see below
          s3-endpoint: ${{ vars.CACHE_S3_ENDPOINT }}
          s3-region: ${{ vars.CACHE_S3_REGION }}
          s3-bucket: ${{ vars.CACHE_S3_BUCKET }}
          s3-prefix: cache
          s3-access-key-id: ${{ secrets.CACHE_S3_ACCESS_KEY_ID }}
          s3-secret-access-key: ${{ secrets.CACHE_S3_SECRET_ACCESS_KEY }}
      - run: dagger call build   # the wrapper of the installed CLI
      # No stop step: start stops the sidecar at the end of the job.
```

**Don't run commands through `dagger/dagger-for-github`'s `call`.** It sets
`DAGGER_CLOUD_TOKEN` from its `cloud-token` input, empty by default, which
would wipe the placeholder the wrapper sets, so the service would export
nothing, and its CLI skips the wrapper's `TRACEPARENT` guard. Run `dagger
call …` in a `run:` step: it finds the wrapper first on `PATH`.

**`version`** is the engine version, and selects both images
([zerobull-consulting/dagger#117](https://github.com/zerobull-consulting/dagger/issues/117),
D4):

- the CLI: `dagger/dagger-for-github`, pinned by commit (it is release
  v8.4.1, `27b130bf0f79a7f6fbbbe0fbca6760dc9bb40a77`), given only
  `version`, so it only installs. The engine is pinned by digest and the
  CLI only by its release tag, so the start **fails when the installed CLI
  doesn't say it is the engine's version** (`dagger version`),
  before it creates or pulls anything;
- the engine: the stock `registry.dagger.io/engine` image of that version,
  pinned by the digest in the service's engine manifest,
  [`cmd/cacheservice/compat/engines.txt`](../../../cmd/cacheservice/compat/engines.txt),
  at the action's commit;
- the service: `ghcr.io/zerobull-consulting/dagger-remote-cache:<version>`,
  a floating tag on the newest service build that accepts that engine,
  verified against it (see
  [Image](../../../cmd/cacheservice/README.md#image)). It is pulled on
  every start, since it moves: a copy left on a self-hosted runner would
  be stale. The two images are pulled at the same time, each bounded at 15
  minutes.

So the service can be a newer build than the action's commit. The action
passes the service only flags every build has kept (`-scope`,
`-drain-timeout`, `-drain-linger`, `-gc-interval`, `-export`, `-v`,
`-debug-listen`, `-debug-token-file`); removing or renaming one of them is
a breaking change for pinned actions. To run one exact build, set
`service-image` to `dagger-remote-cache:<commit SHA>`, such as the action's
own commit.

It must be an `accept` line of that manifest; without it, the action runs
the first one, the release CI's `sidecar demo` job runs. Any other version
fails the step before anything is installed, created or pulled. Each service release
accepts a verified list of engine versions and refuses every other engine
(see [Engine versions](../../../cmd/cacheservice/README.md#engine-versions)),
so the two images the version selects always agree.

**The start is ready once the service has welcomed the engine.** The
service logs `engine welcomed` after the hello, or `engine refused: its
version is not accepted`; the start waits for one or the other, for up to
120s, and fails the step on a refusal, with the service's reason
([zerobull-consulting/dagger#146](https://github.com/zerobull-consulting/dagger/issues/146)).
A refused engine would run the job's builds without the remote cache, and
at the start, a refusal is a misconfiguration: a `service-image` that
doesn't accept the version. The service's own rule, that a refusal leaves
builds running, is for a fleet whose engines change under a running
service; to keep a job going without the cache, give this step
`continue-on-error: true`. A service that neither welcomes nor refuses the
engine fails the step too, at that bound. One exception: a `service-image`
given explicitly may be from before the welcome line, so when it logs
neither line 10s after the engine connected, the start goes on with a
warning that it can't tell whether the engine was refused (a refusal comes
within milliseconds of the connection, so one is still caught).

**Overrides:**

- `service-image` replaces the service image, such as one built in the job
  or `dagger-remote-cache:<commit SHA>` to pin one build; `version` still
  selects the engine. CI's `sidecar demo` job uses it to test the pull
  request's own build.

The images were `ghcr.io/zerobull-consulting/cacheservice:<commit SHA>`
before; that name is still published for a transition, with the same
digests, and its tags are never deleted. The fork publishes no engine image
of its own.

## Inputs

| Input | Required | Default | What it is |
| --- | --- | --- | --- |
| `role` | yes | | `writer`: exports what the job builds, with `-gc-interval=0` (scheduled GC is a job of its own, [zerobull-consulting/dagger#61](https://github.com/zerobull-consulting/dagger/issues/61)). `read-only`: `-export=false`, never writes to or deletes from the bucket; give it a [read-only key](../../../cmd/cacheservice/README.md#a-read-only-key). |
| `version` | no | the manifest's first `accept` line | The engine version, such as `v1.0.0-beta.25`: an `accept` line of `cmd/cacheservice/compat/engines.txt` at the action's commit. Selects the CLI, the engine (by the manifest's digest) and the service (`dagger-remote-cache:<version>`). |
| `service-image` | no | `ghcr.io/zerobull-consulting/dagger-remote-cache:<version>` | Overrides the service image. The step fails if it refuses the engine. |
| `s3-endpoint`, `s3-region`, `s3-bucket` | yes | | The store. |
| `s3-access-key-id`, `s3-secret-access-key` | yes | | The key, from the caller's secrets. Never defaulted. A `Read/Write/Delete` key for a writer, a `Read` key for a read-only sidecar; **never the admin key** that sets lifecycle rules (on Spaces, a full-access key): the action never sets them, so no job needs it ([The keys](../../../cmd/cacheservice/README.md#the-keys)). |
| `s3-prefix` | no | empty: the whole bucket | The prefix the service [owns](../../../cmd/cacheservice/README.md#bucket-ownership): nothing else may write under it. Segments of letters, digits, `.`, `_` and `-`, separated by `/`, none of them `.` or `..`, with no leading or trailing `/`. |
| `s3-path-style` | no | the service's | `true` for path-style addressing, such as MinIO's, or `false`. |
| `namespace` | no | `default` | The namespace entries are stored in and imported from (`-scope`). Writers and readers that should share a cache use the same one. |
| `drain-timeout` | no | `10m` | How long the stop lets the service finish its exports (`-drain-timeout`): hours, minutes and seconds, such as `10m` or `1h30m`. The job's `timeout-minutes` must allow for it. |
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
| `version`, `service-image`, `engine-image` | What the sidecar runs. |
| `runner-host` | The engine, as `_EXPERIMENTAL_DAGGER_RUNNER_HOST` (`docker-container://<name>-engine`). |
| `service-url` | The service, as the engine and the CLI reach it: on the sidecar network's gateway, which only this runner reaches. |
| `service-container`, `engine-container` | The containers. |
| `logs` | Where `stop` puts the service's and the engine's logs (`service.log`, `engine.log`). |
| `debug-token-file` | With `debug-port`: the file holding the debug API's token; empty otherwise. |

It also sets `_EXPERIMENTAL_DAGGER_RUNNER_HOST`, `DAGGER_CLOUD_URL` and
`DAGGER_CLOUD_TOKEN` (a placeholder) in the job's environment, for a CLI of
the job's own.

**The CLI it sets up** is a wrapper around the CLI the official action installed. It
unsets `TRACEPARENT`, `TRACESTATE`, `OTEL_*`, `DAGGER_*` and
`_EXPERIMENTAL_DAGGER_*` (as the e2e harness runs its CLI: a `DAGGER_ENGINE`
would pick another engine), then sets the engine and the service's URL,
whatever a later step's environment holds. A CLI that
continues an inherited, unsampled `TRACEPARENT`, or lacks the Cloud
settings, sends the service no session spans, and nothing is exported:
silently (see
[Running it as a sidecar](../../../cmd/cacheservice/README.md#running-it-as-a-sidecar)).
It also fails at once, with the service's log, if the service isn't running.
Another CLI (the official action's own, run through its `call`) gets the
variables, but not the `TRACEPARENT` guard. In a local run
(`hack/remote-cache/dev.sh demo`) there is no installed CLI: with no
`SIDECAR_CLI`, [`sidecar.sh`](sidecar.sh) copies the engine image's own,
which always matches.

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
  120s, and, with the engine's log, when the engine exits, or the service
  refuses it or doesn't welcome it within 120s. A `version` the manifest
  doesn't accept, or a CLI that isn't the engine's version, fails it
  before anything is created. Pulls are bounded at 15 minutes each.
- **During the job:** every `dagger` run through the wrapper fails if the
  service has exited.
- **Stop:** fails when the service exited before the stop, or its stop
  wasn't clean.

## The installer's probe

`dagger/dagger-for-github` looks for a `dagger` on `PATH` first and runs
`dagger --silent version` on it, a flag the CLI of this release rejects, so
its install step fails whenever any `dagger` is on `PATH`: the one a first
`start` of the same job installed, or its wrapper, which stays on `PATH` after
its stop. The runner puts every `GITHUB_PATH` entry in front of a step's own
`PATH`, so `start` puts a small shim first instead
(`$RUNNER_TEMP/remote-cache-installer-probe/dagger`): it answers that one
probe with no version, so the official action installs (about 1.5s), and runs
the installed CLI for any other command. A job can therefore start a second
sidecar after stopping the first.

## The stop at the end of the job

`start` is a composite action around a small JavaScript action
([`start/run`](start/run/index.js)), whose `main` runs `sidecar.sh start` and
whose `post` step, with `post-if: always()`, runs `sidecar.sh stop` at the
end of the job: after a failed step, a cancelled job, or a job that passed.
It holds no logic of its own: the Docker steps are one script, the one the
demo's local run and the `stop` action run too. (A composite action has no
`post` step of its own; CI's run of this checks that a nested JavaScript
action's post step runs at the job's end, after a failure.)

The stop is the one described below, and it has **no `timeout-minutes` of its
own**: the post step is not a step of yours. The job's `timeout-minutes`
bounds it (the shim also bounds its own child process, by the stop's
documented bound plus 5 minutes). So set the job's timeout above the job's
work plus `drain-timeout` (10m) plus 2 minutes: the drain's wait, the
engine's stop (cut off at 90s), then the cleanup. A job cancelled while the
sidecar drains gets the runner's cancellation grace (about 5 minutes) for the
stop, so a long drain can be cut off then; set a shorter `drain-timeout`
where cancelled jobs matter.

## stop

```yaml
- if: always()
  # drain-timeout (10m) plus 2 minutes: the drain, the engine's stop, the cleanup.
  timeout-minutes: 12
  uses: zerobull-consulting/dagger/.github/actions/remote-cache/stop@<commit SHA>
  with:
    name: remote-cache   # the same name as the start's
```

Use it to stop earlier than the job's end, with a bound of its own, or
because a step needs the export done. `start`'s post step then finds no
state and does nothing; stopping twice is safe (the second finds nothing to
stop).

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

The steps run one after another, so the whole stop takes at most its probe
of the service (whether it is still running: about 10s in all, against a
Docker daemon that doesn't answer, and a probe that gets no answer is never
taken for the service's exit;
[zerobull-consulting/dagger#125](https://github.com/zerobull-consulting/dagger/issues/125)),
then the drain's wait (`drain-timeout` plus 5s), then the engine's stop (30s, its `docker
stop` cut off at 90s), then the wait for the service (which has exited by
then, unless it overran its own bound) and the cleanup, which take seconds.
Give the step a `timeout-minutes` of at least `drain-timeout` plus 2
minutes: 12 for `10m`. The start passes the service
`-drain-linger`, which a service image from before the linger doesn't take:
pin the images to the action's commit, as above.

**Exit codes**, the service's:

| Code | Meaning | The step |
| --- | --- | --- |
| 0 | A clean stop: everything ended within the bound. | passes |
| 1 | The stop was unclean: the bound cut the shutdown off with work still running, something else (such as engine connections still closing) left it unclean, a signal arrived during the shutdown, or an HTTP server failed, including one that failed as the stop ended. A shutdown that ends in error is logged as `stopped uncleanly`, with the reason in `error=` and what was running (the message says the shutdown's bound passed only when it did); a signal during the shutdown is logged as `signal during the shutdown: exiting at once`, and a step that overran its own bound as `stopped: the shutdown overran its bound`. | fails |
| 137 | Killed before its stop ended, not by the stop (such as out of memory). What it was exporting is lost. | fails |
| any | Still running past the stop's bound, `drain-timeout` plus 25s: the stop stopped it. | fails |
| any | The service had exited before the stop. | fails |

Known gaps of the action: a read-only start doesn't wait for the service's first merge
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
[demo](../../../hack/remote-cache/demo/README.md) through these actions on
every pull request, against MinIO, with an unsampled `TRACEPARENT` in the
job's environment (the wrapper must keep the CLI from continuing it, or the
export is never stored):

- a writer phase through `start`, with `debug-port` (it checks that the
  debug API refuses a request without the token and answers one with it,
  and that the installed CLI is the engine's version), the demo, and an
  explicit `stop`, after which `start`'s own post step has nothing to do;
- a read-only phase on a fresh engine with a read-only key, with **no stop
  step** and a failing step before the job's end: `start`'s post step stops
  it, which a post step of the job
  ([`verify-stopped`](../../../hack/remote-cache/ci/verify-stopped/action.yml))
  checks, by the containers being gone.

Each start's time is in the job's step summary. `sidecar_test.sh` tests the
script against a fake `docker` (including the CLI's version check, and that
the two pulls overlap), and `node --test start/run/index.test.js` the shim.
Locally, `hack/remote-cache/dev.sh demo` runs the phases with `sidecar.sh`,
without `SIDECAR_CLI`.

The job builds the service's image from the pull request, but runs the stock
Dagger engine release in `STOCK_ENGINE`, the first accept line of the
service's engine manifest, not an engine built from this branch: the
engine's remote cache client comes from upstream, and a step of the job
warns when this branch's client differs from that release's. The service
refuses an engine whose version isn't an accept line
([zerobull-consulting/dagger#117](https://github.com/zerobull-consulting/dagger/issues/117)).

## Changing the shim

`start/action.yml` pins the shim, [`start/run`](start/run/action.yml), by
commit: a composite action can't reference a sibling at its own commit (a
local `uses: ./...` resolves against the caller's workspace, not the
action's, which an outside caller's job would not have). The shim runs the
`sidecar.sh` of the same checkout as `start/action.yml` (`SIDECAR_SCRIPT`),
so the pin fixes only the shim. Its tests: `node --test start/run/index.test.js`.

The pin must be a commit that is already on `feature/remote-cache`, so it
stays fetchable whatever happens to a pull request's branch or head ref. So a
shim change takes two pull requests:

1. Change `start/run` (and its tests), and merge. Leave the pin as it is.
2. In a following pull request, change the pin in `start/action.yml` to the
   commit that merge made on `feature/remote-cache` (the pin may be any full
   commit id on the branch whose `start/run` is the one you want).

`hack/remote-cache/check-shim-pin.sh` enforces it, in the `fork paths` job: the
pin must be a full 40-character commit id and an ancestor of
`origin/feature/remote-cache`. A pull request that changes `start/run` and the
pin together fails it, because the new pin can't be on the branch yet; that is
the point. Its test is `hack/remote-cache/check-shim-pin_test.sh`.
