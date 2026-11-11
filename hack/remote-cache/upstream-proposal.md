# A reference implementation of the remote cache protocol, on object storage

*Draft for the Dagger maintainers. Not yet posted anywhere.*

## Table of Contents

- [Problem](#problem)
- [What this is, and is not](#what-this-is-and-is-not)
- [Limits](#limits)
- [Options for the tree](#options-for-the-tree)
- [Layout, if any of it lands](#layout-if-any-of-it-lands)
- [How it is tested](#how-it-is-tested)
- [What stays out](#what-stays-out)
- [Status](#status)

## Problem

1. **A protocol with no inspectable counterpart** - `engine/remotecache` and `engine/remotecache/protocol` connect an engine to "the remote cache service", but the tree holds no service, so the engine side is exercised only against fakes.
2. **Nothing for people who self-host** - Teams that cannot or will not use a hosted cache have no working example of how an engine's cache is stored, expired and served from a bucket they own.
3. **Drift** - The service in this proposal follows the protocol from outside the tree, by hand, release by release.

## What this is, and is not

Dagger offers caching as part of its paid platform, together with compute and scheduling. This proposal does not ask Dagger to carry a competing product, and the service is not one:

| | |
| --- | --- |
| **Is** | A reference architecture for an engine cache on object storage: how the protocol's exports, imports, lookups, garbage collection and bucket ownership map onto S3 conditional writes. A working, tested example of the protocol's other half. |
| **Is not** | A hosted service, a scheduler, a compute layer, or a managed multi-tenant cache. It has none of the integration a platform offers. |

It was built by one person, with AI assistance, in a few weeks, and is used in CI by one private consumer. The platform's investment is of a different order; this proposal does not claim parity with it.

## Limits

| Limit | What is true today |
| --- | --- |
| **Object storage only** | S3-compatible stores: MinIO is tested in CI; DigitalOcean Spaces by hand. |
| **A sidecar** | The service starts beside the engine in a job and stops with it. It does not run as a shared, always-on service in anything we have tested. |
| **Concurrent writers** | Safe by design, best avoided. Index updates are conditional writes with retry (`UpdateIndex`, up to 50 attempts), racing exports are resolved by `If-None-Match` (`TestVerifyRacingExports`, `TestWritersRaceOnEmptyPrefix`). Throughput under contention is not measured. The fork's workflows run one writer per prefix. |
| **Break-even** | Observed startup and drain time for the service is about 30 seconds. That makes it a good fit for caching work like dependency installation from a package manager lockfile, or toolchain builds that can be separated from application code, and a poor fit for small, quick items that a runner-local cache already serves repeatedly. The action reports the start time and `hack/remote-cache/speedup/run.sh` counts cache hits against executed steps. A private consumer, a small (~60k lines) Next.js web application, uses it to cache an Alpine customization and an npm install. The same cache serves several pipelines, each with a slightly different set of packages, and the net saving, after the service's own startup and drain time, is about 3 minutes. It is a key part of its solo developer's velocity. |
| **One engine version** | The service accepts the engine version it was built with. The protocol's handshake carries `EngineVersion`; how a tree-built service would enforce it is to be settled. |

## Options for the tree

Each is a complete answer; a "no" to all of them is fine, and leaves the service where it is.

| Option | What Dagger would carry | Cost to Dagger |
| --- | --- | --- |
| **A. Conformance suite** | The service's protocol tests and the fake engine, as a test harness for `engine/remotecache` that runs against any implementation, plus a short "how an implementation behaves" document | Low: tests and docs; no binary, no image |
| **B. Reference example** | A, plus the service as an example module or `contrib/` directory, labeled unsupported | Medium: a directory that builds and is tested |
| **C. Companion** | B as a supported component, with its image published | High; likely overlaps the paid offering |
| **D. Nothing** | Nothing; the service stays an independent project that follows the protocol | None |

The proposal is for A, with B open to discussion. C is not proposed.

The service imports 49 packages from `internal/buildkit`, plus `engine/snapshots`, `internal/fsutil`, `internal/cloud` and others. Go does not allow another module to import `internal/`, so as an independent project it keeps a synced copy of what it needs. In the tree, those imports are ordinary.

## Layout, if any of it lands

A nested Go module, as `internal/fastbolt` and `sdk/go` use: its own `go.mod` with a `replace` to the root, which keeps the S3 SDK out of the root `go.mod`.

| Piece | Proposed location | Today |
| --- | --- | --- |
| Conformance tests, fake engine (A) | `engine/remotecache/conformance/` | `cmd/cacheservice/internal/fakeengine/` and the protocol tests |
| Service packages, binary (B) | `contrib/remote-cache/`, nested module | `cmd/cacheservice/` |
| Build and test | A dev module under `.dagger/modules/`, like `engine-dev` and `test-split` | `.dagger/modules/cacheservice` (exists) |

## How it is tested

| Tier | What | Runs |
| --- | --- | --- |
| Unit | Hermetic Go tests, no store, no Docker | `test-split:test-packages` |
| Store | The `TestMinIO...` tests against MinIO as a Dagger service | `dagger check cacheservice:test-minio` (exists; 125 tests pass) |
| Recorded | The few tests that pin where Spaces differs from MinIO, replayed from fixtures recorded against a real bucket | CI, no secrets, no bucket *(planned)* |
| Live | Real-engine end-to-end and live Spaces | Sporadically, by the service's maintainer, outside the tree |

Recorded fixtures show that the client builds the expected requests and parses the recorded answers. They do not show that a store still behaves that way; re-recording is the refresh.

## What stays out

| Not proposed | Why |
| --- | --- |
| Compatibility manifest, accept lines, release check | Needed only because the service is versioned apart from the engine |
| Fork-paths check, upstream sync | No fork |
| Spaces bucket, Terraform, Spaces workflows | Replaced by the recorded tier |
| The sidecar GitHub Action | Consumer tooling; stays with its author |
| Prompt templates and process docs | Fork process |

## Status

Draft. The service works and runs the author's CI; the MinIO check and the image build and publish already run as Dagger modules in the fork. Nothing here has been reviewed by the maintainers.
