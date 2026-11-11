# A remote cache service for the engine's remote cache protocol

*Draft for the Dagger maintainers. Not yet posted anywhere.*

## Table of Contents

- [Problem](#problem)
- [Solution](#solution)
- [Where it lives](#where-it-lives)
- [How it is tested](#how-it-is-tested)
- [What stays out](#what-stays-out)
- [Landing it](#landing-it)
- [Status](#status)

## Problem

1. **Half a protocol** - `engine/remotecache` and `engine/remotecache/protocol` connect an engine to "the remote cache service", but the tree holds no service. The engine side can only be exercised against fakes.
2. **No open implementation** - Anyone who wants the feature, including Dagger's own CI, needs a service that speaks the protocol. Today the only working one is a fork's.
3. **Drift** - A service developed apart from the engine follows the protocol by chasing it. The fork does this by hand, release by release, with a compatibility manifest and an hourly check.
4. **Not includable as it stands** - The fork's service is `cmd/cacheservice` with 70 Go files and ~27k lines of production code, ~45k lines of tests, and ~9.6k lines of scripts and workflows. Its tests lean on a live DigitalOcean Spaces bucket, which an upstream project should not need to keep.

## Solution

Contribute the service as the reference counterpart of the protocol: one binary, built and tested from the same tree and commit as the engine, with MinIO-backed tests, recorded fixtures for the one real store it has to behave against, and none of the fork's release or sync machinery.

The service imports 49 packages from `internal/buildkit`, plus `engine/snapshots`, `internal/fsutil`, `internal/cloud` and others. Go does not allow another module to import `internal/`, so a standalone repository would have to copy or sync that code. That is the cost the fork pays today. In the tree, those imports are ordinary.

| Option | Cost | Verdict |
| --- | --- | --- |
| **In the tree, nested Go module** | One more directory to own; S3 SDK stays out of the root `go.mod` | Proposed |
| **Separate repository** | Copy or sync `internal/` packages; version-pair the service with every engine release | Only if the maintainers don't want it in core |
| **Stay a fork** | Every protocol change is a manual follow-up by the fork's author | Works today; doesn't scale |

## Where it lives

A nested module follows the pattern `internal/fastbolt` and `sdk/go` already use: its own `go.mod` with a `replace` to the root. Import visibility is by path, so it keeps access to `internal/`.

| Piece | Proposed location | Today |
| --- | --- | --- |
| Binary (`main` only) | `cmd/dagger-remote-cache/` (named for its image) | `cmd/cacheservice/` |
| Service packages (server, exporter, importer, blobstore, metastore, gc, lifecycle) | `engine/remotecache/service/...`, nested module | `cmd/cacheservice/*` |
| Test support (S3 guard, fake engine) | `engine/remotecache/service/internal/` | `cmd/cacheservice/internal/` |
| Build, test and publish | A dev module under `.dagger/modules/`, like `engine-dev` and `test-split` | `.dagger/modules/cacheservice` (exists) |
| Consumer-facing CI integration (sidecar that starts the service beside an engine) | To be decided with the maintainers: a module under `modules/`, or kept in the author's repository | `.github/actions/remote-cache/` |

The service accepts the engine whose version it was built with. The protocol's handshake already carries `EngineVersion`; this replaces the fork's accept-line manifest, which exists only because the two ship separately.

## How it is tested

| Tier | What | Runs |
| --- | --- | --- |
| Unit | Hermetic Go tests, no store, no Docker | `test-split:test-packages` picks them up |
| Store | The `TestMinIO...` tests against MinIO as a Dagger service, with a read-only user for the permission tests | `dagger check cacheservice:test-minio` (exists; 125 tests pass) |
| Recorded | The few tests that pin where DigitalOcean Spaces differs from MinIO (copy hashing, conditional copies, bucket policy behavior), replayed from fixtures recorded against a real bucket | CI, no secrets, no bucket |
| Live | Real-engine end-to-end and the live Spaces run | Sporadically, by the maintainer of the service, outside the tree |

Recorded fixtures are request-and-response pairs matched on method, normalized key, query and body digest, with credentials, signatures, dates and run-unique prefixes removed. They show that the client builds the expected requests and parses the recorded answers. They do not show that Spaces still behaves that way; re-recording is the refresh.

## What stays out

These exist because the service is versioned apart from the engine, or because the fork needs them. They stay with the fork.

| Not proposed | Why |
| --- | --- |
| Engine compatibility manifest, accept lines, release check | Compatibility by construction in the tree |
| Fork-paths check, upstream sync, merge-commit rules | No fork |
| Spaces bucket, Terraform, Spaces workflows | Replaced by the recorded tier |
| Real-engine e2e in CI | Out of scope for now; live tier |
| Prompt templates and work-item process docs | Fork process |

## Landing it

Slices a reviewer can read, each with its tests and docs:

1. Test support and the S3 guard, with the module's `test-minio` check.
2. Blob store and metastore.
3. Importer and exporter.
4. Server and garbage collection.
5. Binary, image and publish module.

Before slice 1, the layout above needs the maintainers' agreement. If they would rather not carry a service in core, the same layout becomes its own repository with a pinned Dagger version and a documented sync of the `internal/` packages it needs.

The measured benefit and who runs it today go here before this is posted: *[add: speedup numbers from the consumer's CI, and how long it has run]*.

## Status

Draft. The service works and is in use; the MinIO check and the image build and publish already run as Dagger modules on the fork. Nothing in this proposal has been reviewed by the maintainers.
