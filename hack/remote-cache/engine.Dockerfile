# An engine image with this fork's remote cache client: the engine,
# dagger-init, dagger-init-session and the CLI built from this repo, copied
# over the release engine image they were last merged with. Everything else in
# the image (crun, CNI plugins, qemu, tini, the SDK manifests, engine.json)
# is the release's. Build it from the repository root (see
# hack/remote-cache/README.md, "Engine image"):
#
#   docker build -f hack/remote-cache/engine.Dockerfile -t dagger-engine .
#
# The context is the repository root; engine.Dockerfile.dockerignore, next to
# this file, keeps it to what the build needs.

# The release the fork last merged from main (internal/version/VERSION is the
# next one, v1.0.0-beta.25). When a merge from main brings a new release, move
# this to it. Both base images are pinned by the digest of their
# multi-platform index; update the tag and the digest together.
ARG BASE_IMAGE=registry.dagger.io/engine:v1.0.0-beta.24@sha256:3ad2b0b80942bb5044e57c1765710a5fab0b913557b6ab7d7f4e81700ac4da35

# golang at go.mod's version, as in cmd/cacheservice/Dockerfile.
FROM --platform=$BUILDPLATFORM golang:1.26.6@sha256:0d1d3a794be25f809dd2cb3160d8c73276c4056a9f8242a138e908ddeee7b6b6 AS build
ARG TARGETOS
ARG TARGETARCH
# The commit, stamped into every binary (`dagger version` prints it). The
# context has no .git, so Go can't stamp it itself.
ARG REVISION=""
# The CLI's default engine: with ENGINE_IMAGE set (CI sets it to this image's
# SHA tag), the CLI copied out of the image provisions this same engine by
# itself, with no _EXPERIMENTAL_DAGGER_RUNNER_HOST. Unset, the CLI falls back
# to registry.dagger.io/engine:<VERSION>, a release that doesn't exist yet.
ARG ENGINE_IMAGE=""
WORKDIR /src
COPY . .
# The cache mounts keep modules and build results between local builds; on a
# fresh runner they start empty.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    common="-s -w -X github.com/dagger/dagger/internal/version.Commit=${REVISION}"; \
    cli="$common"; \
    if [ -n "$ENGINE_IMAGE" ]; then \
      cli="$cli -X github.com/dagger/dagger/internal/cmd/dagger.RunnerHost=image://${ENGINE_IMAGE}"; \
    fi; \
    export CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH; \
    # dfexcludepatterns and dfparents: the Dockerfile frontend's COPY/ADD
    # --exclude and --parents, as in .dagger/modules/engine-dev.
    go build -trimpath -tags dfexcludepatterns,dfparents -ldflags "$common" -o /out/dagger-engine ./cmd/engine; \
    go build -trimpath -ldflags "$common" -o /out/dagger-init ./cmd/init; \
    go build -trimpath -ldflags "$common" -o /out/dagger-init-session ./cmd/init-session; \
    go build -trimpath -ldflags "$cli" -o /out/dagger ./cmd/dagger

FROM ${BASE_IMAGE}
ARG BASE_IMAGE
ARG REVISION=""
LABEL org.opencontainers.image.source="https://github.com/zerobull-consulting/dagger" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.title="dagger-engine" \
      org.opencontainers.image.description="Dagger engine with the remote cache client (feature/remote-cache)" \
      org.opencontainers.image.base.name="${BASE_IMAGE}"
COPY --from=build /out/dagger-engine /out/dagger-init /out/dagger-init-session /out/dagger /usr/local/bin/
# The release's entrypoint (dagger-entrypoint.sh), user and environment are
# kept as they are.
