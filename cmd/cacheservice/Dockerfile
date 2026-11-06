# The cache service's image: a static binary on distroless, nonroot.
# Build it from the repository root (see the README, "Image"):
#
#   docker build -f cmd/cacheservice/Dockerfile -t cacheservice .
#
# The context is the repository root, since the module is the whole monorepo;
# Dockerfile.dockerignore, next to this file, keeps it to what the build needs.

# golang at go.mod's version. Both base images are pinned by the digest of
# their multi-platform index; update the tag and the digest together.
FROM --platform=$BUILDPLATFORM golang:1.26.6@sha256:0d1d3a794be25f809dd2cb3160d8c73276c4056a9f8242a138e908ddeee7b6b6 AS build
ARG TARGETOS
ARG TARGETARCH
# Recorded in the ownership marker this image creates (README, "Bucket
# ownership").
ARG REVISION=""
# The upstream release this build is (compat.Release), reported as its base
# in its identity. It must be cmd/cacheservice/compat/engines.txt's base line,
# or the service refuses to start. Empty, as for a build that tracks
# upstream main, the service reports its base as "dev".
ARG RELEASE=""
WORKDIR /src
COPY . .
# The cache mounts keep modules and build results between local builds; on a
# fresh runner they start empty.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w -X github.com/dagger/dagger/cmd/cacheservice/ownership.ServiceVersion=${REVISION} -X github.com/dagger/dagger/cmd/cacheservice/compat.Release=${RELEASE}" \
      -o /out/cacheservice ./cmd/cacheservice

# CA certificates, tzdata, and user 65532 (nonroot); no shell.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG REVISION=""
LABEL org.opencontainers.image.source="https://github.com/zerobull-consulting/dagger" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.title="cacheservice"
COPY --from=build /out/cacheservice /cacheservice
USER 65532:65532
# The engine WebSocket and the OTLP receivers (-listen). The debug API
# (-debug-listen, 127.0.0.1:8091) is the container's own loopback; to publish
# it, bind it to :8091, which then needs -debug-token-file (a token file
# mounted read-only, readable by 65532). The metrics address
# (-metrics-listen) is off by default.
EXPOSE 8090
# No HEALTHCHECK: there is no shell or HTTP client in the image, and the
# engine reconnects with backoff.
ENTRYPOINT ["/cacheservice"]
