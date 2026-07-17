# syntax=docker/dockerfile:1.7

FROM golang:1.26-alpine AS build

ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -buildid= -X main.version=${VERSION}" \
      -o /out/shard ./cmd/shard && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -buildid=" \
      -o /out/tracker ./cmd/tracker && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -buildid=" \
      -o /out/source ./cmd/source

FROM alpine:3.23

ARG VERSION=dev
ARG COMMIT=unknown

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 65532 -S saveweb && \
    adduser -u 65532 -S -D -H -G saveweb saveweb

COPY --from=build /out/shard /out/source /out/tracker /usr/local/bin/
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

LABEL org.opencontainers.image.title="SavewebHQ" \
      org.opencontainers.image.description="Explicit distributed queue for Saveweb web archive workers" \
      org.opencontainers.image.source="https://github.com/saveweb/hq" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"

USER 65532:65532
EXPOSE 8080 8081 9081

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["serve"]
