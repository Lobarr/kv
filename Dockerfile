# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/kv ./main.go

FROM alpine:3.24

RUN apk --no-cache add ca-certificates \
    && addgroup -S kv && adduser -S -G kv kv \
    && mkdir -p /data && chown kv:kv /data

COPY --from=builder /out/kv /usr/local/bin/kv

USER kv
VOLUME ["/data"]
EXPOSE 8080 8081

ENTRYPOINT ["/usr/local/bin/kv"]
CMD ["-data-path=/data"]
