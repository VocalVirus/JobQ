# Multi-stage build: compile a static binary, then ship it in a tiny runtime
# image. Keeps the final image small (no Go toolchain, no source) and fast to
# rebuild — the go.mod/go.sum layer is cached until dependencies actually change.

# ---- build stage ----
# Pinned to the same Go version as go.mod's `go` directive so the build image is
# never older than the directive requires.
FROM golang:1.26.5-alpine AS build
WORKDIR /src

# Download deps first, on their own layer, so edits to source don't re-fetch them.
COPY go.mod go.sum ./
RUN go mod download

# Then the source, and build a static binary (CGO off → no libc dependency,
# so it runs on a minimal base image).
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /jobq ./cmd/jobq

# ---- runtime stage ----
FROM alpine:3.20
# Run as a non-root user — good hygiene for a network service.
RUN adduser -D -u 10001 jobq
USER jobq
COPY --from=build /jobq /jobq
EXPOSE 8080
ENTRYPOINT ["/jobq"]
