# Stage 1: Build the static binary
FROM golang:1.25-alpine AS build

WORKDIR /src
# Copy dependency manifests
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked linux binary
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/broker \
    ./cmd/broker

# Stage 2: Create a minimal runner container running as non-root user
FROM alpine:3.20

# Create broker group and user, add curl for health checks
RUN addgroup -S broker && \
    adduser -S -G broker broker && \
    apk add --no-cache ca-certificates curl

# Copy binary from build stage
COPY --from=build /out/broker /usr/local/bin/broker

# Create data directory and assign permissions to broker user
RUN mkdir -p /var/lib/broker && \
    chown -R broker:broker /var/lib/broker

# Run as non-root user
USER broker

# Expose ports: 9092 (gRPC), 9093 (HTTP metrics/health)
EXPOSE 9092 9093

# Mount persistent data directory volume
VOLUME ["/var/lib/broker"]

# Run the broker binary directly
ENTRYPOINT ["/usr/local/bin/broker"]
