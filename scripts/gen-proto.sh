#!/usr/bin/env bash
set -euo pipefail

# Ensure gen/proto directory exists
mkdir -p gen/proto

export PATH="$PATH:$(go env GOPATH)/bin"

protoc \
  --proto_path=api/proto \
  --go_out=gen/proto \
  --go_opt=paths=source_relative \
  --go-grpc_out=gen/proto \
  --go-grpc_opt=paths=source_relative \
  api/proto/broker/v1/broker.proto
