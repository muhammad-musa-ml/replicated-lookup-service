#!/bin/sh
# Generate go/lookuppb from ../proto/lookup.proto.
# Needs protoc, protoc-gen-go and protoc-gen-go-grpc on PATH (see go/Dockerfile).
set -e
cd "$(dirname "$0")"
mkdir -p lookuppb
protoc -I ../proto \
  --go_out=lookuppb --go_opt=paths=source_relative \
  --go-grpc_out=lookuppb --go-grpc_opt=paths=source_relative \
  ../proto/lookup.proto
