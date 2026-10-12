#!/bin/sh
# Run the Go tests without a local Go toolchain. From the repository root:
#   docker run --rm -v "$PWD":/src -v rls-gocache:/go/pkg golang:1.26-bookworm sh /src/go/test_in_docker.sh
set -e
apt-get update -qq
apt-get install -y -qq --no-install-recommends protobuf-compiler > /dev/null
export GOFLAGS=-buildvcs=false
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
cd /src/go
sh gen.sh
go vet ./...
go test -count=1 ./...
