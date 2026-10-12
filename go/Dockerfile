# build context is the repository root
FROM golang:1.26-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12 \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
WORKDIR /src
COPY proto/lookup.proto proto/lookup.proto
COPY go/go.mod go/go.sum go/gen.sh go/
WORKDIR /src/go
RUN go mod download
COPY go/ ./
RUN sh gen.sh && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/dataset-go ./cmd/server

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/dataset-go /dataset-go
ENV PORT=50051
ENTRYPOINT ["/dataset-go"]
