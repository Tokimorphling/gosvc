MODULE     := example.com/gosvc
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS    := -s -w \
	-X $(MODULE)/internal/version.Version=$(VERSION) \
	-X $(MODULE)/internal/version.Commit=$(COMMIT) \
	-X $(MODULE)/internal/version.BuildTime=$(BUILD_TIME)

.PHONY: all build run test race vet fmt lint proto clean docker help

all: build

## build: compile the service and the load generator into ./bin
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/gosvc ./cmd/gosvc
	go build -trimpath -o bin/bench ./cmd/bench

## run: build and start the service with the example config
run: build
	./bin/gosvc -c configs/config.example.json

## test: run unit tests
test:
	go test ./...

## race: run unit tests with the race detector
race:
	go test -race ./...

## vet: run go vet
vet:
	go vet ./...

## fmt: format all Go sources
fmt:
	gofmt -w .

## lint: fail when sources are not formatted or vet reports problems
lint: vet
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }

## proto: regenerate protobuf/gRPC code
proto:
	protoc --proto_path=. \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/greeter/v1/greeter.proto

## bench: smoke-run the load generator against a local instance
bench: build
	./bin/bench -mode rest -http-addr 127.0.0.1:8080 -c 20 -d 5s

## clean: remove build artifacts
clean:
	rm -rf bin

## docker: build the container image
docker:
	docker build -f deploy/Dockerfile -t gosvc:$(VERSION) .

## help: list targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
