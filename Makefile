MODULE     := github.com/Tokimorphling/gosvc
GOLANGCI_LINT_VERSION ?= v2.14.0
GOPLS_VERSION ?= v0.23.0
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS    := -s -w \
	-X $(MODULE)/examples/app.Version=$(VERSION) \
	-X $(MODULE)/examples/app.Commit=$(COMMIT) \
	-X $(MODULE)/examples/app.BuildTime=$(BUILD_TIME)

.PHONY: all build run test race vet fmt lint lint-editor modernize modernize-check proto clean docker help bench-core

all: build

## build: compile the service and the load generator into ./bin
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/gosvc ./examples/app/cmd/gosvc
	go build -trimpath -o bin/bench ./examples/app/cmd/bench

## run: build and start the service with the example config
run: build
	./bin/gosvc -c configs/config.example.toml

## test: run unit tests (library, example app and the kitex example module)
test:
	go test ./...
	cd examples/kitex && go test ./...

## race: run unit tests with the race detector
race:
	go test -race ./...
	cd examples/kitex && go test -race ./...

## vet: run go vet in both modules
vet:
	go vet ./...
	cd examples/kitex && go vet ./...

## fmt: format all Go sources
fmt:
	gofmt -w .

## modernize-check: report Go toolchain modernization fixes without editing files
modernize-check:
	go fix -diff ./...
	cd examples/kitex && go fix -diff ./...

## modernize: apply Go toolchain modernization fixes, then check for remaining fixes
modernize:
	go fix ./...
	cd examples/kitex && go fix ./...
	$(MAKE) modernize-check

## lint-editor: check handwritten sources with the pinned gopls default analyzers
lint-editor:
	GOPLS_VERSION="$(GOPLS_VERSION)" bash scripts/check-gopls.sh

## lint: check formatting, vet, modernization, gopls and golangci-lint in both modules
lint: vet modernize-check lint-editor
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run
	cd examples/kitex && go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run

## proto: regenerate protobuf/gRPC code for the example app
proto:
	protoc --proto_path=. \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		examples/app/api/greeter/v1/greeter.proto

## kitex: regenerate the Kitex example code (requires the kitex CLI; runs
## inside examples/kitex, which is a standalone module so kitex stays out of
## the library's go.mod)
kitex:
	cd examples/kitex && kitex -module $(MODULE)/examples/kitex -gen-path api idl/echo.thrift

## bench: smoke-run the load generator against a local instance
bench: build
	./bin/bench -mode rest -http-addr 127.0.0.1:8080 -c 20 -d 5s

## bench-core: measure dispatch, readiness and bounded scheduling without IO
bench-core:
	go test ./jsonrpc ./health ./internal/workerpool -run '^$$' -bench 'BenchmarkDispatcher|BenchmarkReadiness|BenchmarkPoolBacklog' -benchmem -benchtime=300ms -count=5 -cpu=8

## clean: remove build artifacts
clean:
	rm -rf bin

## docker: build the container image
docker:
	docker build -f deploy/Dockerfile -t gosvc:$(VERSION) .

## help: list targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
