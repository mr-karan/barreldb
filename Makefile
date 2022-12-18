APP-BIN := ./bin/barreldb.bin

LAST_COMMIT := $(shell git rev-parse --short HEAD)
LAST_COMMIT_DATE := $(shell git show -s --format=%ci ${LAST_COMMIT})
VERSION := $(shell git describe --tags)
BUILDSTR := ${VERSION} (Commit: ${LAST_COMMIT_DATE} (${LAST_COMMIT}), Build: $(shell date +"%Y-%m-%d% %H:%M:%S %z"))

.PHONY: build
build: ## Build binary.
	go build -o ${APP-BIN} -ldflags="-X 'main.buildString=${BUILDSTR}'" ./cmd/server/

.PHONY: run
run: ## Run binary.
	./${APP-BIN} --config=./cmd/server/config.sample.toml

.PHONY: clean
clean: ## Remove temporary files and the `bin` folder.
	rm -rf bin
	rm -rf data/barrel_*

.PHONY: fresh
fresh: build run

.PHONY: test
test:
	go test -v -failfast -race -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt ./...

.PHONY: bench
bench:
	go test -bench=. -benchmem ./...
