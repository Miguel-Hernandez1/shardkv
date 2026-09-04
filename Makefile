.PHONY: build test up down logs bench demo proto vet tidy

BINARY_SERVER   = bin/server
BINARY_CLI      = bin/shardkv-cli
BINARY_BENCH    = bin/bench

build: vet
	mkdir -p bin
	go build -o $(BINARY_SERVER) ./cmd/server
	go build -o $(BINARY_CLI)    ./cmd/shardkv-cli
	go build -o $(BINARY_BENCH)  ./cmd/bench

vet:
	go vet ./...

tidy:
	go mod tidy

test:
	go test -race -timeout 120s ./...

up:
	docker compose -f deploy/docker-compose.yml up -d --build

down:
	docker compose -f deploy/docker-compose.yml down

logs:
	docker compose -f deploy/docker-compose.yml logs -f

bench: build
	./$(BINARY_BENCH) --addr localhost:8081 --ops 10000 --concurrency 32

demo: build
	./scripts/demo.sh

clean:
	rm -rf bin
