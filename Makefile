.PHONY: proto
proto:
	bash scripts/gen-proto.sh

.PHONY: build
build:
	mkdir -p bin
	go build -o bin/broker ./cmd/broker
	go build -o bin/producer ./cmd/producer
	go build -o bin/consumer ./cmd/consumer
	go build -o bin/admin ./cmd/admin
	go build -o bin/bench_producer ./cmd/bench_producer
	go build -o bin/bench_consumer ./cmd/bench_consumer

.PHONY: test
test:
	go test -v ./...

.PHONY: docker
docker:
	docker build -t distributed-message-broker:latest .

.PHONY: bench
bench: build
	bash benchmarks/scripts/run-benchmarks.sh
