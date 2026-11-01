# Everything runs in Docker except `make chaos`, which runs the services as
# local processes against the dockerised Postgres and Kafka.
SHELL := /bin/bash
COMPOSE := docker compose
-include .env
PG_PORT ?= 5432
KAFKA_PORT ?= 9092
LOCAL := PG_ADMIN_URL=postgres://postgres:postgres@localhost:$(PG_PORT)/postgres?sslmode=disable KAFKA_BROKERS=localhost:$(KAFKA_PORT)

.PHONY: test test-integration up down chaos bench demo

test:                 ## unit + property tests, race detector
	go test -race ./...

test-integration:     ## Testcontainers: needs a Docker daemon
	go test -race -tags integration -count=1 ./...

up:                   ## whole stack: postgres, kafka, services, prometheus, grafana
	$(COMPOSE) --profile app up -d --build --wait

down:
	$(COMPOSE) --profile app --profile load down -v

chaos:                ## crash matrix: every failpoint x 5 seeds, writes docs/chaos-matrix.md
	$(COMPOSE) up -d --wait postgres kafka
	go build -o bin/ledgersaga ./cmd/ledgersaga
	$(LOCAL) ./bin/ledgersaga chaos -seeds 5 -parallel 4

bench:                ## load at 100/1000 clients for both relays + crash recovery, writes results/
	./scripts/bench.sh

demo: up              ## one transfer end to end
	curl -s -XPOST localhost:$${API_PORT:-8080}/transfers -H 'Idempotency-Key: demo-1' -d '{"from":1,"to":2,"amount":100,"currency":"USD"}'
