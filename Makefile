SHELL := /bin/bash
PWD := $(shell pwd)

default: docker-image

all: docker-image

docker-image:
	# Server & Client
	docker build -f ./server/Dockerfile -t "server:latest" ./server
	docker build -f ./client/Dockerfile -t "client:latest" ./client
	# Workers
	docker build -f ./workers/filters/Dockerfile -t "workers-filters:latest" ./workers/filters
	docker build -f ./workers/joiners/Dockerfile -t "workers-joiners:latest" ./workers/joiners
	docker build -f ./workers/aggregators/Dockerfile -t "workers-aggregators:latest" ./workers/aggregators

.PHONY: docker-image

docker-compose-up:
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

test:
	docker compose -f docker-compose-test.yaml up --build
.PHONY: test

test-clean:
	docker compose -f docker-compose-test.yaml stop -t 1
	docker compose -f docker-compose-test.yaml down
	
.PHONY: test-clean

test-rebuild:
	docker compose -f docker-compose-test.yaml stop -t 1
	docker compose -f docker-compose-test.yaml down
	docker compose -f docker-compose-test.yaml build --no-cache
	docker compose -f docker-compose-test.yaml up
	
.PHONY: test-rebuild
