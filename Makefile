SHELL := /bin/bash
PWD := $(shell pwd)

default: docker-image

all: docker-image

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" ./server
	docker build -f ./client/Dockerfile -t "client:latest" ./client

.PHONY: docker-image

docker-compose-up: docker-image
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
	docker compose -f docker-compose-test.yaml up

test-clean:
	docker compose -f docker-compose-test.yaml stop -t 1
	docker compose -f docker-compose-test.yaml down
	docker rmi tp-distribuidos-grupo13-client:latest tp-distribuidos-grupo13-server:latest