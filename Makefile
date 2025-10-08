SHELL := /bin/bash
PWD := $(shell pwd)

DOCKER ?= docker-compose.yaml

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

up:
	docker compose -f ${DOCKER} up -d --build
.PHONY: docker-compose-up

down:
	docker compose -f ${DOCKER} stop -t 1
	docker compose -f ${DOCKER} down
.PHONY: docker-compose-down

rebuild:
	docker compose -f ${DOCKER} stop -t 1
	docker compose -f ${DOCKER} down
	docker compose -f ${DOCKER} build --no-cache
	docker compose -f ${DOCKER} up -d

logs:
	docker compose -f ${DOCKER} logs -f
.PHONY: docker-compose-logs

test:
	docker compose -f ${DOCKER} up --build
.PHONY: test

images-clean:
	# Stop and remove all containers first
	docker compose -f ${DOCKER} down --remove-orphans || true
	docker container prune -f || true
	
	# Remove images with force and ignore errors if they don't exist
	docker rmi -f tp-distribuidos-grupo13-server:latest || true
	docker rmi -f tp-distribuidos-grupo13-client1:latest || true
	docker rmi -f tp-distribuidos-grupo13-filter_year:latest || true
	docker rmi -f tp-distribuidos-grupo13-filter_amount:latest || true
	docker rmi -f tp-distribuidos-grupo13-filter_hour:latest || true
	docker rmi -f tp-distribuidos-grupo13-aggregator_products_service:latest || true
	docker rmi -f tp-distribuidos-grupo13-joiner_items_service:latest || true
	docker rmi -f tp-distribuidos-grupo13-maximizer_products_1_service:latest || true
	docker rmi -f tp-distribuidos-grupo13-maximizer_products_2_service:latest || true
	docker rmi -f tp-distribuidos-grupo13-maximizer_products_3_service:latest || true
	docker rmi -f tp-distribuidos-grupo13-maximizer_absolute_service:latest || true
	# Clean up any dangling images
	docker image prune -f || true


.PHONY: images-clean

hard-down:
	- docker compose -f ${DOCKER} down --remove-orphans --timeout 20
	- docker kill $$(docker ps -q --filter "name=tp-distribuidos-grupo13") 
	- docker rm -f $$(docker ps -aq --filter "name=tp-distribuidos-grupo13")
.PHONY: hard-down

prune:
	docker image prune -a -f
	docker system prune -f
.PHONY: prune