SHELL := /bin/bash
PWD := $(shell pwd)

DOCKER ?= docker-compose.yaml
QUERY ?= all

ifeq ($(QUERY),all)
CONFIG := q-all-config.ini
else ifeq ($(QUERY),1)
CONFIG := q1-config.ini
else ifeq ($(QUERY),2)
CONFIG := q2-config.ini
else ifeq ($(QUERY),3)
CONFIG := q3-config.ini
else ifeq ($(QUERY),4)
CONFIG := q4-config.ini
else
$(error Invalid QUERY value '$(QUERY)'. Use 'all', '1', '2', '3', or '4'.)
endif

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

build:
	# Generate docker-compose file based on the selected query configuration
	python3 generar-compose.py --config=${CONFIG}

up:
	make clean-results
	python3 generar-compose.py --config=${CONFIG}
	docker compose -f ${DOCKER} up -d --build
.PHONY: docker-compose-up

test:
	# Run the docker-compose setup
	make clean-results
	python3 generar-compose.py --config=${CONFIG}
	docker compose -f ${DOCKER} up --build
.PHONY: test

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
	> logs.txt
	docker compose -f ${DOCKER} logs -f > logs.txt 
.PHONY: docker-compose-logs

clean-results:
	rm -rf .results/client-1/*
	rm -rf .results/client-2/*
	rm -rf .results/client-3/*
.PHONY: clean-results

images-clean:
	# Stop and remove all containers first
	docker compose -f ${DOCKER} down --remove-orphans || true
	docker container prune -f || true
	
	# Remove all images from this project by name pattern
	docker images --format "table {{.Repository}}:{{.Tag}}" | grep -E "tp-distribuidos-grupo13|server|client|filter|aggregator|joiner|maximizer" | xargs -r docker rmi -f || true
	
	# Remove specific images that might remain
	docker rmi -f tp-distribuidos-grupo13-server || true
	docker rmi -f tp-distribuidos-grupo13-client1 || true
	docker rmi -f tp-distribuidos-grupo13-filter_year_1 || true
	docker rmi -f tp-distribuidos-grupo13-aggregator_products || true
	docker rmi -f tp-distribuidos-grupo13-maximizer_products_1 || true
	docker rmi -f tp-distribuidos-grupo13-maximizer_products_4 || true
	docker rmi -f tp-distribuidos-grupo13-maximizer_products_7 || true
	docker rmi -f tp-distribuidos-grupo13-maximizer_absolute || true
	docker rmi -f tp-distribuidos-grupo13-joiner_items || true
	
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