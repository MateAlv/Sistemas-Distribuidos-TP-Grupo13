SHELL := /bin/bash
PWD := $(shell pwd)

DOCKER ?= docker-compose.yaml
QUERY ?= all
COMPOSE_SCRIPT := scripts/generar-compose.py

ifeq ($(QUERY),all)
CONFIG := config/config.ini
else ifeq ($(QUERY),1)
CONFIG := config/q1-config.ini
else ifeq ($(QUERY),2)
CONFIG := config/q2-config.ini
else ifeq ($(QUERY),3)
CONFIG := config/q3-config.ini
else ifeq ($(QUERY),4)
CONFIG := config/q4-config.ini
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

compose:
	# Generate docker-compose file based on the selected query configuration
	python3 $(COMPOSE_SCRIPT) --config=${CONFIG}
.PHONY: compose

build:
	# Generate docker-compose file based on the selected query configuration
	python3 $(COMPOSE_SCRIPT) --config=${CONFIG}
	docker compose -f ${DOCKER} build
.PHONY: build

up:
	make clean-results
	python3  $(COMPOSE_SCRIPT) --config=${CONFIG}
	docker compose -f ${DOCKER} up -d --build
.PHONY: docker-compose-up

test:
	# Run the docker-compose setup
	make clean-results
	-docker compose -f ${DOCKER} down -v --remove-orphans
	-docker run --rm -v $(PWD)/data/persistence:/persistence alpine sh -c 'rm -rf /persistence/*'
	python3  $(COMPOSE_SCRIPT) --config=config/config-test.ini
	@echo "Running tests... Logs redirected to logs.txt"
	@if docker compose -f ${DOCKER} up --build > logs.txt 2>&1; then \
		echo "Test passed"; \
	else \
		echo "Test failed. Check logs.txt"; \
		exit 1; \
	fi
.PHONY: test

test-small:
	# Clean up previous run
	-docker compose -f ${DOCKER} down -v --remove-orphans
	-docker run --rm -v $(PWD)/data/persistence:/persistence -v $(PWD)/.results:/results alpine sh -c 'rm -rf /persistence/* /results/*'
	# Generate small dataset
	python3 scripts/create_small_dataset.py
	# Run the docker-compose setup
	python3 $(COMPOSE_SCRIPT) --config=config/config-small.ini
	docker compose -f ${DOCKER} up --build
.PHONY: test-small

down:
	docker compose -f ${DOCKER} stop -t 1
	docker compose -f ${DOCKER} down
.PHONY: docker-compose-down

rebuild:
	docker compose -f ${DOCKER} stop -t 1
	docker compose -f ${DOCKER} down
	docker compose -f ${DOCKER} build --no-cache
	docker compose -f ${DOCKER} up -d

	docker compose -f ${DOCKER} up -d
.PHONY: rebuild

logs:
	> logs.txt
	docker compose -f ${DOCKER} logs -f > logs.txt 
.PHONY: docker-compose-logs

clean-results:
	find .results -name "*.csv" -type f -delete
.PHONY: clean-results

hard-down:
	- docker compose -f ${DOCKER} down --remove-orphans --timeout 20
	- docker kill $$(docker ps -q --filter "name=tp-distribuidos-grupo13") 
	- docker rm -f $$(docker ps -aq --filter "name=tp-distribuidos-grupo13")
.PHONY: hard-down

prune:
	docker image prune -a -f
	docker system prune -f
	docker system prune -a --volumes
	sudo sh -c 'truncate -s 0 /var/lib/docker/containers/*/*-json.log || true'
.PHONY: prune