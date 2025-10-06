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

up:
	docker compose -f $0 up -d --build
.PHONY: docker-compose-up

down:
	docker compose -f $0 stop -t 1
	docker compose -f $0 down
.PHONY: docker-compose-down

rebuild:
	docker compose -f $0 stop -t 1
	docker compose -f $0 down
	docker compose -f $0 build --no-cache
	docker compose -f $0 up -d

logs:
	docker compose -f $0 logs -f
.PHONY: docker-compose-logs

test:
	docker compose -f $0 up --build
.PHONY: test

images-clean:
	docker rmi tp-distribuidos-grupo13-server:latest 
	docker rmi tp-distribuidos-grupo13-client_id_1:latest 
	docker rmi tp-distribuidos-grupo13-filter_year_id_1_service:latest   
	docker rmi tp-distribuidos-grupo13-aggregator_products_id_1_service:latest 
	docker rmi tp-distribuidos-grupo13-joiner_items_id_1_service:latest 
	docker rmi tp-distribuidos-grupo13-maximizer_max_1_3_id_1_service:latest 
	docker rmi tp-distribuidos-grupo13-maximizer_max_4_6_id_1_service:latest 
	docker rmi tp-distribuidos-grupo13-maximizer_max_7_8_id_1_service:latest 
	docker rmi tp-distribuidos-grupo13-maximizer_max_absolute_id_1_service:latest

.PHONY: images-clean
