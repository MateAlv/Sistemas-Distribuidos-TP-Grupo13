#!/usr/bin/env python3
import yaml
import os


def read_config(path: str):
    meta = {
        "compose_name": "default_compose",
        "output_file": "docker-compose.yaml"
    }
    nodes = {}

    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or ":" not in line:
                continue

            key, value = line.split(":", 1)
            key, value = key.strip(), value.strip()

            if key.lower() in ("compose_name", "output_file"):
                meta[key.lower()] = value
            elif key.startswith("NODO_"):
                nodes[key] = int(value)
            else:
                print(f"Ignorando línea desconocida: {line}")

    return meta, nodes

def get_main_folder(nodo: str) -> str:
    mapping = {
        "SERVER": "server",
        "FILTER_YEAR": "workers",
        "FILTER_HOUR": "workers",
        "FILTER_AMOUNT": "workers",
        "AGGREGATOR_PRODUCTS": "workers",
        "MAXIMIZER_1_3": "workers",
        "MAXIMIZER_4_6": "workers",   
        "MAXIMIZER_7_8": "workers",
        "MAXIMIZER_ABSOLUTE": "workers",
        "ITEMS_JOINER": "joiners",
        "STORES_JOINER": "joiners",
        "USERS_JOINER": "joiners",
    }
    return mapping.get(nodo, nodo.lower())

def get_lib_dependecies(nodo: str):
    dependecies = []
    main_folder = get_main_folder(nodo)
    dependecies.append(f"./utils:/{main_folder}/utils:ro")
    dependecies.append(f"./middleware:/{main_folder}/middleware:ro")
    return dependecies

def define_rabbitmq(compose: dict):
    compose["services"]["rabbitmq"] = {
        "image": "rabbitmq:3-management",
        "container_name": "rabbitmq",
        "ports": [
            "5672:5672",   # RabbitMQ main port
            "15672:15672"  # RabbitMQ management UI
        ],
        "healthcheck": {
            "test": ["CMD", "rabbitmq-diagnostics", "status"],
            "interval": "30s",
            "timeout": "10s",
            "retries": 5,
        },
        "networks": ["testing_net"]
    }

def define_server(compose: dict):
    compose["services"]["server"] = {
        "container_name": "server",
        "build": {
            "context": ".",             # project root
            "dockerfile": "server/Dockerfile"
        },
        "entrypoint": ["python3", "main.py"],
        "environment": {
            "PYTHONUNBUFFERED": "1",
            "CLI_CLIENTS": "1",
        },
        "volumes": [
            "./server/config.ini:/config.ini:ro",
            dep for dep in get_lib_dependecies("server")
        ],
        "networks": ["testing_net"],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}}
    }

def define_network(compose: dict):
    compose["networks"] = {
        "testing_net": {
            "driver": "bridge"
        }
    }

def get_docker_folder(nodo: str) -> str:
    mapping = {
        "SERVER": "server",
        "FILTER_YEAR": "workers/filter",
        "FILTER_HOUR": "workers/filter",
        "FILTER_AMOUNT": "workers/filter",
        "AGGREGATOR_PRODUCTS": "workers/aggregators",
        "MAXIMIZER_1_3": "workers/maximizer",
        "MAXIMIZER_4_6": "workers/maximizer",   
        "MAXIMIZER_7_8": "workers/maximizer",
        "MAXIMIZER_ABSOLUTE": "workers/maximizer",
        "ITEMS_JOINER": "workers/joiners",
        "STORES_JOINER": "workers/joiners",
        "USERS_JOINER": "workers/joiners",
    }
    return mapping.get(nodo, nodo.lower())

def generate_compose(meta: dict, nodes: dict):
    compose = {
        "name": meta.get("compose_name", "tp-distribuidos-grupo13"),
        "services": {}
    }
    define_rabbitmq(compose)
    define_server(compose)
    for nodo, cantidad in nodes.items():
        if cantidad == 0:
            continue
        for i in range(1, cantidad + 1):
            service_name = f"{nodo.lower()}_{i}"
            compose["services"][service_name] = {
                "build": {
                    "context": ".",             # project root
                    "dockerfile": f"workers/{nodo.lower()}/Dockerfile"
                },
                "entrypoint": ["python3", "main.py"],
                "container_name": service_name,
                "environment": {
                    "NODO": nodo,
                    "INSTANCE_ID": str(i)
                }
            }
    define_network(compose)
    return compose


def main():
    meta, nodes = read_config("compose-config.ini")
    compose = generate_compose(meta, nodes)

    output_file = meta.get("output_file", "docker-compose.yaml")

    with open(output_file, "w") as f:
        yaml.dump(compose, f, sort_keys=False)

    print(f"Archivo '{output_file}' generado correctamente.")


if __name__ == "__main__":
    main()
