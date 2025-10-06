#!/usr/bin/env python3
import yaml
import os


from yaml.representer import SafeRepresenter

class FlowList(list):
    pass

def flow_list_representer(dumper, data):
    return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=True)


def read_config(path: str):
    meta = {
        "compose_name": "default_compose",
        "output_file": "docker-compose.yaml",
        "data_path": "./.data",
        "logging_level": "INFO",
    }
    nodes = {}

    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or ":" not in line:
                continue

            key, value = line.split(":", 1)
            key, value = key.strip(), value.strip()

            if key.lower() in ("compose_name", "output_file", "data_path", "logging_level"):
                meta[key.lower()] = value
            else:
                nodes[key] = int(value)
                
    yaml.add_representer(FlowList, flow_list_representer)
    
    return meta, nodes

def define_rabbitmq(compose: dict):
    compose["services"]["rabbitmq"] = {
        "image": "rabbitmq:3-management",
        "container_name": "rabbitmq",
        "hostname": "rabbitmq",
        "ports": [
            "5672:5672",   # RabbitMQ main port
            "15672:15672"  # RabbitMQ management UI
        ],
        "healthcheck": {
            "test": FlowList(["CMD", "rabbitmqctl", "status"]),
            "interval": "5s",
            "timeout": "5s",
            "retries": 10,
        },
        "networks": ["testing_net"]
    }

def define_server(compose: dict, client_amount: int):
    compose["services"]["server"] = {
        "container_name": "server",
        "build": {
            "context": ".",             # project root
            "dockerfile": "server/Dockerfile"
        },
        "entrypoint": FlowList(["python3", "main.py"]),
        "environment": [
            "PYTHONUNBUFFERED=1",
            f"CLI_CLIENTS={client_amount}",
        ],
        "volumes": [
            "./server/config.ini:/config.ini:ro",
            "./utils:/server/utils:ro",
            "./middleware:/server/middleware:ro",
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

def get_filter_config_path(nodo: str):
    if nodo not in ("FILTER_YEAR", "FILTER_HOUR", "FILTER_AMOUNT"):
        raise ValueError(f"Tipo de filtro inválido: {nodo}")
    n = nodo.split("_")[1]
    return f"/workers/filter/config/config_{n.lower()}.ini"

def is_filter(nodo: str):
    return nodo.startswith("FILTER_")

def define_filter(meta: dict, compose: dict, nodo: str, index: int):
    service_name = f"{nodo.lower()}_id_{index}_service"
    config_path = get_filter_config_path(nodo)
    compose["services"][service_name] = {
        "build": {
            "context": ".",             # project root
            "dockerfile": f"workers/filter/Dockerfile"
        },
        "entrypoint": FlowList(["python3", "main.py", "--filter", f"{nodo.split("_")[1].lower()}"]),
        "container_name": service_name,
        "environment": [
            "PYTHONUNBUFFERED=1",
            f"LOGGING_LEVEL={meta['logging_level']}",
        ],
        "volumes": [
            f".{config_path}:{config_path}:ro",
            "./utils:/workers/utils:ro",
            "./middleware:/workers/middleware:ro",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
            "rabbitmq": {"condition": "service_healthy"},
        }
    }

def is_aggregator(nodo: str):
    return nodo.startswith("AGGREGATOR_")

def define_aggregator(meta: dict, compose: dict, nodo: str, index: int):
    service_name = f"{nodo.lower()}_id_{index}_service"
    compose["services"][service_name] = {
        "build": {
            "context": ".",             # project root
            "dockerfile": f"workers/aggregators/Dockerfile"
        },
        "entrypoint": FlowList(["python3", "main.py"]),
        "container_name": service_name,
        "environment": [
            "PYTHONUNBUFFERED=1",
            f"LOGGING_LEVEL={meta['logging_level']}",
            f"AGGREGATOR_TYPE={nodo.split('_')[1].upper()}",
        ],
        "volumes": [
            "./utils:/workers/utils:ro",
            "./middleware:/workers/middleware:ro",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
            "rabbitmq": {"condition": "service_healthy"},
        }
    }

def get_maximizer_range(nodo: str):
    # MAXIMIZER_MAX_ABSOLUTE -> 0
    # MAXIMIZER_MAX_1_3 -> 1
    # MAXIMIZER_MAX_4_6 -> 2
    # MAXIMIZER_MAX_7_8 -> 3
    r = nodo.split("_")[2]
    if r == "ABSOLUTE":
        return 0
    elif r == "1":
        return 1
    elif r == "4":
        return 2
    elif r == "7":
        return 3

def is_maximizer(nodo: str):
    return nodo.startswith("MAXIMIZER_")

def define_maximizer(meta: dict, compose: dict, nodo: str, index: int):
    service_name = f"{nodo.lower()}_id_{index}_service"
    compose["services"][service_name] = {
        "build": {
            "context": ".",             # project root
            "dockerfile": f"workers/maximizers/Dockerfile"
        },
        "entrypoint": FlowList(["python3", "main.py"]),
        "container_name": service_name,
        "environment": [
            "PYTHONUNBUFFERED=1",
            f"LOGGING_LEVEL={meta['logging_level']}",
            f"MAXIMIZER_TYPE={nodo.split('_')[1].upper()}",
            f"MAXIMIZER_RANGE={get_maximizer_range(nodo)}",
        ],
        "volumes": [
            "./utils:/workers/utils:ro",
            "./middleware:/workers/middleware:ro",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
            "rabbitmq": {"condition": "service_healthy"},
        }
    }

def is_joiner(nodo: str):
    return nodo.startswith("JOINER_")

def define_joiner(meta: dict, compose: dict, nodo: str, index: int):
    service_name = f"{nodo.lower()}_id_{index}_service"
    compose["services"][service_name] = {
        "build": {
            "context": ".",             # project root
            "dockerfile": f"workers/joiners/Dockerfile"
        },
        "entrypoint": FlowList(["python3", "main.py"]),
        "container_name": service_name,
        "environment": [
            "PYTHONUNBUFFERED=1",
            f"LOGGING_LEVEL={meta['logging_level']}",
            f"JOINER_TYPE={nodo.split('_')[1].upper()}",
        ],
        "volumes": [
            "./utils:/workers/utils:ro",
            "./middleware:/workers/middleware:ro",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
            "rabbitmq": {"condition": "service_healthy"},
        }
    }

def is_client(nodo: str):
    return nodo == "CLIENT"

def define_client(meta: dict, compose: dict, nodo: str, index: int):
    service_name = f"{nodo.lower()}_id_{index}"
    compose["services"][service_name] = {
        "build": {
            "context": ".",             # project root
            "dockerfile": f"client/Dockerfile"
        },
        "entrypoint": FlowList(["python3", "main.py"]),
        "container_name": service_name,
        "environment": [
            f"CLI_ID={index}",
            "CLI_DATA_DIR=/data",
            "CLI_OUTPUT_DIR=/data/output",
            "DATA_MODE=tree",
            "SERVER_ADDRESS=server:12345",
        ],
        "volumes": [
            f".{meta['data_path']}:/data:ro",
            f"./.results/client_{index}:/data/output:rw",
            "./client/config.ini:/config.ini:ro",
            "./utils:/client/utils:ro",
            "./middleware:/client/middleware:ro",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
        }
    }
    return index

def generate_compose(meta: dict, nodes: dict):
    compose = {
        "name": meta.get("compose_name", "tp-distribuidos-grupo13"),
        "services": {}
    }
    define_rabbitmq(compose)
    for nodo, cantidad in nodes.items():
        if cantidad == 0:
            continue
        for i in range(1, cantidad + 1):
            if is_filter(nodo):
                define_filter(meta, compose, nodo, i)
            elif is_aggregator(nodo):
                define_aggregator(meta, compose, nodo, i)
            elif is_maximizer(nodo):
                define_maximizer(meta, compose, nodo, i)
            elif is_joiner(nodo):
                define_joiner(meta, compose, nodo, i)
            elif is_client(nodo):
                client_amount = define_client(meta, compose, nodo, i)
            else:
                raise ValueError(f"Tipo de nodo inválido: {nodo}")
    if client_amount == 0:
        raise ValueError("Debe haber al menos un cliente.")
    define_server(compose, client_amount) 
    define_network(compose)
    return compose


def main():

    parser = argparse.ArgumentParser(description="Ejemplo de lectura de archivo de configuración")
    
    # Definimos el argumento --config
    parser.add_argument(
        "--config",          # nombre del parámetro
        required=True,       # obligatorio
        help="Archivo de configuración a usar"
    )

    args = parser.parse_args()

    meta, nodes = read_config(args.config)
    compose = generate_compose(meta, nodes)

    output_file = meta.get("output_file", "docker-compose.yaml")

    with open(output_file, "w") as f:
        yaml.dump(compose, f, sort_keys=False)

    print(f"Archivo '{output_file}' generado correctamente.")


if __name__ == "__main__":
    main()
