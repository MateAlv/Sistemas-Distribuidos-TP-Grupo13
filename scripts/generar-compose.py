#!/usr/bin/env python3
import yaml
import os
import argparse

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
        "output_path": "./.results",
    }
    nodes = {}

    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or ":" not in line:
                continue

            key, value = line.split(":", 1)
            key, value = key.strip(), value.strip()
            if key.startswith("#"):
                continue
            if key.lower() in ("compose_name", "output_file", "data_path", "logging_level", "output_path"):
                meta[key.lower()] = value
            else:
                nodes[key.upper()] = int(value)
                
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

def define_filter(meta: dict, compose: dict, nodo: str, worker_id: int):
    base_service_name = f"{nodo.lower()}_service"
    service_name = f"{base_service_name}-{worker_id}" if worker_id > 1 else base_service_name
    config_path = get_filter_config_path(nodo)
    filter_type = nodo.split("_")[1].lower()
    
    compose["services"][service_name] = {
        "build": {
            "context": ".",             # project root
            "dockerfile": f"workers/filter/Dockerfile"
        },
        "entrypoint": FlowList(["python3", "main.py", "--filter", filter_type]),
        "container_name": service_name,
        "environment": [
            "PYTHONUNBUFFERED=1",
            f"LOGGING_LEVEL={meta['logging_level']}",
            f"WORKER_ID={worker_id}",
        ],
        "volumes": [
            f".{config_path}:{config_path}:ro",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
            "rabbitmq": {"condition": "service_healthy"},
        }
    }
    return service_name

def is_aggregator(nodo: str):
    return nodo.startswith("AGGREGATOR_")

def define_aggregator(meta: dict, compose: dict, nodo: str, worker_id: int):
    base_service_name = f"{nodo.lower()}_service"
    service_name = f"{base_service_name}-{worker_id}" if worker_id > 1 else base_service_name
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
            f"WORKER_ID={worker_id}",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
            "rabbitmq": {"condition": "service_healthy"},
        }
    }
    return service_name
    
def get_maximizer_range(nodo: str):
    # MAXIMIZER_MAX_ABSOLUTE -> 0
    # MAXIMIZER_MAX_1_3 -> 1
    # MAXIMIZER_MAX_4_6 -> 4
    # MAXIMIZER_MAX_7_8 -> 7
    # MAXIMIZER_TOP3_ABSOLUTE -> 0
    # MAXIMIZER_TOP3_1 -> 1
    # MAXIMIZER_TOP3_4 -> 4
    # MAXIMIZER_TOP3_7 -> 7
    r = nodo.split("_")[2]
    if r == "ABSOLUTE":
        return 0
    elif r == "1" or r == "1_3":
        return 1
    elif r == "2":
        return 2
    elif r == "3":
        return 3
    elif r == "4" or r == "4_6":
        return 4
    elif r == "7" or r == "7_8":
        return 7
    else:
        # Try to extract just the first number for other patterns
        import re
        match = re.match(r'(\d+)', r)
        if match:
            return int(match.group(1))
        return None

def is_maximizer(nodo: str):
    return nodo.startswith("MAXIMIZER_")

def define_maximizer(meta: dict, compose: dict, nodo: str, worker_id: int):
    base_service_name = f"{nodo.lower()}_service"
    service_name = f"{base_service_name}-{worker_id}" if worker_id > 1 else base_service_name
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
            f"WORKER_ID={worker_id}",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
            "rabbitmq": {"condition": "service_healthy"},
        }
    }
    return service_name

def is_joiner(nodo: str):
    # JOINER_ITEMS
    # JOINER_STORES_TPV
    # JOINER_STORES_TOP3
    # JOINER_USERS
    return nodo.startswith("JOINER_")

def get_joiner_type(nodo: str):
    # JOINER_ITEMS -> ITEMS
    # JOINER_STORES_TPV -> STORES_TPV
    # JOINER_STORES_TOP3 -> STORES_TOP3
    # JOINER_USERS -> USERS
    return nodo.split("_", 1)[1].upper()

def define_joiner(meta: dict, compose: dict, nodo: str, worker_id: int):
    base_service_name = f"{nodo.lower()}_service"
    service_name = f"{base_service_name}-{worker_id}" if worker_id > 1 else base_service_name
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
            f"JOINER_TYPE={get_joiner_type(nodo)}",
            f"WORKER_ID={worker_id}",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
            "rabbitmq": {"condition": "service_healthy"},
        }
    }
    return service_name

def is_client(nodo: str):
    return nodo == "CLIENT"

def define_client(meta: dict, compose: dict, nodo: str, index: int):
    service_name = f"{nodo.lower()}-{index}"
    output_path = meta.get("output_path", "../.results")
    compose["services"][service_name] = {
        "build": {
            "context": ".",             # project root
            "dockerfile": f"client/Dockerfile"
        },
        "entrypoint": FlowList(["python3", "main.py"]),
        "container_name": service_name,
        "environment": [
            f"CLIENT_ID={index}",
            "CLI_DATA_DIR=/data",
            "CLI_OUTPUT_DIR=/output",
            "DATA_MODE=tree",
            "SERVER_ADDRESS=server:12345",
        ],
        "volumes": [
            f".{meta['data_path']}:/data:ro",
            f"{output_path.rstrip('/')}/client-{index}:/output",
            "./client/config.ini:/config.ini:ro",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
        }
    }

def generate_compose(meta: dict, nodes: dict, services: dict = None):
    compose = {
        "name": meta.get("compose_name", "tp-distribuidos-grupo13"),
        "services": {}
    }
    define_rabbitmq(compose)
    client_amount = 0
    
    # Contadores independientes para cada tipo de worker
    worker_counters = {}
    
    for nodo, cantidad in nodes.items():
        if cantidad == 0:
            continue
        
        if is_client(nodo):
            for i in range(1, cantidad + 1):
                define_client(meta, compose, nodo, i)
            client_amount = cantidad
        else: 
            # Inicializar contador para este tipo de worker si no existe
            if nodo not in worker_counters:
                worker_counters[nodo] = 0
            
            for worker_instance in range(cantidad):
                worker_counters[nodo] += 1
                worker_id = worker_counters[nodo]
                
                if is_filter(nodo):
                    service_name = define_filter(meta, compose, nodo, worker_id)
                elif is_aggregator(nodo):
                    service_name = define_aggregator(meta, compose, nodo, worker_id)
                elif is_maximizer(nodo):
                    service_name = define_maximizer(meta, compose, nodo, worker_id)
                elif is_joiner(nodo):
                    service_name = define_joiner(meta, compose, nodo, worker_id)
                else:
                    raise ValueError(f"Tipo de nodo inválido: {nodo}")
                
                # Para scaling, necesitamos el nombre base del servicio
                base_service_name = f"{nodo.lower()}_service"
                if base_service_name not in services:
                    services[base_service_name] = 0
                services[base_service_name] += 1
    if client_amount == 0:
        raise ValueError("Debe haber al menos un cliente.")
    define_server(compose, client_amount) 
    define_network(compose)
    return compose, client_amount


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
    
    output_file = meta.get("output_file", "docker-compose.yaml")

    services = {}

    compose, client_count = generate_compose(meta, nodes, services)


    with open(output_file, "w") as f:
        yaml.dump(compose, f, sort_keys=False)
        
    print(f"Archivo '{output_file}' generado correctamente.")
        
    print(f"    Datos de entrada para los clientes en: {meta.get('data_path', './.data')}")
    
    print(f"    Directorio de salida para los resultados en: {meta.get('output_path', './.results')}")

    print()
    print("Servicios definidos y sus cantidades:")
    print(f"\n - Clientes configurados: {client_count}")

    for service, cantidad in services.items():
        print(f"  - {service}: {cantidad}")


if __name__ == "__main__":
    main()
