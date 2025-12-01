#!/usr/bin/env python3
import yaml
import os
import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from yaml.representer import SafeRepresenter
from workers.common.sharding import parse_shards_spec, ShardingConfigError, slugify_shard_id

PRODUCT_ITEM_IDS = list(range(1, 9))
PURCHASE_STORE_IDS = list(range(1, 11))

class FlowList(list):
    pass

def flow_list_representer(dumper, data):
    return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=True)


def _partition_ids(ids, shard_count):
    if shard_count <= 0:
        return []
    if shard_count > len(ids):
        raise ValueError(
            f"No se pueden crear {shard_count} shards con solo {len(ids)} ids disponibles."
        )

    base = len(ids) // shard_count
    remainder = len(ids) % shard_count

    partitions = []
    index = 0
    for shard_index in range(shard_count):
        size = base + (1 if shard_index < remainder else 0)
        subset = ids[index : index + size]
        index += size
        if not subset:
            raise ValueError(
                f"Shard vacío generado al particionar ids {ids} en {shard_count} shards."
            )
        partitions.append(subset)
    return partitions


def build_shard_spec(ids, shard_count, prefix):
    partitions = _partition_ids(ids, shard_count)
    entries = []
    for subset in partitions:
        start = subset[0]
        end = subset[-1]
        if start == end:
            shard_id = f"{prefix}_{start}"
            ids_spec = f"{start}"
        else:
            shard_id = f"{prefix}_{start}_{end}"
            ids_spec = f"{start}-{end}"
        entries.append(f"{shard_id}:{ids_spec}")
    return ";".join(entries)


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
            if key.lower() in ("compose_name", "output_file", "data_path", "logging_level", "output_path", "chaos_enabled", "chaos_interval"):
                meta[key.lower()] = value
            else:
                try:
                    nodes[key.upper()] = int(value)
                except ValueError:
                    meta[key.lower()] = value
                
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
        "environment": [
            "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=-rabbit vm_memory_high_watermark.relative 0.6"
        ],
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
            "CONTAINER_NAME=server",
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
            f"CONTAINER_NAME={service_name}",
        ],
        "volumes": [
            f".{config_path}:{config_path}:ro",
            "./data/persistence:/data/persistence",
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
    agg_type = nodo.split("_")[1].upper()
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
            f"AGGREGATOR_TYPE={agg_type}",
            f"WORKER_ID={worker_id}",
            f"CONTAINER_NAME={service_name}",
        ],
        "volumes": [
            "./data/persistence:/data/persistence",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
            "rabbitmq": {"condition": "service_healthy"},
        }
    }

    if agg_type == "PRODUCTS":
        shard_spec = meta.get("max_shards")
        if not shard_spec:
            raise ValueError("MAX_SHARDS debe definirse en el archivo de configuración para AGGREGATOR_PRODUCTS.")
        compose["services"][service_name]["environment"].append(f"MAX_SHARDS={shard_spec}")
    elif agg_type == "PURCHASES":
        shard_spec = meta.get("top3_shards")
        if not shard_spec:
            raise ValueError("TOP3_SHARDS debe definirse en el archivo de configuración para AGGREGATOR_PURCHASES.")
        compose["services"][service_name]["environment"].append(f"TOP3_SHARDS={shard_spec}")

    return service_name
    
def is_maximizer(nodo: str):
    return nodo.startswith("MAXIMIZER_")

def define_maximizer(meta: dict, compose: dict, nodo: str, worker_id: int, max_shards, top3_shards, nodes: dict):
    parts = nodo.split("_")
    if len(parts) < 3:
        raise ValueError(f"Nombre de maximizer inválido: {nodo}")

    max_type = parts[1].upper()
    role = parts[2].upper()

    base_service_name = f"{nodo.lower()}_service"
    service_name = f"{base_service_name}-{worker_id}" if worker_id > 1 else base_service_name
    env = [
        "PYTHONUNBUFFERED=1",
        f"LOGGING_LEVEL={meta['logging_level']}",
        f"MAXIMIZER_TYPE={max_type}",
        f"WORKER_ID={worker_id}",
        f"CONTAINER_NAME={service_name}",
    ]

    # Calculate expected inputs
    expected_inputs = 1
    if role == "PARTIAL":
        if max_type == "MAX":
            expected_inputs = nodes.get("AGGREGATOR_PRODUCTS", 1)
        elif max_type == "TOP3":
            expected_inputs = nodes.get("AGGREGATOR_PURCHASES", 1)
    elif role == "ABSOLUTE":
        if max_type == "MAX":
             expected_inputs = nodes.get("MAXIMIZER_MAX_PARTIAL", 1)
        elif max_type == "TOP3":
             expected_inputs = nodes.get("MAXIMIZER_TOP3_PARTIAL", 1)
    
    env.append(f"EXPECTED_INPUTS={expected_inputs}")

    if role == "PARTIAL":
        if max_type == "MAX":
            if not max_shards:
                raise ValueError("MAX_SHARDS debe definirse para crear maximizers parciales de MAX.")
            if worker_id > len(max_shards):
                raise ValueError(
                    f"No hay suficientes shards definidos para {nodo}. worker_id={worker_id}, shards={len(max_shards)}"
                )
            shard = max_shards[worker_id - 1]
            shard_slug = slugify_shard_id(shard.shard_id)
            service_name = f"maximizer_max_{shard_slug}_service"
            env.append(f"MAX_SHARD_ID={shard.shard_id}")
        elif max_type == "TOP3":
            if not top3_shards:
                raise ValueError("TOP3_SHARDS debe definirse para crear maximizers parciales de TOP3.")
            if worker_id > len(top3_shards):
                raise ValueError(
                    f"No hay suficientes shards definidos para {nodo}. worker_id={worker_id}, shards={len(top3_shards)}"
                )
            shard = top3_shards[worker_id - 1]
            shard_slug = slugify_shard_id(shard.shard_id)
            service_name = f"maximizer_top3_{shard_slug}_service"
            env.append(f"TOP3_SHARD_ID={shard.shard_id}")
        else:
            raise ValueError(f"Tipo de maximizer parcial inválido: {max_type}")
    elif role == "ABSOLUTE":
        if max_type == "MAX":
            if not max_shards:
                raise ValueError("MAX_PARTIAL_SHARDS requiere MAX_SHARDS configurado.")
            shard_ids = ",".join(shard.shard_id for shard in max_shards)
            env.append(f"MAX_PARTIAL_SHARDS={shard_ids}")
        elif max_type == "TOP3":
            if not top3_shards:
                raise ValueError("TOP3_PARTIAL_SHARDS requiere TOP3_SHARDS configurado.")
            shard_ids = ",".join(shard.shard_id for shard in top3_shards)
            env.append(f"TOP3_PARTIAL_SHARDS={shard_ids}")
        else:
            raise ValueError(f"Tipo de maximizer absoluto inválido: {max_type}")
    else:
        raise ValueError(f"Rol de maximizer desconocido: {role}")

    compose["services"][service_name] = {
        "build": {
            "context": ".",
            "dockerfile": f"workers/maximizers/Dockerfile"
        },
        "entrypoint": FlowList(["python3", "main.py"]),
        "container_name": service_name,
        "environment": env,
        "volumes": [
            "./data/persistence:/data/persistence",
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

def define_joiner(meta: dict, compose: dict, nodo: str, worker_id: int, nodes: dict):
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
            f"CONTAINER_NAME={service_name}",
        ],
        "volumes": [
            "./data/persistence:/data/persistence",
        ],
        "networks": ["testing_net"],
        "depends_on": {
            "server": {"condition": "service_started"},
            "rabbitmq": {"condition": "service_healthy"},
        }
    }

    # Calculate expected inputs
    expected_inputs = 1
    joiner_type = get_joiner_type(nodo)
    if joiner_type == "STORES_TPV":
        expected_inputs = nodes.get("AGGREGATOR_TPV", 1)
    elif joiner_type == "STORES_TOP3":
        # Receives from Absolute Top3 Maximizer (which is 1)
        expected_inputs = nodes.get("MAXIMIZER_TOP3_ABSOLUTE", 1)
    elif joiner_type == "ITEMS":
        # Receives from Absolute Max Maximizer (which is 1)
        expected_inputs = nodes.get("MAXIMIZER_MAX_ABSOLUTE", 1)
    elif joiner_type == "USERS":
        pass
        
    compose["services"][service_name]["environment"].append(f"EXPECTED_INPUTS={expected_inputs}")

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

def define_monitor(meta: dict, compose: dict, count: int):
    for i in range(1, count + 1):
        service_name = f"monitor_{i}"
        compose["services"][service_name] = {
            "build": {
                "context": ".",
                "dockerfile": "monitor/Dockerfile"
            },
            "container_name": service_name,
            "environment": [
                "PYTHONUNBUFFERED=1",
                f"CONTAINER_NAME={service_name}",
                f"LOGGING_LEVEL={meta['logging_level']}",
                "MONITOR_CONFIG_PATH=/monitor/config/config.ini",
            ],
            "volumes": [
                "/var/run/docker.sock:/var/run/docker.sock",
            ],
            "networks": ["testing_net"],
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            }
        }

def define_chaos_monkey(meta: dict, compose: dict):
    compose["services"]["chaos_monkey"] = {
        "build": {
            "context": ".",
            "dockerfile": "chaos_monkey/Dockerfile"
        },
        "container_name": "chaos_monkey",
        "environment": [
            "PYTHONUNBUFFERED=1",
            f"CHAOS_ENABLED={meta.get('chaos_enabled', 'false')}",
            f"CHAOS_INTERVAL={meta.get('chaos_interval', '30')}",
            "CONTAINER_NAME=chaos_monkey",
        ],
        "volumes": [
            "/var/run/docker.sock:/var/run/docker.sock",
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

    max_partials = nodes.get("MAXIMIZER_MAX_PARTIAL", 0)
    top3_partials = nodes.get("MAXIMIZER_TOP3_PARTIAL", 0)

    agg_products = nodes.get("AGGREGATOR_PRODUCTS", 0)
    agg_purchases = nodes.get("AGGREGATOR_PURCHASES", 0)

    if agg_products > 0 and max_partials == 0:
        raise ValueError("AGGREGATOR_PRODUCTS requiere MAXIMIZER_MAX_PARTIAL > 0.")
    if max_partials > 0 and agg_products == 0:
        raise ValueError("MAXIMIZER_MAX_PARTIAL definido pero AGGREGATOR_PRODUCTS es 0.")

    if agg_purchases > 0 and top3_partials == 0:
        raise ValueError("AGGREGATOR_PURCHASES requiere MAXIMIZER_TOP3_PARTIAL > 0.")
    if top3_partials > 0 and agg_purchases == 0:
        raise ValueError("MAXIMIZER_TOP3_PARTIAL definido pero AGGREGATOR_PURCHASES es 0.")

    max_shards_spec = meta.get("max_shards")
    top3_shards_spec = meta.get("top3_shards")

    if not max_shards_spec and max_partials:
        try:
            max_shards_spec = build_shard_spec(PRODUCT_ITEM_IDS, max_partials, "items")
        except ValueError as exc:
            raise ValueError(f"No se puede generar MAX_SHARDS automáticamente: {exc}") from exc
        meta["max_shards"] = max_shards_spec

    if not top3_shards_spec and top3_partials:
        try:
            top3_shards_spec = build_shard_spec(PURCHASE_STORE_IDS, top3_partials, "stores")
        except ValueError as exc:
            raise ValueError(f"No se puede generar TOP3_SHARDS automáticamente: {exc}") from exc
        meta["top3_shards"] = top3_shards_spec

    try:
        max_shards = parse_shards_spec(max_shards_spec, worker_kind="MAX") if max_shards_spec else []
    except ShardingConfigError as exc:
        raise ValueError(f"MAX_SHARDS inválido: {exc}") from exc

    try:
        top3_shards = parse_shards_spec(top3_shards_spec, worker_kind="TOP3") if top3_shards_spec else []
    except ShardingConfigError as exc:
        raise ValueError(f"TOP3_SHARDS inválido: {exc}") from exc

    if nodes.get("MAXIMIZER_MAX_PARTIAL", 0) not in (0, len(max_shards)):
        raise ValueError(
            f"MAXIMIZER_MAX_PARTIAL debe coincidir con la cantidad de shards definidos ({len(max_shards)})."
        )
    if nodes.get("MAXIMIZER_TOP3_PARTIAL", 0) not in (0, len(top3_shards)):
        raise ValueError(
            f"MAXIMIZER_TOP3_PARTIAL debe coincidir con la cantidad de shards definidos ({len(top3_shards)})."
        )

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
                    service_name = define_maximizer(meta, compose, nodo, worker_id, max_shards, top3_shards, nodes)
                elif is_joiner(nodo):
                    service_name = define_joiner(meta, compose, nodo, worker_id, nodes)
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
    
    monitor_count = nodes.get("MONITORS", 3) # Default to 3 monitors
    define_monitor(meta, compose, monitor_count)
    services["monitor"] = monitor_count

    if meta.get("chaos_enabled", "false").lower() == "true":
        define_chaos_monkey(meta, compose)
        services["chaos_monkey"] = 1
        
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
