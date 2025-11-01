import os
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple


class ShardingConfigError(ValueError):
    """Raised when shard configuration is invalid or missing."""


@dataclass(frozen=True)
class ShardConfig:
    shard_id: str
    ids: Tuple[int, ...]
    queue_name: str

    def __post_init__(self):
        if not self.shard_id:
            raise ShardingConfigError("Shard id cannot be empty.")
        if not self.ids:
            raise ShardingConfigError(f"Shard '{self.shard_id}' must declare at least one id.")


_QUEUE_PREFIXES = {
    "MAX": "max",
    "TOP3": "top3",
}


def slugify_shard_id(shard_id: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", shard_id.lower()).strip("_")
    return slug or "shard"


def queue_name_for(worker_kind: str, shard_id: str) -> str:
    try:
        prefix = _QUEUE_PREFIXES[worker_kind.upper()]
    except KeyError as exc:
        raise ShardingConfigError(f"Unsupported worker kind '{worker_kind}'.") from exc
    return f"to_{prefix}_{slugify_shard_id(shard_id)}"


def parse_id_token(token: str) -> List[int]:
    token = token.strip()
    if not token:
        raise ShardingConfigError("Empty id token detected.")

    if "-" in token:
        parts = token.split("-", 1)
        if len(parts) != 2:
            raise ShardingConfigError(f"Invalid range token '{token}'.")
        start, end = parts
        try:
            start_id = int(start)
            end_id = int(end)
        except ValueError as exc:
            raise ShardingConfigError(f"Range bounds must be integers: '{token}'.") from exc
        if start_id > end_id:
            raise ShardingConfigError(f"Range start must be <= end: '{token}'.")
        return list(range(start_id, end_id + 1))

    try:
        return [int(token)]
    except ValueError as exc:
        raise ShardingConfigError(f"Id token is not an integer: '{token}'.") from exc


def parse_ids_spec(ids_spec: str) -> Tuple[int, ...]:
    ids: List[int] = []
    for raw_token in ids_spec.split(","):
        raw_token = raw_token.strip()
        if not raw_token:
            continue
        ids.extend(parse_id_token(raw_token))
    if not ids:
        raise ShardingConfigError("Shard must specify at least one id.")
    return tuple(sorted(set(ids)))


def parse_shards_spec(spec: str, *, worker_kind: str) -> List[ShardConfig]:
    entries = [entry.strip() for entry in spec.split(";") if entry.strip()]
    if not entries:
        raise ShardingConfigError("Shard specification is empty.")

    shards: List[ShardConfig] = []
    seen_ids: Dict[int, str] = {}

    for entry in entries:
        if ":" not in entry:
            raise ShardingConfigError(
                f"Shard entry '{entry}' must follow the format '<shard_id>:<id-list>'."
            )
        shard_id_raw, ids_spec = entry.split(":", 1)
        shard_id = shard_id_raw.strip()
        ids = parse_ids_spec(ids_spec)
        queue_name = queue_name_for(worker_kind, shard_id)

        for shard_item in ids:
            if shard_item in seen_ids:
                raise ShardingConfigError(
                    f"Id '{shard_item}' appears in shards '{seen_ids[shard_item]}' and '{shard_id}'."
                )
            seen_ids[shard_item] = shard_id

        shards.append(ShardConfig(shard_id=shard_id, ids=ids, queue_name=queue_name))

    return shards


def load_shards_from_env(env_var: str, *, worker_kind: str) -> List[ShardConfig]:
    spec = os.getenv(env_var)
    if not spec:
        raise ShardingConfigError(
            f"Environment variable '{env_var}' is required to configure sharding for {worker_kind}."
        )
    return parse_shards_spec(spec, worker_kind=worker_kind)


def build_id_lookup(shards: Sequence[ShardConfig]) -> Dict[int, ShardConfig]:
    lookup: Dict[int, ShardConfig] = {}
    for shard in shards:
        for shard_id in shard.ids:
            lookup[shard_id] = shard
    return lookup


def shard_by_id(shards: Sequence[ShardConfig], shard_id: str) -> ShardConfig:
    for shard in shards:
        if shard.shard_id == shard_id:
            return shard
    raise ShardingConfigError(f"Shard '{shard_id}' not found in configuration.")

