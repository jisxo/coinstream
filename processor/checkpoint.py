import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import orjson
import redis as redis_lib
from cachetools import TTLCache

from processor.config import ProcessorConfig
from processor.sink import build_clickhouse_client
from processor.window import OhlcAgg


def save_checkpoint(state: Dict[Tuple[str, int], Dict[int, OhlcAgg]], dedup: TTLCache, offsets: List[Dict[str, Any]], config: ProcessorConfig) -> None:
    payload = {
        "state": _serialize_state(state),
        "dedup": _serialize_dedup(dedup),
        "offsets": offsets,
        "timestamp": int(time.time() * 1000),
    }
    store = config.checkpoint_store.lower()
    if store == "redis":
        _save_to_redis(payload, config)
    elif store == "clickhouse":
        _save_to_clickhouse(payload, config)
    else:
        _save_to_file(payload, config.checkpoint_file_path)


def load_checkpoint(config: ProcessorConfig) -> Optional[Tuple[Dict[str, Dict[int, OhlcAgg]], TTLCache, List[Dict[str, Any]]]]:
    store = config.checkpoint_store.lower()
    payload: Optional[Dict[str, Any]] = None
    if store == "redis":
        payload = _load_from_redis(config)
    elif store == "clickhouse":
        payload = _load_from_clickhouse(config)
    else:
        payload = _load_from_file(config.checkpoint_file_path)
    if not payload:
        return None
    state = _deserialize_state(payload["state"])
    dedup = _deserialize_dedup(payload.get("dedup", []), config)
    offsets = payload.get("offsets", [])
    return state, dedup, offsets


def _serialize_state(state: Dict[Tuple[str, int], Dict[int, OhlcAgg]]) -> List[Dict[str, Any]]:
    entries = []
    for (symbol, window_ms), windows in state.items():
        for agg in windows.values():
            entries.append(agg.to_snapshot(window_ms))
    return entries


def _deserialize_state(entries: List[Dict[str, Any]]) -> Dict[Tuple[str, int], Dict[int, OhlcAgg]]:
    reconstructed: Dict[Tuple[str, int], Dict[int, OhlcAgg]] = {}
    for entry in entries:
        window_ms = entry.get("window_ms", 60000)
        agg = OhlcAgg.from_snapshot(entry)
        symbol_windows = reconstructed.setdefault((agg.symbol, window_ms), {})
        symbol_windows[agg.window_start_ms] = agg
    return reconstructed


def _serialize_dedup(dedup: TTLCache) -> List[List[Any]]:
    return [[key[0], key[1]] for key in dedup.keys()]


def _deserialize_dedup(entries: List[List[Any]], config: ProcessorConfig) -> TTLCache:
    cache = TTLCache(maxsize=config.dedup_maxsize, ttl=config.dedup_ttl_seconds)
    for symbol, agg_trade_id in entries:
        cache[(symbol, agg_trade_id)] = 1
    return cache


def _save_to_redis(payload: Dict[str, Any], config: ProcessorConfig) -> None:
    client = redis_lib.from_url(config.checkpoint_redis_url)
    client.set(config.checkpoint_redis_key, orjson.dumps(payload))


def _load_from_redis(config: ProcessorConfig) -> Optional[Dict[str, Any]]:
    client = redis_lib.from_url(config.checkpoint_redis_url)
    raw = client.get(config.checkpoint_redis_key)
    if raw is None:
        return None
    return orjson.loads(raw)


def _save_to_file(payload: Dict[str, Any], path: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    tmp.write_bytes(orjson.dumps(payload))
    tmp.replace(target)


def _load_from_file(path: str) -> Optional[Dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return None
    return orjson.loads(file_path.read_bytes())


def _save_to_clickhouse(payload: Dict[str, Any], config: ProcessorConfig) -> None:
    client = build_clickhouse_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_database,
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS processor_checkpoints (
            id UUID DEFAULT generateUUIDv4(),
            state String,
            dedup String,
            offsets String,
            created_at DateTime64(3, 'UTC')
        ) ENGINE = MergeTree()
        ORDER BY created_at
        """
    )
    state_blob = orjson.dumps(payload["state"]).decode()
    dedup_blob = orjson.dumps(payload["dedup"]).decode()
    offsets_blob = orjson.dumps(payload["offsets"]).decode()
    client.insert(
        "processor_checkpoints",
        [
            {
                "state": state_blob,
                "dedup": dedup_blob,
                "offsets": offsets_blob,
                "created_at": datetime.utcnow(),
            }
        ],
    )


def _load_from_clickhouse(config: ProcessorConfig) -> Optional[Dict[str, Any]]:
    client = build_clickhouse_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_database,
    )
    rows = client.query(
        "SELECT state, dedup, offsets FROM processor_checkpoints ORDER BY created_at DESC LIMIT 1"
    ).result
    if not rows:
        return None
    state_blob, dedup_blob, offsets_blob = rows[0]
    return {
        "state": orjson.loads(state_blob),
        "dedup": orjson.loads(dedup_blob),
        "offsets": orjson.loads(offsets_blob),
    }
