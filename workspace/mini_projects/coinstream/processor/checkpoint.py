import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import redis as redis_lib
from cachetools import TTLCache

from processor.config import ProcessorConfig
from processor.sink import build_clickhouse_client
from processor.window import OhlcAgg


def save_checkpoint(state: Dict[str, Dict[int, OhlcAgg]], dedup: TTLCache, offsets: List[Dict[str, Any]], config: ProcessorConfig) -> None:
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
    payload = None
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


def _serialize_state(state: Dict[str, Dict[int, OhlcAgg]]) -> List[Dict[str, Any]]:
    entries = []
    for symbol, windows in state.items():
        for agg in windows.values():
            entries.append(agg.to_snapshot())
    return entries


def _deserialize_state(entries: List[Dict[str, Any]]) -> Dict[str, Dict[int, OhlcAgg]]:
    reconstructed: Dict[str, Dict[int, OhlcAgg]] = {}
    for entry in entries:
        agg = OhlcAgg.from_snapshot(entry)
        symbol_windows = reconstructed.setdefault(agg.symbol, {})
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
    client.set(config.checkpoint_redis_key, json.dumps(payload))


def _load_from_redis(config: ProcessorConfig) -> Optional[Dict[str, Any]]:
    client = redis_lib.from_url(config.checkpoint_redis_url)
    raw = client.get(config.checkpoint_redis_key)
    if raw is None:
        return None
    return json.loads(raw)


def _save_to_file(payload: Dict[str, Any], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(payload))


def _load_from_file(path: str) -> Optional[Dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return None
    return json.loads(file_path.read_text())


def _save_to_clickhouse(payload: Dict[str, Any], config: ProcessorConfig) -> None:
    client = build_clickhouse_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_database,
    )
    # Placeholder: store JSON payload in a lightweight table
    client.command(
        "CREATE TABLE IF NOT EXISTS checkpoints (payload String, created_at DateTime64(3, 'UTC')) ENGINE = Log"
    )
    client.insert("checkpoints", [{"payload": json.dumps(payload), "created_at": datetime.utcnow()}])


def _load_from_clickhouse(config: ProcessorConfig) -> Optional[Dict[str, Any]]:
    client = build_clickhouse_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_database,
    )
    rows = client.query("SELECT payload FROM checkpoints ORDER BY created_at DESC LIMIT 1").result
    if not rows:
        return None
    return json.loads(rows[0][0])
