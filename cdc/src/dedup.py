from __future__ import annotations

import hashlib
import time
from typing import Any, Dict, Optional


def _hash_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def build_fallback_event_id(
    *,
    source_table: str,
    source_pk: str,
    op_type: str,
    source_updated_at: str,
    source_offset: int,
) -> str:
    return _hash_text(
        f"{source_table}|{source_pk}|{op_type}|{source_updated_at}|{source_offset}"
    )


def build_event_id(
    *,
    source_table: str,
    source_pk: str,
    op_type: str,
    source_updated_at: str,
    source_offset: int,
    source_tx_id: Optional[int],
    source_lsn: Optional[int],
    payload_event_id: Optional[str] = None,
) -> str:
    # Rule 1: explicit event_id from payload/metadata wins.
    if payload_event_id:
        return payload_event_id

    # Rule 2: Debezium transactional identity.
    if source_tx_id is not None and source_lsn is not None:
        return f"{source_table}:{source_tx_id}:{source_lsn}:{op_type}:{source_pk}"

    # Rule 3: fallback deterministic hash.
    return build_fallback_event_id(
        source_table=source_table,
        source_pk=source_pk,
        op_type=op_type,
        source_updated_at=source_updated_at,
        source_offset=source_offset,
    )


def build_dedup_key(
    *,
    event_id: str,
    source_table: str,
    source_pk: str,
    op_type: str,
    source_updated_at: str,
    source_offset: int,
) -> str:
    if event_id:
        return event_id
    return _hash_text(
        f"{source_table}|{source_pk}|{op_type}|{source_updated_at}|{source_offset}"
    )


class DedupTracker:
    def __init__(self, ttl_seconds: int = 600) -> None:
        self.ttl_seconds = ttl_seconds
        self._seen_at: Dict[str, float] = {}

    def is_duplicate(self, key: str, now_ts: Optional[float] = None) -> bool:
        now = now_ts if now_ts is not None else time.time()
        self._evict(now)
        if key in self._seen_at:
            return True
        self._seen_at[key] = now
        return False

    def _evict(self, now_ts: float) -> None:
        cutoff = now_ts - self.ttl_seconds
        expired = [k for k, ts in self._seen_at.items() if ts < cutoff]
        for key in expired:
            self._seen_at.pop(key, None)

