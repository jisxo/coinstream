from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        # interpret as unix milliseconds when large enough
        ts = float(value)
        if ts > 10_000_000_000:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).astimezone(timezone.utc)
    raise ValueError(f"Unsupported datetime value: {value!r}")


@dataclass(frozen=True)
class DebeziumSource:
    db: Optional[str]
    schema: Optional[str]
    table: Optional[str]
    tx_id: Optional[int]
    lsn: Optional[int]
    ts_ms: Optional[int]

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "DebeziumSource":
        return cls(
            db=payload.get("db"),
            schema=payload.get("schema"),
            table=payload.get("table"),
            tx_id=payload.get("txId"),
            lsn=payload.get("lsn"),
            ts_ms=payload.get("ts_ms"),
        )

    @property
    def source_table(self) -> str:
        schema = self.schema or "public"
        table = self.table or "unknown"
        return f"{schema}.{table}"


@dataclass(frozen=True)
class DebeziumEnvelope:
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    source: DebeziumSource
    op: str
    ts_ms: Optional[int]

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "DebeziumEnvelope":
        return cls(
            before=payload.get("before"),
            after=payload.get("after"),
            source=DebeziumSource.from_dict(payload.get("source", {})),
            op=str(payload.get("op", "")),
            ts_ms=payload.get("ts_ms"),
        )


@dataclass(frozen=True)
class CdcRawEvent:
    event_id: str
    source_table: str
    op_type: str
    source_pk: str
    source_updated_at: datetime
    source_tx_id: Optional[int]
    source_offset: int
    kafka_topic: str
    kafka_partition: int
    dedup_key: str
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    is_duplicate: int = 0


@dataclass(frozen=True)
class OrderState:
    order_id: int
    user_id: int
    status: str
    amount: float
    source_updated_at: datetime
    is_deleted: int
    source_offset: int

