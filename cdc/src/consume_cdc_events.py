from __future__ import annotations

import json
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from cdc.src.dedup import DedupTracker, build_dedup_key, build_event_id
    from cdc.src.models import CdcRawEvent, DebeziumEnvelope, parse_datetime
except ImportError:  # pragma: no cover
    from dedup import DedupTracker, build_dedup_key, build_event_id
    from models import CdcRawEvent, DebeziumEnvelope, parse_datetime


class GracefulExit:
    def __init__(self) -> None:
        self.stop = False

    def handler(self, *_args: Any) -> None:
        self.stop = True


def _json_loads(raw: Optional[bytes]) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {}


def _json_dumps(payload: Optional[Dict[str, Any]]) -> str:
    if payload is None:
        return "{}"
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _resolve_source_pk(envelope: DebeziumEnvelope, key_obj: Dict[str, Any]) -> str:
    for candidate in (envelope.after, envelope.before, key_obj):
        if isinstance(candidate, dict) and "order_id" in candidate:
            return str(candidate["order_id"])
    return "unknown"


def _resolve_source_updated_at(envelope: DebeziumEnvelope) -> datetime:
    for candidate in (envelope.after, envelope.before):
        if isinstance(candidate, dict) and candidate.get("updated_at"):
            return parse_datetime(candidate["updated_at"])
    ts_ms = envelope.ts_ms or envelope.source.ts_ms or int(time.time() * 1000)
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


def debezium_msg_to_raw_event(msg: Any, dedup: DedupTracker) -> Optional[CdcRawEvent]:
    key_obj = _json_loads(msg.key())
    payload = _json_loads(msg.value())
    if not payload:
        return None

    envelope = DebeziumEnvelope.from_dict(payload)
    source_table = envelope.source.source_table
    source_pk = _resolve_source_pk(envelope, key_obj)
    source_updated_at = _resolve_source_updated_at(envelope)
    op_type = envelope.op or "u"
    source_offset = int(msg.offset())

    event_id = build_event_id(
        source_table=source_table,
        source_pk=source_pk,
        op_type=op_type,
        source_updated_at=source_updated_at.isoformat(),
        source_offset=source_offset,
        source_tx_id=envelope.source.tx_id,
        source_lsn=envelope.source.lsn,
        payload_event_id=payload.get("event_id"),
    )
    dedup_key = build_dedup_key(
        event_id=event_id,
        source_table=source_table,
        source_pk=source_pk,
        op_type=op_type,
        source_updated_at=source_updated_at.isoformat(),
        source_offset=source_offset,
    )
    is_duplicate = 1 if dedup.is_duplicate(dedup_key) else 0

    return CdcRawEvent(
        event_id=event_id,
        source_table=source_table,
        op_type=op_type,
        source_pk=source_pk,
        source_updated_at=source_updated_at,
        source_tx_id=envelope.source.tx_id,
        source_offset=source_offset,
        kafka_topic=msg.topic(),
        kafka_partition=int(msg.partition()),
        dedup_key=dedup_key,
        before=envelope.before,
        after=envelope.after,
        is_duplicate=is_duplicate,
    )


def _build_clickhouse_client() -> Any:
    import clickhouse_connect  # local import for optional dependency

    return clickhouse_connect.get_client(
        host=os.getenv("CDC_CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CDC_CLICKHOUSE_PORT", "8124")),
        username=os.getenv("CDC_CLICKHOUSE_USER", "default"),
        password=os.getenv("CDC_CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CDC_CLICKHOUSE_DB", "coinstream_cdc"),
    )


def _build_consumer() -> Any:
    from confluent_kafka import Consumer  # local import for optional dependency

    return Consumer(
        {
            "bootstrap.servers": os.getenv("CDC_KAFKA_BROKERS", "localhost:9093"),
            "group.id": os.getenv("CDC_CONSUMER_GROUP", "cdc_raw_consumer"),
            "auto.offset.reset": os.getenv("CDC_AUTO_OFFSET_RESET", "earliest"),
            "enable.auto.commit": False,
        }
    )


def insert_raw_events(ch: Any, rows: List[CdcRawEvent]) -> None:
    if not rows:
        return

    columns = [
        "event_id",
        "source_table",
        "op_type",
        "source_pk",
        "source_updated_at",
        "source_tx_id",
        "source_offset",
        "kafka_topic",
        "kafka_partition",
        "dedup_key",
        "before_json",
        "after_json",
        "is_duplicate",
        "ingested_at",
    ]
    now_utc = datetime.now(tz=timezone.utc)
    values = []
    for row in rows:
        values.append(
            [
                row.event_id,
                row.source_table,
                row.op_type,
                row.source_pk,
                row.source_updated_at,
                row.source_tx_id,
                row.source_offset,
                row.kafka_topic,
                row.kafka_partition,
                row.dedup_key,
                _json_dumps(row.before),
                _json_dumps(row.after),
                row.is_duplicate,
                now_utc,
            ]
        )
    ch.insert("cdc_raw_events", values, column_names=columns)


def upsert_offsets(
    ch: Any,
    *,
    consumer_name: str,
    offset_rows: List[Tuple[str, int, int]],
) -> None:
    if not offset_rows:
        return

    latest: Dict[Tuple[str, int], int] = {}
    for topic, partition, offset in offset_rows:
        key = (topic, partition)
        latest[key] = max(offset, latest.get(key, -1))

    now_utc = datetime.now(tz=timezone.utc)
    values = [
        [consumer_name, topic, partition, offset, now_utc]
        for (topic, partition), offset in latest.items()
    ]
    ch.insert(
        "cdc_consumer_offsets",
        values,
        column_names=["consumer_name", "topic", "partition", "offset", "updated_at"],
    )


def consume_loop() -> None:
    topic = os.getenv("CDC_TOPIC", "cdc_coinstream.public.orders")
    consumer_name = os.getenv("CDC_CONSUMER_GROUP", "cdc_raw_consumer")
    batch_size = int(os.getenv("CDC_CONSUMER_BATCH_SIZE", "200"))

    ch = _build_clickhouse_client()
    consumer = _build_consumer()
    consumer.subscribe([topic])

    dedup = DedupTracker(ttl_seconds=int(os.getenv("CDC_DEDUP_TTL_SECONDS", "900")))
    raw_rows: List[CdcRawEvent] = []
    offsets: List[Tuple[str, int, int]] = []
    exit_ctl = GracefulExit()
    signal.signal(signal.SIGINT, exit_ctl.handler)
    signal.signal(signal.SIGTERM, exit_ctl.handler)

    while not exit_ctl.stop:
        msg = consumer.poll(1.0)
        if msg is None:
            if raw_rows:
                insert_raw_events(ch, raw_rows)
                upsert_offsets(ch, consumer_name=consumer_name, offset_rows=offsets)
                consumer.commit(asynchronous=False)
                raw_rows.clear()
                offsets.clear()
            continue
        if msg.error():
            continue

        event = debezium_msg_to_raw_event(msg, dedup)
        if event is None:
            continue

        raw_rows.append(event)
        offsets.append((msg.topic(), int(msg.partition()), int(msg.offset())))

        if len(raw_rows) >= batch_size:
            insert_raw_events(ch, raw_rows)
            upsert_offsets(ch, consumer_name=consumer_name, offset_rows=offsets)
            consumer.commit(asynchronous=False)
            raw_rows.clear()
            offsets.clear()

    if raw_rows:
        insert_raw_events(ch, raw_rows)
        upsert_offsets(ch, consumer_name=consumer_name, offset_rows=offsets)
        consumer.commit(asynchronous=False)

    consumer.close()


if __name__ == "__main__":
    consume_loop()
