from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

try:
    from cdc.src.models import CdcRawEvent, OrderState, parse_datetime
except ImportError:  # pragma: no cover
    from models import CdcRawEvent, OrderState, parse_datetime


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _json_loads(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return None
    return None


def _normalize_status(status: str) -> str:
    return (status or "").strip().lower()


def _is_newer_event(
    current: Optional[OrderState],
    incoming_updated_at: datetime,
    incoming_offset: int,
) -> bool:
    if current is None:
        return True
    if incoming_updated_at > current.source_updated_at:
        return True
    if incoming_updated_at == current.source_updated_at and incoming_offset > current.source_offset:
        return True
    return False


def _extract_order_id(event: CdcRawEvent) -> int:
    for src in (event.after, event.before):
        if isinstance(src, dict) and src.get("order_id") is not None:
            return int(src["order_id"])
    return int(event.source_pk)


def apply_cdc_event(
    current_state: Optional[OrderState],
    event: CdcRawEvent,
    seen_event_ids: Optional[Set[str]] = None,
) -> Tuple[Optional[OrderState], bool, str]:
    if seen_event_ids is not None:
        if event.event_id in seen_event_ids:
            return current_state, False, "duplicate"
        seen_event_ids.add(event.event_id)

    if event.is_duplicate == 1:
        return current_state, False, "duplicate"

    if not _is_newer_event(current_state, event.source_updated_at, event.source_offset):
        return current_state, False, "old_event"

    op = (event.op_type or "").lower()
    order_id = _extract_order_id(event)

    if op == "d":
        base_user_id = current_state.user_id if current_state else 0
        base_status = current_state.status if current_state else "deleted"
        base_amount = current_state.amount if current_state else 0.0
        next_state = OrderState(
            order_id=order_id,
            user_id=base_user_id,
            status=base_status,
            amount=base_amount,
            source_updated_at=event.source_updated_at,
            is_deleted=1,
            source_offset=event.source_offset,
        )
        return next_state, True, "delete"

    payload = event.after or {}
    if not payload:
        return current_state, False, "missing_after"

    next_state = OrderState(
        order_id=order_id,
        user_id=int(payload.get("user_id", current_state.user_id if current_state else 0)),
        status=str(payload.get("status", current_state.status if current_state else "created")),
        amount=float(payload.get("amount", current_state.amount if current_state else 0.0)),
        source_updated_at=parse_datetime(payload.get("updated_at", event.source_updated_at)),
        is_deleted=0,
        source_offset=event.source_offset,
    )
    return next_state, True, "upsert"


def aggregate_hourly_metrics(states: Iterable[OrderState]) -> List[Dict[str, Any]]:
    bucketed: Dict[datetime, Dict[str, Any]] = {}
    for state in states:
        if state.is_deleted == 1:
            continue
        bucket = state.source_updated_at.replace(minute=0, second=0, microsecond=0)
        entry = bucketed.setdefault(
            bucket,
            {
                "bucket_start": bucket,
                "order_count": 0,
                "paid_order_count": 0,
                "canceled_order_count": 0,
                "total_amount": 0.0,
            },
        )
        status = _normalize_status(state.status)
        entry["order_count"] += 1
        entry["paid_order_count"] += 1 if status in {"paid", "completed"} else 0
        entry["canceled_order_count"] += 1 if status in {"canceled", "cancelled"} else 0
        entry["total_amount"] += float(state.amount)

    rows = list(bucketed.values())
    rows.sort(key=lambda x: x["bucket_start"])
    for row in rows:
        row["total_amount"] = round(row["total_amount"], 2)
    return rows


def _build_clickhouse_client() -> Any:
    import clickhouse_connect  # local import for optional dependency

    return clickhouse_connect.get_client(
        host=os.getenv("CDC_CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CDC_CLICKHOUSE_PORT", "8124")),
        username=os.getenv("CDC_CLICKHOUSE_USER", "default"),
        password=os.getenv("CDC_CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CDC_CLICKHOUSE_DB", "coinstream_cdc"),
    )


def _result_to_dicts(result: Any) -> List[Dict[str, Any]]:
    cols = result.column_names
    return [dict(zip(cols, row)) for row in result.result_rows]


def fetch_partitions(ch: Any, topic: str) -> List[int]:
    result = ch.query(
        """
        SELECT DISTINCT kafka_partition
        FROM cdc_raw_events
        WHERE kafka_topic = {topic:String}
        ORDER BY kafka_partition
        """,
        parameters={"topic": topic},
    )
    return [int(row[0]) for row in result.result_rows]


def fetch_last_offset(ch: Any, consumer_name: str, topic: str, partition: int) -> int:
    result = ch.query(
        """
        SELECT max(offset) AS offset
        FROM cdc_consumer_offsets FINAL
        WHERE consumer_name = {consumer_name:String}
          AND topic = {topic:String}
          AND partition = {partition:Int32}
        """,
        parameters={
            "consumer_name": consumer_name,
            "topic": topic,
            "partition": partition,
        },
    )
    if not result.result_rows or result.result_rows[0][0] is None:
        return -1
    return int(result.result_rows[0][0])


def fetch_new_raw_events(
    ch: Any,
    topic: str,
    partition: int,
    last_offset: int,
    limit: int,
) -> List[CdcRawEvent]:
    result = ch.query(
        """
        SELECT
            event_id,
            source_table,
            op_type,
            source_pk,
            source_updated_at,
            source_tx_id,
            source_offset,
            kafka_topic,
            kafka_partition,
            dedup_key,
            before_json,
            after_json,
            is_duplicate
        FROM cdc_raw_events
        WHERE kafka_topic = {topic:String}
          AND kafka_partition = {partition:Int32}
          AND source_offset > {last_offset:Int64}
        ORDER BY source_offset
        LIMIT {limit:Int32}
        """,
        parameters={
            "topic": topic,
            "partition": partition,
            "last_offset": last_offset,
            "limit": limit,
        },
    )
    rows = _result_to_dicts(result)
    events: List[CdcRawEvent] = []
    for row in rows:
        events.append(
            CdcRawEvent(
                event_id=str(row["event_id"]),
                source_table=str(row["source_table"]),
                op_type=str(row["op_type"]),
                source_pk=str(row["source_pk"]),
                source_updated_at=parse_datetime(row["source_updated_at"]),
                source_tx_id=row["source_tx_id"],
                source_offset=int(row["source_offset"]),
                kafka_topic=str(row["kafka_topic"]),
                kafka_partition=int(row["kafka_partition"]),
                dedup_key=str(row["dedup_key"]),
                before=_json_loads(str(row["before_json"])),
                after=_json_loads(str(row["after_json"])),
                is_duplicate=int(row["is_duplicate"]),
            )
        )
    return events


def fetch_current_states(ch: Any, order_ids: Sequence[int]) -> Dict[int, OrderState]:
    if not order_ids:
        return {}
    unique_ids = sorted(set(order_ids))
    csv_ids = ",".join(str(v) for v in unique_ids)
    result = ch.query(
        f"""
        SELECT
            order_id,
            user_id,
            status,
            amount,
            source_updated_at,
            is_deleted,
            source_offset
        FROM order_current_state FINAL
        WHERE order_id IN ({csv_ids})
        """
    )
    state_map: Dict[int, OrderState] = {}
    for row in _result_to_dicts(result):
        state_map[int(row["order_id"])] = OrderState(
            order_id=int(row["order_id"]),
            user_id=int(row["user_id"]),
            status=str(row["status"]),
            amount=float(row["amount"]),
            source_updated_at=parse_datetime(row["source_updated_at"]),
            is_deleted=int(row["is_deleted"]),
            source_offset=int(row["source_offset"]),
        )
    return state_map


def insert_states(ch: Any, states: Sequence[OrderState]) -> None:
    if not states:
        return
    now_utc = _utc_now()
    columns = [
        "order_id",
        "user_id",
        "status",
        "amount",
        "source_updated_at",
        "is_deleted",
        "load_ts",
        "source_offset",
        "version",
    ]
    values = []
    for state in states:
        version = int(state.source_updated_at.timestamp() * 1000) * 10_000 + int(state.source_offset)
        values.append(
            [
                state.order_id,
                state.user_id,
                state.status,
                round(state.amount, 2),
                state.source_updated_at,
                state.is_deleted,
                now_utc,
                state.source_offset,
                version,
            ]
        )
    ch.insert("order_current_state", values, column_names=columns)


def upsert_apply_offset(
    ch: Any,
    *,
    consumer_name: str,
    topic: str,
    partition: int,
    offset: int,
) -> None:
    ch.insert(
        "cdc_consumer_offsets",
        [[consumer_name, topic, partition, int(offset), _utc_now()]],
        column_names=["consumer_name", "topic", "partition", "offset", "updated_at"],
    )


def refresh_mart_hourly(ch: Any) -> None:
    ch.command(
        """
        INSERT INTO mart_order_metrics_hourly
        SELECT
            toStartOfHour(source_updated_at) AS bucket_start,
            count() AS order_count,
            countIf(lower(status) IN ('paid', 'completed')) AS paid_order_count,
            countIf(lower(status) IN ('canceled', 'cancelled')) AS canceled_order_count,
            toDecimal64(sum(amount), 2) AS total_amount,
            now64(3) AS load_ts
        FROM order_current_state FINAL
        WHERE is_deleted = 0
        GROUP BY bucket_start
        ORDER BY bucket_start
        """
    )


def apply_loop() -> None:
    ch = _build_clickhouse_client()
    topic = os.getenv("CDC_TOPIC", "cdc_coinstream.public.orders")
    consumer_name = os.getenv("CDC_APPLY_CONSUMER_NAME", "cdc_state_applier")
    batch_limit = int(os.getenv("CDC_APPLY_BATCH_LIMIT", "500"))
    poll_interval = float(os.getenv("CDC_APPLY_POLL_SECONDS", "2.0"))

    while True:
        partitions = fetch_partitions(ch, topic)
        if not partitions:
            time.sleep(poll_interval)
            continue

        processed_any = False
        for partition in partitions:
            last_offset = fetch_last_offset(ch, consumer_name, topic, partition)
            events = fetch_new_raw_events(ch, topic, partition, last_offset, batch_limit)
            if not events:
                continue

            order_ids = [_extract_order_id(event) for event in events]
            state_map = fetch_current_states(ch, order_ids)
            seen_event_ids: Set[str] = set()
            upsert_rows: List[OrderState] = []
            max_offset = last_offset

            for event in events:
                max_offset = max(max_offset, event.source_offset)
                order_id = _extract_order_id(event)
                current_state = state_map.get(order_id)
                next_state, applied, _reason = apply_cdc_event(current_state, event, seen_event_ids)
                if not applied or next_state is None:
                    continue
                state_map[order_id] = next_state
                upsert_rows.append(next_state)

            if upsert_rows:
                insert_states(ch, upsert_rows)
                processed_any = True

            upsert_apply_offset(
                ch,
                consumer_name=consumer_name,
                topic=topic,
                partition=partition,
                offset=max_offset,
            )

        if processed_any:
            refresh_mart_hourly(ch)

        time.sleep(poll_interval)


if __name__ == "__main__":
    apply_loop()
