from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
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


def _to_hour_bucket(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _is_newer_event(
    current: Optional[OrderState],
    incoming_updated_at: datetime,
    incoming_offset: int,
) -> bool:
    if current is None:
        return True
    # Prefer source_offset ordering for CDC state transition.
    if incoming_offset > current.source_offset:
        return True
    if incoming_offset < current.source_offset:
        return False
    if incoming_updated_at > current.source_updated_at:
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
    snapshot_policy: str = "upsert",
) -> Tuple[Optional[OrderState], bool, str]:
    if seen_event_ids is not None:
        if event.event_id in seen_event_ids:
            return current_state, False, "duplicate"
        seen_event_ids.add(event.event_id)

    if event.is_duplicate == 1:
        return current_state, False, "duplicate"

    op = (event.op_type or "").lower()
    if op == "r" and snapshot_policy == "ignore":
        return current_state, False, "snapshot_ignored"

    if not _is_newer_event(current_state, event.source_updated_at, event.source_offset):
        return current_state, False, "old_event"

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


@dataclass
class ApplyBatchResult:
    upsert_rows: List[OrderState]
    max_offset: int
    changed_buckets: Set[datetime]
    reason_counts: Dict[str, int] = field(default_factory=dict)


class ApplyLoopMetrics:
    def __init__(self) -> None:
        self.applied_count = 0
        self.skipped_duplicate = 0
        self.skipped_old_event = 0
        self.skipped_missing_after = 0
        self.skipped_snapshot = 0
        self.apply_error_count = 0
        self.offset_commit_count = 0
        self._last_log_ts = time.time()

    def add_reason_counts(self, reason_counts: Dict[str, int]) -> None:
        self.applied_count += reason_counts.get("upsert", 0) + reason_counts.get("delete", 0)
        self.skipped_duplicate += reason_counts.get("duplicate", 0)
        self.skipped_old_event += reason_counts.get("old_event", 0)
        self.skipped_missing_after += reason_counts.get("missing_after", 0)
        self.skipped_snapshot += reason_counts.get("snapshot_ignored", 0)

    def maybe_log(self, every_seconds: int = 30) -> None:
        now = time.time()
        if now - self._last_log_ts < every_seconds:
            return
        print(
            "[cdc-apply-metrics] "
            f"applied={self.applied_count} "
            f"skip_duplicate={self.skipped_duplicate} "
            f"skip_old_event={self.skipped_old_event} "
            f"skip_missing_after={self.skipped_missing_after} "
            f"skip_snapshot={self.skipped_snapshot} "
            f"errors={self.apply_error_count} "
            f"offset_commit={self.offset_commit_count}",
            flush=True,
        )
        self._last_log_ts = now


def build_state_updates(
    events: Sequence[CdcRawEvent],
    state_map: Dict[int, OrderState],
    snapshot_policy: str = "upsert",
) -> ApplyBatchResult:
    upsert_rows: List[OrderState] = []
    changed_buckets: Set[datetime] = set()
    seen_event_ids: Set[str] = set()
    reason_counts: Dict[str, int] = {}
    max_offset = -1

    for event in events:
        max_offset = max(max_offset, event.source_offset)
        order_id = _extract_order_id(event)
        current_state = state_map.get(order_id)
        old_bucket = _to_hour_bucket(current_state.source_updated_at) if current_state else None

        next_state, applied, reason = apply_cdc_event(
            current_state,
            event,
            seen_event_ids,
            snapshot_policy=snapshot_policy,
        )
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        if not applied or next_state is None:
            continue

        state_map[order_id] = next_state
        upsert_rows.append(next_state)
        if old_bucket is not None:
            changed_buckets.add(old_bucket)
        changed_buckets.add(_to_hour_bucket(next_state.source_updated_at))

    return ApplyBatchResult(
        upsert_rows=upsert_rows,
        max_offset=max_offset,
        changed_buckets=changed_buckets,
        reason_counts=reason_counts,
    )


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
        username=os.getenv("CDC_CLICKHOUSE_USER", "cdc"),
        password=os.getenv("CDC_CLICKHOUSE_PASSWORD", "cdc123"),
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


def refresh_mart_hourly_for_buckets(ch: Any, buckets: Set[datetime]) -> None:
    if not buckets:
        return

    sorted_buckets = sorted(_to_hour_bucket(bucket) for bucket in buckets)
    bucket_expr = ", ".join(
        f"toDateTime('{bucket.strftime('%Y-%m-%d %H:%M:%S')}', 'UTC')" for bucket in sorted_buckets
    )
    # Recalculate only affected buckets. We emit zero rows for empty buckets so FINAL can
    # converge to the latest snapshot even when all rows in a bucket were deleted.
    ch.command(
        f"""
        INSERT INTO mart_order_metrics_hourly
        SELECT
            b.bucket_start AS bucket_start,
            toUInt64(coalesce(a.order_count, 0)) AS order_count,
            toUInt64(coalesce(a.paid_order_count, 0)) AS paid_order_count,
            toUInt64(coalesce(a.canceled_order_count, 0)) AS canceled_order_count,
            toDecimal64(coalesce(a.total_amount, 0), 2) AS total_amount,
            now64(3) AS load_ts
        FROM (SELECT arrayJoin([{bucket_expr}]) AS bucket_start) b
        LEFT JOIN
        (
            SELECT
                toStartOfHour(source_updated_at) AS bucket_start,
                count() AS order_count,
                countIf(lower(status) IN ('paid', 'completed')) AS paid_order_count,
                countIf(lower(status) IN ('canceled', 'cancelled')) AS canceled_order_count,
                sum(amount) AS total_amount
            FROM order_current_state FINAL
            WHERE is_deleted = 0
              AND toStartOfHour(source_updated_at) IN ({bucket_expr})
            GROUP BY bucket_start
        ) a USING bucket_start
        ORDER BY bucket_start
        """
    )


def apply_loop() -> None:
    ch = _build_clickhouse_client()
    topic = os.getenv("CDC_TOPIC", "cdc_coinstream.public.orders")
    consumer_name = os.getenv("CDC_APPLY_CONSUMER_NAME", "cdc_state_applier")
    snapshot_policy = os.getenv("CDC_SNAPSHOT_POLICY", "upsert").strip().lower()
    if snapshot_policy not in {"upsert", "ignore"}:
        snapshot_policy = "upsert"
    batch_limit = int(os.getenv("CDC_APPLY_BATCH_LIMIT", "500"))
    poll_interval = float(os.getenv("CDC_APPLY_POLL_SECONDS", "2.0"))
    log_interval = int(os.getenv("CDC_METRICS_LOG_INTERVAL_SEC", "30"))
    metrics = ApplyLoopMetrics()

    while True:
        partitions = fetch_partitions(ch, topic)
        if not partitions:
            metrics.maybe_log(log_interval)
            time.sleep(poll_interval)
            continue

        for partition in partitions:
            last_offset = fetch_last_offset(ch, consumer_name, topic, partition)
            events = fetch_new_raw_events(ch, topic, partition, last_offset, batch_limit)
            if not events:
                continue

            order_ids = [_extract_order_id(event) for event in events]
            state_map = fetch_current_states(ch, order_ids)
            batch = build_state_updates(events, state_map, snapshot_policy=snapshot_policy)
            metrics.add_reason_counts(batch.reason_counts)

            try:
                if batch.upsert_rows:
                    insert_states(ch, batch.upsert_rows)
                refresh_mart_hourly_for_buckets(ch, batch.changed_buckets)

                # Offset is updated only after state/mart writes complete successfully.
                offset_to_commit = max(last_offset, batch.max_offset)
                upsert_apply_offset(
                    ch,
                    consumer_name=consumer_name,
                    topic=topic,
                    partition=partition,
                    offset=offset_to_commit,
                )
                metrics.offset_commit_count += 1
            except Exception as exc:
                metrics.apply_error_count += 1
                print(
                    "[cdc-apply-error] "
                    f"partition={partition} last_offset={last_offset} err={exc}",
                    flush=True,
                )

        metrics.maybe_log(log_interval)
        time.sleep(poll_interval)


if __name__ == "__main__":
    apply_loop()
