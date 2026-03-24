from __future__ import annotations

from datetime import datetime, timezone

from cdc.src.apply_cdc_to_state import aggregate_hourly_metrics, build_state_updates
from cdc.src.models import CdcRawEvent, OrderState


def _dt(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def _event(
    *,
    event_id: str,
    op_type: str,
    source_offset: int,
    source_updated_at: str,
    order_id: int,
    user_id: int = 100,
    status: str = "created",
    amount: str = "10.00",
) -> CdcRawEvent:
    after = None
    before = None
    if op_type != "d":
        after = {
            "order_id": order_id,
            "user_id": user_id,
            "status": status,
            "amount": amount,
            "updated_at": source_updated_at,
        }
    else:
        before = {
            "order_id": order_id,
            "user_id": user_id,
            "status": status,
            "amount": amount,
            "updated_at": source_updated_at,
        }

    return CdcRawEvent(
        event_id=event_id,
        source_table="public.orders",
        op_type=op_type,
        source_pk=str(order_id),
        source_updated_at=_dt(source_updated_at),
        source_tx_id=1,
        source_offset=source_offset,
        kafka_topic="cdc_coinstream.public.orders",
        kafka_partition=0,
        dedup_key=event_id,
        before=before,
        after=after,
        is_duplicate=0,
    )


def test_raw_to_state_to_mart_flow():
    state_map: dict[int, OrderState] = {}
    events = [
        _event(
            event_id="e1",
            op_type="c",
            source_offset=1,
            source_updated_at="2026-03-21T10:00:00Z",
            order_id=101,
            user_id=1001,
            status="created",
            amount="10.00",
        ),
        _event(
            event_id="e2",
            op_type="u",
            source_offset=2,
            source_updated_at="2026-03-21T10:01:00Z",
            order_id=101,
            user_id=1001,
            status="paid",
            amount="15.00",
        ),
        _event(
            event_id="e3",
            op_type="c",
            source_offset=3,
            source_updated_at="2026-03-21T10:02:00Z",
            order_id=102,
            user_id=1002,
            status="created",
            amount="20.00",
        ),
        _event(
            event_id="e4",
            op_type="d",
            source_offset=4,
            source_updated_at="2026-03-21T10:03:00Z",
            order_id=102,
            user_id=1002,
            status="created",
            amount="20.00",
        ),
    ]

    batch = build_state_updates(events, state_map, snapshot_policy="upsert")
    assert batch.max_offset == 4
    assert len(batch.upsert_rows) == 4
    assert batch.reason_counts["upsert"] == 3
    assert batch.reason_counts["delete"] == 1
    assert len(batch.changed_buckets) == 1

    # current state
    assert state_map[101].status == "paid"
    assert state_map[101].is_deleted == 0
    assert state_map[102].is_deleted == 1

    # mart view from current active state
    mart_rows = aggregate_hourly_metrics(state_map.values())
    assert len(mart_rows) == 1
    row = mart_rows[0]
    assert row["order_count"] == 1
    assert row["paid_order_count"] == 1
    assert row["canceled_order_count"] == 0
    assert row["total_amount"] == 15.0
