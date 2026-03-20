from __future__ import annotations

from datetime import datetime, timezone

from cdc.src.apply_cdc_to_state import aggregate_hourly_metrics, apply_cdc_event
from cdc.src.models import CdcRawEvent, OrderState


def _dt(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def _event(
    *,
    event_id: str,
    op_type: str,
    source_offset: int,
    source_updated_at: str,
    before=None,
    after=None,
    is_duplicate: int = 0,
) -> CdcRawEvent:
    return CdcRawEvent(
        event_id=event_id,
        source_table="public.orders",
        op_type=op_type,
        source_pk=str((after or before or {}).get("order_id", 1)),
        source_updated_at=_dt(source_updated_at),
        source_tx_id=100,
        source_offset=source_offset,
        kafka_topic="cdc_coinstream.public.orders",
        kafka_partition=0,
        dedup_key=event_id,
        before=before,
        after=after,
        is_duplicate=is_duplicate,
    )


def test_insert_apply():
    event = _event(
        event_id="e1",
        op_type="c",
        source_offset=10,
        source_updated_at="2026-03-20T10:00:00Z",
        after={
            "order_id": 1,
            "user_id": 101,
            "status": "created",
            "amount": "19.50",
            "updated_at": "2026-03-20T10:00:00Z",
        },
    )
    next_state, applied, reason = apply_cdc_event(None, event, seen_event_ids=set())
    assert applied is True
    assert reason == "upsert"
    assert next_state is not None
    assert next_state.order_id == 1
    assert next_state.user_id == 101
    assert next_state.amount == 19.5
    assert next_state.is_deleted == 0


def test_update_apply():
    current = OrderState(
        order_id=1,
        user_id=101,
        status="created",
        amount=19.5,
        source_updated_at=_dt("2026-03-20T10:00:00Z"),
        is_deleted=0,
        source_offset=10,
    )
    event = _event(
        event_id="e2",
        op_type="u",
        source_offset=11,
        source_updated_at="2026-03-20T10:05:00Z",
        after={
            "order_id": 1,
            "user_id": 101,
            "status": "paid",
            "amount": "22.00",
            "updated_at": "2026-03-20T10:05:00Z",
        },
    )
    next_state, applied, reason = apply_cdc_event(current, event, seen_event_ids=set())
    assert applied is True
    assert reason == "upsert"
    assert next_state is not None
    assert next_state.status == "paid"
    assert next_state.amount == 22.0
    assert next_state.source_offset == 11


def test_delete_apply():
    current = OrderState(
        order_id=2,
        user_id=202,
        status="created",
        amount=11.0,
        source_updated_at=_dt("2026-03-20T10:00:00Z"),
        is_deleted=0,
        source_offset=20,
    )
    event = _event(
        event_id="e3",
        op_type="d",
        source_offset=21,
        source_updated_at="2026-03-20T10:07:00Z",
        before={"order_id": 2, "user_id": 202, "status": "created", "amount": "11.00"},
    )
    next_state, applied, reason = apply_cdc_event(current, event, seen_event_ids=set())
    assert applied is True
    assert reason == "delete"
    assert next_state is not None
    assert next_state.is_deleted == 1
    assert next_state.order_id == 2


def test_duplicate_ignored():
    event = _event(
        event_id="dup-1",
        op_type="u",
        source_offset=30,
        source_updated_at="2026-03-20T11:00:00Z",
        after={
            "order_id": 3,
            "user_id": 303,
            "status": "paid",
            "amount": "30.00",
            "updated_at": "2026-03-20T11:00:00Z",
        },
    )
    seen = {"dup-1"}
    next_state, applied, reason = apply_cdc_event(None, event, seen_event_ids=seen)
    assert applied is False
    assert reason == "duplicate"
    assert next_state is None


def test_old_event_ignored():
    current = OrderState(
        order_id=4,
        user_id=404,
        status="paid",
        amount=55.0,
        source_updated_at=_dt("2026-03-20T12:00:00Z"),
        is_deleted=0,
        source_offset=100,
    )
    event = _event(
        event_id="old-1",
        op_type="u",
        source_offset=90,
        source_updated_at="2026-03-20T11:59:00Z",
        after={
            "order_id": 4,
            "user_id": 404,
            "status": "created",
            "amount": "50.00",
            "updated_at": "2026-03-20T11:59:00Z",
        },
    )
    next_state, applied, reason = apply_cdc_event(current, event, seen_event_ids=set())
    assert applied is False
    assert reason == "old_event"
    assert next_state == current


def test_mart_aggregation():
    states = [
        OrderState(1, 11, "paid", 10.0, _dt("2026-03-20T10:10:00Z"), 0, 1),
        OrderState(2, 12, "canceled", 5.0, _dt("2026-03-20T10:20:00Z"), 0, 2),
        OrderState(3, 13, "created", 7.0, _dt("2026-03-20T10:50:00Z"), 1, 3),  # deleted
        OrderState(4, 14, "completed", 40.0, _dt("2026-03-20T11:05:00Z"), 0, 4),
    ]
    metrics = aggregate_hourly_metrics(states)
    assert len(metrics) == 2

    first = metrics[0]
    assert first["order_count"] == 2
    assert first["paid_order_count"] == 1
    assert first["canceled_order_count"] == 1
    assert first["total_amount"] == 15.0

    second = metrics[1]
    assert second["order_count"] == 1
    assert second["paid_order_count"] == 1
    assert second["canceled_order_count"] == 0
    assert second["total_amount"] == 40.0
