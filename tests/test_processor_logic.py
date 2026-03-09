import datetime as dt

from cachetools import TTLCache

from processor.processing import (
    create_dedup_cache,
    compute_watermark,
    is_duplicate_event,
    is_event_late,
)
from processor.window import OhlcAgg


def test_watermark_marks_late_event() -> None:
    watermark = compute_watermark(max_event_time_ms=20_000, allowed_lateness_ms=5_000)
    assert watermark == 15_000
    assert not is_event_late(event_time_ms=16_000, watermark_ms=watermark)
    assert is_event_late(event_time_ms=14_000, watermark_ms=watermark)


def test_dedup_cache_skips_duplicates() -> None:
    cache: TTLCache[tuple[str, int], int] = create_dedup_cache(maxsize=100, ttl=60)
    assert not is_duplicate_event(cache, "BTCUSDT", 1)
    cache[("BTCUSDT", 1)] = 1
    assert is_duplicate_event(cache, "BTCUSDT", 1)


def test_ohlcagg_handles_out_of_order_updates() -> None:
    agg = OhlcAgg(window_start_ms=0, window_end_ms=60_000, symbol="BTCUSDT")
    agg.update(event_time_ms=10_000, agg_trade_id=1, price=40_000, qty=0.1)
    agg.update(event_time_ms=5_000, agg_trade_id=2, price=41_000, qty=0.2)
    assert agg.open_price == 41_000
    assert agg.close_price == 40_000
    assert agg.high == 41_000
    assert agg.low == 40_000
    assert agg.trade_count == 2
    assert agg.volume == 0.30000000000000004
