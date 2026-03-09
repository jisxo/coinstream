from cachetools import TTLCache


def create_dedup_cache(maxsize: int, ttl: int) -> TTLCache:
    return TTLCache(maxsize=maxsize, ttl=ttl)


def is_duplicate_event(dedup: TTLCache, symbol: str, agg_trade_id: int) -> bool:
    key = (symbol, agg_trade_id)
    if key in dedup:
        return True
    dedup[key] = 1
    return False


def compute_watermark(max_event_time_ms: int, allowed_lateness_ms: int) -> int:
    if max_event_time_ms <= 0:
        return 0
    return max(0, max_event_time_ms - allowed_lateness_ms)


def is_event_late(event_time_ms: int, watermark_ms: int) -> bool:
    return watermark_ms > 0 and event_time_ms < watermark_ms
