from datetime import datetime
from typing import Iterable

from processor.window import OhlcAgg


def ensure_clickhouse_schema(ch_client) -> None:
    # Create databases/tables at processor startup so ClickHouse init scripts
    # are not required for deployment environments with restricted init hooks.
    ch_client.command("CREATE DATABASE IF NOT EXISTS crypto")
    ch_client.command("CREATE DATABASE IF NOT EXISTS coinstream")
    ch_client.command(
        """
        CREATE TABLE IF NOT EXISTS crypto.ohlc_1m
        (
            window_start DateTime64(3, 'UTC'),
            window_end   DateTime64(3, 'UTC'),
            symbol       LowCardinality(String),
            open         Float64,
            high         Float64,
            low          Float64,
            close        Float64,
            volume       Float64,
            trade_count  UInt64,
            vwap         Float64,
            emitted_at   DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(emitted_at)
        PARTITION BY toDate(window_start)
        ORDER BY (symbol, window_start)
        """
    )
    ch_client.command(
        """
        CREATE TABLE IF NOT EXISTS coinstream.mart_ohlcv_1m
        (
            window_start DateTime64(3, 'UTC'),
            window_end DateTime64(3, 'UTC'),
            symbol String,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            trade_count UInt64,
            vwap Float64,
            late_event_count UInt64 DEFAULT 0,
            window_ms UInt32,
            emitted_at DateTime64(3, 'UTC'),
            result_version UInt64,
            created_at DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(result_version)
        PARTITION BY toDate(window_start)
        ORDER BY (symbol, window_start)
        """
    )
    ch_client.command(
        """
        CREATE TABLE IF NOT EXISTS coinstream.mart_pipeline_health_1m
        (
            window_start DateTime64(3, 'UTC'),
            throughput UInt64,
            p95_latency_ms Float64,
            consumer_lag_ms UInt64,
            freshness_seconds Float64,
            late_event_count UInt64,
            created_at DateTime64(3, 'UTC')
        )
        ENGINE = MergeTree()
        PARTITION BY toDate(window_start)
        ORDER BY window_start
        """
    )


def write_mart_ohlcv(ch_client, rows: Iterable[dict]) -> None:
    column_names = [
        "window_start",
        "window_end",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
        "late_event_count",
        "window_ms",
        "emitted_at",
        "result_version",
        "created_at",
    ]
    records = []
    for row in rows:
        records.append(
            (
                row["window_start"],
                row["window_end"],
                row["symbol"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["trade_count"],
                row["vwap"],
                row.get("late_event_count", 0),
                row.get("window_ms", 60000),
                row.get("emitted_at", row["window_end"]),
                int(row["window_start"].timestamp() * 1000),
                datetime.utcnow(),
            )
        )
    if not records:
        return
    ch_client.insert("coinstream.mart_ohlcv_1m", records, column_names=column_names)


def write_mart_pipeline_health(
    ch_client,
    window_start,
    throughput: int,
    p95_latency_ms: float,
    consumer_lag_ms: int,
    freshness_seconds: float,
) -> None:
    column_names = [
        "window_start",
        "throughput",
        "p95_latency_ms",
        "consumer_lag_ms",
        "freshness_seconds",
        "late_event_count",
        "created_at",
    ]
    row = (
        window_start,
        throughput,
        p95_latency_ms,
        consumer_lag_ms,
        freshness_seconds,
        0,
        datetime.utcnow(),
    )
    ch_client.insert(
        "coinstream.mart_pipeline_health_1m",
        [row],
        column_names=column_names,
    )
