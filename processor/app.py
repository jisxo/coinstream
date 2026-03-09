import signal
import socket
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import orjson
from cachetools import TTLCache
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import start_http_server

import pyarrow as pa
import pyarrow.parquet as pq

from processor.config import ProcessorConfig
from processor.metrics import ProcessorMetrics
from processor.checkpoint import load_checkpoint, save_checkpoint
from processor.sink import build_clickhouse_client, build_s3_client, ensure_s3_bucket
from processor.mart import ensure_clickhouse_schema, write_mart_ohlcv, write_mart_pipeline_health
from processor.window import OhlcAgg, window_bounds

BOOTSTRAP_RETRIES = 90
BOOTSTRAP_RETRY_DELAY_SECONDS = 2.0


class GracefulExit:
    def __init__(self) -> None:
        self.stop = False

    def handler(self, *_args) -> None:
        self.stop = True


def normalize_message(value: bytes) -> Dict[str, object]:
    return orjson.loads(value)


def _connect_clickhouse_with_retry(config: ProcessorConfig):
    last_exc = None
    base_host = config.clickhouse_host.strip()
    host_candidates = [base_host]
    if base_host and not base_host.endswith(".internal"):
        host_candidates.append(f"{base_host}.internal")

    for attempt in range(1, BOOTSTRAP_RETRIES + 1):
        for host in host_candidates:
            try:
                socket.getaddrinfo(host, config.clickhouse_port)
                ch_client = build_clickhouse_client(
                    host=host,
                    port=config.clickhouse_port,
                    username=config.clickhouse_user,
                    password=config.clickhouse_password,
                    database=config.clickhouse_database,
                )
                ensure_clickhouse_schema(ch_client)
                print(f"[bootstrap] clickhouse connected: {host}:{config.clickhouse_port}", flush=True)
                return ch_client
            except Exception as exc:  # startup retry
                last_exc = exc
                print(
                    f"[bootstrap] clickhouse connect failed ({attempt}/{BOOTSTRAP_RETRIES}) host={host!r}: {exc}",
                    flush=True,
                )
        time.sleep(BOOTSTRAP_RETRY_DELAY_SECONDS)
    raise RuntimeError("clickhouse bootstrap failed") from last_exc


def _capture_offsets(consumer: Consumer) -> List[Dict[str, int]]:
    partitions = consumer.assignment()
    if not partitions:
        return []
    positions = consumer.position(partitions)
    return [
        {"topic": tp.topic, "partition": tp.partition, "offset": pos.offset}
        for tp, pos in zip(partitions, positions)
    ]


def flush_windows(
    state: Dict[Tuple[str, int], Dict[int, OhlcAgg]],
    watermark_ms: int,
    ch_client,
    s3_client,
    config: ProcessorConfig,
    metrics: ProcessorMetrics,
) -> List[dict]:
    rows: list[dict] = []
    for key, windows in list(state.items()):
        flush_keys = [ws for ws, agg in windows.items() if agg.window_end_ms <= watermark_ms]
        if not flush_keys:
            continue
        flush_keys.sort()
        win_ms = key[1]
        for ws in flush_keys:
            agg = windows.pop(ws)
            row = agg.to_row()
            row["window_ms"] = win_ms
            rows.append(row)
        if not windows:
            state.pop(key)

    if not rows:
        return []

    columns = [
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
        "emitted_at",
    ]
    column_rows = [[row[col] for col in columns] for row in rows]
    ch_client.insert("ohlc_1m", column_rows, column_names=columns)
    write_mart_ohlcv(ch_client, rows)

    batches: Dict[tuple[str, str, int], list[dict]] = {}
    for row in rows:
        ws = row["window_start"]
        key = (ws.strftime("%Y-%m-%d"), ws.strftime("H"), row.get("window_ms", 0))
        batches.setdefault(key, []).append(row)

    for (dt_part, hour_part, win_ms), batch in batches.items():
        table = pa.Table.from_pylist(batch)
        local_path = f"/tmp/ohlc_1m_{dt_part}_{hour_part}_{win_ms}_{int(time.time()*1000)}.parquet"
        pq.write_table(table, local_path, compression="zstd")
        s3_key = (
            f"{config.s3_prefix}/dt={dt_part}/hour={hour_part}/win={win_ms}/"
            f"ohlc_1m_{int(time.time()*1000)}.parquet"
        )
        with open(local_path, "rb") as f:
            s3_client.upload_fileobj(f, config.s3_bucket, s3_key)

    for row in rows:
        low = row["low"]
        if low and low != 0.0:
            volatility = (row["high"] - low) / low
        else:
            volatility = 0.0
        metrics.symbol_volatility.labels(symbol=row["symbol"]).set(volatility)

    metrics.windows_emitted.inc(len(rows))
    return rows


def main() -> None:
    config = ProcessorConfig.from_env()
    metrics = ProcessorMetrics()
    start_http_server(config.metrics_port)

    ch_client = _connect_clickhouse_with_retry(config)
    s3_client = build_s3_client(
        endpoint_url=config.s3_endpoint,
        access_key=config.s3_access_key,
        secret_key=config.s3_secret_key,
    )
    ensure_s3_bucket(s3_client, config.s3_bucket)

    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka.brokers,
            "group.id": config.consumer_group,
            "auto.offset.reset": config.auto_offset_reset,
            "enable.auto.commit": True,
            "fetch.min.bytes": 1_048_576,
            "fetch.wait.max.ms": 100,
            "max.partition.fetch.bytes": 4_194_304,
        }
    )
    consumer.subscribe([config.kafka.topic_trades])
    consumer.poll(0)

    dedup = TTLCache(maxsize=config.dedup_maxsize, ttl=config.dedup_ttl_seconds)  # dedup cache
    state: dict[tuple[str, int], dict[int, OhlcAgg]] = {}  # (symbol, window_ms) -> windows
    max_event_time_ms = 0
    last_flush = time.time()
    saved_offsets = None
    checkpoint_data = load_checkpoint(config)
    if checkpoint_data:
        state, dedup, saved_offsets = checkpoint_data
        if state:
            max_event_time_ms = max(
                (agg.window_end_ms for windows in state.values() for agg in windows.values()),
                default=0,
            )
        metrics.checkpoint_restores.inc()
    assignment = consumer.assignment()
    if saved_offsets and assignment:
        offsets_map = { (entry["topic"], entry["partition"]): entry["offset"] for entry in saved_offsets }
        for tp in assignment:
            key = (tp.topic, tp.partition)
            if key in offsets_map:
                consumer.seek(TopicPartition(tp.topic, tp.partition, offsets_map[key]))

    exit_ctl = GracefulExit()
    signal.signal(signal.SIGINT, exit_ctl.handler)
    signal.signal(signal.SIGTERM, exit_ctl.handler)

    last_checkpoint = time.time()
    while not exit_ctl.stop:
        msg = consumer.poll(1.0)
        if msg is None:
            pass  # no message yet
        elif msg.error():
            continue
        else:
            metrics.kafka_records_consumed.inc()
            try:
                payload = normalize_message(msg.value())
                symbol = payload["symbol"]
                event_time_ms = int(payload["event_time_ms"])
                agg_trade_id = int(payload["agg_trade_id"])
                price = float(payload["price"])
                qty = float(payload["quantity"])

                key = (symbol, agg_trade_id)
                if key in dedup:
                    metrics.dedup_skipped.inc()
                    continue
                dedup[key] = 1  # mark seen

                if event_time_ms > max_event_time_ms:
                    max_event_time_ms = event_time_ms
                    metrics.max_event_time.set(max_event_time_ms)

                metrics.consumer_lag.set(max(0, int(time.time() * 1000) - max_event_time_ms))

                for win_ms in config.window_ms_list:
                    wstart, wend = window_bounds(event_time_ms, win_ms)
                    key = (symbol, win_ms)
                    windows = state.setdefault(key, {})  # per symbol/window length
                    agg = windows.get(wstart)
                    if agg is None:
                        agg = OhlcAgg(window_start_ms=wstart, window_end_ms=wend, symbol=symbol)
                        windows[wstart] = agg
                    agg.update(event_time_ms=event_time_ms, agg_trade_id=agg_trade_id, price=price, qty=qty)
                metrics.state_windows.set(sum(len(w) for w in state.values()))
                metrics.dedup_cache_size.set(len(dedup))
            except Exception:
                metrics.parse_failed.inc()

        # calculate watermark
        watermark_ms = max_event_time_ms - config.allowed_lateness_ms if max_event_time_ms else 0
        metrics.watermark.set(watermark_ms)

        # flush
        if time.time() - last_flush >= 1.0:
            t0 = time.perf_counter()
            flushed_rows = flush_windows(state, watermark_ms, ch_client, s3_client, config, metrics)
            if flushed_rows:
                duration = time.perf_counter() - t0
                throughput = len(flushed_rows)
                latency_ms = duration * 1000
                lag_ms = max(0, int(time.time() * 1000 - max_event_time_ms))
                freshness_seconds = lag_ms / 1000
                window_start = max(row["window_start"] for row in flushed_rows)

                metrics.processor_messages.inc(len(flushed_rows))
                metrics.pipeline_throughput.set(throughput)
                metrics.pipeline_latency_ms.set(latency_ms)
                metrics.pipeline_lag_ms.set(lag_ms)
                metrics.pipeline_freshness_seconds.set(freshness_seconds)
                metrics.pipeline_window_start_ms.set(window_start.timestamp() * 1000)
                metrics.emission_latency.observe(duration)
                write_mart_pipeline_health(
                    ch_client,
                    window_start,
                    throughput,
                    latency_ms,
                    lag_ms,
                    freshness_seconds,
                )
            last_flush = time.time()

        # checkpoint
        if time.time() - last_checkpoint >= config.checkpoint_interval_ms / 1000:
            checkpoint_offsets = _capture_offsets(consumer)
            checkpoint_start = time.perf_counter()
            save_checkpoint(state, dedup, checkpoint_offsets, config)
            metrics.checkpoint_snapshots.inc()
            metrics.checkpoint_duration.observe(time.perf_counter() - checkpoint_start)
            last_checkpoint = time.time()


    consumer.close()


if __name__ == "__main__":
    main()
