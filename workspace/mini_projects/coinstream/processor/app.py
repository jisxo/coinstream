import signal
import time
from typing import Dict

import orjson
from cachetools import TTLCache
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import start_http_server

import pyarrow as pa
import pyarrow.parquet as pq

from processor.config import ProcessorConfig
from processor.metrics import ProcessorMetrics
from processor.checkpoint import load_checkpoint, save_checkpoint
from processor.sink import build_clickhouse_client, build_s3_client
from processor.window import OhlcAgg, window_bounds


class GracefulExit:
    def __init__(self) -> None:
        self.stop = False

    def handler(self, *_args) -> None:
        self.stop = True


def normalize_message(value: bytes) -> Dict[str, object]:
    return orjson.loads(value)


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
    state: Dict[str, Dict[int, OhlcAgg]],
    watermark_ms: int,
    ch_client,
    s3_client,
    config: ProcessorConfig,
    metrics: ProcessorMetrics,
) -> int:
    rows = []
    for symbol, windows in list(state.items()):
        flush_keys = [ws for ws, agg in windows.items() if agg.window_end_ms <= watermark_ms]
        if not flush_keys:
            continue
        flush_keys.sort()
        for ws in flush_keys:
            agg = windows.pop(ws)
            rows.append(agg.to_row())
        if not windows:
            state.pop(symbol)

    if not rows:
        return 0

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

    batches: Dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        ws = row["window_start"]
        key = (ws.strftime("%Y-%m-%d"), ws.strftime("%H"))
        batches.setdefault(key, []).append(row)

    for (dt_part, hour_part), batch in batches.items():
        table = pa.Table.from_pylist(batch)
        local_path = f"/tmp/ohlc_1m_{dt_part}_{hour_part}_{int(time.time()*1000)}.parquet"
        pq.write_table(table, local_path, compression="zstd")
        s3_key = f"{config.s3_prefix}/dt={dt_part}/hour={hour_part}/ohlc_1m_{int(time.time()*1000)}.parquet"
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
    return len(rows)


def main() -> None:
    config = ProcessorConfig.from_env()
    metrics = ProcessorMetrics()
    start_http_server(config.metrics_port)

    ch_client = build_clickhouse_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_database,
    )
    s3_client = build_s3_client(
        endpoint_url=config.s3_endpoint,
        access_key=config.s3_access_key,
        secret_key=config.s3_secret_key,
    )

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

    dedup = TTLCache(maxsize=config.dedup_maxsize, ttl=config.dedup_ttl_seconds)
    state: dict[str, dict[int, OhlcAgg]] = {}
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
            pass
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
                dedup[key] = 1

                if event_time_ms > max_event_time_ms:
                    max_event_time_ms = event_time_ms
                    metrics.max_event_time.set(max_event_time_ms)

                metrics.consumer_lag.set(max(0, int(time.time() * 1000) - max_event_time_ms))

                wstart, wend = window_bounds(event_time_ms, config.window_ms)
                sym_windows = state.setdefault(symbol, {})
                agg = sym_windows.get(wstart)
                if agg is None:
                    agg = OhlcAgg(window_start_ms=wstart, window_end_ms=wend, symbol=symbol)
                    sym_windows[wstart] = agg
                agg.update(event_time_ms=event_time_ms, agg_trade_id=agg_trade_id, price=price, qty=qty)
            except Exception:
                metrics.parse_failed.inc()

        # calculate watermark
        watermark_ms = max_event_time_ms - config.allowed_lateness_ms if max_event_time_ms else 0
        metrics.watermark.set(watermark_ms)

        # flush
        if time.time() - last_flush >= 1.0:
            t0 = time.perf_counter()
            flushed = flush_windows(state, watermark_ms, ch_client, s3_client, config, metrics)
            if flushed:
                metrics.emission_latency.observe(time.perf_counter() - t0)
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
