import asyncio
import signal
import time
from typing import Any, Dict

import orjson
import websockets
from prometheus_client import start_http_server

from ingest.config import IngestConfig
from ingest.metrics import IngestMetrics
from ingest.producer import build_producer


def normalize_event(msg: Dict[str, Any]) -> Dict[str, Any]:
    data = msg.get("data", msg)
    event_time_ms = int(data["E"])
    symbol = str(data["s"]).upper()
    agg_trade_id = int(data["a"])
    price = float(data["p"])
    qty = float(data["q"])
    is_buyer_maker = bool(data.get("m", False))
    stream = str(msg.get("stream", ""))

    return {
        "event_time_ms": event_time_ms,
        "ingest_time_ms": int(time.time() * 1000),
        "symbol": symbol,
        "agg_trade_id": agg_trade_id,
        "price": price,
        "quantity": qty,
        "is_buyer_maker": 1 if is_buyer_maker else 0,
        "stream": stream,
    }


class GracefulExit:
    def __init__(self) -> None:
        self.stop = asyncio.Event()

    def handler(self, *_args: Any) -> None:
        self.stop.set()


async def run() -> None:
    config = IngestConfig.from_env()
    metrics = IngestMetrics()
    start_http_server(config.metrics_port)
    producer = build_producer(config.kafka.brokers)

    exit_ctl = GracefulExit()
    signal.signal(signal.SIGINT, exit_ctl.handler)
    signal.signal(signal.SIGTERM, exit_ctl.handler)

    url = config.ws_url()
    backoff = 1.0

    while not exit_ctl.stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                backoff = 1.0
                async for raw in ws:
                    if exit_ctl.stop.is_set():
                        break

                    metrics.ws_messages_received.inc()
                    try:
                        msg = orjson.loads(raw)
                        payload = orjson.dumps(normalize_event(msg))
                        key = msg["data"]["s"].upper().encode("utf-8")

                        metrics.kafka_producer_inflight.inc()
                        t0 = time.perf_counter()

                        def delivery_cb(err: Any, _msg: Any) -> None:
                            metrics.kafka_producer_inflight.dec()
                            if err is None:
                                metrics.kafka_messages_sent.inc()
                                metrics.kafka_produce_latency.observe(time.perf_counter() - t0)

                        producer.produce(
                            config.kafka.topic_trades,
                            value=payload,
                            key=key,
                            on_delivery=delivery_cb,
                        )
                        producer.poll(0)

                    except Exception as exc:
                        try:
                            metrics.kafka_messages_dlq.inc()
                            dlq_payload = orjson.dumps(
                                {
                                    "error": str(exc),
                                    "raw": raw[:2000],
                                    "ingest_time_ms": int(time.time() * 1000),
                                }
                            )
                            producer.produce(config.kafka.topic_dlq, value=dlq_payload, key=b"dlq")
                            producer.poll(0)
                        except Exception:
                            pass

        except Exception:
            metrics.ws_reconnects.inc()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    producer.flush(10)


if __name__ == "__main__":
    asyncio.run(run())
