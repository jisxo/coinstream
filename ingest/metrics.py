from prometheus_client import Counter, Gauge, Histogram


class IngestMetrics:
    def __init__(self) -> None:
        self.ws_messages_received = Counter("ws_messages_received_total", "Total WS messages received")
        self.kafka_messages_sent = Counter("kafka_messages_sent_total", "Total messages successfully produced to Kafka")
        self.kafka_messages_dlq = Counter("kafka_messages_dlq_total", "Total messages sent to DLQ")
        self.ws_reconnects = Counter("ws_reconnect_total", "Total reconnect attempts")
        self.ingest_reconnects = Counter("ingest_reconnects_total", "Total ingest reconnect attempts")
        self.kafka_producer_inflight = Gauge("kafka_producer_inflight", "Approx in-flight produce messages")
        self.kafka_produce_latency = Histogram("kafka_produce_latency_seconds", "Kafka produce latency (seconds)")
