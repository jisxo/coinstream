from prometheus_client import Counter, Gauge, Histogram


class ProcessorMetrics:
    def __init__(self) -> None:
        self.kafka_records_consumed = Counter("kafka_records_consumed_total", "Kafka records consumed")
        self.dedup_skipped = Counter("dedup_skipped_total", "Records skipped due to dedup")
        self.parse_failed = Counter("records_parse_failed_total", "Records failed to parse")
        self.windows_emitted = Counter("windows_emitted_total", "Aggregated windows emitted")
        self.emission_latency = Histogram("window_emission_latency_seconds", "Time to emit a batch of windows (seconds)")
        self.consumer_lag = Gauge("consumer_lag_ms_approx", "Approx lag in ms between now and max event time seen")
        self.max_event_time = Gauge("max_event_time_ms", "Max event time seen (ms)")
        self.watermark = Gauge("watermark_ms", "Current watermark (ms)")
        self.symbol_volatility = Gauge("symbol_volatility_ratio", "1-minute high/low ratio per symbol", ["symbol"])
