from prometheus_client import Counter, Gauge, Histogram


class ProcessorMetrics:
    def __init__(self) -> None:
        self.kafka_records_consumed = Counter("kafka_records_consumed_total", "Kafka records consumed")
        self.dedup_skipped = Counter("dedup_skipped_total", "Records skipped due to dedup")
        self.late_events = Counter("late_events_total", "Events arriving after the watermark")
        self.parse_failed = Counter("records_parse_failed_total", "Records failed to parse")
        self.windows_emitted = Counter("windows_emitted_total", "Aggregated windows emitted")
        self.processor_messages = Counter("processor_messages_total", "Total rows emitted to sinks")
        self.emission_latency = Histogram("window_emission_latency_seconds", "Time to emit a batch of windows (seconds)")
        self.consumer_lag = Gauge("consumer_lag_ms_approx", "Approx lag in ms between now and max event time seen")
        self.max_event_time = Gauge("max_event_time_ms", "Max event time seen (ms)")
        self.watermark = Gauge("watermark_ms", "Current watermark (ms)")
        self.processor_lag = Gauge("processor_lag_seconds", "Processor lag behind watermark (seconds)")
        self.symbol_volatility = Gauge("symbol_volatility_ratio", "1-minute high/low ratio per symbol", ["symbol"])
        self.checkpoint_snapshots = Counter("checkpoint_snapshots_total", "Checkpoint snapshots created")
        self.checkpoint_restores = Counter("checkpoint_restores_total", "Checkpoint restores applied")
        self.checkpoint_duration = Histogram("checkpoint_duration_seconds", "Time spent persisting a checkpoint")
        self.state_windows = Gauge("state_window_count", "Number of in-flight windows")
        self.dedup_cache_size = Gauge("dedup_cache_size", "Dedup cache size")
        self.pipeline_throughput = Gauge("processor_pipeline_throughput", "Rows emitted per flush")
        self.pipeline_latency_ms = Gauge("processor_pipeline_latency_ms", "Latest flush latency (ms)")
        self.pipeline_lag_ms = Gauge("processor_pipeline_lag_ms", "Latest computed lag (ms)")
        self.pipeline_freshness_seconds = Gauge("processor_pipeline_freshness_seconds", "Latest freshness in seconds")
        self.pipeline_window_start_ms = Gauge("processor_pipeline_window_start_ms", "Timestamp of most recent emitted window (ms)")
