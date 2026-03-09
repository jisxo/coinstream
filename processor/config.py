import os
from dataclasses import dataclass

from shared.config import KafkaConfig


def _parse_window_ms_list(raw: str | None, fallback: str) -> tuple[int, ...]:
    candidate = raw or fallback
    return tuple(sorted({int(x.strip()) for x in candidate.split(",") if x.strip()}))


@dataclass(frozen=True)
class ProcessorConfig:
    kafka: KafkaConfig
    window_ms_list: tuple[int, ...]
    allowed_lateness_ms: int
    dedup_ttl_seconds: int
    dedup_maxsize: int
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_database: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    s3_prefix: str
    metrics_port: int
    consumer_group: str
    auto_offset_reset: str
    checkpoint_interval_ms: int
    checkpoint_store: str
    checkpoint_redis_url: str
    checkpoint_redis_key: str
    checkpoint_file_path: str

    @classmethod
    def from_env(cls) -> "ProcessorConfig":
        return cls(
            kafka=KafkaConfig.from_env(),
            window_ms_list=_parse_window_ms_list(os.getenv("WINDOW_MS_LIST"), os.getenv("WINDOW_MS", "60000")),
            allowed_lateness_ms=int(os.getenv("ALLOWED_LATENESS_MS", "10000")),
            dedup_ttl_seconds=int(os.getenv("DEDUP_TTL_SECONDS", "600")),
            dedup_maxsize=int(os.getenv("DEDUP_MAXSIZE", "500000")),
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            clickhouse_user=os.getenv("CLICKHOUSE_USER", "default"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database=os.getenv("CLICKHOUSE_DATABASE", "crypto"),
            s3_endpoint=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            s3_access_key=os.getenv("S3_ACCESS_KEY", "minioadmin"),
            s3_secret_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
            s3_bucket=os.getenv("S3_BUCKET", "crypto"),
            s3_prefix=os.getenv("S3_PREFIX", "ohlc_1m"),
            metrics_port=int(os.getenv("PROCESSOR_METRICS_PORT", "8000")),
            consumer_group=os.getenv("CONSUMER_GROUP", "ohlc_processor_v1"),
            auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "latest"),
            checkpoint_interval_ms=int(os.getenv("CHECKPOINT_INTERVAL_MS", "15000")),
            checkpoint_store=os.getenv("CHECKPOINT_STORE", "redis"),
            checkpoint_redis_url=os.getenv("CHECKPOINT_REDIS_URL", "redis://redis:6379/0"),
            checkpoint_redis_key=os.getenv("CHECKPOINT_REDIS_KEY", "coinstream:checkpoint"),
            checkpoint_file_path=os.getenv("CHECKPOINT_FILE_PATH", "/tmp/coinstream_checkpoint.json"),
        )
