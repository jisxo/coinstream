import os
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaConfig:
    brokers: str
    topic_trades: str
    topic_dlq: str

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        return cls(
            brokers=os.getenv("KAFKA_BROKERS", "redpanda:9092"),
            topic_trades=os.getenv("TOPIC_TRADES", "trades_raw"),
            topic_dlq=os.getenv("TOPIC_DLQ", "trades_dlq"),
        )
