import os
from dataclasses import dataclass
from typing import List

from shared.config import KafkaConfig


@dataclass(frozen=True)
class IngestConfig:
    kafka: KafkaConfig
    symbols: List[str]
    binance_ws_base: str
    metrics_port: int

    @classmethod
    def from_env(cls) -> "IngestConfig":
        raw_symbols = os.getenv("SYMBOLS", "btcusdt,ethusdt")
        symbols = [s.strip().lower() for s in raw_symbols.split(",") if s.strip()]
        return cls(
            kafka=KafkaConfig.from_env(),
            symbols=symbols,
            binance_ws_base=os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443/stream?streams="),
            metrics_port=int(os.getenv("METRICS_PORT", "8000")),
        )

    def ws_url(self) -> str:
        streams = "/".join([f"{s}@aggTrade" for s in self.symbols])
        return f"{self.binance_ws_base}{streams}"
