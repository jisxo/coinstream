import datetime as dt
import math
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class OhlcAgg:
    window_start_ms: int
    window_end_ms: int
    symbol: str

    open_price: Optional[float] = None
    high: float = -math.inf
    low: float = math.inf
    close_price: Optional[float] = None

    open_event_time_ms: Optional[int] = None
    close_event_time_ms: Optional[int] = None
    open_trade_id: Optional[int] = None
    close_trade_id: Optional[int] = None

    volume: float = 0.0
    trade_count: int = 0
    vwap_numer: float = 0.0

    def update(self, event_time_ms: int, agg_trade_id: int, price: float, qty: float) -> None:
        if self.open_event_time_ms is None or event_time_ms < self.open_event_time_ms or (
            event_time_ms == self.open_event_time_ms and (self.open_trade_id is None or agg_trade_id < self.open_trade_id)
        ):
            self.open_event_time_ms = event_time_ms
            self.open_trade_id = agg_trade_id
            self.open_price = price

        if self.close_event_time_ms is None or event_time_ms > self.close_event_time_ms or (
            event_time_ms == self.close_event_time_ms and (self.close_trade_id is None or agg_trade_id > self.close_trade_id)
        ):
            self.close_event_time_ms = event_time_ms
            self.close_trade_id = agg_trade_id
            self.close_price = price

        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price

        self.volume += qty
        self.vwap_numer += price * qty
        self.trade_count += 1

    def to_row(self) -> dict:
        vwap = (self.vwap_numer / self.volume) if self.volume > 0 else 0.0
        ws = dt.datetime.fromtimestamp(self.window_start_ms / 1000.0, tz=dt.timezone.utc)
        we = dt.datetime.fromtimestamp(self.window_end_ms / 1000.0, tz=dt.timezone.utc)
        emitted = dt.datetime.now(tz=dt.timezone.utc)
        return {
            "window_start": ws,
            "window_end": we,
            "symbol": self.symbol,
            "open": float(self.open_price or 0.0),
            "high": float(self.high if self.high != -math.inf else 0.0),
            "low": float(self.low if self.low != math.inf else 0.0),
            "close": float(self.close_price or 0.0),
            "volume": float(self.volume),
            "trade_count": int(self.trade_count),
            "vwap": float(vwap),
            "emitted_at": emitted,
        }


def window_bounds(event_time_ms: int, window_ms: int) -> Tuple[int, int]:
    start = (event_time_ms // window_ms) * window_ms
    return start, start + window_ms
