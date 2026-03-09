from __future__ import annotations

from dataclasses import dataclass
from typing import Any

SCHEMA_VERSION = "1.1.0"

SPOT_SYMBOLS = ["SOLUSDC", "SOLUSDT", "BTCUSDC", "BTCUSDT", "ETHUSDT"]
KLINE_INTERVALS = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]


@dataclass(frozen=True)
class RecordEnvelope:
    schema_version: str
    source: str
    source_ts_ms: int
    ingest_ts_ms: int
    payload: dict[str, Any]


# Canonical columns per dataset for consistency.
KLINE_COLUMNS = [
    "schema_version",
    "source",
    "market",
    "symbol",
    "timeframe",
    "open_time_ms",
    "close_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "source_ts_ms",
    "ingest_ts_ms",
]

BOOK_TICKER_COLUMNS = [
    "schema_version",
    "source",
    "market",
    "symbol",
    "event_time_ms",
    "bid_price",
    "bid_qty",
    "ask_price",
    "ask_qty",
    "source_ts_ms",
    "ingest_ts_ms",
]

AGG_TRADE_COLUMNS = [
    "schema_version",
    "source",
    "market",
    "symbol",
    "agg_trade_id",
    "price",
    "qty",
    "first_trade_id",
    "last_trade_id",
    "trade_time_ms",
    "is_buyer_maker",
    "source_ts_ms",
    "ingest_ts_ms",
]

FUNDING_COLUMNS = [
    "schema_version",
    "source",
    "market",
    "symbol",
    "funding_time_ms",
    "funding_rate",
    "mark_price",
    "source_ts_ms",
    "ingest_ts_ms",
]

OPEN_INTEREST_COLUMNS = [
    "schema_version",
    "source",
    "market",
    "symbol",
    "event_time_ms",
    "open_interest",
    "source_ts_ms",
    "ingest_ts_ms",
]

BASIS_COLUMNS = [
    "schema_version",
    "source",
    "market",
    "symbol",
    "event_time_ms",
    "spot_mid",
    "perp_mid",
    "basis_bps",
    "source_ts_ms",
    "ingest_ts_ms",
]

# Coverage / provenance diagnostics for historical source validity.
SOURCE_COVERAGE_KEYS = [
    "agg_trades",
    "book_ticker",
    "open_interest",
    "basis",
    "funding",
]

FEATURE_COVERAGE_COLUMNS = [
    "coverage_agg_trades",
    "coverage_book_ticker",
    "coverage_open_interest",
    "coverage_basis",
    "historical_liquidity_ok",
    "historical_derivatives_ok",
]
