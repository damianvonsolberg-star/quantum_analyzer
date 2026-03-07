from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import time
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .schemas import KLINE_INTERVALS, SCHEMA_VERSION, SPOT_SYMBOLS
from .store import write_partitioned_parquet

SPOT_API = "https://api.binance.com"
FUTURES_API = "https://fapi.binance.com"

INTERVAL_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
}


@dataclass
class BinanceIngestConfig:
    out_dir: str = "data/binance"
    retries: int = 5
    backoff_seconds: float = 1.0
    timeout_seconds: int = 30


class BinanceClient:
    def __init__(self, cfg: BinanceIngestConfig):
        self.cfg = cfg

    def _get_json(self, base_url: str, path: str, params: dict[str, Any]) -> Any:
        query = urlencode(params)
        url = f"{base_url}{path}?{query}"

        last_error: Exception | None = None
        for i in range(self.cfg.retries):
            try:
                req = Request(url, headers={"User-Agent": "quantum-analyzer/0.1"})
                with urlopen(req, timeout=self.cfg.timeout_seconds) as resp:
                    payload = resp.read().decode("utf-8")
                    return json.loads(payload)
            except Exception as e:  # noqa: BLE001
                last_error = e
                time.sleep(self.cfg.backoff_seconds * (2**i))
        raise RuntimeError(f"Failed request after retries: {url}") from last_error

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor = start_ms
        step = INTERVAL_MS[interval] * limit

        while cursor < end_ms:
            chunk_end = min(end_ms, cursor + step)
            data = self._get_json(
                SPOT_API,
                "/api/v3/klines",
                {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": chunk_end,
                    "limit": limit,
                },
            )
            if not data:
                cursor = chunk_end + INTERVAL_MS[interval]
                continue

            ingest_ts = int(time.time() * 1000)
            for k in data:
                open_ts = int(k[0])
                close_ts = int(k[6])
                if open_ts < start_ms or open_ts > end_ms:
                    continue
                rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "source": "binance_spot_klines",
                        "market": "spot",
                        "symbol": symbol,
                        "timeframe": interval,
                        "open_time_ms": open_ts,
                        "close_time_ms": close_ts,
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "volume": float(k[5]),
                        "quote_volume": float(k[7]),
                        "trade_count": int(k[8]),
                        "taker_buy_base_volume": float(k[9]),
                        "taker_buy_quote_volume": float(k[10]),
                        "source_ts_ms": open_ts,
                        "ingest_ts_ms": ingest_ts,
                    }
                )
            cursor = int(data[-1][0]) + INTERVAL_MS[interval]

        return rows

    def fetch_book_ticker(self, symbol: str) -> list[dict[str, Any]]:
        data = self._get_json(SPOT_API, "/api/v3/ticker/bookTicker", {"symbol": symbol})
        now_ms = int(time.time() * 1000)
        return [
            {
                "schema_version": SCHEMA_VERSION,
                "source": "binance_spot_book_ticker",
                "market": "spot",
                "symbol": symbol,
                "event_time_ms": now_ms,
                "bid_price": float(data["bidPrice"]),
                "bid_qty": float(data["bidQty"]),
                "ask_price": float(data["askPrice"]),
                "ask_qty": float(data["askQty"]),
                "source_ts_ms": now_ms,
                "ingest_ts_ms": now_ms,
            }
        ]

    def fetch_agg_trades(self, symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        # Binance endpoint limit is 1000; we page by fromId fallback to time windows.
        data = self._get_json(
            SPOT_API,
            "/api/v3/aggTrades",
            {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000},
        )
        ingest_ts = int(time.time() * 1000)
        rows: list[dict[str, Any]] = []
        for t in data:
            trade_ts = int(t["T"])
            rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "source": "binance_spot_agg_trades",
                    "market": "spot",
                    "symbol": symbol,
                    "agg_trade_id": int(t["a"]),
                    "price": float(t["p"]),
                    "qty": float(t["q"]),
                    "first_trade_id": int(t["f"]),
                    "last_trade_id": int(t["l"]),
                    "trade_time_ms": trade_ts,
                    "is_buyer_maker": bool(t["m"]),
                    "source_ts_ms": trade_ts,
                    "ingest_ts_ms": ingest_ts,
                }
            )
        return rows

    def fetch_funding(self, symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        data = self._get_json(
            FUTURES_API,
            "/fapi/v1/fundingRate",
            {
                "symbol": symbol,
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": 1000,
            },
        )
        ingest_ts = int(time.time() * 1000)
        return [
            {
                "schema_version": SCHEMA_VERSION,
                "source": "binance_futures_funding",
                "market": "futures",
                "symbol": symbol,
                "funding_time_ms": int(x["fundingTime"]),
                "funding_rate": float(x["fundingRate"]),
                "mark_price": float(x["markPrice"]),
                "source_ts_ms": int(x["fundingTime"]),
                "ingest_ts_ms": ingest_ts,
            }
            for x in data
        ]

    def fetch_open_interest(self, symbol: str) -> list[dict[str, Any]]:
        data = self._get_json(FUTURES_API, "/fapi/v1/openInterest", {"symbol": symbol})
        now_ms = int(time.time() * 1000)
        return [
            {
                "schema_version": SCHEMA_VERSION,
                "source": "binance_futures_open_interest",
                "market": "futures",
                "symbol": symbol,
                "event_time_ms": now_ms,
                "open_interest": float(data["openInterest"]),
                "source_ts_ms": now_ms,
                "ingest_ts_ms": now_ms,
            }
        ]


def _parse_dt(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def backfill_range(
    cfg: BinanceIngestConfig,
    start: datetime,
    end: datetime,
    symbols: list[str],
    intervals: list[str],
) -> dict[str, int]:
    client = BinanceClient(cfg)
    start_ms = _to_ms(start)
    end_ms = _to_ms(end)

    counts: dict[str, int] = {}

    for symbol in symbols:
        for interval in intervals:
            klines = client.fetch_klines(symbol, interval, start_ms, end_ms)
            files = write_partitioned_parquet(
                rows=klines,
                root=cfg.out_dir,
                dataset="klines",
                market="spot",
                symbol=symbol,
                timeframe=interval,
                ts_field="source_ts_ms",
            )
            counts[f"klines:{symbol}:{interval}"] = len(klines)
            counts[f"files:klines:{symbol}:{interval}"] = len(files)

        book = client.fetch_book_ticker(symbol)
        write_partitioned_parquet(book, cfg.out_dir, "book_ticker", "spot", symbol, ts_field="source_ts_ms")
        agg = client.fetch_agg_trades(symbol, start_ms, end_ms)
        write_partitioned_parquet(agg, cfg.out_dir, "agg_trades", "spot", symbol, ts_field="source_ts_ms")
        counts[f"agg:{symbol}"] = len(agg)

    # Futures: SOLUSDT perpetual
    futures_symbol = "SOLUSDT"
    funding = client.fetch_funding(futures_symbol, start_ms, end_ms)
    write_partitioned_parquet(funding, cfg.out_dir, "funding", "futures", futures_symbol, ts_field="source_ts_ms")
    counts["funding:SOLUSDT"] = len(funding)

    oi = client.fetch_open_interest(futures_symbol)
    write_partitioned_parquet(oi, cfg.out_dir, "open_interest", "futures", futures_symbol, ts_field="source_ts_ms")
    counts["open_interest:SOLUSDT"] = len(oi)

    # Basis proxy: spot mid vs perpetual mark sampled at ingest time.
    now_ms = int(time.time() * 1000)
    spot = client.fetch_book_ticker("SOLUSDT")[0]
    perp_oi = oi[0]
    perp_mark = funding[-1]["mark_price"] if funding else spot["ask_price"]
    spot_mid = (spot["bid_price"] + spot["ask_price"]) / 2
    basis_bps = ((perp_mark - spot_mid) / spot_mid) * 10_000 if spot_mid else 0.0
    basis_row = [
        {
            "schema_version": SCHEMA_VERSION,
            "source": "binance_futures_basis_proxy",
            "market": "futures",
            "symbol": "SOLUSDT",
            "event_time_ms": now_ms,
            "spot_mid": float(spot_mid),
            "perp_mid": float(perp_mark),
            "basis_bps": float(basis_bps),
            "source_ts_ms": now_ms,
            "ingest_ts_ms": now_ms,
        }
    ]
    write_partitioned_parquet(basis_row, cfg.out_dir, "basis", "futures", "SOLUSDT", ts_field="source_ts_ms")
    counts["basis:SOLUSDT"] = 1

    return counts


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Backfill Binance market data into parquet partitions")
    p.add_argument("--start", required=True, help="UTC start datetime (e.g. 2026-02-01T00:00:00Z)")
    p.add_argument("--end", required=True, help="UTC end datetime (e.g. 2026-03-01T00:00:00Z)")
    p.add_argument("--out", default="data/binance", help="Output root directory")
    p.add_argument("--symbols", default=",".join(SPOT_SYMBOLS), help="Comma-separated spot symbols")
    p.add_argument("--intervals", default=",".join(KLINE_INTERVALS), help="Comma-separated kline intervals")
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--timeout", type=int, default=30)
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    cfg = BinanceIngestConfig(out_dir=args.out, retries=args.retries, timeout_seconds=args.timeout)

    start = _parse_dt(args.start)
    end = _parse_dt(args.end)
    symbols = [x.strip().upper() for x in args.symbols.split(",") if x.strip()]
    intervals = [x.strip() for x in args.intervals.split(",") if x.strip()]

    counts = backfill_range(cfg, start, end, symbols, intervals)
    print(json.dumps(counts, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
