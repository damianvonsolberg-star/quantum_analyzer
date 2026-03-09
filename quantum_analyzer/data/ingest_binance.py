from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
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
    "1d": 86_400_000,
    "1w": 604_800_000,
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
        """Paginate aggTrades over full requested time range.

        Fail closed on empty windows by returning only truly fetched rows;
        no synthetic backfill is introduced.
        """
        cursor = start_ms
        ingest_ts = int(time.time() * 1000)
        rows: list[dict[str, Any]] = []
        seen_ids: set[int] = set()

        while cursor <= end_ms:
            data = self._get_json(
                SPOT_API,
                "/api/v3/aggTrades",
                {"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": 1000},
            )
            if not data:
                break

            last_trade_ts = cursor
            for t in data:
                trade_id = int(t["a"])
                if trade_id in seen_ids:
                    continue
                seen_ids.add(trade_id)

                trade_ts = int(t["T"])
                if trade_ts < start_ms or trade_ts > end_ms:
                    continue

                rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "source": "binance_spot_agg_trades",
                        "market": "spot",
                        "symbol": symbol,
                        "agg_trade_id": trade_id,
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
                last_trade_ts = max(last_trade_ts, trade_ts)

            if len(data) < 1000:
                break
            # advance cursor; +1 to avoid replaying same event time bucket forever
            if last_trade_ts <= cursor:
                break
            cursor = last_trade_ts + 1

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


def _coverage_ratio_rows(rows: list[dict[str, Any]], start_ms: int, end_ms: int, ts_field: str = "source_ts_ms") -> float:
    if not rows:
        return 0.0
    ts = sorted(int(r.get(ts_field, 0) or 0) for r in rows if r.get(ts_field) is not None)
    if not ts:
        return 0.0
    span = max(end_ms - start_ms, 1)
    covered = max(min(ts[-1], end_ms) - max(ts[0], start_ms), 0)
    return float(min(1.0, covered / span))


def backfill_range(
    cfg: BinanceIngestConfig,
    start: datetime,
    end: datetime,
    symbols: list[str],
    intervals: list[str],
) -> dict[str, int | float]:
    client = BinanceClient(cfg)
    start_ms = _to_ms(start)
    end_ms = _to_ms(end)

    counts: dict[str, int | float] = {}
    coverage: dict[str, float] = {
        "agg_trades": 0.0,
        "book_ticker": 0.0,
        "open_interest": 0.0,
        "basis": 0.0,
        "funding": 0.0,
    }

    for symbol in symbols:
        for interval in intervals:
            if interval not in INTERVAL_MS:
                raise ValueError(f"Unsupported interval: {interval}")
            klines = client.fetch_klines(symbol, interval, start_ms, end_ms)
            files = write_partitioned_parquet(
                rows=klines,
                root=cfg.out_dir,
                dataset="klines",
                market="spot",
                symbol=symbol,
                timeframe=interval,
                ts_field="source_ts_ms",
                overwrite=False,
            )
            counts[f"klines:{symbol}:{interval}"] = len(klines)
            counts[f"files:klines:{symbol}:{interval}"] = len(files)
            counts[f"rows_written:klines:{symbol}:{interval}"] = int(sum(int(f.get("rows", 0) or 0) for f in files))
            counts[f"checksums:klines:{symbol}:{interval}"] = len({f.get('checksum_sha256') for f in files})

        # Historical backfill: aggTrades is time-series and safe to paginate.
        # Do NOT persist one-off bookTicker snapshots as historical liquidity.
        agg = client.fetch_agg_trades(symbol, start_ms, end_ms)
        agg_files = write_partitioned_parquet(agg, cfg.out_dir, "agg_trades", "spot", symbol, ts_field="source_ts_ms", overwrite=False)
        counts[f"agg:{symbol}"] = len(agg)
        counts[f"files:agg:{symbol}"] = len(agg_files)
        counts[f"rows_written:agg:{symbol}"] = int(sum(int(f.get("rows", 0) or 0) for f in agg_files))
        counts[f"checksums:agg:{symbol}"] = len({f.get('checksum_sha256') for f in agg_files})
        counts[f"book_ticker:{symbol}"] = 0
        coverage["agg_trades"] = max(coverage["agg_trades"], _coverage_ratio_rows(agg, start_ms, end_ms, ts_field="source_ts_ms"))

    # Futures: SOLUSDT perpetual
    futures_symbol = "SOLUSDT"
    funding = client.fetch_funding(futures_symbol, start_ms, end_ms)
    funding_files = write_partitioned_parquet(funding, cfg.out_dir, "funding", "futures", futures_symbol, ts_field="source_ts_ms", overwrite=False)
    counts["funding:SOLUSDT"] = len(funding)
    counts["files:funding:SOLUSDT"] = len(funding_files)
    counts["rows_written:funding:SOLUSDT"] = int(sum(int(f.get("rows", 0) or 0) for f in funding_files))
    counts["checksums:funding:SOLUSDT"] = len({f.get('checksum_sha256') for f in funding_files})
    coverage["funding"] = _coverage_ratio_rows(funding, start_ms, end_ms, ts_field="source_ts_ms")

    # Historical backfill must not fabricate one-off OI/basis snapshots.
    # Persisting only true sampled time-series is allowed.
    counts["open_interest:SOLUSDT"] = 0
    counts["basis:SOLUSDT"] = 0

    # explicit source coverage diagnostics persisted for doctor/feature gating
    counts["coverage:agg_trades"] = coverage["agg_trades"]
    counts["coverage:book_ticker"] = coverage["book_ticker"]
    counts["coverage:open_interest"] = coverage["open_interest"]
    counts["coverage:basis"] = coverage["basis"]
    counts["coverage:funding"] = coverage["funding"]

    cov_payload = {
        "schema_version": SCHEMA_VERSION,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "coverage": coverage,
        "notes": {
            "book_ticker": "historical book_ticker sampling not available in this backfill path",
            "open_interest": "one-off snapshot disabled for historical backfills",
            "basis": "one-off basis snapshot disabled for historical backfills",
        },
    }
    cov_path = Path(cfg.out_dir) / "source_coverage.json"
    cov_path.parent.mkdir(parents=True, exist_ok=True)
    cov_path.write_text(json.dumps(cov_payload, indent=2), encoding="utf-8")

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
