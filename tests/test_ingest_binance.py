from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from quantum_analyzer.data.ingest_binance import (
    BinanceClient,
    BinanceIngestConfig,
    INTERVAL_MS,
    backfill_range,
)
from quantum_analyzer.data.store import partition_path


class FakeClient(BinanceClient):
    def _get_json(self, base_url: str, path: str, params: dict):  # type: ignore[override]
        if path.endswith("/klines"):
            start = int(params["startTime"])
            interval = params["interval"]
            step = INTERVAL_MS[interval]
            rows = []
            for i in range(3):
                t = start + i * step
                rows.append([t, "1", "2", "0.5", "1.5", "10", t + step - 1, "15", 5, "4", "6", "0"])
            return rows
        if path.endswith("/ticker/bookTicker"):
            return {"bidPrice": "10", "bidQty": "1", "askPrice": "10.2", "askQty": "1.1"}
        if path.endswith("/aggTrades"):
            t = int(params["startTime"])
            return [{"a": 1, "p": "10", "q": "2", "f": 1, "l": 2, "T": t, "m": False}]
        if path.endswith("/fundingRate"):
            t = int(params["startTime"])
            return [{"fundingTime": t, "fundingRate": "0.0001", "markPrice": "10.1"}]
        if path.endswith("/openInterest"):
            return {"openInterest": "1000"}
        raise AssertionError(f"Unexpected endpoint {path}")


def test_partition_path() -> None:
    p = partition_path("/tmp/x", "klines", "spot", "SOLUSDT", "2026-03-07", "1m")
    assert "dataset" not in str(p)  # ensure format not duplicated
    assert "timeframe=1m" in str(p)


def test_backfill_writes_partitions(tmp_path: Path, monkeypatch) -> None:
    pytest.importorskip("pyarrow")
    cfg = BinanceIngestConfig(out_dir=str(tmp_path), retries=1)

    def fake_client_init(self, _cfg):
        self.cfg = _cfg

    monkeypatch.setattr("quantum_analyzer.data.ingest_binance.BinanceClient", FakeClient)

    start = datetime.now(timezone.utc) - timedelta(days=1)
    end = datetime.now(timezone.utc)
    counts = backfill_range(cfg, start, end, ["SOLUSDT"], ["1m", "4h"])

    assert counts["klines:SOLUSDT:1m"] > 0
    assert counts["klines:SOLUSDT:4h"] > 0

    files = list(tmp_path.rglob("*.parquet"))
    assert files, "Expected parquet files to be written"
