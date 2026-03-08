from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
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
            start = int(params["startTime"])
            end = int(params["endTime"])
            out = []
            # for large historical ranges in backfill tests, return sparse small batch
            # so ingest completes quickly.
            if (end - start) > 100_000:
                for i in range(10):
                    t = start + i * 60_000
                    if t > end:
                        break
                    out.append({"a": t, "p": "10", "q": "2", "f": t, "l": t, "T": t, "m": False})
                return out

            # for pagination test ranges, emit up to 1000 records/call.
            t = start
            while t <= end and len(out) < 1000:
                out.append({"a": t, "p": "10", "q": "2", "f": t, "l": t, "T": t, "m": False})
                t += 1
            return out
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


def test_ingest_aggtrades_paginates_full_range() -> None:
    cfg = BinanceIngestConfig(out_dir="/tmp/unused", retries=1)
    c = FakeClient(cfg)
    rows = c.fetch_agg_trades("SOLUSDT", 0, 2500)
    assert len(rows) >= 2501
    assert rows[0]["trade_time_ms"] == 0
    assert rows[-1]["trade_time_ms"] >= 2500


def test_historical_book_ticker_not_fabricated_from_single_snapshot(tmp_path: Path, monkeypatch) -> None:
    pytest.importorskip("pyarrow")
    cfg = BinanceIngestConfig(out_dir=str(tmp_path), retries=1)

    monkeypatch.setattr("quantum_analyzer.data.ingest_binance.BinanceClient", FakeClient)

    start = datetime.now(timezone.utc) - timedelta(days=1)
    end = datetime.now(timezone.utc)
    counts = backfill_range(cfg, start, end, ["SOLUSDT"], ["1m", "4h"])

    assert counts["klines:SOLUSDT:1m"] > 0
    assert counts["klines:SOLUSDT:4h"] > 0
    assert counts["book_ticker:SOLUSDT"] == 0
    assert counts["open_interest:SOLUSDT"] == 0
    assert counts["basis:SOLUSDT"] == 0

    files = list(tmp_path.rglob("*.parquet"))
    assert files, "Expected parquet files to be written"
    assert not any("dataset=book_ticker" in str(p) for p in files)
    assert not any("dataset=open_interest" in str(p) for p in files)
    assert not any("dataset=basis" in str(p) for p in files)
    assert (tmp_path / "source_coverage.json").exists()


def test_historical_open_interest_not_forward_filled_without_coverage(tmp_path: Path, monkeypatch) -> None:
    pytest.importorskip("pyarrow")
    cfg = BinanceIngestConfig(out_dir=str(tmp_path), retries=1)
    monkeypatch.setattr("quantum_analyzer.data.ingest_binance.BinanceClient", FakeClient)

    start = datetime.now(timezone.utc) - timedelta(days=2)
    end = datetime.now(timezone.utc)
    counts = backfill_range(cfg, start, end, ["SOLUSDT"], ["1h"])

    assert counts["open_interest:SOLUSDT"] == 0
    assert counts["coverage:open_interest"] == 0.0
    assert counts["coverage:basis"] == 0.0


def test_append_safe_write_creates_new_parts_and_manifest(tmp_path: Path, monkeypatch) -> None:
    pytest.importorskip("pyarrow")
    cfg = BinanceIngestConfig(out_dir=str(tmp_path), retries=1)
    monkeypatch.setattr("quantum_analyzer.data.ingest_binance.BinanceClient", FakeClient)

    start = datetime.now(timezone.utc) - timedelta(hours=6)
    end = datetime.now(timezone.utc)
    c1 = backfill_range(cfg, start, end, ["SOLUSDT"], ["1m"])
    c2 = backfill_range(cfg, start, end, ["SOLUSDT"], ["1m"])

    # repeated run should append new parts, not overwrite silently
    k_parts = list((tmp_path / "klines").rglob("*.parquet"))
    assert len(k_parts) >= 2
    assert c1["files:klines:SOLUSDT:1m"] >= 1
    assert c2["files:klines:SOLUSDT:1m"] >= 1

    # per-partition manifest/watermark should exist
    manifests = list((tmp_path / "klines").rglob("_manifest.json"))
    assert manifests
    m = json.loads(manifests[0].read_text())
    assert "entries" in m and m["entries"]
    assert "latest_watermark" in m

    # source-level coverage manifest remains present
    cov_path = tmp_path / "source_coverage.json"
    assert cov_path.exists()
    cov = json.loads(cov_path.read_text())
    assert cov["coverage"]["book_ticker"] == 0.0
    assert cov["coverage"]["open_interest"] == 0.0
    assert cov["coverage"]["basis"] == 0.0
