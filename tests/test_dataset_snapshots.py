from __future__ import annotations

from pathlib import Path

import pandas as pd

from quantum_analyzer.data.schemas import KLINE_INTERVALS
from quantum_analyzer.datasets.snapshots import build_snapshot


def _write_klines(root: Path, symbol: str, timeframe: str = "1h") -> None:
    p = root / "klines" / "market=spot" / f"symbol={symbol}" / f"timeframe={timeframe}" / "date=2026-03-08"
    p.mkdir(parents=True, exist_ok=True)
    base = 1_700_000_000_000
    df = pd.DataFrame(
        {
            "open_time_ms": [base + i * 3_600_000 for i in range(10)],
            "close": [80 + i for i in range(10)],
            "high": [81 + i for i in range(10)],
            "low": [79 + i for i in range(10)],
        }
    )
    df.to_parquet(p / "part-000.parquet", index=False)


def test_timeframe_validation_consistency():
    assert "1d" in KLINE_INTERVALS
    assert "1w" in KLINE_INTERVALS


def test_snapshot_id_is_deterministic(tmp_path: Path):
    data_root = tmp_path / "data"
    _write_klines(data_root, "SOLUSDC")
    _write_klines(data_root, "BTCUSDC")

    s1 = build_snapshot(data_root, tmp_path / "artifacts")
    s2 = build_snapshot(data_root, tmp_path / "artifacts")
    assert s1.snapshot_id == s2.snapshot_id
    assert s1.payload["symbols"]["primary_trading_symbol"] == "SOLUSDC"
