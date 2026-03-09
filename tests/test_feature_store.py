from __future__ import annotations

from pathlib import Path

import pandas as pd

from quantum_analyzer.datasets.snapshots import build_snapshot
import json

from quantum_analyzer.features.feature_store import build_feature_snapshot, load_feature_snapshot


def _write_klines(root: Path, symbol: str, timeframe: str = "1h") -> None:
    p = root / "klines" / "market=spot" / f"symbol={symbol}" / f"timeframe={timeframe}" / "date=2026-03-08"
    p.mkdir(parents=True, exist_ok=True)
    base = 1_700_000_000_000
    df = pd.DataFrame(
        {
            "open_time_ms": [base + i * 3_600_000 for i in range(200)],
            "close": [80 + 0.1 * i for i in range(200)],
            "high": [80.5 + 0.1 * i for i in range(200)],
            "low": [79.5 + 0.1 * i for i in range(200)],
        }
    )
    df.to_parquet(p / "part-000.parquet", index=False)


def _write_empty(root: Path, dataset: str):
    p = root / dataset / "market=spot" / "symbol=SOLUSDC" / "date=2026-03-08"
    p.mkdir(parents=True, exist_ok=True)
    pd.DataFrame().to_parquet(p / "part-000.parquet", index=False)


def test_feature_store_writes_and_loads(tmp_path: Path):
    data_root = tmp_path / "data"
    _write_klines(data_root, "SOLUSDC")
    _write_klines(data_root, "BTCUSDC")
    # optional datasets exist but empty
    _write_empty(data_root, "agg_trades")
    _write_empty(data_root, "book_ticker")

    snap = build_snapshot(data_root, tmp_path / "artifacts")
    fs = build_feature_snapshot(data_root=data_root, snapshot_manifest=snap.payload, out_root=tmp_path / "features")
    assert fs.features_path.exists()
    df = load_feature_snapshot(tmp_path / "features", snap.snapshot_id)
    assert not df.empty
    assert "micro_range_pos_24h" in df.columns

    manifest = json.loads(fs.manifest_path.read_text(encoding="utf-8"))
    assert manifest["symbols"]["trading_symbol"] == "SOLUSDC"
    assert manifest["symbols"]["price_source_symbol"] == "SOLUSDT"
    assert "registry_version_hash" in manifest
    assert "subsets_version_hash" in manifest
