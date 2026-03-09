from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import pandas as pd

from quantum_analyzer.datasets.catalog import load_dataset_frame
from quantum_analyzer.features.build_features import build_feature_frame
from quantum_analyzer.features.registry import feature_versions


@dataclass
class FeatureSnapshot:
    snapshot_id: str
    features_path: Path
    manifest_path: Path


def _feature_store_dir(root: str | Path, snapshot_id: str) -> Path:
    return Path(root) / snapshot_id


def build_feature_snapshot(
    *,
    data_root: str | Path,
    snapshot_manifest: dict[str, Any],
    out_root: str | Path,
    timeframe: str = "1h",
) -> FeatureSnapshot:
    sid = str(snapshot_manifest["snapshot_id"])
    fdir = _feature_store_dir(out_root, sid)
    fdir.mkdir(parents=True, exist_ok=True)

    fpath = fdir / "features.parquet"
    mpath = fdir / "feature_manifest.json"

    expected = {
        "snapshot_id": sid,
        "timeframe": timeframe,
        "feature_versions": feature_versions(),
    }

    if mpath.exists() and fpath.exists():
        try:
            old = json.loads(mpath.read_text(encoding="utf-8"))
            if old.get("snapshot_id") == expected["snapshot_id"] and old.get("feature_versions") == expected["feature_versions"]:
                return FeatureSnapshot(snapshot_id=sid, features_path=fpath, manifest_path=mpath)
        except Exception:
            pass

    syms = snapshot_manifest.get("symbols", {})
    primary = syms.get("primary_trading_symbol", "SOLUSDC")
    btc = syms.get("context_symbol", "BTCUSDT")

    sol = load_dataset_frame(data_root, "klines", "spot", primary, timeframe=timeframe)
    btc_df = load_dataset_frame(data_root, "klines", "spot", btc, timeframe=timeframe)
    agg = load_dataset_frame(data_root, "agg_trades", "spot", primary)
    book = load_dataset_frame(data_root, "book_ticker", "spot", primary)
    funding = load_dataset_frame(data_root, "funding", "futures", "SOLUSDT")
    basis = load_dataset_frame(data_root, "basis", "futures", "SOLUSDT")
    oi = load_dataset_frame(data_root, "open_interest", "futures", "SOLUSDT")

    feats = build_feature_frame(sol, btc_df, agg, book, funding, basis, oi)
    feats = feats.reset_index().rename(columns={"index": "ts"})
    feats.to_parquet(fpath, index=False)

    manifest = {
        **expected,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "rows": int(len(feats)),
        "built_from": {
            "primary": primary,
            "context": btc,
        },
    }
    mpath.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return FeatureSnapshot(snapshot_id=sid, features_path=fpath, manifest_path=mpath)


def load_feature_snapshot(out_root: str | Path, snapshot_id: str) -> pd.DataFrame:
    p = _feature_store_dir(out_root, snapshot_id) / "features.parquet"
    if not p.exists():
        raise FileNotFoundError(f"Feature snapshot not found: {p}")
    df = pd.read_parquet(p)
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
        df = df.set_index("ts").sort_index()
    return df
