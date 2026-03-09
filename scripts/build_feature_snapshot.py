#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantum_analyzer.datasets.snapshots import build_snapshot, load_snapshot_manifest
from quantum_analyzer.features.feature_store import build_feature_snapshot


def _load_cfg(path: str) -> dict:
    p = Path(path)
    txt = p.read_text(encoding="utf-8")
    if p.suffix == ".json":
        return json.loads(txt)
    try:
        import yaml  # type: ignore

        return yaml.safe_load(txt) or {}
    except ModuleNotFoundError as e:
        raise RuntimeError("YAML parsing requires pyyaml. Use a .json config or install pyyaml.") from e


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--snapshot", default="latest")
    ap.add_argument("--build-snapshot", action="store_true", help="Build a new snapshot before feature build")
    args = ap.parse_args()

    cfg = _load_cfg(args.config).get("research", {})
    timeframe = cfg.get("explorer_timeframe", "1h")

    snapshot_dir = "artifacts/data_quality"
    if args.build_snapshot:
        snap = build_snapshot(
            data_root=cfg.get("data_root", "data/binance"),
            out_dir=snapshot_dir,
            primary_symbol=cfg.get("primary_symbol", "SOLUSDC"),
            price_source_symbol=cfg.get("price_source_symbol", "SOLUSDT"),
            btc_context_candidates=cfg.get("context_symbols", ["BTCUSDC", "BTCUSDT"]),
            timeframe=timeframe,
            min_coverage_ratio=float(cfg.get("snapshot", {}).get("min_coverage_ratio", 0.0)),
            max_gap_ratio=float(cfg.get("snapshot", {}).get("max_gap_ratio", 1.0)),
            optional_proxies=cfg.get("optional_proxies", {}),
        )
    else:
        snap = load_snapshot_manifest(snapshot_dir, args.snapshot)
    feat = build_feature_snapshot(
        data_root=cfg.get("data_root", "data/binance"),
        snapshot_manifest=snap.payload,
        out_root="artifacts/features",
        timeframe=timeframe,
    )

    print(json.dumps({"snapshot_id": snap.snapshot_id, "snapshot_manifest": str(snap.manifest_path), "feature_manifest": str(feat.manifest_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
