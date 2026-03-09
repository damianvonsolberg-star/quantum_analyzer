#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantum_analyzer.data.ingest_binance import BinanceIngestConfig, backfill_range


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
    args = ap.parse_args()

    cfg = _load_cfg(args.config).get("research", {})
    out = cfg.get("data_root", "data/binance")
    days = float(cfg.get("backfill_days_cycle", cfg.get("backfill_days", 30)))
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    c = BinanceIngestConfig(out_dir=out)
    counts = backfill_range(
        c,
        start,
        end,
        symbols=cfg.get("symbols_cycle", cfg.get("symbols", ["SOLUSDC", "SOLUSDT", "BTCUSDC", "BTCUSDT"])),
        intervals=cfg.get("timeframes_cycle", cfg.get("timeframes", ["5m", "15m", "1h", "4h", "1d", "1w"])),
    )
    print(json.dumps(counts, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
