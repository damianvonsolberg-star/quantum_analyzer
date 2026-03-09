from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from quantum_analyzer.data.schemas import SCHEMA_VERSION
from .catalog import load_dataset_frame, resolve_context_symbol


@dataclass
class DatasetSnapshot:
    snapshot_id: str
    manifest_path: Path
    payload: dict[str, Any]


def _hash_obj(obj: dict[str, Any]) -> str:
    raw = json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


INTERVAL_MS = {
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
    "1w": 604_800_000,
}


def _missing_interval_ratio(ts: pd.Series, interval_ms: int) -> float:
    if ts.empty:
        return 1.0
    t = pd.to_datetime(ts, unit="ms", utc=True, errors="coerce").dropna().sort_values().unique()
    if len(t) < 2:
        return 1.0
    diffs = pd.Series(t[1:] - t[:-1]).dt.total_seconds() * 1000
    miss = (diffs > (interval_ms * 1.5)).sum()
    return float(miss / max(len(diffs), 1))


def _collect_manifest_checksums(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for mf in sorted(root.rglob("_manifest.json")):
        try:
            j = json.loads(mf.read_text(encoding="utf-8"))
            entries = j.get("entries", []) if isinstance(j, dict) else []
            checks = sorted(str(e.get("checksum_sha256")) for e in entries if isinstance(e, dict) and e.get("checksum_sha256"))
            key = str(mf.relative_to(root.parent))
            out[key] = hashlib.sha256("|".join(checks).encode("utf-8")).hexdigest()[:16]
        except Exception:
            continue
    return out


def build_snapshot(
    data_root: str | Path,
    out_dir: str | Path,
    *,
    primary_symbol: str = "SOLUSDC",
    price_source_symbol: str = "SOLUSDT",
    btc_context_candidates: list[str] | None = None,
    timeframe: str = "1h",
    min_coverage_ratio: float = 0.0,
    max_gap_ratio: float = 1.0,
    optional_proxies: dict[str, dict[str, Any]] | None = None,
) -> DatasetSnapshot:
    btc_context_candidates = btc_context_candidates or ["BTCUSDC", "BTCUSDT"]

    sol = load_dataset_frame(data_root, "klines", "spot", primary_symbol, timeframe=timeframe)
    if sol.empty:
        raise ValueError(f"No klines for primary symbol {primary_symbol} timeframe={timeframe}")
    btc_symbol = resolve_context_symbol(data_root, btc_context_candidates)
    if not btc_symbol:
        raise ValueError("No BTC context symbol found (tried BTCUSDC/BTCUSDT)")
    btc = load_dataset_frame(data_root, "klines", "spot", btc_symbol, timeframe=timeframe)
    if btc.empty:
        raise ValueError(f"No klines for context symbol {btc_symbol} timeframe={timeframe}")

    # quality checks
    sol_dups = int(sol.duplicated(subset=["open_time_ms"]).sum()) if "open_time_ms" in sol.columns else 0
    btc_dups = int(btc.duplicated(subset=["open_time_ms"]).sum()) if "open_time_ms" in btc.columns else 0
    interval_ms = INTERVAL_MS.get(timeframe)
    if interval_ms is None:
        raise ValueError(f"Unsupported snapshot timeframe: {timeframe}")
    sol_missing_ratio = _missing_interval_ratio(sol["open_time_ms"], interval_ms)
    btc_missing_ratio = _missing_interval_ratio(btc["open_time_ms"], interval_ms)

    aligned_bars = int(len(set(sol["open_time_ms"]).intersection(set(btc["open_time_ms"]))))
    coverage_ratio = float(aligned_bars / max(min(len(sol), len(btc)), 1))

    quality = {
        "sol_duplicates": sol_dups,
        "btc_duplicates": btc_dups,
        "sol_missing_interval_ratio": sol_missing_ratio,
        "btc_missing_interval_ratio": btc_missing_ratio,
        "aligned_bars": aligned_bars,
        "coverage_ratio": coverage_ratio,
        "quality_ok": bool(
            coverage_ratio >= float(min_coverage_ratio)
            and sol_missing_ratio <= float(max_gap_ratio)
            and btc_missing_ratio <= float(max_gap_ratio)
            and sol_dups == 0
            and btc_dups == 0
        ),
        "thresholds": {
            "min_coverage_ratio": float(min_coverage_ratio),
            "max_gap_ratio": float(max_gap_ratio),
        },
    }

    proxy_cfg = optional_proxies or {}
    proxies = {
        "gold": {"enabled": bool(proxy_cfg.get("gold", {}).get("enabled", False)), "available": False, "quality_flag": "not_loaded"},
        "dxy": {"enabled": bool(proxy_cfg.get("dxy", {}).get("enabled", False)), "available": False, "quality_flag": "not_loaded"},
        "btc_dominance": {"enabled": bool(proxy_cfg.get("btc_dominance", {}).get("enabled", False)), "available": False, "quality_flag": "not_loaded"},
        "total_crypto_market": {"enabled": bool(proxy_cfg.get("total_crypto_market", {}).get("enabled", False)), "available": False, "quality_flag": "not_loaded"},
    }

    data_root_path = Path(data_root).resolve()
    checksum_inputs = {
        "sol_klines": _collect_manifest_checksums(data_root_path / "klines" / "market=spot" / f"symbol={primary_symbol}" / f"timeframe={timeframe}"),
        "btc_klines": _collect_manifest_checksums(data_root_path / "klines" / "market=spot" / f"symbol={btc_symbol}" / f"timeframe={timeframe}"),
    }

    payload = {
        "schema_version": SCHEMA_VERSION,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "data_root": str(data_root_path),
        "symbols": {
            "primary_trading_symbol": primary_symbol,
            "price_source_symbol": price_source_symbol,
            "context_symbol": btc_symbol,
            "quote_currency": "USDC",
        },
        "timeframe": timeframe,
        "quality": quality,
        "coverage": {
            "sol_rows": int(len(sol)),
            "btc_rows": int(len(btc)),
            "sol_min_ts_ms": int(sol["open_time_ms"].min()),
            "sol_max_ts_ms": int(sol["open_time_ms"].max()),
            "btc_min_ts_ms": int(btc["open_time_ms"].min()),
            "btc_max_ts_ms": int(btc["open_time_ms"].max()),
        },
        "checksum_inputs": checksum_inputs,
        "proxies": proxies,
    }

    if not quality["quality_ok"]:
        raise ValueError(
            "snapshot quality failed: "
            f"coverage_ratio={coverage_ratio:.3f}, "
            f"sol_missing={sol_missing_ratio:.3f}, btc_missing={btc_missing_ratio:.3f}, "
            f"sol_dups={sol_dups}, btc_dups={btc_dups}"
        )

    snapshot_id = _hash_obj({k: v for k, v in payload.items() if k != "built_at"})
    payload["snapshot_id"] = snapshot_id

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    mpath = out / f"snapshot_{snapshot_id}.json"
    mpath.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    (out / "latest_snapshot.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return DatasetSnapshot(snapshot_id=snapshot_id, manifest_path=mpath, payload=payload)


def load_snapshot_manifest(out_dir: str | Path, snapshot: str = "latest") -> DatasetSnapshot:
    root = Path(out_dir)
    path = root / "latest_snapshot.json" if snapshot == "latest" else root / f"snapshot_{snapshot}.json"
    if not path.exists():
        raise FileNotFoundError(f"Snapshot manifest not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    sid = str(payload.get("snapshot_id") or "")
    if not sid:
        raise ValueError(f"Snapshot manifest missing snapshot_id: {path}")
    return DatasetSnapshot(snapshot_id=sid, manifest_path=path, payload=payload)
