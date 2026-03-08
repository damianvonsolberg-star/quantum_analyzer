from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def append_registry(root: str | Path, rows: list[dict[str, Any]]) -> Path:
    out = Path(root)
    out.mkdir(parents=True, exist_ok=True)
    reg = out / "registry.parquet"
    df_new = pd.DataFrame(rows)
    if reg.exists():
        df_old = pd.read_parquet(reg)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    if "experiment_id" in df.columns and "completed_at" in df.columns:
        # deterministic dedupe by experiment_id keep last
        df = df.sort_values(["experiment_id", "completed_at"]).drop_duplicates("experiment_id", keep="last")
    df.to_parquet(reg, index=False)
    return reg


def write_manifest(root: str | Path, manifest: dict[str, Any]) -> Path:
    p = Path(root) / "run_manifest.json"
    p.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return p


def write_failures(root: str | Path, failures: list[dict[str, Any]]) -> Path:
    p = Path(root) / "failures.json"
    p.write_text(json.dumps(failures, indent=2), encoding="utf-8")
    return p
