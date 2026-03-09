from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def write_feature_importance_drift(rows: list[dict[str, Any]], out_dir: str | Path) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    records = []
    for r in rows:
        for f in r.get("parent_features", []) or []:
            records.append({"candidate_id": r.get("candidate_id"), "feature": f, "robustness": r.get("robustness_score", 0.0)})
    df = pd.DataFrame(records)
    p = out / "feature_importance_drift.csv"
    if df.empty:
        pd.DataFrame(columns=["candidate_id", "feature", "robustness"]).to_csv(p, index=False)
    else:
        agg = df.groupby("feature", as_index=False)["robustness"].mean().sort_values("robustness", ascending=False)
        agg.to_csv(p, index=False)
    return p


def write_signal_decay_monitor(rows: list[dict[str, Any]], out_dir: str | Path) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["candidate_id", "robustness_score", "decay_flag"])
    else:
        df["decay_flag"] = df["robustness_score"].astype(float) < 0.35
        df = df[["candidate_id", "robustness_score", "survival_status", "decay_flag"]]
    p = out / "signal_decay_monitor.csv"
    df.to_csv(p, index=False)
    return p
