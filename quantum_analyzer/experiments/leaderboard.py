from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def write_leaderboard(registry_path: str | Path, out_root: str | Path, top_n: int = 50) -> tuple[Path, Path]:
    reg = pd.read_parquet(registry_path)
    reg_ok = reg[reg["hard_gate_pass"] == True].copy()  # noqa: E712
    score_col = "robust_score" if "robust_score" in reg_ok.columns else "score"
    reg_ok = reg_ok.sort_values(score_col, ascending=False)
    top = reg_ok.head(top_n).copy()
    top["leaderboard_rank"] = range(1, len(top) + 1)

    out = Path(out_root)
    out.mkdir(parents=True, exist_ok=True)
    pq = out / "leaderboard.parquet"
    js = out / "leaderboard.json"
    top.to_parquet(pq, index=False)
    payload = {
        "ranked": top.to_dict(orient="records"),
        "by_family": (
            top.groupby("candidate_family")[score_col].mean().sort_values(ascending=False).to_dict()
            if "candidate_family" in top.columns
            else {}
        ),
    }
    js.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return pq, js
