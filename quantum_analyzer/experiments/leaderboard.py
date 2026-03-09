from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def write_leaderboard(registry_path: str | Path, out_root: str | Path, top_n: int = 50) -> tuple[Path, Path]:
    reg = pd.read_parquet(registry_path)
    score_col = "promoted_score" if "promoted_score" in reg.columns else ("robust_score" if "robust_score" in reg.columns else "score")

    reg_ok = reg[reg["hard_gate_pass"] == True].copy()  # noqa: E712
    reg_ok = reg_ok.sort_values(score_col, ascending=False)
    top = reg_ok.head(top_n).copy()
    top["leaderboard_rank"] = range(1, len(top) + 1)

    rejected = reg[reg["hard_gate_pass"] != True].copy()  # noqa: E712
    rej_cols = [c for c in ["experiment_id", "candidate_id", "candidate_family", "hard_gate_failures", score_col] if c in rejected.columns]
    rejected_view = rejected[rej_cols].head(top_n) if rej_cols else rejected.head(top_n)

    out = Path(out_root)
    out.mkdir(parents=True, exist_ok=True)
    pq = out / "leaderboard.parquet"
    js = out / "leaderboard.json"
    top.to_parquet(pq, index=False)
    payload = {
        "ranked": top.to_dict(orient="records"),
        "rejected": rejected_view.to_dict(orient="records"),
        "by_family": (
            top.groupby("candidate_family")[score_col].mean().sort_values(ascending=False).to_dict()
            if "candidate_family" in top.columns and not top.empty
            else {}
        ),
    }
    js.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return pq, js
