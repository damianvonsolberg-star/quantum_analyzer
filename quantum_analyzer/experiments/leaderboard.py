from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def write_leaderboard(registry_path: str | Path, out_root: str | Path, top_n: int = 50) -> tuple[Path, Path]:
    reg = pd.read_parquet(registry_path)
    reg_ok = reg[reg["hard_gate_pass"] == True].copy()  # noqa: E712
    reg_ok = reg_ok.sort_values("score", ascending=False)
    top = reg_ok.head(top_n)

    out = Path(out_root)
    out.mkdir(parents=True, exist_ok=True)
    pq = out / "leaderboard.parquet"
    js = out / "leaderboard.json"
    top.to_parquet(pq, index=False)
    js.write_text(json.dumps(top.to_dict(orient="records"), indent=2, default=str), encoding="utf-8")
    return pq, js
