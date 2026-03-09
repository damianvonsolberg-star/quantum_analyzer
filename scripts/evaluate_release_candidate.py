#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantum_analyzer.monitoring.release_gates import evaluate_release_gates


def _load_top_candidate(explorer_root: Path) -> dict:
    pq = explorer_root / "leaderboard.parquet"
    if not pq.exists():
        return {}
    df = pd.read_parquet(pq)
    if df.empty:
        return {}
    return dict(df.sort_values("leaderboard_rank", ascending=True).iloc[0].to_dict())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--explorer-root", default="artifacts/explorer")
    ap.add_argument("--discovery-root", default="artifacts/discovery")
    ap.add_argument("--promoted-root", default="artifacts/promoted")
    args = ap.parse_args()

    explorer_root = Path(args.explorer_root)
    promoted_root = Path(args.promoted_root)
    promoted_root.mkdir(parents=True, exist_ok=True)

    cand = _load_top_candidate(explorer_root)
    if not cand:
        gate = {
            "passed": False,
            "overall_state": "NO_EDGE",
            "failures": ["no_candidate"],
            "metrics": {},
        }
    else:
        gate = evaluate_release_gates(cand).to_dict()

    (promoted_root / "release_gate_report.json").write_text(json.dumps(gate, indent=2), encoding="utf-8")

    # benchmark comparison artifact
    rows = [{
        "benchmark": "HOLD_WAIT",
        "candidate_return_pct": float(cand.get("return_pct", 0.0) if cand else 0.0),
        "benchmark_return_pct": float(cand.get("baseline_wait_return_pct", 0.0) if cand else 0.0),
        "lift_pct": float((cand.get("return_pct", 0.0) - cand.get("baseline_wait_return_pct", 0.0)) if cand else 0.0),
    }, {
        "benchmark": "ALWAYS_LONG",
        "candidate_return_pct": float(cand.get("return_pct", 0.0) if cand else 0.0),
        "benchmark_return_pct": float(cand.get("baseline_always_long_return_pct", 0.0) if cand else 0.0),
        "lift_pct": float((cand.get("return_pct", 0.0) - cand.get("baseline_always_long_return_pct", 0.0)) if cand else 0.0),
    }, {
        "benchmark": "BTC_FOLLOW",
        "candidate_return_pct": float(cand.get("return_pct", 0.0) if cand else 0.0),
        "benchmark_return_pct": float(cand.get("baseline_btc_follow_return_pct", 0.0) if cand else 0.0),
        "lift_pct": float((cand.get("return_pct", 0.0) - cand.get("baseline_btc_follow_return_pct", 0.0)) if cand else 0.0),
    }, {
        "benchmark": "RANDOM_ACTION",
        "candidate_return_pct": float(cand.get("return_pct", 0.0) if cand else 0.0),
        "benchmark_return_pct": 0.0,
        "lift_pct": float(cand.get("return_pct", 0.0) if cand else 0.0),
    }, {
        "benchmark": "MOMENTUM_SIMPLE",
        "candidate_return_pct": float(cand.get("return_pct", 0.0) if cand else 0.0),
        "benchmark_return_pct": 0.0,
        "lift_pct": float(cand.get("return_pct", 0.0) if cand else 0.0),
    }, {
        "benchmark": "MEAN_REVERSION_SIMPLE",
        "candidate_return_pct": float(cand.get("return_pct", 0.0) if cand else 0.0),
        "benchmark_return_pct": 0.0,
        "lift_pct": float(cand.get("return_pct", 0.0) if cand else 0.0),
    }]
    pd.DataFrame(rows).to_csv(promoted_root / "benchmark_comparison.csv", index=False)

    reliability = {
        "overall_state": gate["overall_state"],
        "passed": gate["passed"],
        "failures": gate["failures"],
        "calibration_reliability": float((gate.get("metrics") or {}).get("calibration_reliability", 0.0)),
        "action_quality": float((gate.get("metrics") or {}).get("action_quality", 0.0)),
    }
    (promoted_root / "signal_reliability_report.json").write_text(json.dumps(reliability, indent=2), encoding="utf-8")
    print(json.dumps(gate, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
