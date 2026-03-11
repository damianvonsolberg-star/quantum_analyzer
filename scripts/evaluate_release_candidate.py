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


def _load_registry(explorer_root: Path) -> pd.DataFrame:
    p = explorer_root / "registry.parquet"
    if p.exists():
        try:
            return pd.read_parquet(p)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def _measured_baseline_returns(cand: dict, registry: pd.DataFrame) -> dict[str, dict[str, float | str | None]]:
    out: dict[str, dict[str, float | str | None]] = {
        "HOLD_WAIT": {"return": None, "source_type": "missing"},
        "ALWAYS_LONG": {"return": None, "source_type": "missing"},
        "BTC_FOLLOW": {"return": None, "source_type": "missing"},
        "RANDOM_ACTION": {"return": None, "source_type": "missing"},
        "MOMENTUM_SIMPLE": {"return": None, "source_type": "missing"},
        "MEAN_REVERSION_SIMPLE": {"return": None, "source_type": "missing"},
    }
    if cand:
        if "baseline_wait_return_pct" in cand:
            out["HOLD_WAIT"] = {"return": float(cand.get("baseline_wait_return_pct") or 0.0), "source_type": "measured_run"}
        if "baseline_always_long_return_pct" in cand:
            out["ALWAYS_LONG"] = {"return": float(cand.get("baseline_always_long_return_pct") or 0.0), "source_type": "measured_run"}
        if "baseline_btc_follow_return_pct" in cand:
            out["BTC_FOLLOW"] = {"return": float(cand.get("baseline_btc_follow_return_pct") or 0.0), "source_type": "measured_run"}
        if "baseline_random_action_return_pct" in cand:
            out["RANDOM_ACTION"] = {"return": float(cand.get("baseline_random_action_return_pct") or 0.0), "source_type": "measured_run"}
        if "baseline_momentum_simple_return_pct" in cand:
            out["MOMENTUM_SIMPLE"] = {"return": float(cand.get("baseline_momentum_simple_return_pct") or 0.0), "source_type": "measured_run"}
        if "baseline_mean_reversion_simple_return_pct" in cand:
            out["MEAN_REVERSION_SIMPLE"] = {"return": float(cand.get("baseline_mean_reversion_simple_return_pct") or 0.0), "source_type": "measured_run"}

    if registry.empty:
        return out

    def _family_mean(name_fragments: list[str]) -> float | None:
        if "candidate_family" not in registry.columns or "return_pct" not in registry.columns:
            return None
        fam = registry["candidate_family"].astype(str).str.lower()
        mask = pd.Series(False, index=registry.index)
        for f in name_fragments:
            mask = mask | fam.str.contains(f)
        sub = registry.loc[mask]
        if sub.empty:
            return None
        return float(sub["return_pct"].astype(float).mean())

    if out["RANDOM_ACTION"]["return"] is None:
        v = _family_mean(["random"])
        if v is not None:
            out["RANDOM_ACTION"] = {"return": v, "source_type": "proxy_family_mean"}
    if out["MOMENTUM_SIMPLE"]["return"] is None:
        v = _family_mean(["momentum", "trend"])
        if v is not None:
            out["MOMENTUM_SIMPLE"] = {"return": v, "source_type": "proxy_family_mean"}
    if out["MEAN_REVERSION_SIMPLE"]["return"] is None:
        v = _family_mean(["mean_reversion", "reversion"])
        if v is not None:
            out["MEAN_REVERSION_SIMPLE"] = {"return": v, "source_type": "proxy_family_mean"}

    return out


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
    registry = _load_registry(explorer_root)
    baselines = _measured_baseline_returns(cand, registry)

    if not cand:
        gate = {
            "passed": False,
            "overall_state": "NO_EDGE",
            "failures": ["no_candidate"],
            "metrics": {},
        }
    else:
        gate = evaluate_release_gates(cand).to_dict()

    required_baselines = ["HOLD_WAIT", "ALWAYS_LONG", "BTC_FOLLOW", "RANDOM_ACTION", "MOMENTUM_SIMPLE", "MEAN_REVERSION_SIMPLE"]
    missing_bench = [k for k in required_baselines if baselines.get(k, {}).get("return") is None]
    proxy_bench = [k for k in required_baselines if baselines.get(k, {}).get("source_type") == "proxy_family_mean"]

    failed_benchmarks: list[str] = []
    if cand:
        cand_ret = float(cand.get("return_pct", 0.0) or 0.0)
        for k in required_baselines:
            bret = baselines.get(k, {}).get("return")
            if bret is not None and cand_ret - float(bret) <= 0.0:
                failed_benchmarks.append(k)

    if missing_bench or failed_benchmarks or proxy_bench:
        gate["passed"] = False
        gate["overall_state"] = "NO_EDGE"
        fails = list(gate.get("failures", []))
        if (missing_bench or proxy_bench) and "benchmark_evidence_incomplete" not in fails:
            fails.append("benchmark_evidence_incomplete")
        if failed_benchmarks and "no_benchmark_outperformance" not in fails:
            fails.append("no_benchmark_outperformance")
        gate["failures"] = fails

    gate["failed_benchmarks"] = failed_benchmarks
    gate["missing_benchmarks"] = missing_bench
    gate["proxy_benchmarks"] = proxy_bench
    if missing_bench or proxy_bench:
        parts = []
        if missing_bench:
            parts.append(f"missing {', '.join(missing_bench)}")
        if proxy_bench:
            parts.append(f"proxy-only {', '.join(proxy_bench)}")
        gate["human_reason"] = "Benchmark evidence incomplete: " + "; ".join(parts)
    elif failed_benchmarks:
        gate["human_reason"] = f"Candidate did not beat: {', '.join(failed_benchmarks)}"
    else:
        gate["human_reason"] = "All required benchmark checks passed"

    (promoted_root / "release_gate_report.json").write_text(json.dumps(gate, indent=2), encoding="utf-8")

    # benchmark comparison artifact
    cand_ret = float(cand.get("return_pct", 0.0) if cand else 0.0)
    rows = []
    for bench in required_baselines:
        b = baselines.get(bench, {})
        bret = b.get("return")
        stype = b.get("source_type", "missing")
        rows.append(
            {
                "benchmark": bench,
                "candidate_return_pct": cand_ret,
                "benchmark_return_pct": (float(bret) if bret is not None else None),
                "lift_pct": (cand_ret - float(bret)) if bret is not None else None,
                "available": bret is not None,
                "source_type": stype,
            }
        )
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
