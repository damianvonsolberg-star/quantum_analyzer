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

from quantum_analyzer.discovery.evaluate import evaluate_discovered_candidate
from quantum_analyzer.discovery.generator import generate_bruteforce, generate_random
from quantum_analyzer.discovery.genealogy import build_genealogy_entry
from quantum_analyzer.discovery.meta_research import write_feature_importance_drift, write_signal_decay_monitor
from quantum_analyzer.discovery.report import write_discovery_report
from quantum_analyzer.discovery.search_evolutionary import run_evolutionary
from quantum_analyzer.discovery.survival import attach_survival_fields
from quantum_analyzer.discovery.transforms import enrich_time_structure
from quantum_analyzer.experiments.evaluator import build_candidate, evaluate_candidate
from quantum_analyzer.backtest.walkforward import WalkForwardConfig
from quantum_analyzer.backtest.engine import BacktestConfig


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
    ap.add_argument("--config", default="config/discovery/discovery_daily.yaml")
    ap.add_argument("--snapshot", default="latest")
    ap.add_argument("--features", default="artifacts/features")
    ap.add_argument("--out-root", default="artifacts/discovery")
    args = ap.parse_args()

    cfg = _load_cfg(args.config).get("discovery", {})
    feature_root = Path(args.features)
    sid = args.snapshot
    if sid == "latest":
        # resolve latest by mtime
        dirs = [p for p in feature_root.iterdir() if p.is_dir()]
        if not dirs:
            raise RuntimeError("no feature snapshots found")
        sid = sorted(dirs, key=lambda p: p.stat().st_mtime)[-1].name

    feat_path = feature_root / sid / "features.parquet"
    if not feat_path.exists():
        raise RuntimeError(f"missing features: {feat_path}")

    feats = pd.read_parquet(feat_path)
    ts_col = "ts" if "ts" in feats.columns else feats.columns[0]
    feats[ts_col] = pd.to_datetime(feats[ts_col], errors="coerce", utc=True)
    feats = feats.set_index(ts_col).sort_index()
    feats = enrich_time_structure(feats)
    close = feats.get("close")
    if close is None:
        close = pd.Series(0.0, index=feats.index)

    feature_names = [c for c in feats.columns if pd.api.types.is_numeric_dtype(feats[c])]

    brut = generate_bruteforce(feature_names, max_terms=int(cfg.get("max_rule_terms", 4))) if "bruteforce" in cfg.get("methods", []) else []
    rnd = generate_random(feature_names, n=int(cfg.get("random_n", 40)), seed=7) if "random" in cfg.get("methods", []) else []
    evo = run_evolutionary(rnd[:20], feature_names, generations=int(cfg.get("evolution_generations", 2))) if "evolutionary" in cfg.get("methods", []) else []

    genomes = brut + rnd + evo
    prior: list[dict] = []
    discovered = []
    surviving = []
    rejected = []
    genealogy_rows = []
    novelty_rows = []

    wf = WalkForwardConfig(train_bars=24 * 30, test_bars=24 * 5, purge_bars=6, embargo_bars=6)
    bt = BacktestConfig()

    for i, g in enumerate(genomes):
        ev = evaluate_discovered_candidate(g, feats, close, prior=prior)
        cid = f"disc_{i:05d}"

        # integrate in same evaluator universe (baseline-comparable)
        cand = build_candidate(cid, "discovery_genome", {"genome": g}, "full_stack", 36, "all")
        bt_res = evaluate_candidate(features=feats, close=close, candidate=cand, walkforward=wf, backtest=bt)

        row = {
            "candidate_id": cid,
            "candidate_family": "discovery_genome",
            "feature_subset": "full_stack",
            "regime_slice": "all",
            "horizon": 36,
            "genome": g,
            "expectancy": float(ev.get("expectancy", 0.0) or 0.0),
            "novelty": float(ev.get("novelty", 0.0) or 0.0),
            "complexity_penalty": float(ev.get("complexity_penalty", 0.0) or 0.0),
            "sample_support": int(ev.get("sample_support", 0) or 0),
            "oos_usefulness": float(ev.get("oos_usefulness", 0.0) or 0.0),
            "neighbor_consistency": float(ev.get("neighbor_consistency", 0.0) or 0.0),
            "cross_window_repeatability": float(ev.get("cross_window_repeatability", 0.0) or 0.0),
            "regime_specialization": float(ev.get("regime_specialization", 0.0) or 0.0),
            "redundancy": float(ev.get("redundancy", 0.0) or 0.0),
            "cost_adjusted_value": float(ev.get("cost_adjusted_value", 0.0) or 0.0),
            "robustness_score": float(bt_res.summary.get("return_pct", 0.0) or 0.0),
            "interpretability_score": float(max(0.0, 1.0 - float(ev.get("complexity_penalty", 0.0) or 0.0))),
        }
        row = attach_survival_fields(row)
        discovered.append(row)
        novelty_rows.append({"candidate_id": cid, "novelty": row["novelty"], "complexity_penalty": row["complexity_penalty"]})

        genealogy_rows.append(
            build_genealogy_entry(
                candidate_id=cid,
                genome=g,
                parent_features=[k for k in [g.get("feature"), g.get("a"), g.get("b")] if isinstance(k, str)] + [t.get("feature") for t in (g.get("terms") or []) if isinstance(t, dict) and isinstance(t.get("feature"), str)],
                method=("evolutionary" if i >= (len(brut) + len(rnd)) else ("random" if i >= len(brut) else "bruteforce")),
                transforms=["time_structure_enrichment"],
                params={k: v for k, v in g.items() if k not in {"rules", "terms"}},
                timeframe="1h",
                validation={
                    "expectancy": row["expectancy"],
                    "sample_support": row["sample_support"],
                    "novelty": row["novelty"],
                },
                robustness_score=float(row["robustness_score"]),
                interpretability_score=float(row["interpretability_score"]),
                survival_status=str(row["survival_status"]),
                rejection_reason=row.get("rejection_reason"),
            )
        )

        if row["survival_status"] == "survived" and row["novelty"] >= float(cfg.get("novelty_min_distance", 0.15)) and row["cost_adjusted_value"] > 0 and row["sample_support"] >= int(cfg.get("min_oos_support", 40)):
            surviving.append(row)
            prior.append(g)
        else:
            rejected.append(row)

    out = Path(args.out_root)
    out.mkdir(parents=True, exist_ok=True)
    (out / "discovered_signals.json").write_text(json.dumps(discovered, indent=2, default=str), encoding="utf-8")
    (out / "surviving_signals.json").write_text(json.dumps(surviving, indent=2, default=str), encoding="utf-8")
    (out / "rejected_signals.json").write_text(json.dumps(rejected, indent=2, default=str), encoding="utf-8")
    (out / "signal_genealogy.json").write_text(json.dumps(genealogy_rows, indent=2, default=str), encoding="utf-8")
    pd.DataFrame(novelty_rows).to_csv(out / "novelty_scores.csv", index=False)

    write_feature_importance_drift(genealogy_rows, out)
    write_signal_decay_monitor(genealogy_rows, out)
    write_discovery_report(genealogy_rows, out)

    # integrate survivors into experiment universe for downstream leaderboard/promotion ingestion
    surv_df = pd.DataFrame(surviving)
    surv_df.to_parquet(out / "surviving_signals.parquet", index=False)

    print(json.dumps({"discovered": len(discovered), "surviving": len(surviving), "rejected": len(rejected), "snapshot": sid}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
