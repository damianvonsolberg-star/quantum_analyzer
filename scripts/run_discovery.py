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
from quantum_analyzer.discovery.search_evolutionary import run_evolutionary
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
    novelty_rows = []

    wf = WalkForwardConfig(train_bars=24 * 30, test_bars=24 * 5, purge_bars=6, embargo_bars=6)
    bt = BacktestConfig()

    for i, g in enumerate(genomes):
        ev = evaluate_discovered_candidate(g, feats, close, prior=prior)
        cid = f"disc_{i:05d}"
        row = {"candidate_id": cid, "genome": g, **ev}
        discovered.append(row)
        novelty_rows.append({"candidate_id": cid, "novelty": ev["novelty"], "complexity_penalty": ev["complexity_penalty"]})

        # integrate in same evaluator universe
        cand = build_candidate(cid, "discovery_genome", {"genome": g}, "full_stack", 36, "all")
        _ = evaluate_candidate(features=feats, close=close, candidate=cand, walkforward=wf, backtest=bt)

        if ev["novelty"] >= float(cfg.get("novelty_min_distance", 0.15)) and ev["cost_adjusted_value"] > 0 and ev["sample_support"] >= int(cfg.get("min_oos_support", 40)):
            surviving.append(row)
            prior.append(g)
        else:
            rejected.append(row)

    out = Path(args.out_root)
    out.mkdir(parents=True, exist_ok=True)
    (out / "discovered_signals.json").write_text(json.dumps(discovered, indent=2, default=str), encoding="utf-8")
    (out / "surviving_signals.json").write_text(json.dumps(surviving, indent=2, default=str), encoding="utf-8")
    (out / "rejected_signals.json").write_text(json.dumps(rejected, indent=2, default=str), encoding="utf-8")
    pd.DataFrame(novelty_rows).to_csv(out / "novelty_scores.csv", index=False)

    print(json.dumps({"discovered": len(discovered), "surviving": len(surviving), "rejected": len(rejected), "snapshot": sid}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
