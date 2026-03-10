from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from quantum_analyzer.backtest.engine import BacktestConfig
from quantum_analyzer.experiments.evaluator import build_candidate, evaluate_candidate
from quantum_analyzer.features.subsets import resolve_feature_subset
from quantum_analyzer.backtest.walkforward import WalkForwardConfig
import json

from quantum_analyzer.paths.archetypes import PathTemplate, save_templates_json

from .scoring import score_result
from .specs import ExperimentSpec


def _slice_window(features: pd.DataFrame, close: pd.Series, window_bars: int) -> tuple[pd.DataFrame, pd.Series]:
    f = features.tail(window_bars).copy()
    c = close.reindex(f.index).copy()
    return f, c


def _apply_feature_subset(features_full: pd.DataFrame, subset_name: str) -> pd.DataFrame:
    cols = resolve_feature_subset(subset_name)
    missing = [c for c in cols if c not in features_full.columns]
    if missing:
        raise ValueError(f"feature subset {subset_name} missing columns: {missing}")
    return features_full[cols].copy()


def _apply_regime_slice(features: pd.DataFrame, close: pd.Series, regime_slice: str) -> tuple[pd.DataFrame, pd.Series]:
    if regime_slice == "all":
        mask = pd.Series(True, index=features.index)
    else:
        if "realized_vol_24h" not in features.columns:
            raise ValueError("regime slicing requires realized_vol_24h in selected feature subset")
        vol = features["realized_vol_24h"].astype(float)
        q1 = float(vol.quantile(0.33))
        q2 = float(vol.quantile(0.66))
        if regime_slice == "low_vol":
            mask = vol <= q1
        elif regime_slice == "mid_vol":
            mask = (vol > q1) & (vol <= q2)
        elif regime_slice == "high_vol":
            mask = vol > q2
        else:
            raise ValueError(f"Unknown regime slice: {regime_slice}")
    f = features.loc[mask].copy()
    c = close.reindex(f.index).copy()
    if f.empty or c.empty:
        raise ValueError(f"Regime slice {regime_slice} produced empty sample")
    return f, c


def run_experiments(
    *,
    specs: list[ExperimentSpec],
    snapshot_id: str,
    features_full: pd.DataFrame | None = None,
    features: pd.DataFrame | None = None,
    close: pd.Series,
    templates: list[PathTemplate],
    out_root: str | Path,
    trading_symbol: str = "SOLUSDC",
    price_source_symbol: str = "SOLUSDT",
    timeframe: str = "1h",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    out = Path(out_root)
    out.mkdir(parents=True, exist_ok=True)

    feat_src = features_full if features_full is not None else features
    if feat_src is None:
        raise ValueError("run_experiments requires features_full (or legacy features) input")

    rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for spec in specs:
        exp_id = spec.experiment_id(snapshot_id)
        exp_dir = out / "experiments" / exp_id
        exp_dir.mkdir(parents=True, exist_ok=True)

        try:
            f_all, c_all = _slice_window(feat_src, close, spec.window_bars)
            f_reg, c = _apply_regime_slice(f_all, c_all, spec.regime_slice)
            f = _apply_feature_subset(f_reg, spec.feature_subset)
            wf = WalkForwardConfig(
                train_bars=max(spec.window_bars - spec.test_bars, 10),
                test_bars=spec.test_bars,
                purge_bars=6,
                embargo_bars=6,
            )
            bt = BacktestConfig(
                turnover_cap=float(spec.policy_params.get("turnover_cap", 0.15)),
                round_trip_cost_bps=float(spec.policy_params.get("round_trip_cost_bps", 15.0)),
                initial_equity=1_000_000,
            )

            family = str(spec.policy_params.get("candidate_family", "trend"))
            cparams = dict(spec.policy_params.get("candidate_params", {}))
            cand = build_candidate(
                candidate_id=f"{family}:{spec.feature_subset}:{spec.horizon}:{spec.regime_slice}",
                family=family,
                params=cparams,
                feature_subset=spec.feature_subset,
                horizon=spec.horizon,
                regime_filter=str(spec.policy_params.get("candidate_regime_filter", spec.regime_slice)),
            )
            cr = evaluate_candidate(
                features=f,
                close=c,
                candidate=cand,
                walkforward=wf,
                backtest=bt,
                out_dir=str(exp_dir),
            )

            # Ensure doctor-compatible artifacts in each experiment dir.
            # 1) persist templates for experiment-level doctor checks
            save_templates_json(templates, exp_dir / "templates.json")

            # 2) inject coverage diagnostics into artifact bundle v2 when available
            bundle_path = exp_dir / "artifact_bundle.json"
            if bundle_path.exists():
                bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
                cov = {
                    "agg_trades": float(f["coverage_agg_trades"].iloc[-1]) if "coverage_agg_trades" in f.columns else 1.0,
                    "book_ticker": float(f["coverage_book_ticker"].iloc[-1]) if "coverage_book_ticker" in f.columns else 1.0,
                    "open_interest": float(f["coverage_open_interest"].iloc[-1]) if "coverage_open_interest" in f.columns else 1.0,
                    "basis": float(f["coverage_basis"].iloc[-1]) if "coverage_basis" in f.columns else 1.0,
                }
                bundle["coverage"] = cov
                bundle_path.write_text(json.dumps(bundle, indent=2, default=str), encoding="utf-8")

                # also write explicit source_coverage.json for doctor precedence
                (exp_dir / "source_coverage.json").write_text(
                    json.dumps({"coverage": cov}, indent=2),
                    encoding="utf-8",
                )

            summary_for_score = dict(cr.summary)
            summary_for_score["strict_robustness"] = True
            sb = score_result(summary_for_score, cr.diagnostics)

            rows.append(
                {
                    "experiment_id": exp_id,
                    "snapshot_id": snapshot_id,
                    "trading_symbol": trading_symbol,
                    "price_source_symbol": price_source_symbol,
                    "timeframe": timeframe,
                    "window_bars": spec.window_bars,
                    "test_bars": spec.test_bars,
                    "horizon": spec.horizon,
                    "feature_subset": spec.feature_subset,
                    "regime_slice": spec.regime_slice,
                    "policy_params": spec.policy_params,
                    "artifact_dir": str(exp_dir),
                    "candidate_id": cr.candidate_id,
                    "candidate_family": cr.family,
                    "return_pct": cr.summary.get("return_pct", 0.0),
                    "max_drawdown": float(cr.diagnostics.get("max_drawdown", 0.0)),
                    "expectancy": (
                        list((cr.diagnostics.get("expectancy_by_template") or {"none": 0.0}).values())[0]
                        if (cr.diagnostics.get("expectancy_by_template") or {})
                        else 0.0
                    ),
                    "calibration_proxy": float(cr.diagnostics.get("calibration_proxy", 0.0)),
                    "turnover": float(cr.diagnostics.get("turnover", 0.0)),
                    "score": sb["score"],
                    "hard_gate_pass": sb["hard_gate_pass"],
                    "score_breakdown": sb,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "status": "ok",
                }
            )
        except Exception as e:  # noqa: BLE001
            failures.append(
                {
                    "experiment_id": exp_id,
                    "snapshot_id": snapshot_id,
                    "trading_symbol": trading_symbol,
                    "price_source_symbol": price_source_symbol,
                    "timeframe": timeframe,
                    "error": str(e),
                    "status": "failed",
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                }
            )

    return rows, failures
