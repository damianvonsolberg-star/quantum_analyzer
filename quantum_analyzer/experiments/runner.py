from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from quantum_analyzer.backtest.engine import BacktestConfig, run_backtest
from quantum_analyzer.backtest.walkforward import WalkForwardConfig
from quantum_analyzer.paths.archetypes import PathTemplate

from .scoring import score_result
from .specs import ExperimentSpec


def _slice_window(features: pd.DataFrame, close: pd.Series, window_bars: int) -> tuple[pd.DataFrame, pd.Series]:
    f = features.tail(window_bars).copy()
    c = close.reindex(f.index).copy()
    return f, c


def run_experiments(
    *,
    specs: list[ExperimentSpec],
    snapshot_id: str,
    features: pd.DataFrame,
    close: pd.Series,
    templates: list[PathTemplate],
    out_root: str | Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    out = Path(out_root)
    out.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for spec in specs:
        exp_id = spec.experiment_id(snapshot_id)
        exp_dir = out / "experiments" / exp_id
        exp_dir.mkdir(parents=True, exist_ok=True)

        try:
            f, c = _slice_window(features, close, spec.window_bars)
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

            r = run_backtest(f, c, templates, wf, bt, out_dir=exp_dir)
            sb = score_result(r.summary, r.diagnostics.to_dict())

            rows.append(
                {
                    "experiment_id": exp_id,
                    "snapshot_id": snapshot_id,
                    "window_bars": spec.window_bars,
                    "test_bars": spec.test_bars,
                    "horizon": spec.horizon,
                    "feature_subset": spec.feature_subset,
                    "regime_slice": spec.regime_slice,
                    "policy_params": spec.policy_params,
                    "artifact_dir": str(exp_dir),
                    "return_pct": r.summary.get("return_pct", 0.0),
                    "max_drawdown": r.diagnostics.max_drawdown,
                    "expectancy": r.diagnostics.expectancy_by_template.get(next(iter(r.diagnostics.expectancy_by_template), "none"), 0.0)
                    if r.diagnostics.expectancy_by_template
                    else 0.0,
                    "calibration_proxy": r.diagnostics.calibration_proxy,
                    "turnover": r.diagnostics.turnover,
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
                    "error": str(e),
                    "status": "failed",
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                }
            )

    return rows, failures
