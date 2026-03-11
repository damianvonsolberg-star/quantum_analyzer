from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import json

from quantum_analyzer.backtest.engine import BacktestConfig, run_backtest
from quantum_analyzer.experiments.runner import run_experiments
from quantum_analyzer.experiments.specs import ExperimentSpec
from quantum_analyzer.contracts import ARTIFACT_SCHEMA_V2
from quantum_analyzer.backtest.walkforward import WalkForwardConfig, purged_walkforward_splits
from quantum_analyzer.paths.archetypes import PathTemplate
from quantum_analyzer.strategies.base import CandidateStrategy


def _synthetic_series(n: int = 24 * 120) -> tuple[pd.DataFrame, pd.Series]:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    idx = pd.DatetimeIndex([start + timedelta(hours=i) for i in range(n)], tz="UTC")
    rng = np.random.default_rng(1)
    close = pd.Series(80 + np.cumsum(rng.normal(0, 0.15, n)), index=idx)

    features = pd.DataFrame(
        {
            "micro_range_pos_24h": rng.normal(0.5, 0.2, n),
            "meso_range_pos_7d": rng.normal(0.5, 0.2, n),
            "realized_vol_24h": np.abs(rng.normal(0.03, 0.01, n)),
            "aggtrade_imbalance": rng.normal(0, 0.2, n),
            "orderbook_imbalance": rng.normal(0, 0.2, n),
            "basis_bps": rng.normal(5, 1, n),
            "oi_zscore": rng.normal(0, 1, n),
        },
        index=idx,
    )
    return features, close


def _templates() -> list[PathTemplate]:
    return [
        PathTemplate(
            template_id="tpl1",
            cluster_id=0,
            sample_count=100,
            action="long",
            expectancy=0.01,
            pf_proxy=1.2,
            robustness=1.0,
            support=0.3,
            oos_stability=0.008,
            archetype_signature=[0.1, 0.0, -0.1],
            meta={},
        )
    ]


def test_purged_walkforward_splits() -> None:
    cfg = WalkForwardConfig(train_bars=100, test_bars=20, purge_bars=5, embargo_bars=5)
    splits = purged_walkforward_splits(220, cfg)
    assert splits
    tr, te = splits[0]
    assert len(tr) > 0 and len(te) > 0


def test_run_backtest_and_export_bundle(tmp_path: Path) -> None:
    features, close = _synthetic_series()
    wf = WalkForwardConfig(train_bars=24 * 60, test_bars=24 * 10, purge_bars=6, embargo_bars=6)
    bt = BacktestConfig(turnover_cap=0.1, round_trip_cost_bps=12, initial_equity=1_000_000)

    result = run_backtest(features, close, _templates(), wf, bt, out_dir=tmp_path)

    assert "ending_equity" in result.summary
    assert result.diagnostics.max_drawdown <= 0.0
    assert isinstance(result.diagnostics.action_rate, float)
    assert (tmp_path / "summary.json").exists()
    assert (tmp_path / "artifact_bundle.json").exists()
    assert (tmp_path / "equity_curve.csv").exists()
    assert (tmp_path / "actions.csv").exists()

    bundle = json.loads((tmp_path / "artifact_bundle.json").read_text())
    assert bundle["schema_version"] == ARTIFACT_SCHEMA_V2
    for sec in ["artifact_meta", "forecast", "proposal", "drift", "summary", "config"]:
        assert sec in bundle

    # forecast/proposal timestamp provenance must be market-derived and aligned
    assert bundle["artifact_meta"].get("latest_timestamp") is not None
    assert bundle["forecast"].get("timestamps", {}).get("as_of") == bundle["artifact_meta"].get("latest_timestamp")
    assert bundle["proposal"].get("timestamp") is not None


def test_90day_walkforward_local_data_contract(tmp_path: Path) -> None:
    # contract-level test: 90d horizon can run with synthetic local data shape
    bars = 24 * 95
    features, close = _synthetic_series(n=bars)
    wf = WalkForwardConfig(train_bars=24 * 60, test_bars=24 * 15, purge_bars=6, embargo_bars=6)
    bt = BacktestConfig()

    result = run_backtest(features, close, _templates(), wf, bt, out_dir=tmp_path / "wf90")
    assert result.summary["test_bars"] > 0
    assert (tmp_path / "wf90" / "summary.json").exists()


def test_experiment_runner_enforces_subset_and_regime_slice(tmp_path: Path, monkeypatch) -> None:
    features, close = _synthetic_series(n=24 * 30)
    features["hour_sin"] = (features.index.hour.astype(float) / 24.0)
    features["dow"] = features.index.dayofweek.astype(float)
    features["btc_return_1h"] = 0.0

    captured_cols = []

    class _DummyCand:
        candidate_id = "dummy"
        family = "trend"

    class _DummyResult:
        candidate_id = "dummy"
        family = "trend"
        summary = {"return_pct": 0.0}
        diagnostics = {"max_drawdown": 0.0, "turnover": 0.0, "calibration_proxy": 0.0, "expectancy_by_template": {"none": 0.0}}

    def _build_candidate(**kwargs):
        return _DummyCand()

    def _evaluate_candidate(*, features, close, candidate, walkforward, backtest, out_dir=None):
        captured_cols.append(set(features.columns))
        assert len(features) > 0
        return _DummyResult()

    monkeypatch.setattr("quantum_analyzer.experiments.runner.build_candidate", _build_candidate)
    monkeypatch.setattr("quantum_analyzer.experiments.runner.evaluate_candidate", _evaluate_candidate)

    specs = [
        ExperimentSpec(window_bars=24 * 14, test_bars=24 * 5, horizon=12, feature_subset="geom_core", regime_slice="low_vol", policy_params={}),
        ExperimentSpec(window_bars=24 * 14, test_bars=24 * 5, horizon=12, feature_subset="full_stack", regime_slice="high_vol", policy_params={}),
    ]

    rows, failures = run_experiments(
        specs=specs,
        snapshot_id="snap1",
        features_full=features,
        close=close,
        templates=[],
        out_root=tmp_path,
    )
    assert len(failures) == 0
    assert len(rows) == 2
    assert "micro_range_pos_24h" in captured_cols[0]
    assert "hour_sin" in captured_cols[1]

    bad_specs = [
        ExperimentSpec(window_bars=24 * 14, test_bars=24 * 5, horizon=12, feature_subset="geom_core", regime_slice="unknown_slice", policy_params={}),
    ]
    rows2, failures2 = run_experiments(
        specs=bad_specs,
        snapshot_id="snap1",
        features_full=features,
        close=close,
        templates=[],
        out_root=tmp_path,
    )
    assert len(rows2) == 0
    assert len(failures2) == 1
    assert "Unknown regime slice" in failures2[0]["error"]


def test_run_backtest_strict_candidate_errors_raise() -> None:
    features, close = _synthetic_series(n=24 * 20)
    wf = WalkForwardConfig(train_bars=24 * 10, test_bars=24 * 3, purge_bars=2, embargo_bars=2)
    bt = BacktestConfig(strict_candidate_errors=True)

    class _Explode(CandidateStrategy):
        def generate_scores(self, features: pd.DataFrame) -> pd.Series:  # type: ignore[override]
            raise RuntimeError("boom_generate")

    cand = _Explode(candidate_id="explode", family="trend")

    try:
        run_backtest(features, close, _templates(), wf, bt, candidate_strategy=cand)
        raise AssertionError("expected strict candidate exception")
    except RuntimeError as e:
        assert "candidate_generate_scores_failed" in str(e)


def test_run_backtest_non_strict_candidate_errors_record_fallback() -> None:
    features, close = _synthetic_series(n=24 * 20)
    wf = WalkForwardConfig(train_bars=24 * 10, test_bars=24 * 3, purge_bars=2, embargo_bars=2)
    bt = BacktestConfig(strict_candidate_errors=False)

    class _Explode(CandidateStrategy):
        def generate_scores(self, features: pd.DataFrame) -> pd.Series:  # type: ignore[override]
            raise RuntimeError("boom_generate")

    cand = _Explode(candidate_id="explode", family="trend")
    result = run_backtest(features, close, _templates(), wf, bt, candidate_strategy=cand)
    assert result.summary.get("strategy_exception_fallback") is True
    assert int(result.summary.get("strategy_exception_count", 0)) >= 1
