from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantum_analyzer.contracts import ActionProposal, ForecastBundle, StateBelief
from quantum_analyzer.features.build_features import build_feature_frame
from quantum_analyzer.forecast.mixture import build_forecast_bundle
from quantum_analyzer.live.artifact_loader import ArtifactLoader
from quantum_analyzer.monitoring.calibration import calibration_drift
from quantum_analyzer.monitoring.drift import (
    action_rate_drift,
    cost_drift,
    feature_psi,
    state_occupancy_drift,
)
from quantum_analyzer.monitoring.governance import DriftThresholds, kill_switch_reason
from quantum_analyzer.policy.risk_caps import DrawdownState, RegimeCaps
from quantum_analyzer.policy.target_position import PolicyInputs, propose_action


@dataclass
class AdvisoryOutput:
    forecast: ForecastBundle
    proposal: ActionProposal
    drift: dict[str, Any]
    kill_switch: bool
    kill_reason: str | None


def default_market_fetcher() -> dict[str, pd.DataFrame]:
    # Placeholder for production adapters; tests inject deterministic data.
    raise NotImplementedError("Provide market fetcher implementation")


def _belief_from_probs(ts: datetime, symbol: str, probs: dict[str, float]) -> StateBelief:
    p = np.array(list(probs.values()), dtype=float)
    p = p / max(p.sum(), 1e-9)
    entropy = float(-(p * np.log(np.clip(p, 1e-12, 1.0))).sum() / np.log(max(len(p), 2)))
    return StateBelief(ts=ts, symbol=symbol, regime_probabilities=probs, entropy=entropy, confidence=float(p.max()))


def run_advisory(
    artifacts_root: str | Path,
    market_fetcher=default_market_fetcher,
    thresholds: DriftThresholds | None = None,
) -> AdvisoryOutput:
    thresholds = thresholds or DriftThresholds()
    loader = ArtifactLoader(artifacts_root)
    bundle_path = loader.latest_bundle_path()
    bundle = loader.load_bundle(bundle_path)
    bundle_dir = Path(bundle_path).parent

    templates = loader.load_templates(bundle_dir)

    data = market_fetcher()
    # expected keys: sol_klines, btc_klines, agg_trades, book_ticker, funding, basis, open_interest
    feats = build_feature_frame(
        data["sol_klines"],
        data["btc_klines"],
        data["agg_trades"],
        data["book_ticker"],
        data["funding"],
        data["basis"],
        data["open_interest"],
    )

    if feats.empty:
        kill_reason = kill_switch_reason(True, 0, 0, 0, 0, 0, thresholds)
        hold = ActionProposal(
            ts=datetime.now(timezone.utc),
            symbol="SOLUSDT",
            action="HOLD",
            score=0.0,
            size_fraction=0.0,
            target_position=0.0,
            expected_edge_bps=0.0,
            expected_cost_bps=0.0,
            reason="No data",
        )
        fb = build_forecast_bundle("SOLUSDT", _belief_from_probs(datetime.now(timezone.utc), "SOLUSDT", {"unknown": 1.0}), templates)
        return AdvisoryOutput(forecast=fb, proposal=hold, drift={}, kill_switch=True, kill_reason=kill_reason)

    last_ts = feats.index[-1].to_pydatetime() if hasattr(feats.index[-1], "to_pydatetime") else feats.index[-1]
    # lightweight belief from current feature sign proxies when model artifact absent
    probs = {
        "trend_up": float(max(0.0, feats["slope_24h"].iloc[-1] if "slope_24h" in feats else 0.2)),
        "trend_down": float(max(0.0, -(feats["slope_24h"].iloc[-1] if "slope_24h" in feats else -0.2))),
        "range_mid_drift": 0.2,
        "stabilization": 0.2,
    }
    s = sum(probs.values())
    probs = {k: v / s for k, v in probs.items()}
    belief = _belief_from_probs(last_ts, "SOLUSDT", probs)

    forecast = build_forecast_bundle("SOLUSDT", belief, templates, calibration_score=0.7)
    proposal = propose_action(
        PolicyInputs(
            forecast=forecast,
            estimated_round_trip_cost_bps=15.0,
            current_position=0.0,
            regime=max(probs, key=probs.get),
            drawdown_state=DrawdownState(drawdown_pct=0.0),
            regime_caps=RegimeCaps(),
        )
    )

    # drift baseline from bundle files if present
    ref_actions_path = bundle_dir / "actions.csv"
    ref_eq_path = bundle_dir / "equity_curve.csv"

    feature_psi_map = {c: 0.0 for c in feats.columns}
    state_drift = 0.0
    act_drift = 0.0
    c_drift = 0.0
    cal_drift = 0.0

    if ref_actions_path.exists() and ref_eq_path.exists():
        ref_actions = pd.read_csv(ref_actions_path)
        ref_p = ref_actions.get("p_up", pd.Series(dtype=float))
        ref_y = ref_actions.get("realized_up", pd.Series(dtype=float))
        cur_p = pd.Series([forecast.distributions["h36"].quantiles.get("p_up", 0.5)])
        cur_y = pd.Series([0.0])
        cal_drift = calibration_drift(ref_p, ref_y, cur_p, cur_y)

        act_drift = action_rate_drift(ref_actions.get("action", pd.Series(dtype=object)), pd.Series([proposal.action]))
        c_drift = cost_drift(ref_actions.get("expected_cost_bps", pd.Series([15.0])), pd.Series([proposal.expected_cost_bps]))

        # small synthetic state drift proxy
        if "state" in ref_actions.columns:
            ref_state = pd.get_dummies(ref_actions["state"])
            cur_state = pd.get_dummies(pd.Series([max(probs, key=probs.get)], name="state"))
            state_drift = state_occupancy_drift(ref_state, cur_state)

    feature_psi_max = max(feature_psi_map.values()) if feature_psi_map else 0.0
    reason = kill_switch_reason(False, feature_psi_max, state_drift, act_drift, c_drift, cal_drift, thresholds)

    return AdvisoryOutput(
        forecast=forecast,
        proposal=proposal,
        drift={
            "feature_psi": feature_psi_map,
            "feature_psi_max": feature_psi_max,
            "state_occupancy_drift": state_drift,
            "action_rate_drift": act_drift,
            "cost_drift_bps": c_drift,
            "calibration_drift": cal_drift,
        },
        kill_switch=reason is not None,
        kill_reason=reason,
    )
