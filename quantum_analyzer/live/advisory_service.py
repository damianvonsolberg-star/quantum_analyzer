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
from quantum_analyzer.live.artifact_loader import ArtifactLoader, ArtifactValidationError
from quantum_analyzer.monitoring.calibration import calibration_drift
from quantum_analyzer.monitoring.drift import action_rate_drift, canonical_drift_metrics, cost_drift, state_occupancy_drift
from quantum_analyzer.monitoring.governance import DriftThresholds, evaluate_governance
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


def _hold_output(reason: str) -> AdvisoryOutput:
    now = datetime.now(timezone.utc)
    fb = ForecastBundle(ts=now, symbol="SOLUSDC", distributions={}, diagnostics={"reason": reason})
    hold = ActionProposal(
        ts=now,
        symbol="SOLUSDC",
        action="HOLD",
        score=0.0,
        size_fraction=0.0,
        target_position=0.0,
        expected_edge_bps=0.0,
        expected_cost_bps=0.0,
        reason=reason,
    )
    gov = {
        "overall_status": "HALT",
        "kill_switch_active": True,
        "kill_switch_reasons": [reason],
        "artifact_staleness": "unknown",
        "data_staleness": "unknown",
        "feature_drift": 0.0,
        "calibration_drift": 0.0,
        "state_occupancy_drift": 0.0,
        "action_rate_drift": 0.0,
        "cost_drift_bps": 0.0,
    }
    return AdvisoryOutput(forecast=fb, proposal=hold, drift={"governance": gov}, kill_switch=True, kill_reason=reason)


def _belief_from_probs(ts: datetime, symbol: str, probs: dict[str, float]) -> StateBelief:
    p = np.array(list(probs.values()), dtype=float)
    p = p / max(p.sum(), 1e-9)
    entropy = float(-(p * np.log(np.clip(p, 1e-12, 1.0))).sum() / np.log(max(len(p), 2)))
    return StateBelief(ts=ts, symbol=symbol, regime_probabilities=probs, entropy=entropy, confidence=float(p.max()))


def run_advisory(
    artifacts_root: str | Path,
    market_fetcher=default_market_fetcher,
    thresholds: DriftThresholds | None = None,
    *,
    allow_dev_fallback: bool = False,
    promoted_root: str | Path | None = None,
) -> AdvisoryOutput:
    """Production advisory path is strictly artifact-backed.

    When required artifacts are missing/invalid, returns HOLD/HALT with explicit reason.
    Heuristic fallback is available only when allow_dev_fallback=True.
    """
    thresholds = thresholds or DriftThresholds()
    loader = ArtifactLoader(artifacts_root)

    # Preferred path: promoted signal bundle from explorer/promotion pipeline.
    promoted = loader.load_promoted_signal(promoted_root)
    if isinstance(promoted, dict) and promoted.get("action"):
        now = datetime.now(timezone.utc)
        action = str(promoted.get("action", "HOLD"))
        target = float(promoted.get("target_position", 0.0) or 0.0)
        conf = float(promoted.get("confidence", 0.0) or 0.0)
        source_meta = promoted.get("source", {}) if isinstance(promoted.get("source"), dict) else {}
        symbols = promoted.get("symbols", {}) if isinstance(promoted.get("symbols"), dict) else {}
        trading_symbol = str(symbols.get("trading_symbol", promoted.get("symbol", "SOLUSDC")))
        supporting = promoted.get("supporting_metrics", {}) if isinstance(promoted.get("supporting_metrics"), dict) else {}
        expected_edge = float(supporting.get("expectancy", supporting.get("expected_edge_bps", 0.0)) or 0.0)
        expected_cost = supporting.get("expected_cost_bps", None)
        expected_cost_bps = float(expected_cost) if expected_cost is not None else float("nan")

        proposal = ActionProposal(
            ts=now,
            symbol=trading_symbol,
            action=action,
            score=conf,
            size_fraction=abs(target),
            target_position=target,
            expected_edge_bps=expected_edge,
            expected_cost_bps=expected_cost_bps,
            reason=str(promoted.get("reason", "promoted_signal")),
            advisory_mode="spot_only",
            target_scope="advisory_sleeve",
            candidate_id=str((supporting.get("supporting_metrics", {}) or {}).get("candidate_id", "")),
            candidate_family=str((supporting.get("supporting_metrics", {}) or {}).get("candidate_family", "")),
            controls={
                "top_alternatives": promoted.get("top_alternatives", []),
                "invalidation_reasons": promoted.get("invalidation_reasons", []),
                "supporting_metrics": supporting,
            },
        )
        forecast = ForecastBundle(ts=now, symbol=trading_symbol, distributions={}, diagnostics={"source": "promoted_signal_bundle", "confidence": conf})
        gov_status = str(source_meta.get("governance_status", "WATCH"))
        drift_payload = promoted.get("drift", {}) if isinstance(promoted.get("drift"), dict) else {}
        gov = {
            "overall_status": gov_status,
            "kill_switch_active": bool(gov_status != "OK"),
            "kill_switch_reasons": (drift_payload.get("kill_switch_reasons") if drift_payload.get("kill_switch_reasons") else (["governance_not_ok"] if gov_status != "OK" else [])),
            "artifact_staleness": str(drift_payload.get("artifact_staleness", "unknown")),
            "data_staleness": str(drift_payload.get("data_staleness", "unknown")),
            "feature_drift": float(drift_payload.get("feature_drift", 0.0) or 0.0),
            "calibration_drift": float(drift_payload.get("calibration_drift", 0.0) or 0.0),
            "state_occupancy_drift": float(drift_payload.get("state_occupancy_drift", 0.0) or 0.0),
            "action_rate_drift": float(drift_payload.get("action_rate_drift", 0.0) or 0.0),
            "cost_drift_bps": float(drift_payload.get("cost_drift_bps", 0.0) or 0.0),
        }
        return AdvisoryOutput(
            forecast=forecast,
            proposal=proposal,
            drift={"governance": gov},
            kill_switch=bool(gov["kill_switch_active"]),
            kill_reason=(gov["kill_switch_reasons"][0] if gov["kill_switch_reasons"] else None),
        )

    # Batch-2 strict behavior: live advisory consumes promoted bundle only,
    # unless explicit dev fallback is enabled.
    if not allow_dev_fallback:
        return _hold_output("missing_promoted_signal_bundle")

    try:
        bundle, bundle_dir, templates, model = loader.load_production_artifacts()
    except ArtifactValidationError as e:
        if not allow_dev_fallback:
            return _hold_output(f"artifact_validation_failed:{e}")
        # explicit dev-only fallback
        bundle_path = loader.latest_bundle_path()
        bundle = loader.load_bundle(bundle_path)
        bundle_dir = Path(bundle_path).parent
        templates = loader.load_templates(bundle_dir)
        model = None

    data = market_fetcher()
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
        return _hold_output("bad_data:no_features")

    last_ts = feats.index[-1].to_pydatetime() if hasattr(feats.index[-1], "to_pydatetime") else feats.index[-1]

    if model is None:
        # explicit dev fallback only, never in strict production mode
        if not allow_dev_fallback:
            return _hold_output("missing_latent_model")
        probs = {"unknown": 1.0}
        belief = _belief_from_probs(last_ts, "SOLUSDT", probs)
    else:
        b = model.predict_state_beliefs(feats.tail(1), symbol="SOLUSDT")
        if not b:
            return _hold_output("state_model_no_output")
        belief = b[-1]

    forecast_cfg = bundle.get("forecast", {}) if isinstance(bundle, dict) else {}
    calibration_score = float(forecast_cfg.get("calibration_score", 0.0) or 0.0)
    forecast = build_forecast_bundle("SOLUSDT", belief, templates, calibration_score=calibration_score)

    policy_cfg = bundle.get("config", {}).get("policy", {}) if isinstance(bundle.get("config", {}), dict) else {}
    est_cost = float(policy_cfg.get("estimated_round_trip_cost_bps", 15.0))
    current_pos = float(policy_cfg.get("current_position", 0.0))

    regime = max(belief.regime_probabilities, key=belief.regime_probabilities.get) if belief.regime_probabilities else "unknown"
    proposal = propose_action(
        PolicyInputs(
            forecast=forecast,
            estimated_round_trip_cost_bps=est_cost,
            current_position=current_pos,
            regime=regime,
            drawdown_state=DrawdownState(drawdown_pct=0.0),
            regime_caps=RegimeCaps(),
        )
    )

    # drift baseline from bundle files if present
    ref_actions_path = bundle_dir / "actions.csv"
    ref_eq_path = bundle_dir / "equity_curve.csv"

    state_drift = 0.0
    act_drift = 0.0
    c_drift = 0.0
    cal_drift = 0.0

    if ref_actions_path.exists() and ref_eq_path.exists():
        ref_actions = pd.read_csv(ref_actions_path)
        ref_p = ref_actions.get("p_up", pd.Series(dtype=float))
        ref_y = ref_actions.get("realized_up", pd.Series(dtype=float))
        cur_p = pd.Series([forecast.distributions.get("h36").quantiles.get("p_up", 0.5) if "h36" in forecast.distributions else 0.5])
        cur_y = pd.Series([0.0])
        cal_drift = calibration_drift(ref_p, ref_y, cur_p, cur_y)

        act_drift = action_rate_drift(ref_actions.get("action", pd.Series(dtype=object)), pd.Series([proposal.action]))
        c_drift = cost_drift(ref_actions.get("expected_cost_bps", pd.Series([est_cost])), pd.Series([proposal.expected_cost_bps]))

        if "state" in ref_actions.columns:
            ref_state = pd.get_dummies(ref_actions["state"])
            cur_state = pd.get_dummies(pd.Series([regime], name="state"))
            state_drift = state_occupancy_drift(ref_state, cur_state)

    # feature_psi reserved: strict mode does not invent synthetic feature drift
    feature_psi_max = 0.0

    metrics = canonical_drift_metrics(
        feature_psi_max=feature_psi_max,
        calibration_drift=cal_drift,
        state_occupancy_drift_value=state_drift,
        action_rate_drift_value=act_drift,
        cost_drift_bps=c_drift,
    )

    artifact_ts = None
    if isinstance(bundle.get("artifact_meta"), dict):
        artifact_ts = bundle.get("artifact_meta", {}).get("latest_timestamp") or bundle.get("artifact_meta", {}).get("produced_at")
    data_ts = str(last_ts)

    governance = evaluate_governance(
        bad_data=False,
        feature_psi_max=metrics["feature_drift"],
        state_drift=metrics["state_occupancy_drift"],
        action_rate_drift=metrics["action_rate_drift"],
        cost_drift_bps=metrics["cost_drift_bps"],
        calibration_drift=metrics["calibration_drift"],
        artifact_timestamp=artifact_ts,
        data_timestamp=data_ts,
        th=thresholds,
    )

    kill_reason = governance.kill_switch_reasons[0] if governance.kill_switch_reasons else None

    return AdvisoryOutput(
        forecast=forecast,
        proposal=proposal,
        drift={"metrics": metrics, "governance": governance.to_dict()},
        kill_switch=governance.kill_switch_active,
        kill_reason=kill_reason,
    )
