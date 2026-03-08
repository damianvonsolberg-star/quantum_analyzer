from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantum_analyzer.backtest.diagnostics import (
    BacktestDiagnostics,
    calibration_proxy,
    expectancy_by_template,
    export_diagnostics_bundle,
    hit_rate_by_state,
    max_drawdown,
)
from quantum_analyzer.backtest.walkforward import WalkForwardConfig, purged_walkforward_splits
from quantum_analyzer.forecast.mixture import build_forecast_bundle
from quantum_analyzer.paths.archetypes import PathTemplate
from quantum_analyzer.policy.risk_caps import DrawdownState, RegimeCaps
from quantum_analyzer.policy.target_position import PolicyInputs, propose_action
from quantum_analyzer.contracts import ARTIFACT_SCHEMA_V2
from quantum_analyzer.state.latent_model import GaussianHMMBaseline


@dataclass
class BacktestConfig:
    turnover_cap: float = 0.15
    round_trip_cost_bps: float = 15.0
    initial_equity: float = 1_000_000.0
    symbol: str = "SOLUSDT"


@dataclass
class BacktestResult:
    summary: dict[str, Any]
    diagnostics: BacktestDiagnostics
    equity_curve: pd.DataFrame
    actions: pd.DataFrame


def _pick_regime_name(belief_row: dict[str, float]) -> str:
    if not belief_row:
        return "unknown"
    return max(belief_row, key=belief_row.get)


def run_backtest(
    features: pd.DataFrame,
    close: pd.Series,
    templates: list[PathTemplate],
    wf_cfg: WalkForwardConfig,
    bt_cfg: BacktestConfig,
    out_dir: str | Path | None = None,
) -> BacktestResult:
    features = features.copy().sort_index()
    close = close.reindex(features.index).astype(float)

    n = len(features)
    splits = purged_walkforward_splits(n, wf_cfg)

    position = 0.0
    equity = bt_cfg.initial_equity

    eq_rows: list[dict[str, Any]] = []
    act_rows: list[dict[str, Any]] = []

    all_test_indices: list[int] = []

    for train_idx, test_idx in splits:
        X_train = features.iloc[train_idx]
        X_test = features.iloc[test_idx]

        model = GaussianHMMBaseline(n_states=10, random_state=7).fit(X_train)
        beliefs = model.predict_state_beliefs(X_test, symbol=bt_cfg.symbol)

        for i_local, b in enumerate(beliefs):
            i = test_idx[i_local]
            if i >= len(close) - 1:
                continue
            all_test_indices.append(i)

            px = float(close.iloc[i])
            px_next = float(close.iloc[i + 1])
            ret_next = px_next / px - 1.0

            forecast = build_forecast_bundle(bt_cfg.symbol, b, templates, calibration_score=0.7)
            regime = _pick_regime_name(b.regime_probabilities)

            ap = propose_action(
                PolicyInputs(
                    forecast=forecast,
                    estimated_round_trip_cost_bps=bt_cfg.round_trip_cost_bps,
                    current_position=position,
                    regime=regime,
                    drawdown_state=DrawdownState(drawdown_pct=0.0),
                    regime_caps=RegimeCaps(),
                    turnover_cap=bt_cfg.turnover_cap,
                )
            )

            target = float(ap.target_position)
            # enforce per-bar turnover again (defensive)
            delta = np.clip(target - position, -bt_cfg.turnover_cap, bt_cfg.turnover_cap)
            new_position = position + delta

            # transaction cost applied on turnover delta (one-way approx)
            one_way_cost = (bt_cfg.round_trip_cost_bps / 2.0) / 10_000.0
            pnl = equity * (new_position * ret_next - abs(delta) * one_way_cost)
            equity = equity + pnl
            position = new_position

            # attach template used for diagnostics (best by expectancy)
            template_id = templates[0].template_id if templates else "none"

            eq_rows.append(
                {
                    "ts": close.index[i + 1],
                    "equity": equity,
                    "pnl": pnl,
                    "ret_next": ret_next,
                    "position": position,
                }
            )
            act_rows.append(
                {
                    "ts": close.index[i],
                    "action": ap.action,
                    "target_position": ap.target_position,
                    "expected_edge_bps": ap.expected_edge_bps,
                    "expected_cost_bps": ap.expected_cost_bps,
                    "reason": ap.reason,
                    "state": regime,
                    "template_id": template_id,
                    "p_up": forecast.distributions["h36"].quantiles.get("p_up", np.nan),
                    "realized_up": 1 if ret_next > 0 else 0,
                    "turnover_abs": abs(delta),
                    "pnl": pnl,
                }
            )

    eq_df = pd.DataFrame(eq_rows).set_index("ts") if eq_rows else pd.DataFrame(columns=["equity", "pnl", "position"])
    act_df = pd.DataFrame(act_rows).set_index("ts") if act_rows else pd.DataFrame(columns=["action"])

    cal = calibration_proxy(act_df.get("p_up", pd.Series(dtype=float)), act_df.get("realized_up", pd.Series(dtype=float))) if not act_df.empty else 0.0
    hr = hit_rate_by_state(act_df.get("state", pd.Series(dtype=object)), act_df.get("pnl", pd.Series(dtype=float))) if not act_df.empty else {}
    exp_tpl = expectancy_by_template(act_df.get("template_id", pd.Series(dtype=object)), act_df.get("pnl", pd.Series(dtype=float))) if not act_df.empty else {}
    action_rate = float((act_df["action"] != "HOLD").mean()) if not act_df.empty else 0.0
    turnover = float(act_df.get("turnover_abs", pd.Series(dtype=float)).sum()) if not act_df.empty else 0.0
    mdd = max_drawdown(eq_df["equity"]) if not eq_df.empty else 0.0

    diag = BacktestDiagnostics(
        calibration_proxy=cal,
        hit_rate_by_state=hr,
        expectancy_by_template=exp_tpl,
        action_rate=action_rate,
        turnover=turnover,
        max_drawdown=mdd,
    )

    summary = {
        "bars": len(features),
        "test_bars": len(all_test_indices),
        "ending_equity": float(eq_df["equity"].iloc[-1]) if not eq_df.empty else bt_cfg.initial_equity,
        "return_pct": float(eq_df["equity"].iloc[-1] / bt_cfg.initial_equity - 1.0) if not eq_df.empty else 0.0,
        "diagnostics": diag.to_dict(),
    }

    if out_dir is not None:
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)

        latest_ts = str(eq_df.index[-1]) if not eq_df.empty else None
        latest_action = act_df.iloc[-1].to_dict() if not act_df.empty else {
            "action": "HOLD",
            "target_position": 0.0,
            "expected_edge_bps": 0.0,
            "expected_cost_bps": 0.0,
            "reason": "no_actions",
        }
        calibration_score = float(max(0.0, 1.0 - cal))

        # canonical artifact bundle v2
        with (out / "artifact_bundle.json").open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "schema_version": ARTIFACT_SCHEMA_V2,
                    "artifact_meta": {
                        "producer": "quantum_analyzer.backtest.engine",
                        "produced_at": datetime.now(timezone.utc).isoformat(),
                        "latest_timestamp": latest_ts,
                    },
                    "forecast": {
                        "confidence": float(latest_action.get("p_up", 0.0) or 0.0),
                        "entropy": None,
                        "calibration_score": calibration_score,
                        "distributions": {"h12": {}, "h36": {}, "h72": {}},
                        "timestamps": {"as_of": latest_ts},
                    },
                    "proposal": {
                        "id": str(latest_action.get("proposal_id", "")),
                        "timestamp": str(latest_action.get("ts", latest_ts)),
                        "action": str(latest_action.get("action", "HOLD")),
                        "target_position": float(latest_action.get("target_position", 0.0) or 0.0),
                        "expected_edge_bps": float(latest_action.get("expected_edge_bps", 0.0) or 0.0),
                        "expected_cost_bps": float(latest_action.get("expected_cost_bps", 0.0) or 0.0),
                        "reason": str(latest_action.get("reason", "")),
                    },
                    "drift": {
                        "governance_status": "OK",
                        "kill_switch": False,
                        "kill_switch_reasons": [],
                        "timestamps": {"as_of": latest_ts},
                    },
                    "summary": summary,
                    "config": {
                        "walkforward": wf_cfg.__dict__,
                        "backtest": bt_cfg.__dict__,
                        "template_count": len(templates),
                    },
                },
                f,
                indent=2,
                default=str,
            )
        export_diagnostics_bundle(
            out,
            summary,
            {
                "equity_curve": eq_df,
                "actions": act_df,
            },
        )

    return BacktestResult(summary=summary, diagnostics=diag, equity_curve=eq_df, actions=act_df)
