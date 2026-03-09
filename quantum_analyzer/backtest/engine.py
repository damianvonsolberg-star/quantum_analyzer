from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _measured_action_stats(history_actions: list[dict[str, Any]], action_label: str) -> tuple[float, float, float, float]:
    rows = [r for r in history_actions if str(r.get("action", "")).upper() == action_label]
    if len(rows) < 10:
        return 0.0, 0.5, 0.0, 0.0
    rets = np.array([float(r.get("ret_next", 0.0)) for r in rows], dtype=float)
    mean_ret = float(np.nanmean(rets))
    p_up = float(np.mean(rets > 0.0))
    q25 = float(np.nanquantile(rets, 0.25))
    q75 = float(np.nanquantile(rets, 0.75))
    return mean_ret, p_up, q25, q75


def _distribution_from_returns(h: int, returns: np.ndarray) -> dict[str, Any]:
    if returns.size == 0:
        return {
            "horizon_hours": h,
            "mean_return": 0.0,
            "std_return": 0.0,
            "quantiles": {"q05": 0.0, "q25": 0.0, "q50": 0.0, "q75": 0.0, "q95": 0.0, "p_up": 0.5, "p_down": 0.5, "p_break_up": 0.0, "p_break_down": 0.0},
            "probability_up": 0.5,
        }
    q05, q25, q50, q75, q95 = [float(np.nanquantile(returns, q)) for q in (0.05, 0.25, 0.5, 0.75, 0.95)]
    p_up = float(np.mean(returns > 0.0))
    p_down = 1.0 - p_up
    return {
        "horizon_hours": h,
        "mean_return": float(np.nanmean(returns)),
        "std_return": float(np.nanstd(returns)),
        "quantiles": {
            "q05": q05,
            "q25": q25,
            "q50": q50,
            "q75": q75,
            "q95": q95,
            "p_up": p_up,
            "p_down": p_down,
            "p_break_up": float(np.mean(returns >= q95)),
            "p_break_down": float(np.mean(returns <= q05)),
        },
        "probability_up": p_up,
    }

from quantum_analyzer.backtest.diagnostics import (
    BacktestDiagnostics,
    action_consistency,
    action_quality_metrics,
    calibration_proxy,
    expectancy_by_template,
    export_diagnostics_bundle,
    hit_rate_by_state,
    max_drawdown,
    performance_by_bucket,
    rolling_performance,
    turnover_cost_sensitivity,
)
from quantum_analyzer.backtest.walkforward import WalkForwardConfig, purged_walkforward_splits
from quantum_analyzer.forecast.mixture import build_forecast_bundle
from quantum_analyzer.paths.archetypes import PathTemplate
from quantum_analyzer.policy.risk_caps import DrawdownState, RegimeCaps
from quantum_analyzer.policy.target_position import PolicyInputs, propose_action
from quantum_analyzer.contracts import ARTIFACT_SCHEMA_V2
from quantum_analyzer.state.latent_model import GaussianHMMBaseline
from quantum_analyzer.strategies.base import CandidateStrategy


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
    candidate_strategy: CandidateStrategy | None = None,
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
    candidate_history: list[dict[str, Any]] = []

    for train_idx, test_idx in splits:
        X_train = features.iloc[train_idx]
        X_test = features.iloc[test_idx]

        model = GaussianHMMBaseline(n_states=10, random_state=7).fit(X_train)
        beliefs = model.predict_state_beliefs(X_test, symbol=bt_cfg.symbol)

        score_series = None
        action_series = None
        candidate_id = "policy_baseline"
        candidate_family = "policy"
        if candidate_strategy is not None:
            candidate_id = candidate_strategy.candidate_id
            candidate_family = candidate_strategy.family
            test_feats = features.iloc[test_idx]
            try:
                score_series = candidate_strategy.generate_scores(test_feats)
            except Exception:
                score_series = pd.Series(0.0, index=test_feats.index)
            try:
                action_series = candidate_strategy.propose_actions(test_feats)
            except Exception:
                action_series = pd.Series("HOLD", index=test_feats.index)

        for i_local, b in enumerate(beliefs):
            i = test_idx[i_local]
            if i >= len(close) - 1:
                continue
            all_test_indices.append(i)

            px = float(close.iloc[i])
            px_next = float(close.iloc[i + 1])
            ret_next = px_next / px - 1.0

            regime = _pick_regime_name(b.regime_probabilities)

            if candidate_strategy is None:
                dyn_calib = float(max(0.0, 1.0 - min(1.0, b.entropy)))
                forecast = build_forecast_bundle(bt_cfg.symbol, b, templates, calibration_score=dyn_calib)
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
                action_label = str(ap.action)
                edge_bps = float(ap.expected_edge_bps)
                reason = str(ap.reason)
                p_up = float(forecast.distributions.get("h36").quantiles.get("p_up", np.nan)) if "h36" in forecast.distributions else np.nan
            else:
                ts = features.index[i]
                action_label = str(action_series.loc[ts]) if action_series is not None and ts in action_series.index else "HOLD"
                if action_label == "BUY":
                    target = 1.0
                elif action_label == "REDUCE":
                    target = max(0.0, position - bt_cfg.turnover_cap)
                elif action_label == "WAIT":
                    target = position
                else:
                    target = position
                mean_ret, p_up, q25, q75 = _measured_action_stats(candidate_history, action_label)
                edge_bps = float(mean_ret * 10_000.0)
                reason = f"{candidate_family} measured OOS action quality"

            # enforce per-bar turnover again (defensive)
            delta = np.clip(target - position, -bt_cfg.turnover_cap, bt_cfg.turnover_cap)
            new_position = position + delta

            # transaction cost applied on turnover delta (one-way approx)
            one_way_cost = (bt_cfg.round_trip_cost_bps / 2.0) / 10_000.0
            pnl = equity * (new_position * ret_next - abs(delta) * one_way_cost)
            equity = equity + pnl
            position = new_position

            eq_rows.append(
                {
                    "ts": close.index[i + 1],
                    "equity": equity,
                    "pnl": pnl,
                    "ret_next": ret_next,
                    "position": position,
                }
            )
            # volatility / BTC regime buckets for diagnostics
            rv = float(features.iloc[i].get("realized_vol_24h", np.nan)) if i < len(features) else np.nan
            if np.isnan(rv):
                vol_bucket = "unknown"
            else:
                vol_bucket = "low" if rv < 0.02 else ("mid" if rv < 0.05 else "high")
            btc_sig = float(features.iloc[i].get("btc_return_1h", 0.0)) if i < len(features) else 0.0
            btc_regime = "btc_up" if btc_sig > 0 else ("btc_down" if btc_sig < 0 else "btc_flat")

            act_rows.append(
                {
                    "ts": close.index[i],
                    "action": action_label,
                    "candidate_id": candidate_id,
                    "candidate_family": candidate_family,
                    "target_position": target,
                    "expected_edge_bps": edge_bps,
                    "expected_cost_bps": bt_cfg.round_trip_cost_bps,
                    "reason": reason,
                    "state": regime,
                    "template_id": candidate_id,
                    "p_up": p_up,
                    "realized_up": 1 if ret_next > 0 else 0,
                    "ret_next": ret_next,
                    "turnover_abs": abs(delta),
                    "pnl": pnl,
                    "vol_bucket": vol_bucket,
                    "btc_regime": btc_regime,
                }
            )
            candidate_history.append({"action": action_label, "ret_next": ret_next})

    eq_df = pd.DataFrame(eq_rows).set_index("ts") if eq_rows else pd.DataFrame(columns=["equity", "pnl", "position"])
    act_df = pd.DataFrame(act_rows).set_index("ts") if act_rows else pd.DataFrame(columns=["action"])

    cal = calibration_proxy(act_df.get("p_up", pd.Series(dtype=float)), act_df.get("realized_up", pd.Series(dtype=float))) if not act_df.empty else 0.0
    hr = hit_rate_by_state(act_df.get("state", pd.Series(dtype=object)), act_df.get("pnl", pd.Series(dtype=float))) if not act_df.empty else {}
    exp_tpl = expectancy_by_template(act_df.get("template_id", pd.Series(dtype=object)), act_df.get("pnl", pd.Series(dtype=float))) if not act_df.empty else {}
    action_rate = float((act_df["action"] != "HOLD").mean()) if not act_df.empty else 0.0
    turnover = float(act_df.get("turnover_abs", pd.Series(dtype=float)).sum()) if not act_df.empty else 0.0
    mdd = max_drawdown(eq_df["equity"]) if not eq_df.empty else 0.0

    aq = action_quality_metrics(act_df.reset_index()) if not act_df.empty else {}
    perf_vol = performance_by_bucket(act_df.reset_index(), "vol_bucket") if not act_df.empty else {}
    perf_btc = performance_by_bucket(act_df.reset_index(), "btc_regime") if not act_df.empty else {}
    perf_family = performance_by_bucket(act_df.reset_index(), "candidate_family") if not act_df.empty else {}
    roll = rolling_performance(eq_df["equity"] if not eq_df.empty else pd.Series(dtype=float))
    consistency = action_consistency(act_df.reset_index()) if not act_df.empty else 0.0
    sens = turnover_cost_sensitivity(act_df.reset_index()) if not act_df.empty else {"turnover": 0.0, "cost_proxy": 0.0}

    diag = BacktestDiagnostics(
        calibration_proxy=cal,
        hit_rate_by_state=hr,
        expectancy_by_template=exp_tpl,
        action_rate=action_rate,
        turnover=turnover,
        max_drawdown=mdd,
        action_quality=aq,
        performance_by_vol_bucket=perf_vol,
        performance_by_btc_regime=perf_btc,
        performance_by_family=perf_family,
        rolling_performance=roll,
        action_consistency=consistency,
        turnover_cost_sensitivity=sens,
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

        # measured forecast distributions from OOS realized returns
        close_arr = close.astype(float)
        ret_h12 = np.array([float(close_arr.iloc[i + 12] / close_arr.iloc[i] - 1.0) for i in all_test_indices if (i + 12) < len(close_arr)], dtype=float)
        ret_h36 = np.array([float(close_arr.iloc[i + 36] / close_arr.iloc[i] - 1.0) for i in all_test_indices if (i + 36) < len(close_arr)], dtype=float)
        ret_h72 = np.array([float(close_arr.iloc[i + 72] / close_arr.iloc[i] - 1.0) for i in all_test_indices if (i + 72) < len(close_arr)], dtype=float)
        d12 = _distribution_from_returns(12, ret_h12)
        d36 = _distribution_from_returns(36, ret_h36)
        d72 = _distribution_from_returns(72, ret_h72)

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
                        "confidence": float(latest_action.get("p_up", d36["quantiles"]["p_up"]) or 0.0),
                        "entropy": None,
                        "calibration_score": calibration_score,
                        "distributions": {"h12": d12, "h36": d36, "h72": d72},
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
                        "governance_status": "BACKTEST_ONLY",
                        "kill_switch": False,
                        "kill_switch_reasons": ["backtest_context_not_live_governance"],
                        "timestamps": {"as_of": latest_ts},
                    },
                    "summary": summary,
                    "config": {
                        "walkforward": wf_cfg.__dict__,
                        "backtest": bt_cfg.__dict__,
                        "template_count": len(templates),
                        "candidate_id": (candidate_strategy.candidate_id if candidate_strategy is not None else "policy_baseline"),
                        "candidate_family": (candidate_strategy.family if candidate_strategy is not None else "policy"),
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
