from __future__ import annotations

from typing import Any


def expand_candidate_grid(base: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    base = base or {}
    families = base.get("families", ["trend", "mean_reversion", "breakout", "regime_switch", "ensemble", "ml_baseline"])
    thresholds = base.get("thresholds", [0.15, 0.25])
    lookbacks = base.get("lookbacks", [24, 72])
    horizons = base.get("holding_horizon", [12, 36])
    cooldowns = base.get("cooldowns", [0, 3])
    subsets = base.get("feature_subsets", ["geom_core", "geom_vol", "full_stack"])
    regime_filters = base.get("regime_filters", ["all", "low_vol", "high_vol"])
    costs = base.get("cost_assumptions", [10.0, 15.0, 20.0])

    out: list[dict[str, Any]] = []
    for fam in families:
        for th in thresholds:
            for lb in lookbacks:
                for hz in horizons:
                    for cd in cooldowns:
                        for ss in subsets:
                            for rg in regime_filters:
                                for c in costs:
                                    out.append(
                                        {
                                            "family": fam,
                                            "params": {
                                                "buy_threshold": float(th),
                                                "reduce_threshold": float(-th),
                                                "lookback": int(lb),
                                                "holding_horizon": int(hz),
                                                "cooldown": int(cd),
                                            },
                                            "feature_subset": ss,
                                            "horizon": int(hz),
                                            "regime_filter": rg,
                                            "cost_assumption_bps": float(c),
                                        }
                                    )
    return out
