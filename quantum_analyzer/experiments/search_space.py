from __future__ import annotations

from collections import defaultdict

from .candidate_grid import expand_candidate_grid
from .specs import ExperimentSpec


def _balanced_candidate_sample(candidates: list[dict], limit: int) -> list[dict]:
    """Round-robin across family+horizon buckets to avoid truncation bias."""
    buckets: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for c in candidates:
        buckets[(str(c.get("family")), int(c.get("horizon", 0)))].append(c)

    out: list[dict] = []
    keys = sorted(buckets.keys())
    i = 0
    while len(out) < limit and keys:
        k = keys[i % len(keys)]
        b = buckets.get(k, [])
        if b:
            out.append(b.pop(0))
            if not b:
                keys = [x for x in keys if x != k]
                i = 0
                continue
        i += 1
    return out


def make_search_space(preset: str = "fast") -> list[ExperimentSpec]:
    preset = preset.lower()
    if preset == "fast":
        windows = [24 * 14, 24 * 30]
        tests = [24 * 5]
        horizons = [12, 36]
        subsets = ["geom_core", "geom_vol_cross"]
    elif preset == "daily":
        windows = [24 * 14, 24 * 30, 24 * 60, 24 * 90]
        tests = [24 * 7, 24 * 10]
        horizons = [12, 36, 72]
        subsets = ["geom_core", "geom_vol", "geom_vol_cross", "geom_vol_flow", "full_stack"]
    else:  # full
        windows = [24 * 30, 24 * 60, 24 * 90, 24 * 120]
        tests = [24 * 7, 24 * 10, 24 * 14]
        horizons = [12, 36, 72]
        subsets = ["geom_core", "geom_vol", "geom_vol_cross", "geom_vol_flow", "full_stack"]

    regimes = ["all", "low_vol", "mid_vol", "high_vol"]
    candidate_grid = expand_candidate_grid(
        {
            "families": ["trend", "mean_reversion", "breakout", "regime_switch", "ensemble", "ml_baseline"],
            "thresholds": [0.15, 0.25],
            "feature_subsets": subsets,
            "regime_filters": regimes,
        }
    )
    # bound runtime for continuous operator cycle
    if preset == "fast":
        candidate_grid = _balanced_candidate_sample(candidate_grid, 24)
    elif preset == "daily":
        candidate_grid = _balanced_candidate_sample(candidate_grid, 96)

    specs: list[ExperimentSpec] = []
    for w in windows:
        for t in tests:
            if t >= w:
                continue
            for h in horizons:
                for cand in candidate_grid:
                    if cand["horizon"] != h:
                        continue
                    p = {
                        "turnover_cap": 0.1,
                        "round_trip_cost_bps": float(cand.get("cost_assumption_bps", 15.0)),
                        "candidate_family": cand["family"],
                        "candidate_params": cand["params"],
                        "candidate_regime_filter": cand["regime_filter"],
                    }
                    specs.append(
                        ExperimentSpec(
                            window_bars=w,
                            test_bars=t,
                            horizon=h,
                            feature_subset=cand["feature_subset"],
                            regime_slice=cand["regime_filter"],
                            policy_params=p,
                            seed=7,
                        )
                    )
    return specs
