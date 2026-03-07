from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import math
import numpy as np

from quantum_analyzer.contracts import ForecastBundle, HorizonDistribution, StateBelief
from quantum_analyzer.paths.archetypes import PathTemplate


@dataclass
class MixtureConfig:
    horizons: tuple[int, int, int] = (12, 36, 72)
    base_sigma: dict[int, float] | None = None

    def sigma_for(self, h: int) -> float:
        if self.base_sigma and h in self.base_sigma:
            return self.base_sigma[h]
        return {12: 0.02, 36: 0.04, 72: 0.06}.get(h, 0.05)


def _normal_quantiles(mu: float, sigma: float) -> dict[str, float]:
    # z for [5,25,50,75,95]
    z = {
        "q05": -1.6448536269514722,
        "q25": -0.6744897501960817,
        "q50": 0.0,
        "q75": 0.6744897501960817,
        "q95": 1.6448536269514722,
    }
    return {k: float(mu + v * sigma) for k, v in z.items()}


def _std_norm_cdf(x: float) -> float:
    # fast erf-based CDF
    return float(0.5 * (1.0 + math.erf(x / math.sqrt(2.0))))


def _state_bias(belief: StateBelief) -> float:
    p = belief.regime_probabilities
    up = p.get("trend_up", 0.0) + p.get("breakout_up", 0.0) + p.get("stabilization", 0.0) * 0.5
    dn = p.get("trend_down", 0.0) + p.get("breakdown_down", 0.0) + p.get("capitulation", 0.0) * 0.5
    return float(up - dn)


def _template_bias(templates: Iterable[PathTemplate]) -> float:
    t = list(templates)
    if not t:
        return 0.0
    w = np.array([max(x.support, 1e-6) for x in t], dtype=float)
    e = np.array([x.expectancy for x in t], dtype=float)
    return float((w * e).sum() / w.sum())


def build_forecast_bundle(
    symbol: str,
    belief: StateBelief,
    templates: list[PathTemplate],
    cfg: MixtureConfig | None = None,
    calibration_score: float = 0.5,
) -> ForecastBundle:
    cfg = cfg or MixtureConfig()

    state_b = _state_bias(belief)
    path_b = _template_bias(templates)

    distributions: dict[str, HorizonDistribution] = {}
    entropy = float(belief.entropy)

    for h in cfg.horizons:
        mu = 0.25 * state_b + 0.75 * path_b * (h / 36.0) ** 0.5
        sigma = cfg.sigma_for(h) * (1.0 + 0.5 * entropy)
        q = _normal_quantiles(mu, sigma)
        p_up = 1.0 - _std_norm_cdf((0.0 - mu) / sigma)

        # breakout tails relative to +-1 sigma events
        p_break_up = 1.0 - _std_norm_cdf((sigma - mu) / sigma)
        p_break_down = _std_norm_cdf((-sigma - mu) / sigma)

        distributions[f"h{h}"] = HorizonDistribution(
            horizon_hours=h,
            mean_return=float(mu),
            std_return=float(sigma),
            quantiles=q,
            probability_up=float(p_up),
        )

        # attach extra directional diagnostics in quantiles payload for contract stability
        distributions[f"h{h}"].quantiles.update(
            {
                "p_up": float(p_up),
                "p_down": float(1.0 - p_up),
                "p_break_up": float(p_break_up),
                "p_break_down": float(p_break_down),
            }
        )

    return ForecastBundle(
        ts=datetime.now(timezone.utc),
        symbol=symbol,
        distributions=distributions,
        diagnostics={
            "entropy": entropy,
            "calibration_score": float(calibration_score),
            "state_bias": state_b,
            "path_bias": path_b,
        },
    )
