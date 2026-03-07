from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PolicyConfig:
    entropy_threshold: float = 0.75
    calibration_threshold: float = 0.55
    no_trade_band_bps: float = 8.0


def edge_bps(mean_return: float) -> float:
    return float(mean_return * 10_000)


def in_no_trade_region(expected_edge_bps: float, expected_cost_bps: float, band_bps: float) -> bool:
    net = expected_edge_bps - expected_cost_bps
    return abs(net) < band_bps
