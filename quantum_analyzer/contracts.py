from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
from typing import Any


@dataclass
class SerializableContract:
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)


@dataclass
class FeatureSnapshot(SerializableContract):
    ts: datetime
    symbol: str
    feature_vector: dict[str, float] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class StateBelief(SerializableContract):
    ts: datetime
    symbol: str
    regime_probabilities: dict[str, float] = field(default_factory=dict)
    entropy: float = 0.0
    confidence: float = 0.0


@dataclass
class HorizonDistribution(SerializableContract):
    horizon_hours: int
    mean_return: float
    std_return: float
    quantiles: dict[str, float] = field(default_factory=dict)
    probability_up: float = 0.5


@dataclass
class ForecastBundle(SerializableContract):
    ts: datetime
    symbol: str
    distributions: dict[str, HorizonDistribution] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        payload["distributions"] = {
            k: v.to_dict() if hasattr(v, "to_dict") else v
            for k, v in self.distributions.items()
        }
        return payload


@dataclass
class ActionProposal(SerializableContract):
    ts: datetime
    symbol: str
    action: str  # e.g. HOLD | LONG | SHORT | REDUCE
    score: float
    size_fraction: float
    rationale: str = ""
    controls: dict[str, Any] = field(default_factory=dict)
    target_position: float = 0.0  # [-1, +1]
    expected_edge_bps: float = 0.0
    expected_cost_bps: float = 0.0
    reason: str = ""
