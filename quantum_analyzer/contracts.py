from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
from typing import Any

ARTIFACT_SCHEMA_V2 = "2.0.0"


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
    target_position: float = 0.0  # generic model target, typically [-1, +1]
    expected_edge_bps: float = 0.0
    expected_cost_bps: float = 0.0
    reason: str = ""
    # explicit advisory semantics (for operator-facing mapping)
    advisory_mode: str = "spot_only"  # spot_only | derivatives_capable
    target_scope: str = "advisory_sleeve"  # advisory_sleeve | whole_wallet


@dataclass
class ArtifactBundleV2(SerializableContract):
    schema_version: str
    artifact_meta: dict[str, Any]
    forecast: dict[str, Any]
    proposal: dict[str, Any]
    drift: dict[str, Any]
    summary: dict[str, Any]
    config: dict[str, Any]

    @staticmethod
    def required_sections() -> list[str]:
        return ["schema_version", "artifact_meta", "forecast", "proposal", "drift", "summary", "config"]

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ArtifactBundleV2":
        missing = [k for k in cls.required_sections() if k not in d]
        if missing:
            raise ValueError(f"artifact bundle missing required sections: {', '.join(missing)}")
        if d.get("schema_version") != ARTIFACT_SCHEMA_V2:
            raise ValueError(f"unsupported schema_version: {d.get('schema_version')}")
        return cls(
            schema_version=str(d["schema_version"]),
            artifact_meta=dict(d["artifact_meta"]),
            forecast=dict(d["forecast"]),
            proposal=dict(d["proposal"]),
            drift=dict(d["drift"]),
            summary=dict(d["summary"]),
            config=dict(d["config"]),
        )


@dataclass
class PromotedSignalBundle(SerializableContract):
    status: str
    action: str
    confidence: float
    target_position: float
    reason: str
    action_masses: dict[str, float] = field(default_factory=dict)
    invalidation_reasons: list[str] = field(default_factory=list)
    source: dict[str, Any] = field(default_factory=dict)
