from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

ARTIFACT_SCHEMA_V2 = "2.0.0"
REQUIRED_BUNDLE_SECTIONS_V2 = [
    "schema_version",
    "artifact_meta",
    "forecast",
    "proposal",
    "drift",
    "summary",
    "config",
]


@dataclass
class ArtifactCheck:
    name: str
    present: bool
    status: str  # pass|warn|fail
    message: str = ""


@dataclass
class DoctorReport:
    artifact_dir: str
    checks: list[ArtifactCheck] = field(default_factory=list)
    schema_versions: list[str] = field(default_factory=list)
    latest_timestamp: str | None = None
    latest_proposal_action: dict[str, Any] = field(default_factory=dict)
    latest_forecast_horizons: list[str] = field(default_factory=list)
    latest_backtest_metrics: dict[str, Any] = field(default_factory=dict)
    missing_files: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    hard_failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.hard_failures) == 0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["ok"] = self.ok
        return payload
