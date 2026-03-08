from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from typing import Any


@dataclass(frozen=True)
class ExperimentSpec:
    window_bars: int
    test_bars: int
    horizon: int
    feature_subset: str
    regime_slice: str
    policy_params: dict[str, Any]
    seed: int = 7

    def experiment_id(self, snapshot_id: str) -> str:
        payload = {
            "snapshot_id": snapshot_id,
            "window_bars": self.window_bars,
            "test_bars": self.test_bars,
            "horizon": self.horizon,
            "feature_subset": self.feature_subset,
            "regime_slice": self.regime_slice,
            "policy_params": self.policy_params,
            "seed": self.seed,
        }
        raw = json.dumps(payload, sort_keys=True).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:16]


@dataclass
class ExplorerRunManifest:
    preset: str
    snapshot_id: str
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    finished_at: str | None = None
    total_specs: int = 0
    succeeded: int = 0
    failed: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "preset": self.preset,
            "snapshot_id": self.snapshot_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "total_specs": self.total_specs,
            "succeeded": self.succeeded,
            "failed": self.failed,
        }
