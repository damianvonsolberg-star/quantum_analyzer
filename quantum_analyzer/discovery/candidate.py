from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DiscoveryCandidate:
    candidate_id: str
    genome: dict[str, Any]
    feature_subset: str = "full_stack"
    method: str = "random"
    novelty: float = 0.0
    complexity: float = 0.0
    diagnostics: dict[str, Any] = field(default_factory=dict)
