from __future__ import annotations

from typing import Any


def signal_decay_status(row: dict[str, Any]) -> dict[str, Any]:
    robust = float(row.get("robustness_score", 0.0) or 0.0)
    specialization = float(row.get("regime_specialization", 0.0) or 0.0)
    status = "ok"
    reasons: list[str] = []
    if robust < 0.35:
        status = "degraded"
        reasons.append("robustness_decay")
    if specialization > 0.9:
        status = "narrow"
        reasons.append("regime_narrowing")
    if robust < 0.2:
        status = "retire"
        reasons.append("retire_signal")
    return {"status": status, "reasons": reasons, "robustness_score": robust, "regime_specialization": specialization}
