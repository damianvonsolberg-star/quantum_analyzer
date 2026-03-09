from __future__ import annotations

from typing import Any


def build_invalidation_notes(row: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    if float(row.get("max_drawdown", 0.0) or 0.0) < -0.35:
        notes.append("drawdown_breach")
    if float(row.get("expectancy", 0.0) or 0.0) <= 0.0:
        notes.append("non_positive_expectancy")
    if float(row.get("action_quality", 0.0) or 0.0) < 0.3:
        notes.append("low_action_quality")
    if float(row.get("sample_support", 0.0) or 0.0) < 20:
        notes.append("low_sample_support")
    if float(row.get("regime_worst", 0.0) or 0.0) < -0.02:
        notes.append("catastrophic_regime_slice")
    return notes
