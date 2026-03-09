from __future__ import annotations

from typing import Any


def _normalize_action(a: str) -> str:
    x = (a or "").upper()
    if x in {"LONG", "BUY", "BUY SPOT"}:
        return "BUY SPOT"
    if x in {"REDUCE", "SELL", "REDUCE SPOT"}:
        return "REDUCE SPOT"
    if x in {"FLAT", "GO FLAT"}:
        return "GO FLAT"
    return "HOLD"


def select_final_signal(
    approved_candidates: list[dict[str, Any]],
    *,
    min_mass: float = 0.35,
    min_margin: float = 0.10,
    require_trustworthy: bool = True,
) -> dict[str, Any]:
    if not approved_candidates:
        return {
            "action": "WAIT",
            "reason": "no_trustworthy_new_signal_found" if require_trustworthy else "no_approved_candidates",
            "confidence": 0.0,
            "target_position": 0.0,
        }

    buckets: dict[str, float] = {"BUY SPOT": 0.0, "HOLD": 0.0, "REDUCE SPOT": 0.0, "GO FLAT": 0.0}
    weighted_targets: list[tuple[float, float]] = []

    for c in approved_candidates:
        action = _normalize_action(str(c.get("action", "HOLD")))
        w = float(c.get("vote_weight", 0.0) or 0.0)
        buckets[action] += max(w, 0.0)
        weighted_targets.append((float(c.get("target_position", 0.0) or 0.0), max(w, 0.0)))

    total = sum(buckets.values())
    if total <= 0:
        return {"action": "HOLD", "reason": "zero_vote_mass", "confidence": 0.0, "target_position": 0.0}

    ranked = sorted(buckets.items(), key=lambda kv: kv[1], reverse=True)
    top_action, top_mass = ranked[0]
    second_mass = ranked[1][1] if len(ranked) > 1 else 0.0

    top_share = top_mass / total
    margin = (top_mass - second_mass) / total

    alternatives = [
        {"action": a, "mass_share": float(m / total)}
        for a, m in ranked[1:4]
    ]

    if top_share < min_mass or margin < min_margin:
        return {
            "action": "HOLD",
            "reason": "weak_action_margin",
            "confidence": float(top_share),
            "target_position": 0.0,
            "action_masses": {k: (v / total) for k, v in buckets.items()},
            "alternatives": alternatives,
        }

    # weighted median target among top-action members
    vals: list[tuple[float, float]] = []
    for c in approved_candidates:
        if _normalize_action(str(c.get("action", "HOLD"))) != top_action:
            continue
        w = float(c.get("vote_weight", 0.0) or 0.0)
        vals.append((float(c.get("target_position", 0.0) or 0.0), max(w, 0.0)))

    if not vals:
        target = 0.0
    else:
        vals = sorted(vals, key=lambda x: x[0])
        wsum = sum(w for _, w in vals)
        if wsum <= 0:
            target = vals[len(vals) // 2][0]
        else:
            c = 0.0
            target = vals[-1][0]
            for v, w in vals:
                c += w / wsum
                if c >= 0.5:
                    target = v
                    break

    # spot-safe clipping here is explicit and reasoned
    if top_action in {"BUY SPOT", "HOLD"}:
        target = max(0.0, target)

    return {
        "action": top_action,
        "reason": "promoted_cluster_consensus",
        "confidence": float(top_share),
        "target_position": float(target),
        "action_masses": {k: (v / total) for k, v in buckets.items()},
        "margin": float(margin),
        "alternatives": alternatives,
    }
