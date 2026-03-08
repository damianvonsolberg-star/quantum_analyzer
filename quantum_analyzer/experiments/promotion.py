from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from quantum_analyzer.contracts import PromotedSignalBundle
from quantum_analyzer.signals.invalidation import build_invalidation_reasons
from quantum_analyzer.signals.selector import select_final_signal


def _assign_candidate_status(row: pd.Series, min_score: float) -> str:
    if not bool(row.get("hard_gate_pass", False)):
        return "rejected"
    if float(row.get("score", 0.0) or 0.0) < min_score:
        return "pending"
    return "approved"


def promote_from_leaderboard(
    explorer_root: str | Path,
    out_root: str | Path,
    *,
    min_score: float = 0.25,
    governance_status: str = "OK",
) -> dict[str, Any]:
    explorer_root = Path(explorer_root)
    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    lb_path = explorer_root / "leaderboard.parquet"
    if not lb_path.exists():
        out = {
            "status": "no_leaderboard",
            "action": "HOLD",
            "reason": "leaderboard_missing",
            "confidence": 0.0,
            "target_position": 0.0,
            "action_masses": {},
            "invalidation_reasons": ["leaderboard_missing"],
            "source": {"approved_candidates": 0},
        }
        (out_root / "current_signal_bundle.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
        return out

    lb = pd.read_parquet(lb_path)
    if lb.empty:
        out = {
            "status": "no_candidates",
            "action": "HOLD",
            "reason": "leaderboard_empty",
            "confidence": 0.0,
            "target_position": 0.0,
            "action_masses": {},
            "invalidation_reasons": ["leaderboard_empty"],
            "source": {"approved_candidates": 0, "leaderboard": str(lb_path)},
        }
        (out_root / "current_signal_bundle.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
        return out

    lb = lb.copy()
    lb["promotion_status"] = lb.apply(lambda r: _assign_candidate_status(r, min_score), axis=1)
    approved = lb[lb["promotion_status"] == "approved"].copy()

    if governance_status.upper() != "OK":
        out = {
            "status": "shadow_only",
            "action": "HOLD",
            "reason": f"governance_{governance_status.lower()}",
            "confidence": 0.0,
            "target_position": 0.0,
            "action_masses": {},
            "invalidation_reasons": ["governance_not_ok"],
            "source": {"approved_candidates": int(len(approved)), "leaderboard": str(lb_path)},
        }
        (out_root / "current_signal_bundle.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
        (out_root / "current_leaderboard.parquet").unlink(missing_ok=True)
        lb.to_parquet(out_root / "current_leaderboard.parquet", index=False)
        (out_root / "current_run_manifest.json").write_text(
            json.dumps({"approved": int(len(approved)), "status": "shadow_only", "governance_status": governance_status}, indent=2),
            encoding="utf-8",
        )
        return out

    if approved.empty:
        out = {
            "status": "no_approved",
            "action": "HOLD",
            "reason": "no_passing_candidates",
            "confidence": 0.0,
            "target_position": 0.0,
            "action_masses": {},
            "invalidation_reasons": ["no_passing_candidates"],
            "source": {"approved_candidates": 0, "leaderboard": str(lb_path)},
        }
        (out_root / "current_signal_bundle.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
        lb.to_parquet(out_root / "current_leaderboard.parquet", index=False)
        (out_root / "current_run_manifest.json").write_text(json.dumps({"approved": 0, "status": "no_approved"}, indent=2), encoding="utf-8")
        return out

    # cluster-level promotion (action family)
    approved_rows: list[dict[str, Any]] = []
    for _, r in approved.iterrows():
        action_family = "BUY SPOT" if float(r.get("return_pct", 0.0) or 0.0) > 0 else "REDUCE SPOT"
        approved_rows.append(
            {
                "action": action_family,
                "target_position": 0.35 if action_family == "BUY SPOT" else 0.0,
                "vote_weight": float(r.get("score", 0.0) or 0.0),
            }
        )

    selected = select_final_signal(approved_rows)
    reasons = build_invalidation_reasons(
        entropy=0.4,
        governance_status=governance_status,
        edge_bps=10.0,
        cost_bps=8.0,
    )

    bundle = PromotedSignalBundle(
        status="approved",
        action=str(selected["action"]),
        confidence=float(selected["confidence"]),
        target_position=float(selected["target_position"]),
        reason=str(selected["reason"]),
        action_masses=selected.get("action_masses", {}),
        invalidation_reasons=reasons,
        source={
            "approved_candidates": int(len(approved)),
            "leaderboard": str(lb_path),
            "promotion_cluster": "action_family",
            "governance_status": governance_status,
        },
    ).to_dict()

    (out_root / "current_signal_bundle.json").write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    (out_root / "current_signal_explainer.json").write_text(
        json.dumps({"top_rows": approved.head(10).to_dict(orient="records")}, indent=2, default=str),
        encoding="utf-8",
    )
    lb.to_parquet(out_root / "current_leaderboard.parquet", index=False)
    (out_root / "current_run_manifest.json").write_text(
        json.dumps({"approved": int(len(approved)), "status": "approved", "governance_status": governance_status}, indent=2),
        encoding="utf-8",
    )

    return bundle
