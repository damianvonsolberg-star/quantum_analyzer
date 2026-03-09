from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from quantum_analyzer.contracts import PromotedSignalBundle
from quantum_analyzer.discovery.survival import attach_survival_fields
from quantum_analyzer.decision.consensus import decide_action
from quantum_analyzer.decision.explainer import explain_decision
from quantum_analyzer.decision.invalidation import build_invalidation_notes
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

    # robust cluster promotion by (candidate_family, regime_slice)
    score_col = "robust_score" if "robust_score" in approved.columns else "score"
    grp_cols = [c for c in ["candidate_family", "regime_slice"] if c in approved.columns]
    if not grp_cols:
        grp_cols = ["promotion_status"]

    ranked = (
        approved.groupby(grp_cols, dropna=False)
        .agg(
            robust_score=(score_col, "mean"),
            support=(score_col, "count"),
            expectancy=("expectancy", "mean") if "expectancy" in approved.columns else (score_col, "mean"),
            target_position=("return_pct", lambda x: 0.30 if float(x.mean()) > 0 else 0.0),
        )
        .reset_index()
        .sort_values("robust_score", ascending=False)
    )

    ranked_rows: list[dict[str, Any]] = []
    approved_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    for _, r in ranked.iterrows():
        action_family = "BUY SPOT" if float(r.get("target_position", 0.0) or 0.0) > 0 else "REDUCE SPOT"
        row = {
            "candidate_id": f"{r.get('candidate_family','unknown')}:{r.get('regime_slice','all')}",
            "candidate_family": str(r.get("candidate_family", "unknown")),
            "regime_slice": str(r.get("regime_slice", "all")),
            "action": action_family,
            "target_position": float(r.get("target_position", 0.0) or 0.0),
            "vote_weight": float(r.get("robust_score", 0.0) or 0.0),
            "robust_score": float(r.get("robust_score", 0.0) or 0.0),
            "confidence": float(r.get("robust_score", 0.0) or 0.0),
            "expectancy": float(r.get("expectancy", 0.0) or 0.0),
            "sample_support": float(r.get("support", 0.0) or 0.0),
            "reason": "robust_cluster",
        }
        row["invalidation_notes"] = build_invalidation_notes(row)
        row.setdefault("oos_usefulness", 0.6)
        row.setdefault("neighbor_consistency", 0.7)
        row.setdefault("cross_window_repeatability", 0.7)
        row.setdefault("regime_specialization", 0.5)
        row.setdefault("redundancy", 0.3)
        row.setdefault("cost_adjusted_value", float(row.get("expectancy", 0.0) or 0.0))
        row = attach_survival_fields(row)
        ranked_rows.append(row)
        if row.get("survival_status") == "survived":
            approved_rows.append({"action": row["action"], "target_position": row["target_position"], "vote_weight": row["vote_weight"]})
        else:
            rejected_rows.append(row)

    # compare discovered vs baseline families and require material improvement for discovery promotion
    fam_scores: dict[str, float] = {}
    for r in ranked_rows:
        fam = str(r.get("candidate_family", "unknown"))
        fam_scores.setdefault(fam, 0.0)
        fam_scores[fam] = max(fam_scores[fam], float(r.get("robust_score", 0.0) or 0.0))
    best_disc = max((v for k, v in fam_scores.items() if "discover" in k), default=0.0)
    best_base = max((v for k, v in fam_scores.items() if "discover" not in k), default=0.0)
    discovered_material = bool(best_disc > (best_base + 0.02))

    selected = select_final_signal(approved_rows)
    decision = decide_action(ranked_rows)
    explain = explain_decision(decision, ranked_rows)

    reasons = build_invalidation_reasons(
        entropy=0.4,
        governance_status=governance_status,
        edge_bps=10.0,
        cost_bps=8.0,
    )
    # include cluster invalidation notes from winner
    if ranked_rows:
        reasons = sorted(set(reasons + list(ranked_rows[0].get("invalidation_notes", []))))

    bundle = PromotedSignalBundle(
        status="approved",
        action=str(selected.get("action", decision["action"])),
        confidence=float(selected.get("confidence", decision["confidence"])),
        target_position=float(selected.get("target_position", 0.0)),
        reason=str(selected.get("reason", decision["reason"])),
        action_masses=selected.get("action_masses", {}),
        invalidation_reasons=reasons,
        top_alternatives=decision.get("top_alternatives", []),
        supporting_metrics=explain,
        source={
            "approved_candidates": int(len(approved_rows)),
            "rejected_candidates": int(len(rejected_rows)),
            "leaderboard": str(lb_path),
            "promotion_cluster": "family_regime",
            "governance_status": governance_status,
            "family_scores": fam_scores,
            "discovered_material_improvement": discovered_material,
        },
    ).to_dict()

    (out_root / "current_signal_bundle.json").write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    (out_root / "current_signal_explainer.json").write_text(
        json.dumps({"decision": explain, "top_rows": approved.head(10).to_dict(orient="records")}, indent=2, default=str),
        encoding="utf-8",
    )
    lb.to_parquet(out_root / "current_leaderboard.parquet", index=False)
    (out_root / "current_run_manifest.json").write_text(
        json.dumps({"approved": int(len(approved)), "status": "approved", "governance_status": governance_status}, indent=2),
        encoding="utf-8",
    )
    # extended machine outputs
    (out_root / "ranked_candidates.json").write_text(json.dumps(ranked_rows, indent=2), encoding="utf-8")
    (out_root / "rejected_candidates.json").write_text(json.dumps(rejected_rows, indent=2), encoding="utf-8")
    (out_root / "signal_genealogy.json").write_text(json.dumps(ranked_rows, indent=2), encoding="utf-8")
    (out_root / "signals_latest.json").write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    (out_root / "regime_state.json").write_text(json.dumps({"regime": ranked_rows[0].get("regime_slice", "all") if ranked_rows else "unknown"}, indent=2), encoding="utf-8")
    (out_root / "backtest_summary.json").write_text(json.dumps({"top": ranked_rows[0] if ranked_rows else {}, "count": len(ranked_rows)}, indent=2), encoding="utf-8")

    return bundle
