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

    # cluster promotion by family/subset/regime/horizon (+local parameter neighborhood when available)
    score_col = "promoted_score" if "promoted_score" in approved.columns else ("robust_score" if "robust_score" in approved.columns else "score")
    grp_cols = [c for c in ["candidate_family", "feature_subset", "regime_slice", "horizon"] if c in approved.columns]
    if "policy_params_hash" in approved.columns:
        grp_cols.append("policy_params_hash")
    if not grp_cols:
        grp_cols = ["promotion_status"]

    def _weighted_median(values: pd.Series, weights: pd.Series) -> float:
        if values.empty:
            return 0.0
        v = values.astype(float).fillna(0.0).to_numpy()
        w = weights.astype(float).fillna(0.0).to_numpy()
        if float(w.sum()) <= 0.0:
            return float(pd.Series(v).median())
        order = v.argsort()
        v_sorted = v[order]
        w_sorted = w[order]
        cdf = w_sorted.cumsum() / w_sorted.sum()
        return float(v_sorted[(cdf >= 0.5).argmax()])

    rows = []
    for keys, g in approved.groupby(grp_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        d = {k: v for k, v in zip(grp_cols, keys)}
        weights = g[score_col].astype(float).fillna(0.0)
        robust_score = float((g[score_col].astype(float).fillna(0.0)).mean())
        support = int(len(g))
        expectancy = float(g["expectancy"].astype(float).mean()) if "expectancy" in g.columns else robust_score
        context_match = float(g["context_match"].astype(float).mean()) if "context_match" in g.columns else 0.5
        agreement = float((weights > 0).mean()) if len(weights) else 0.0
        cluster_score = float(max(0.0, min(1.0, robust_score * (0.7 + 0.3 * context_match) * agreement)))

        if "target_position" in g.columns:
            tp = _weighted_median(g["target_position"], weights)
        else:
            # Do not synthesize target from sign(return); require measured target evidence.
            tp = 0.0

        d.update(
            {
                "robust_score": robust_score,
                "cluster_score": cluster_score,
                "support": support,
                "agreement": agreement,
                "context_match": context_match,
                "expectancy": expectancy,
                "target_position": float(tp),
            }
        )
        rows.append(d)

    ranked = pd.DataFrame(rows).sort_values("cluster_score", ascending=False)

    ranked_rows: list[dict[str, Any]] = []
    approved_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    for _, r in ranked.iterrows():
        action_family = "BUY SPOT" if float(r.get("target_position", 0.0) or 0.0) > 0 else "REDUCE SPOT"
        row = {
            "candidate_id": f"{r.get('candidate_family','unknown')}:{r.get('feature_subset','all')}:{r.get('regime_slice','all')}:{r.get('horizon','na')}",
            "candidate_family": str(r.get("candidate_family", "unknown")),
            "feature_subset": str(r.get("feature_subset", "all")),
            "regime_slice": str(r.get("regime_slice", "all")),
            "horizon": r.get("horizon", None),
            "action": action_family,
            "target_position": float(r.get("target_position", 0.0) or 0.0),
            "vote_weight": float(r.get("cluster_score", 0.0) or 0.0),
            "robust_score": float(r.get("robust_score", 0.0) or 0.0),
            "cluster_score": float(r.get("cluster_score", 0.0) or 0.0),
            "confidence": float(r.get("cluster_score", 0.0) or 0.0),
            "expectancy": float(r.get("expectancy", 0.0) or 0.0),
            "sample_support": float(r.get("support", 0.0) or 0.0),
            "agreement": float(r.get("agreement", 0.0) or 0.0),
            "context_match": float(r.get("context_match", 0.5) or 0.5),
            "reason": "cluster_consensus",
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

    top = ranked_rows[0] if ranked_rows else {}
    ent = top.get("entropy", top.get("entropy_quality", None))
    cost = top.get("expected_cost_bps", top.get("cost_drift_bps", None))
    if ent is None or cost is None:
        reasons = ["invalidation_unavailable_measured_entropy_or_cost_missing"]
    else:
        reasons = build_invalidation_reasons(
            entropy=float(ent),
            governance_status=governance_status,
            edge_bps=float(top.get("expectancy", 0.0) or 0.0) * 10_000.0,
            cost_bps=float(cost),
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
