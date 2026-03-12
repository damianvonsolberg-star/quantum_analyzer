#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime, timezone


def _normalize_spot_action(action_raw: str) -> str:
    x = str(action_raw or "").strip().upper()
    if x in {"BUY", "LONG", "BUY SPOT"}:
        return "BUY"
    if x in {"REDUCE", "SELL", "SHORT", "REDUCE SPOT", "GO FLAT", "FLAT"}:
        return "REDUCE"
    if x == "HOLD":
        return "HOLD"
    return "WAIT"


def _bundle_subject(bundle: dict) -> dict:
    supporting = bundle.get("supporting_metrics", {}) if isinstance(bundle.get("supporting_metrics"), dict) else {}
    winner = supporting.get("supporting_metrics", {}) if isinstance(supporting.get("supporting_metrics"), dict) else {}
    selected_cluster = supporting.get("selected_cluster", {}) if isinstance(supporting.get("selected_cluster"), dict) else {}
    source = bundle.get("source", {}) if isinstance(bundle.get("source"), dict) else {}
    candidate_id = winner.get("candidate_id") or selected_cluster.get("candidate_id")
    return {
        "candidate_id": (str(candidate_id) if candidate_id else None),
        "promotion_cluster": (str(source.get("promotion_cluster")) if source.get("promotion_cluster") else None),
        "candidate_family": (str(winner.get("candidate_family")) if winner.get("candidate_family") else None),
        "feature_subset": (str(winner.get("feature_subset")) if winner.get("feature_subset") else None),
        "regime_slice": (str(winner.get("regime_slice")) if winner.get("regime_slice") else None),
        "horizon": (int(winner.get("horizon")) if isinstance(winner.get("horizon"), (int, float)) else None),
    }


def _is_cycle_stale() -> tuple[bool, str]:
    p = Path("artifacts/research_cycle_status.json")
    if not p.exists():
        return True, "missing_research_cycle_status"
    try:
        j = json.loads(p.read_text(encoding="utf-8"))
        fin = j.get("finished_at") or j.get("started_at")
        if not fin:
            return True, "missing_cycle_timestamp"
        t = datetime.fromisoformat(str(fin).replace("Z", "+00:00"))
        age_min = (datetime.now(timezone.utc) - t).total_seconds() / 60.0
        if age_min > 60:
            return True, f"cycle_stale_{int(age_min)}m"
        if str(j.get("state", "")).lower() == "failed":
            return True, "cycle_failed"
        return False, "fresh"
    except Exception:
        return True, "invalid_cycle_status"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-root", default="artifacts/promoted")
    args = ap.parse_args()

    root = Path(args.artifacts_root)
    now = datetime.now(timezone.utc).isoformat()
    bundle_path = root / "current_signal_bundle.json"
    if not bundle_path.exists():
        out = {
            "status": "missing_signal_bundle",
            "timestamp": now,
            "updated_at": now,
            "action_raw": "WAIT",
            "action_spot": "WAIT",
            "action": "WAIT",
            "target_position_raw": None,
            "target_position_spot": None,
            "target_position": None,
            "confidence": None,
            "entropy": None,
            "expected_edge_bps": None,
            "expected_cost_bps": None,
            "reason": "no_promoted_bundle",
            "selection_reasons": [],
            "risk_notes": ["No promoted signal bundle available."],
            "alternatives": [],
            "invalidation": [],
            "symbol": None,
            "timeframe": None,
            "source_ids": {},
            "warnings": ["advisory_only", "missing_signal_bundle", "missing_symbol_semantics", "missing_timeframe_semantics"],
        }
    else:
        b = json.loads(bundle_path.read_text(encoding="utf-8"))
        bundle_subject = _bundle_subject(b)
        supporting = (b.get("supporting_metrics", {}) or {}) if isinstance(b.get("supporting_metrics"), dict) else {}
        source = (b.get("source", {}) or {}) if isinstance(b.get("source"), dict) else {}
        expect = supporting.get("expectancy", None)
        edge_bps = float(expect) * 10_000.0 if isinstance(expect, (float, int)) else None
        cost_bps = supporting.get("expected_cost_bps", None)

        action_raw = str(b.get("action", "WAIT"))
        action_spot = _normalize_spot_action(action_raw)
        target_raw = (float(b.get("target_position")) if isinstance(b.get("target_position"), (float, int)) else None)
        target_spot = (max(0.0, target_raw) if target_raw is not None else None)

        out = {
            "status": b.get("status", "approved"),
            "timestamp": now,
            "updated_at": now,
            "action_raw": action_raw,
            "action_spot": action_spot,
            "action": action_spot,
            "target_position_raw": target_raw,
            "target_position_spot": target_spot,
            "target_position": target_spot,
            "confidence": (float(b.get("confidence")) if isinstance(b.get("confidence"), (float, int)) else None),
            "entropy": supporting.get("entropy", None),
            "expected_edge_bps": edge_bps,
            "expected_cost_bps": (float(cost_bps) if isinstance(cost_bps, (float, int)) else None),
            "expectancy": (float(expect) if isinstance(expect, (float, int)) else None),
            "regime": supporting.get("regime_explanation", "unknown"),
            "why_selected": b.get("reason", ""),
            "reason": b.get("reason", ""),
            "selection_reasons": supporting.get("reasons", []) if isinstance(supporting.get("reasons"), list) else [],
            "risk_notes": supporting.get("risk_notes", []) if isinstance(supporting.get("risk_notes"), list) else [],
            "alternatives": b.get("top_alternatives", []),
            "invalidation": b.get("invalidation_reasons", []),
            "symbol": (str(source.get("trading_symbol")) if source.get("trading_symbol") else None),
            "timeframe": (str(source.get("timeframe")) if source.get("timeframe") else None),
            "source_ids": {
                "leaderboard": source.get("leaderboard"),
                "promotion_cluster": source.get("promotion_cluster"),
            },
            "subject_ids": bundle_subject,
            "warnings": ["advisory_only"],
        }

    # release-gate override: refuse overconfident promotion when no edge
    rg = None
    rg_path = root / "release_gate_report.json"
    if rg_path.exists():
        try:
            rg = json.loads(rg_path.read_text(encoding="utf-8"))
        except Exception:
            rg = None

    # release-gate presence is mandatory for actionable trust semantics.
    if rg is None:
        out.update({
            "status": "insufficient_evidence",
            "reason": "missing_release_gate_report",
            "release_state": "NO_EDGE",
            "release_gate": {
                "passed": False,
                "overall_state": "NO_EDGE",
                "failures": ["missing_release_gate_report"],
                "failed_benchmarks": [],
                "missing_benchmarks": [],
                "human_reason": "Release gate report missing",
            },
            "release_gate_failures": ["missing_release_gate_report"],
        })
        ws = out.setdefault("warnings", [])
        if isinstance(ws, list):
            ws.append("missing_release_gate_report")

    # semantic completeness gate: missing core measured semantics -> insufficient evidence
    missing_semantics = []
    if out.get("target_position") is None:
        missing_semantics.append("missing_measured_target")
    if not out.get("symbol"):
        missing_semantics.append("missing_symbol_semantics")
    if not out.get("timeframe"):
        missing_semantics.append("missing_timeframe_semantics")
    if missing_semantics:
        out.update({
            "status": "insufficient_evidence",
            "reason": "missing_required_semantics",
            "release_state": "NO_EDGE",
        })
        ws = out.setdefault("warnings", [])
        if isinstance(ws, list):
            ws.extend(missing_semantics)

    if isinstance(rg, dict):
        out["release_gate"] = {
            "passed": bool(rg.get("passed", False)),
            "overall_state": rg.get("overall_state", "NO_EDGE"),
            "failures": rg.get("failures", []),
            "failed_benchmarks": rg.get("failed_benchmarks", []),
            "missing_benchmarks": rg.get("missing_benchmarks", []),
            "proxy_benchmarks": rg.get("proxy_benchmarks", []),
            "human_reason": rg.get("human_reason"),
        }
        out["release_state"] = rg.get("overall_state", out.get("release_state", "NO_EDGE"))
        rg_subject = rg.get("evaluated_subject", {}) if isinstance(rg.get("evaluated_subject"), dict) else {}
        bundle_subject = out.get("subject_ids", {}) if isinstance(out.get("subject_ids"), dict) else {}
        b_cid = bundle_subject.get("candidate_id")
        b_cluster = bundle_subject.get("promotion_cluster")
        r_cid = rg_subject.get("candidate_id")
        r_cluster = rg_subject.get("promotion_cluster")
        structured_keys = ["candidate_family", "feature_subset", "regime_slice", "horizon"]
        structured_available = all(bundle_subject.get(k) is not None and rg_subject.get(k) is not None for k in structured_keys)
        structured_match = (
            all(str(bundle_subject.get(k)) == str(rg_subject.get(k)) for k in structured_keys)
            if structured_available
            else None
        )
        subject_mismatch = False
        subject_missing = False
        if b_cid or b_cluster:
            if not (r_cid or r_cluster):
                subject_missing = True
            if b_cid and r_cid and str(b_cid) != str(r_cid) and structured_match is not True:
                subject_mismatch = True
            if b_cluster and r_cluster and str(b_cluster) != str(r_cluster):
                subject_mismatch = True
        if subject_missing or subject_mismatch:
            reason_code = "release_gate_subject_missing" if subject_missing else "release_gate_subject_mismatch"
            out.update({
                "status": "insufficient_evidence",
                "reason": reason_code,
                "release_state": "NO_EDGE",
                "release_gate_failures": sorted(set(list(out.get("release_gate_failures", [])) + [reason_code])),
            })
            out["release_gate"]["passed"] = False
            out["release_gate"]["overall_state"] = "NO_EDGE"
            fails = out["release_gate"].get("failures", [])
            if not isinstance(fails, list):
                fails = []
            if reason_code not in fails:
                fails.append(reason_code)
            out["release_gate"]["failures"] = fails
            ws = out.setdefault("warnings", [])
            if isinstance(ws, list):
                ws.append(reason_code)
        if not bool(rg.get("passed", False)):
            out.update({
                "status": "no_edge",
                "confidence": (min(float(out.get("confidence")), 0.2) if isinstance(out.get("confidence"), (float, int)) else None),
                "reason": "release_gates_failed",
                "release_state": rg.get("overall_state", "NO_EDGE"),
                "release_gate_failures": rg.get("failures", []),
            })
            hr = rg.get("human_reason")
            if hr:
                out["risk_notes"] = list(out.get("risk_notes", [])) + [str(hr)]
            ws = out.setdefault("warnings", [])
            if isinstance(ws, list):
                ws.append("release_gates_failed")

    stale, stale_reason = _is_cycle_stale()
    if stale:
        out.update({
            "status": "stale_cycle",
            "confidence": (min(float(out.get("confidence")), 0.2) if isinstance(out.get("confidence"), (float, int)) else None),
            "reason": stale_reason,
        })
        ws = out.setdefault("warnings", [])
        if isinstance(ws, list):
            ws.append("research_cycle_stale_or_failed")

    non_actionable = {"stale_cycle", "no_edge", "missing_signal_bundle", "insufficient_evidence"}
    is_edge = str(out.get("release_state", "")).upper() == "EDGE"
    is_ok = (str(out.get("status", "")).lower() == "approved") and is_edge

    ks_reasons = out.get("release_gate_failures", []) if isinstance(out.get("release_gate_failures"), list) else []
    if not ks_reasons:
        r = out.get("reason")
        if isinstance(r, str) and r:
            ks_reasons = [r]

    out["governance"] = {
        "overall_status": "OK" if is_ok else "WATCH",
        "kill_switch_active": (not is_ok) or (str(out.get("status", "")).lower() in non_actionable),
        "kill_switch_reasons": ks_reasons,
    }
    out["freshness"] = {
        "policy": "cycle_ttl_60m",
        "state": "stale" if stale else "fresh",
        "reason": stale_reason,
    }

    out_path = root / "advisory_latest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
