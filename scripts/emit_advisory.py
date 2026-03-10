#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime, timezone


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
            "target_position_raw": 0.0,
            "target_position_spot": 0.0,
            "target_position": 0.0,
            "confidence": 0.0,
            "entropy": None,
            "expected_edge_bps": None,
            "expected_cost_bps": None,
            "reason": "no_promoted_bundle",
            "selection_reasons": [],
            "risk_notes": ["No promoted signal bundle available."],
            "alternatives": [],
            "invalidation": [],
            "symbol": "SOLUSDC",
            "timeframe": "1h",
            "source_ids": {},
            "warnings": ["advisory_only"],
        }
    else:
        b = json.loads(bundle_path.read_text(encoding="utf-8"))
        supporting = (b.get("supporting_metrics", {}) or {}) if isinstance(b.get("supporting_metrics"), dict) else {}
        source = (b.get("source", {}) or {}) if isinstance(b.get("source"), dict) else {}
        expect = supporting.get("expectancy", None)
        edge_bps = float(expect) * 10_000.0 if isinstance(expect, (float, int)) else None
        cost_bps = supporting.get("expected_cost_bps", None)

        action_raw = str(b.get("action", "WAIT"))
        action_spot = action_raw if action_raw in {"BUY", "HOLD", "REDUCE", "WAIT"} else "WAIT"

        out = {
            "status": b.get("status", "approved"),
            "timestamp": now,
            "updated_at": now,
            "action_raw": action_raw,
            "action_spot": action_spot,
            "action": action_spot,
            "target_position_raw": (float(b.get("target_position")) if isinstance(b.get("target_position"), (float, int)) else None),
            "target_position_spot": (float(b.get("target_position")) if isinstance(b.get("target_position"), (float, int)) else None),
            "target_position": (float(b.get("target_position")) if isinstance(b.get("target_position"), (float, int)) else None),
            "confidence": float(b.get("confidence", 0.0) or 0.0),
            "entropy": supporting.get("entropy", None),
            "expected_edge_bps": edge_bps,
            "expected_cost_bps": (float(cost_bps) if isinstance(cost_bps, (float, int)) else None),
            "expectancy": expect if isinstance(expect, (float, int)) else 0.0,
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
            "action": "WAIT",
            "action_spot": "WAIT",
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
        }
        if not bool(rg.get("passed", False)):
            out.update({
                "status": "no_edge",
                "action": "WAIT",
                "action_spot": "WAIT",
                "confidence": min(float(out.get("confidence", 0.0) or 0.0), 0.2),
                "reason": "release_gates_failed",
                "release_state": rg.get("overall_state", "NO_EDGE"),
                "release_gate_failures": rg.get("failures", []),
            })
            ws = out.setdefault("warnings", [])
            if isinstance(ws, list):
                ws.append("release_gates_failed")

    stale, stale_reason = _is_cycle_stale()
    if stale:
        out.update({
            "status": "stale_cycle",
            "action": "WAIT" if out.get("action") not in {"HOLD", "WAIT"} else out.get("action"),
            "action_spot": "WAIT" if out.get("action_spot") not in {"HOLD", "WAIT"} else out.get("action_spot"),
            "confidence": min(float(out.get("confidence", 0.0) or 0.0), 0.2),
            "reason": stale_reason,
        })
        ws = out.setdefault("warnings", [])
        if isinstance(ws, list):
            ws.append("research_cycle_stale_or_failed")

    out["governance"] = {
        "overall_status": "OK" if out.get("status") not in {"stale_cycle", "no_edge", "missing_signal_bundle"} else "WATCH",
        "kill_switch_active": bool(out.get("status") in {"stale_cycle", "no_edge", "missing_signal_bundle"}),
        "kill_switch_reasons": out.get("release_gate_failures", []) if isinstance(out.get("release_gate_failures"), list) else [],
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
