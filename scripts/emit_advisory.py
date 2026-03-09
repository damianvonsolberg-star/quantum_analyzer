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
    bundle_path = root / "current_signal_bundle.json"
    if not bundle_path.exists():
        out = {
            "status": "missing_signal_bundle",
            "action": "WAIT",
            "confidence": 0.0,
            "reason": "no_promoted_bundle",
        }
    else:
        b = json.loads(bundle_path.read_text(encoding="utf-8"))
        out = {
            "status": b.get("status", "approved"),
            "action": b.get("action", "WAIT"),
            "confidence": b.get("confidence", 0.0),
            "expectancy": (b.get("supporting_metrics", {}) or {}).get("expectancy", 0.0),
            "regime": (b.get("supporting_metrics", {}) or {}).get("regime_explanation", "unknown"),
            "why_selected": b.get("reason", ""),
            "alternatives": b.get("top_alternatives", []),
            "invalidation": b.get("invalidation_reasons", []),
            "warnings": ["advisory_only"],
        }

    # release-gate override: refuse overconfident promotion when no edge
    rg_path = root / "release_gate_report.json"
    if rg_path.exists():
        try:
            rg = json.loads(rg_path.read_text(encoding="utf-8"))
            if not bool(rg.get("passed", False)):
                out.update({
                    "status": "no_edge",
                    "action": "WAIT",
                    "confidence": min(float(out.get("confidence", 0.0) or 0.0), 0.2),
                    "reason": "release_gates_failed",
                    "release_state": rg.get("overall_state", "NO_EDGE"),
                    "release_gate_failures": rg.get("failures", []),
                })
                ws = out.setdefault("warnings", [])
                if isinstance(ws, list):
                    ws.append("release_gates_failed")
        except Exception:
            pass

    stale, stale_reason = _is_cycle_stale()
    if stale:
        out.update({
            "status": "stale_cycle",
            "action": "WAIT" if out.get("action") not in {"HOLD", "WAIT"} else out.get("action"),
            "confidence": min(float(out.get("confidence", 0.0) or 0.0), 0.2),
            "reason": stale_reason,
        })
        ws = out.setdefault("warnings", [])
        if isinstance(ws, list):
            ws.append("research_cycle_stale_or_failed")

    out_path = root / "advisory_latest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
