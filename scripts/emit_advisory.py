#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


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

    out_path = root / "advisory_latest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
