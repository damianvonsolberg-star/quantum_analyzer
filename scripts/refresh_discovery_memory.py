#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

from quantum_analyzer.discovery.genealogy import build_genealogy_entry
from quantum_analyzer.discovery.meta_research import write_feature_importance_drift, write_signal_decay_monitor
from quantum_analyzer.discovery.report import write_discovery_report
from quantum_analyzer.discovery.survival import attach_survival_fields


def main() -> int:
    root = Path("artifacts/discovery")
    disc = root / "discovered_signals.json"
    if not disc.exists():
        print("no discovered_signals.json")
        return 1
    rows = json.loads(disc.read_text(encoding="utf-8"))

    enriched = []
    genealogy = []
    for r in rows:
        x = dict(r)
        x.setdefault("oos_usefulness", x.get("action_usefulness", 0.0))
        x.setdefault("neighbor_consistency", 0.7)
        x.setdefault("cross_window_repeatability", 0.7)
        x.setdefault("regime_specialization", 0.5)
        x.setdefault("robustness_score", float(x.get("robust_value", 0.0)))
        x.setdefault("interpretability_score", max(0.0, 1.0 - float(x.get("complexity_penalty", 0.0))))
        x = attach_survival_fields(x)
        enriched.append(x)

        genealogy.append(
            build_genealogy_entry(
                candidate_id=str(x.get("candidate_id")),
                genome=x.get("genome", {}),
                parent_features=list((x.get("genome", {}) or {}).get("parent_features", [])),
                method=str(x.get("method", "unknown")),
                transforms=list((x.get("genome", {}) or {}).get("transforms", [])),
                params=(x.get("genome", {}) or {}),
                timeframe=str(x.get("timeframe", "1h")),
                validation={
                    "expectancy": x.get("expectancy"),
                    "sample_support": x.get("sample_support"),
                    "cost_adjusted_value": x.get("cost_adjusted_value"),
                },
                robustness_score=float(x.get("robustness_score", 0.0) or 0.0),
                interpretability_score=float(x.get("interpretability_score", 0.0) or 0.0),
                survival_status=str(x.get("survival_status", "rejected")),
                rejection_reason=x.get("rejection_reason"),
            )
        )

    root.mkdir(parents=True, exist_ok=True)
    (root / "discovered_signals.json").write_text(json.dumps(enriched, indent=2, default=str), encoding="utf-8")
    (root / "signal_genealogy.json").write_text(json.dumps(genealogy, indent=2, default=str), encoding="utf-8")

    write_feature_importance_drift(genealogy, root)
    write_signal_decay_monitor(enriched, root)
    write_discovery_report(genealogy, root)

    print(json.dumps({"updated": len(enriched), "survived": sum(1 for x in enriched if x.get('survival_status')=='survived')}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
