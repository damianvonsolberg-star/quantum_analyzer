from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_json(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        j = json.loads(p.read_text(encoding="utf-8"))
        return j if isinstance(j, list) else []
    except Exception:
        return []


def discovery_summary(root: str | Path = "artifacts/discovery") -> dict[str, Any]:
    r = Path(root)
    surv = load_json(r / "surviving_signals.json")
    rej = load_json(r / "rejected_signals.json")
    disc = load_json(r / "discovered_signals.json")
    return {
        "discovered": len(disc),
        "surviving": len(surv),
        "rejected": len(rej),
        "uses_discovered_signal": any("discover" in str(x.get("candidate_id", "")) for x in surv),
        "survivors": surv,
        "rejections": rej,
    }
