from __future__ import annotations

from pathlib import Path
from typing import Any


def write_discovery_report(rows: list[dict[str, Any]], out_dir: str | Path) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    survivors = [r for r in rows if r.get("survival_status") == "survived"]
    rejected = [r for r in rows if r.get("survival_status") != "survived"]

    lines = [
        "# Discovery Report",
        "",
        f"Total candidates: {len(rows)}",
        f"Survivors: {len(survivors)}",
        f"Rejected: {len(rejected)}",
        "",
        "## Survivors",
    ]
    if not survivors:
        lines.append("- No trustworthy new signal found")
    else:
        for s in survivors[:20]:
            lines.append(f"- {s.get('candidate_id')}: robustness={s.get('robustness_score',0):.3f}, interpretability={s.get('interpretability_score',0):.3f}")

    lines += ["", "## Rejections"]
    for r in rejected[:30]:
        lines.append(f"- {r.get('candidate_id')}: {r.get('rejection_reason','unknown_reason')}")

    p = out / "discovery_report.md"
    p.write_text("\n".join(lines), encoding="utf-8")
    return p
