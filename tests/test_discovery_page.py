from __future__ import annotations

import json
from pathlib import Path

from ui.discovery import discovery_summary


def test_discovery_summary_reads_artifacts(tmp_path: Path):
    root = tmp_path / "discovery"
    root.mkdir(parents=True, exist_ok=True)
    (root / "discovered_signals.json").write_text(json.dumps([{"candidate_id": "d1"}]))
    (root / "surviving_signals.json").write_text(json.dumps([{"candidate_id": "discover:d1"}]))
    (root / "rejected_signals.json").write_text(json.dumps([{"candidate_id": "d2"}]))

    s = discovery_summary(root)
    assert s["discovered"] == 1
    assert s["surviving"] == 1
    assert s["rejected"] == 1
    assert s["uses_discovered_signal"] is True
