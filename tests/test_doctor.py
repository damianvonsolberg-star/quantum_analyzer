from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pandas as pd


def _seed_artifacts(root: Path, include_optional: bool = True) -> None:
    root.mkdir(parents=True, exist_ok=True)

    (root / "artifact_bundle.json").write_text(
        json.dumps(
            {
                "summary": {"ok": True},
                "forecast": {"distributions": {"h12": {}, "h36": {}, "h72": {}}},
                "schema_version": "1.0.0",
            }
        )
    )
    (root / "summary.json").write_text(
        json.dumps(
            {
                "bars": 100,
                "test_bars": 20,
                "ending_equity": 1010000,
                "return_pct": 0.01,
                "diagnostics": {"max_drawdown": -0.02},
                "schema_version": "1.0.0",
            }
        )
    )

    pd.DataFrame({"ts": ["2026-03-01T00:00:00Z"], "equity": [1_000_000], "pnl": [0]}).to_csv(root / "equity_curve.csv", index=False)
    pd.DataFrame(
        {
            "ts": ["2026-03-01T00:00:00Z"],
            "action": ["HOLD"],
            "target_position": [0.0],
            "expected_edge_bps": [5.0],
            "expected_cost_bps": [10.0],
            "reason": ["test"],
        }
    ).to_csv(root / "actions.csv", index=False)

    if include_optional:
        (root / "templates.json").write_text("[]")


def test_doctor_runs_clean_and_emits_report(tmp_path: Path) -> None:
    art = tmp_path / "art"
    _seed_artifacts(art, include_optional=True)

    script = Path(__file__).resolve().parents[1] / "scripts" / "qa_doctor.py"
    proc = subprocess.run(
        [sys.executable, str(script), "--artifacts", str(art)],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + "\n" + proc.stderr

    report = art / "doctor_report.json"
    assert report.exists()
    data = json.loads(report.read_text())
    assert data["ok"] is True
    assert data["latest_forecast_horizons"] == ["h12", "h36", "h72"]


def test_doctor_warns_on_optional_missing_not_fail(tmp_path: Path) -> None:
    art = tmp_path / "art"
    _seed_artifacts(art, include_optional=False)

    script = Path(__file__).resolve().parents[1] / "scripts" / "qa_doctor.py"
    proc = subprocess.run(
        [sys.executable, str(script), "--artifacts", str(art)],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0
    data = json.loads((art / "doctor_report.json").read_text())
    assert data["ok"] is True
    assert data["warnings"]


def test_doctor_fails_on_missing_required(tmp_path: Path) -> None:
    art = tmp_path / "art"
    art.mkdir(parents=True, exist_ok=True)
    # only one file, others missing
    (art / "summary.json").write_text("{}")

    script = Path(__file__).resolve().parents[1] / "scripts" / "qa_doctor.py"
    proc = subprocess.run(
        [sys.executable, str(script), "--artifacts", str(art)],
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    data = json.loads((art / "doctor_report.json").read_text())
    assert data["hard_failures"]
