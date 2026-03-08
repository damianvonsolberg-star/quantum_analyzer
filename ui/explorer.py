from __future__ import annotations

import json
from pathlib import Path
import subprocess
from typing import Any


def explorer_paths(root: str | Path) -> dict[str, Path]:
    r = Path(root)
    return {
        "root": r,
        "lock": r / ".run.lock",
        "manifest": r / "run_manifest.json",
        "leaderboard_json": r / "leaderboard.json",
        "leaderboard_parquet": r / "leaderboard.parquet",
        "promoted_bundle": Path("artifacts/promoted/current_signal_bundle.json"),
    }


def load_json(path: Path) -> dict[str, Any] | list[Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def run_explorer_preset(preset: str, root: str | Path) -> tuple[bool, str]:
    p = explorer_paths(root)
    lock = p["lock"]
    if lock.exists():
        return False, "run_in_progress"

    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text("running", encoding="utf-8")
    try:
        cmd = ["python3", "scripts/run_explorer.py", "--preset", preset, "--artifacts-root", str(p["root"])]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            return False, (r.stderr or r.stdout or "explorer_failed").strip()
        return True, "ok"
    finally:
        lock.unlink(missing_ok=True)


def run_promotion(explorer_root: str | Path, governance_status: str = "OK") -> tuple[bool, str]:
    cmd = [
        "python3",
        "scripts/promote_signal.py",
        "--explorer-root",
        str(explorer_root),
        "--out-root",
        "artifacts/promoted",
        "--governance-status",
        governance_status,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        return False, (r.stderr or r.stdout or "promotion_failed").strip()
    return True, "ok"
