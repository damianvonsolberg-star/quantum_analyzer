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


def _latest_experiment_artifacts_dir(explorer_root: str | Path) -> Path | None:
    root = Path(explorer_root)
    candidates = sorted(root.rglob("artifact_bundle.json"), key=lambda x: x.stat().st_mtime)
    if not candidates:
        return None
    return candidates[-1].parent


def run_advise_now(explorer_root: str | Path, artifacts_dir: str | Path, governance_status: str = "OK") -> tuple[bool, dict[str, Any]]:
    p = explorer_paths(explorer_root)
    ok1, msg1 = run_explorer_preset("fast", explorer_root)
    if not ok1:
        return False, {"step": "explorer", "error": msg1}

    ok2, msg2 = run_promotion(explorer_root, governance_status=governance_status)
    if not ok2:
        return False, {"step": "promotion", "error": msg2}

    # Prefer doctor validation against fresh explorer experiment artifacts,
    # not potentially stale UI fixture directories.
    doctor_target = _latest_experiment_artifacts_dir(explorer_root)
    if doctor_target is None:
        doctor_target = Path(artifacts_dir)

    doctor_cmd = ["python3", "scripts/qa_doctor.py", "--artifacts", str(doctor_target)]
    r = subprocess.run(doctor_cmd, capture_output=True, text=True)
    if r.returncode != 0:
        status = {
            "step": "doctor",
            "ok": False,
            "doctor_artifacts": str(doctor_target),
            "error": (r.stderr or r.stdout or "doctor_failed").strip(),
        }
    else:
        status = {"step": "doctor", "ok": True, "doctor_artifacts": str(doctor_target), "message": "GO_ADVISORY"}

    from datetime import datetime, timezone

    status["last_run_at"] = datetime.now(timezone.utc).isoformat()
    status_path = p["root"] / "advise_now_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
    return bool(status.get("ok", False)), status
