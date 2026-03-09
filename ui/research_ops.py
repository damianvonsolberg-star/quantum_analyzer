from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
from typing import Any


def _lock_path(root: str | Path) -> Path:
    return Path(root) / ".research_cycle.lock"


def acquire_lock(root: str | Path, stale_minutes: int = 15) -> tuple[bool, str]:
    p = _lock_path(root)
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            started = datetime.fromisoformat(str(data.get("started_at")))
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)

            # auto-clear stale/stranded lock using status hints
            now = datetime.now(timezone.utc)
            status_path = Path(root) / "research_cycle_status.json"
            if status_path.exists():
                try:
                    st = json.loads(status_path.read_text(encoding="utf-8"))
                    finished_at = st.get("finished_at")
                    state = str(st.get("state", "")).lower()
                    if finished_at:
                        fin = datetime.fromisoformat(str(finished_at))
                        if fin.tzinfo is None:
                            fin = fin.replace(tzinfo=timezone.utc)
                        # normal completion
                        if fin >= started:
                            p.unlink(missing_ok=True)
                        # stranded lock after older completion metadata
                        elif (now - started) > timedelta(minutes=3) and state != "running":
                            p.unlink(missing_ok=True)
                except Exception:
                    pass

            if p.exists() and now - started > timedelta(minutes=stale_minutes):
                p.unlink(missing_ok=True)
            if p.exists():
                return False, "run_in_progress"
        except Exception:
            return False, "run_in_progress"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"started_at": datetime.now(timezone.utc).isoformat()}, indent=2), encoding="utf-8")
    return True, "ok"


def release_lock(root: str | Path) -> None:
    _lock_path(root).unlink(missing_ok=True)


def run_research_cycle(config: str = "config/research/solusdc_research.json", discovery_config: str = "config/discovery/discovery_daily.json") -> tuple[bool, dict[str, Any]]:
    root = Path("artifacts")
    ok, msg = acquire_lock(root)
    if not ok:
        return True, {"ok": True, "state": "running", "step": "lock", "error": msg, "message": "research_cycle_already_running"}

    status_path = Path("artifacts/research_cycle_status.json")
    status: dict[str, Any] = {"started_at": datetime.now(timezone.utc).isoformat(), "ok": False, "state": "running"}
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
    try:
        steps = [
            {
                "name": "update_market_data",
                "cmd": ["python3", "scripts/update_market_data.py", "--config", config],
                "timeout": 90,
                "critical": False,
            },
            {
                "name": "build_feature_snapshot",
                "cmd": ["python3", "scripts/build_feature_snapshot.py", "--config", config, "--build-snapshot"],
                "timeout": 120,
                "critical": True,
            },
            {
                "name": "run_explorer",
                "cmd": ["python3", "scripts/run_explorer.py", "--config", config, "--preset", "fast", "--build-snapshot", "--artifacts-root", "artifacts/explorer"],
                "timeout": 240,
                "critical": True,
            },
            {
                "name": "run_discovery",
                "cmd": ["python3", "scripts/run_discovery.py", "--config", discovery_config, "--snapshot", "latest"],
                "timeout": 180,
                "critical": False,
            },
            {
                "name": "promote_signal",
                "cmd": ["python3", "scripts/promote_signal.py", "--explorer-root", "artifacts/explorer", "--out-root", "artifacts/promoted"],
                "timeout": 120,
                "critical": True,
            },
            {
                "name": "emit_advisory",
                "cmd": ["python3", "scripts/emit_advisory.py", "--artifacts-root", "artifacts/promoted"],
                "timeout": 60,
                "critical": True,
            },
        ]
        status.setdefault("warnings", [])
        for s in steps:
            c = s["cmd"]
            status.update({"current_step": s["name"], "current_cmd": c})
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
            try:
                r = subprocess.run(c, capture_output=True, text=True, timeout=int(s["timeout"]))
            except subprocess.TimeoutExpired:
                msg = f"step_timeout:{s['name']}"
                if s["critical"]:
                    status.update({"ok": False, "state": "failed", "failed_cmd": c, "error": msg, "finished_at": datetime.now(timezone.utc).isoformat()})
                    status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
                    return False, status
                status["warnings"].append(msg)
                continue

            if r.returncode != 0:
                err = (r.stderr or r.stdout or "failed").strip()
                if s["critical"]:
                    status.update({"ok": False, "state": "failed", "failed_cmd": c, "error": err, "finished_at": datetime.now(timezone.utc).isoformat()})
                    status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
                    return False, status
                status["warnings"].append(f"{s['name']}:{err}")
                continue
        status["ok"] = True
        status["state"] = "idle"
        status["finished_at"] = datetime.now(timezone.utc).isoformat()
        status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        return True, status
    finally:
        release_lock(root)
