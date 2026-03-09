from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import uuid
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


def _compact_error(msg: str, max_chars: int = 420) -> str:
    s = " ".join(str(msg).split())
    return s if len(s) <= max_chars else (s[:max_chars] + " …")


def _write_failure_log(root: Path, run_id: str, step: str, err: str) -> None:
    d = root / "research_cycle_failures"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{run_id}_{step}.log"
    p.write_text(err, encoding="utf-8")


def run_research_cycle(config: str = "config/research/solusdc_research.json", discovery_config: str = "config/discovery/discovery_daily.json") -> tuple[bool, dict[str, Any]]:
    root = Path("artifacts")
    ok, msg = acquire_lock(root)
    if not ok:
        return True, {"ok": True, "state": "running", "step": "lock", "error": msg, "message": "research_cycle_already_running"}

    status_path = Path("artifacts/research_cycle_status.json")
    run_id = uuid.uuid4().hex[:12]
    status: dict[str, Any] = {
        "run_id": run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "ok": False,
        "state": "running",
        "steps": [],
        "warning_count": 0,
    }
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
            t0 = datetime.now(timezone.utc)
            status.update({"current_step": s["name"], "current_cmd": c})
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
            env = os.environ.copy()
            env.setdefault("LOKY_MAX_CPU_COUNT", "4")
            try:
                r = subprocess.run(c, capture_output=True, text=True, timeout=int(s["timeout"]), env=env)
                dt_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
            except subprocess.TimeoutExpired:
                msg = f"step_timeout:{s['name']}"
                dt_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
                status["steps"].append({"name": s["name"], "ok": False, "duration_ms": dt_ms, "critical": bool(s["critical"]), "error": msg})
                if s["critical"]:
                    status.update({"ok": False, "state": "failed", "failed_cmd": c, "error": _compact_error(msg), "error_full": msg, "warning_count": len(status.get("warnings", [])), "finished_at": datetime.now(timezone.utc).isoformat()})
                    _write_failure_log(root, run_id, s["name"], msg)
                    status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
                    return False, status
                status["warnings"].append(msg)
                status["warning_count"] = len(status["warnings"])
                continue

            if r.returncode != 0:
                err = (r.stderr or r.stdout or "failed").strip()
                status["steps"].append({"name": s["name"], "ok": False, "duration_ms": dt_ms, "critical": bool(s["critical"]), "error": _compact_error(err)})
                if s["critical"]:
                    status.update({"ok": False, "state": "failed", "failed_cmd": c, "error": _compact_error(err), "error_full": err, "warning_count": len(status.get("warnings", [])), "finished_at": datetime.now(timezone.utc).isoformat()})
                    _write_failure_log(root, run_id, s["name"], err)
                    status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
                    return False, status
                status["warnings"].append(f"{s['name']}:{err}")
                status["warning_count"] = len(status["warnings"])
                continue

            status["steps"].append({"name": s["name"], "ok": True, "duration_ms": dt_ms, "critical": bool(s["critical"])})

        status["ok"] = True
        status["state"] = "idle"
        status["warning_count"] = len(status.get("warnings", []))
        status["finished_at"] = datetime.now(timezone.utc).isoformat()
        status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        return True, status
    finally:
        release_lock(root)
