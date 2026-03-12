from __future__ import annotations

import os
from pathlib import Path
import sys
from typing import Any

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.config import UIConfig


def _resolve_default_artifact_dir(cfg: UIConfig) -> str:
    env_dir = os.getenv("ARTIFACT_DIR", "").strip()
    if env_dir:
        return env_dir

    candidate = Path(cfg.default_artifacts_dir)
    if (candidate / "artifact_bundle.json").exists():
        return str(candidate)

    return str(candidate)


def _ui_data_dir() -> Path:
    p = os.getenv("UI_DATA_DIR", str((ROOT / "ui_data").resolve()))
    d = Path(p)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _state_file() -> Path:
    return _ui_data_dir() / "ui_state.json"


def load_persisted_artifact_dir() -> str | None:
    sf = _state_file()
    if not sf.exists():
        return None
    try:
        import json

        data = json.loads(sf.read_text(encoding="utf-8"))
        v = data.get("artifact_dir")
        return str(v) if v else None
    except Exception:
        return None


def persist_artifact_dir(path: str) -> None:
    sf = _state_file()
    try:
        import json

        payload = {"artifact_dir": path}
        sf.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass


def _resolve_artifact_dir_path(raw_path: str) -> Path:
    p = Path(str(raw_path).strip())
    if not p.is_absolute():
        p = (ROOT / p).resolve()
    return p


def _promoted_candidate_id() -> str | None:
    p = ROOT / "artifacts" / "promoted" / "current_signal_bundle.json"
    if not p.exists():
        return None
    try:
        import json

        j = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(j, dict):
            return None
        sm = j.get("supporting_metrics", {}) if isinstance(j.get("supporting_metrics"), dict) else {}
        winner = sm.get("supporting_metrics", {}) if isinstance(sm.get("supporting_metrics"), dict) else {}
        sel = sm.get("selected_cluster", {}) if isinstance(sm.get("selected_cluster"), dict) else {}
        cid = winner.get("candidate_id") or sel.get("candidate_id")
        return str(cid) if cid else None
    except Exception:
        return None


def _latest_from_leaderboard() -> str | None:
    lb_path = ROOT / "artifacts" / "explorer" / "leaderboard.parquet"
    if not lb_path.exists():
        return None
    try:
        lb = pd.read_parquet(lb_path)
        if lb.empty:
            return None
        if "artifact_dir" not in lb.columns:
            return None

        # Prefer the most recent run for the currently promoted winner candidate.
        promoted_cid = _promoted_candidate_id()
        if promoted_cid and "candidate_id" in lb.columns:
            m = lb[lb["candidate_id"].astype(str) == str(promoted_cid)].copy()
            if not m.empty:
                if "completed_at" in m.columns:
                    m = m.sort_values("completed_at", ascending=False)
                elif "leaderboard_rank" in m.columns:
                    m = m.sort_values("leaderboard_rank", ascending=True)
                for raw in m["artifact_dir"].dropna().astype(str).tolist():
                    p = _resolve_artifact_dir_path(raw)
                    if p.exists() and (p / "artifact_bundle.json").exists():
                        return str(p)

        if "leaderboard_rank" in lb.columns:
            lb = lb.sort_values("leaderboard_rank", ascending=True)
        for raw in lb["artifact_dir"].dropna().astype(str).tolist():
            p = _resolve_artifact_dir_path(raw)
            if p.exists() and (p / "artifact_bundle.json").exists():
                return str(p)
    except Exception:
        return None
    return None


def latest_operator_artifact_dir() -> str | None:
    # 1) explicit advise_now doctor path (if present and fresh)
    p = ROOT / "artifacts" / "explorer" / "advise_now_status.json"
    if p.exists():
        try:
            import json
            from datetime import datetime, timezone

            j = json.loads(p.read_text(encoding="utf-8"))
            d = j.get("doctor_artifacts")
            ts = j.get("last_run_at")
            fresh = False
            if ts:
                t = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                if t.tzinfo is None:
                    t = t.replace(tzinfo=timezone.utc)
                age_min = (datetime.now(timezone.utc) - t).total_seconds() / 60.0
                fresh = age_min <= 60
            if d and Path(str(d)).exists() and fresh:
                return str(d)
        except Exception:
            pass

    # 2) preferred fallback: current leaderboard top artifact dir
    lb = _latest_from_leaderboard()
    if lb:
        return lb

    # 3) fallback to newest experiment artifact dir from continuous cycle
    exp_root = ROOT / "artifacts" / "explorer" / "experiments"
    if exp_root.exists():
        try:
            dirs = [x for x in exp_root.iterdir() if x.is_dir() and (x / "artifact_bundle.json").exists()]
            if dirs:
                # Always follow the true newest run to avoid pinning to old historical traces.
                newest = sorted(dirs, key=lambda d: d.stat().st_mtime, reverse=True)[0]
                return str(newest)
        except Exception:
            pass

    return None


def init_state() -> None:
    cfg = UIConfig()
    persisted = load_persisted_artifact_dir()
    hint = latest_operator_artifact_dir()
    default_artifacts = hint or persisted or _resolve_default_artifact_dir(cfg)

    # Always prioritize latest operator hint to avoid getting stuck on stale persisted dirs.
    current = st.session_state.get("artifact_dir")
    if hint and current != hint:
        st.session_state["artifact_dir"] = hint
        persist_artifact_dir(hint)
    else:
        if not current:
            st.session_state["artifact_dir"] = default_artifacts
            persist_artifact_dir(default_artifacts)
        else:
            st.session_state.setdefault("artifact_dir", default_artifacts)

    st.session_state.setdefault("wallet_address", cfg.default_wallet)
    st.session_state.setdefault("rpc_url", cfg.default_rpc)
    st.session_state.setdefault("refresh_seconds", cfg.default_refresh_seconds)
    st.session_state.setdefault("live_ticker_seconds", 5)
    st.session_state.setdefault("research_cycle_minutes", 15)
    st.session_state.setdefault("discovery_cycle_minutes", 60)


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        import json

        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def artifact_paths(artifact_dir: str) -> dict[str, Path]:
    base = Path(artifact_dir)
    return {
        "bundle": base / "artifact_bundle.json",
        "summary": base / "summary.json",
        "equity": base / "equity_curve.csv",
        "actions": base / "actions.csv",
        "templates_json": base / "templates.json",
        "templates_parquet": base / "templates.parquet",
        "doctor": base / "doctor_report.json",
    }
