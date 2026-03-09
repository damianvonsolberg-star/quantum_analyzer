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


def _latest_operator_artifact_hint() -> str | None:
    # 1) explicit advise_now doctor path (if present)
    p = ROOT / "artifacts" / "explorer" / "advise_now_status.json"
    if p.exists():
        try:
            import json

            j = json.loads(p.read_text(encoding="utf-8"))
            d = j.get("doctor_artifacts")
            if d and Path(str(d)).exists():
                return str(d)
        except Exception:
            pass

    # 2) fallback to newest experiment artifact dir from continuous cycle
    exp_root = ROOT / "artifacts" / "explorer" / "experiments"
    if exp_root.exists():
        try:
            dirs = [x for x in exp_root.iterdir() if x.is_dir() and (x / "artifact_bundle.json").exists()]
            if dirs:
                newest = sorted(dirs, key=lambda d: d.stat().st_mtime, reverse=True)[0]
                return str(newest)
        except Exception:
            pass

    return None


def init_state() -> None:
    cfg = UIConfig()
    persisted = load_persisted_artifact_dir()
    hint = _latest_operator_artifact_hint()
    default_artifacts = hint or persisted or _resolve_default_artifact_dir(cfg)

    # Always prioritize latest operator hint to avoid getting stuck on stale persisted dirs.
    current = st.session_state.get("artifact_dir")
    if hint and current != hint:
        st.session_state["artifact_dir"] = hint
        persist_artifact_dir(hint)
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
