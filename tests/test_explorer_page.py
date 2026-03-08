from __future__ import annotations

import json
from pathlib import Path

from ui.explorer import explorer_paths, load_json, run_explorer_preset


def test_explorer_paths_build(tmp_path: Path):
    p = explorer_paths(tmp_path)
    assert p["root"] == tmp_path
    assert p["lock"].name == ".run.lock"


def test_load_json_handles_missing(tmp_path: Path):
    assert load_json(tmp_path / "missing.json") is None


def test_run_explorer_respects_lock(tmp_path: Path):
    p = explorer_paths(tmp_path)
    p["lock"].parent.mkdir(parents=True, exist_ok=True)
    p["lock"].write_text("running")
    ok, msg = run_explorer_preset("fast", tmp_path)
    assert ok is False
    assert msg == "run_in_progress"
