from __future__ import annotations

from pathlib import Path

import json
import pandas as pd

import ui.state as state


def _touch_bundle(dir_path: Path) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    (dir_path / "artifact_bundle.json").write_text("{}", encoding="utf-8")


def test_latest_operator_artifact_dir_prefers_leaderboard(monkeypatch, tmp_path: Path):
    root = tmp_path
    exp_a = root / "artifacts" / "explorer" / "experiments" / "run_a"
    exp_b = root / "artifacts" / "explorer" / "experiments" / "run_b"
    _touch_bundle(exp_a)
    _touch_bundle(exp_b)

    lb = pd.DataFrame(
        [
            {"leaderboard_rank": 1, "artifact_dir": "artifacts/explorer/experiments/run_b"},
            {"leaderboard_rank": 2, "artifact_dir": "artifacts/explorer/experiments/run_a"},
        ]
    )
    lb_path = root / "artifacts" / "explorer" / "leaderboard.parquet"
    lb_path.parent.mkdir(parents=True, exist_ok=True)
    lb.to_parquet(lb_path, index=False)

    monkeypatch.setattr(state, "ROOT", root)
    got = state.latest_operator_artifact_dir()
    assert got == str(exp_b.resolve())


def test_latest_operator_artifact_dir_falls_back_to_newest_experiment(monkeypatch, tmp_path: Path):
    root = tmp_path
    exp_root = root / "artifacts" / "explorer" / "experiments"
    exp_old = exp_root / "old_run"
    exp_new = exp_root / "new_run"
    _touch_bundle(exp_old)
    _touch_bundle(exp_new)

    # Ensure deterministic newest ordering by mtime.
    exp_old_bundle = exp_old / "artifact_bundle.json"
    exp_new_bundle = exp_new / "artifact_bundle.json"
    exp_old_bundle.touch()
    exp_new_bundle.touch()

    monkeypatch.setattr(state, "ROOT", root)
    got = state.latest_operator_artifact_dir()
    assert got == str(exp_new.resolve())


def test_latest_operator_artifact_dir_prefers_promoted_candidate_most_recent_run(monkeypatch, tmp_path: Path):
    root = tmp_path
    exp_a = root / "artifacts" / "explorer" / "experiments" / "run_a_old_rank1"
    exp_b = root / "artifacts" / "explorer" / "experiments" / "run_b_new_promoted_candidate"
    _touch_bundle(exp_a)
    _touch_bundle(exp_b)

    lb = pd.DataFrame(
        [
            {
                "leaderboard_rank": 1,
                "candidate_id": "cand_other",
                "artifact_dir": "artifacts/explorer/experiments/run_a_old_rank1",
                "completed_at": "2026-03-12T00:00:00+00:00",
            },
            {
                "leaderboard_rank": 30,
                "candidate_id": "cand_promoted",
                "artifact_dir": "artifacts/explorer/experiments/run_b_new_promoted_candidate",
                "completed_at": "2026-03-12T01:00:00+00:00",
            },
        ]
    )
    lb_path = root / "artifacts" / "explorer" / "leaderboard.parquet"
    lb_path.parent.mkdir(parents=True, exist_ok=True)
    lb.to_parquet(lb_path, index=False)

    promoted = root / "artifacts" / "promoted"
    promoted.mkdir(parents=True, exist_ok=True)
    promoted_bundle = {
        "supporting_metrics": {
            "supporting_metrics": {"candidate_id": "cand_promoted"},
            "selected_cluster": {"candidate_id": "cand_promoted"},
        }
    }
    (promoted / "current_signal_bundle.json").write_text(json.dumps(promoted_bundle), encoding="utf-8")

    monkeypatch.setattr(state, "ROOT", root)
    got = state.latest_operator_artifact_dir()
    assert got == str(exp_b.resolve())
