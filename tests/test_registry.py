from pathlib import Path

import pandas as pd

from quantum_analyzer.experiments.registry import append_registry


def test_registry_append_dedup(tmp_path: Path):
    rows = [
        {"experiment_id": "a", "completed_at": "2026-01-01T00:00:00Z", "score": 0.1},
        {"experiment_id": "a", "completed_at": "2026-01-01T00:01:00Z", "score": 0.2},
    ]
    p = append_registry(tmp_path, rows)
    df = pd.read_parquet(p)
    assert len(df) == 1
    assert float(df.iloc[0]["score"]) == 0.2
