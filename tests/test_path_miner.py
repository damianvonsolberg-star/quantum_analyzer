from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from quantum_analyzer.paths.archetypes import save_templates_json, save_templates_parquet
from quantum_analyzer.paths.miner import MinerConfig, mine_path_templates


def _mk_df(n: int = 24 * 80) -> pd.DataFrame:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    idx = [start + timedelta(hours=i) for i in range(n)]
    rng = np.random.default_rng(123)
    trend = np.linspace(0, 1.5, n)
    cyc = np.sin(np.arange(n) / 8) * 0.4
    close = 80 + trend + cyc + rng.normal(0, 0.08, n)
    rv = pd.Series(close).pct_change().rolling(24, min_periods=24).std().fillna(0.01).values
    return pd.DataFrame({"close": close, "realized_vol_24h": rv}, index=pd.DatetimeIndex(idx, tz="UTC"))


def test_miner_returns_templates_with_required_fields() -> None:
    df = _mk_df()
    cfg = MinerConfig(window_bars=24 * 2, n_clusters=4, min_support=10, purge_bars=12, embargo_bars=12)
    templates = mine_path_templates(df, cfg)

    assert isinstance(templates, list)
    if templates:
        t = templates[0]
        assert t.sample_count >= cfg.min_support
        assert isinstance(t.expectancy, float)
        assert isinstance(t.pf_proxy, float)
        assert 0.0 <= t.support <= 1.0


def test_template_persistence_json_and_parquet(tmp_path: Path) -> None:
    df = _mk_df()
    cfg = MinerConfig(window_bars=24 * 2, n_clusters=3, min_support=8)
    templates = mine_path_templates(df, cfg)

    json_path = tmp_path / "templates.json"
    save_templates_json(templates, json_path)
    assert json_path.exists()

    try:
        import pyarrow  # noqa: F401
    except Exception:  # noqa: BLE001
        return

    pq_path = tmp_path / "templates.parquet"
    save_templates_parquet(templates, pq_path)
    assert pq_path.exists()
