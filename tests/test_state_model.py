from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from quantum_analyzer.state.belief_filter import normalized_entropy
from quantum_analyzer.state.latent_model import GaussianHMMBaseline


def _synthetic_features(n: int = 400) -> pd.DataFrame:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    idx = [start + timedelta(hours=i) for i in range(n)]

    x = np.linspace(0, 10, n)
    # mix two latent regimes: smooth trend + noisy chop
    feat1 = np.sin(x) + np.where(np.arange(n) % 50 < 25, 0.5, -0.5)
    feat2 = np.cos(x * 0.7) + np.random.default_rng(7).normal(0, 0.1, size=n)
    feat3 = np.gradient(feat1)

    return pd.DataFrame(
        {
            "micro_range_pos_24h": feat1,
            "realized_vol_24h": np.abs(feat2),
            "aggtrade_imbalance": feat3,
            "basis_bps": feat2 * 10,
        },
        index=pd.DatetimeIndex(idx, tz="UTC"),
    )


def test_entropy_bounds() -> None:
    p = np.array([0.9, 0.1])
    h = normalized_entropy(p)
    assert 0.0 <= h <= 1.0


def test_gaussian_hmm_fit_predict_and_transition() -> None:
    X = _synthetic_features(350)
    model = GaussianHMMBaseline(n_states=10, random_state=7).fit(X)

    probs = model.predict_proba(X)
    assert probs.shape[0] == X.shape[0]
    assert probs.shape[1] == 10

    tm = model.transition_matrix()
    assert tm.shape == (10, 10)
    row_sums = tm.sum(axis=1)
    assert np.allclose(row_sums, 1.0)


def test_statebelief_output_and_serialization(tmp_path: Path) -> None:
    X = _synthetic_features(260)
    model = GaussianHMMBaseline(n_states=10, random_state=7).fit(X)
    beliefs = model.predict_state_beliefs(X, symbol="SOLUSDT")

    assert beliefs
    b0 = beliefs[0]
    d = b0.to_dict()
    j = b0.to_json()
    assert isinstance(d, dict)
    assert isinstance(j, str)
    assert "regime_probabilities" in d

    mpath = tmp_path / "latent_model.pkl"
    model.save(mpath)
    restored = GaussianHMMBaseline.load(mpath)
    probs2 = restored.predict_proba(X)
    assert probs2.shape == (X.shape[0], 10)


def test_fit_predict_on_real_sample_like_data() -> None:
    # "real sample" contract: DataFrame resembling real feature columns and index.
    n = 180
    idx = pd.date_range("2026-02-01", periods=n, freq="H", tz="UTC")
    rng = np.random.default_rng(42)
    X = pd.DataFrame(
        {
            "micro_range_pos_24h": rng.normal(0.5, 0.15, n),
            "meso_range_pos_7d": rng.normal(0.5, 0.2, n),
            "realized_vol_24h": np.abs(rng.normal(0.03, 0.01, n)),
            "orderbook_imbalance": rng.normal(0.0, 0.2, n),
            "basis_bps": rng.normal(5.0, 2.0, n),
            "oi_zscore": rng.normal(0.0, 1.0, n),
        },
        index=idx,
    )

    model = GaussianHMMBaseline(n_states=10, random_state=11).fit(X)
    beliefs = model.predict_state_beliefs(X, symbol="SOLUSDT")
    assert len(beliefs) == n
    assert all(0.0 <= b.entropy <= 1.0 for b in beliefs)
