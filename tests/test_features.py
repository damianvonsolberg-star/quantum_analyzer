from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from quantum_analyzer.features.build_features import build_feature_frame


def _mk_klines(start: datetime, n: int, base: float) -> pd.DataFrame:
    ts = [start + timedelta(hours=i) for i in range(n)]
    opens = np.linspace(base, base * 1.1, n)
    closes = opens + np.sin(np.arange(n) / 10) * 0.5
    highs = np.maximum(opens, closes) + 0.3
    lows = np.minimum(opens, closes) - 0.3
    return pd.DataFrame(
        {
            "open_time_ms": [int(t.timestamp() * 1000) for t in ts],
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": np.full(n, 1000.0),
        }
    )


def test_build_feature_frame_contracts() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    n = 24 * 40

    sol = _mk_klines(start, n, 80.0)
    btc = _mk_klines(start, n, 100000.0)

    agg = pd.DataFrame(
        {
            "trade_time_ms": sol["open_time_ms"],
            "qty": np.full(n, 5.0),
            "price": sol["close"],
            "is_buyer_maker": [False if i % 2 == 0 else True for i in range(n)],
        }
    )

    book = pd.DataFrame(
        {
            "source_ts_ms": sol["open_time_ms"],
            "bid_price": sol["close"] - 0.01,
            "ask_price": sol["close"] + 0.01,
            "bid_qty": np.full(n, 10.0),
            "ask_qty": np.full(n, 9.0),
        }
    )

    funding = pd.DataFrame(
        {
            "source_ts_ms": sol["open_time_ms"],
            "funding_rate": np.full(n, 0.0001),
        }
    )

    basis = pd.DataFrame(
        {
            "source_ts_ms": sol["open_time_ms"],
            "basis_bps": np.full(n, 5.0),
        }
    )

    oi = pd.DataFrame(
        {
            "source_ts_ms": sol["open_time_ms"],
            "open_interest": np.linspace(1000, 1500, n),
        }
    )

    feats = build_feature_frame(sol, btc, agg, book, funding, basis, oi)

    assert isinstance(feats.index, pd.DatetimeIndex)

    required = {
        "micro_range_pos_24h",
        "meso_range_pos_7d",
        "macro_range_pos_30d",
        "range_width",
        "compression_ratio",
        "slope_1h",
        "slope_4h",
        "curvature_4h",
        "realized_vol_24h",
        "vol_of_vol_24h",
        "aggtrade_imbalance",
        "orderbook_imbalance",
        "spread_bps",
        "depth_usd_10bps",
        "funding_rate",
        "basis_bps",
        "oi_zscore",
        "sol_btc_rel_strength",
        "btc_regime_score",
        "source_ts_ms",
        "effective_ts_ms",
    }
    missing = required.difference(feats.columns)
    assert not missing, f"Missing features: {missing}"

    # timestamp alignment and no lookahead timestamp leakage field-level contract
    assert (feats["effective_ts_ms"] == feats["source_ts_ms"]).all()
