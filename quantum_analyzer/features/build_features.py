from __future__ import annotations

import pandas as pd

from .cross_asset import compute_cross_asset_features
from .orderflow import compute_orderflow_features
from .range_coords import compute_range_features
from .structural_breaks import compute_structural_break_features
from .volatility import compute_volatility_features


def build_feature_frame(
    sol_klines: pd.DataFrame,
    btc_klines: pd.DataFrame,
    agg_trades: pd.DataFrame,
    book_ticker: pd.DataFrame,
    funding: pd.DataFrame,
    basis: pd.DataFrame,
    open_interest: pd.DataFrame,
) -> pd.DataFrame:
    """Build the full feature frame indexed by timestamp (UTC).

    Inputs should already be filtered to avoid future leakage.
    This function assumes each row timestamp is event time and uses only historical
    rolling windows and forward-fill from past events.
    """
    df = sol_klines.copy().sort_values("open_time_ms")
    idx = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)
    df = df.set_index(idx)

    base = pd.DataFrame(index=df.index)
    base["close"] = df["close"].astype(float)
    base["high"] = df["high"].astype(float)
    base["low"] = df["low"].astype(float)

    range_f = compute_range_features(base)
    vol_f = compute_volatility_features(base)
    of_f = compute_orderflow_features(agg_trades=agg_trades, book_ticker=book_ticker, kline_index=base.index)

    btc = btc_klines.copy().sort_values("open_time_ms")
    btc_idx = pd.to_datetime(btc["open_time_ms"], unit="ms", utc=True)
    btc_close = btc.set_index(btc_idx)["close"].astype(float)
    cross_f = compute_cross_asset_features(base["close"], btc_close)

    funding_s = pd.Series(
        funding["funding_rate"].astype(float).values,
        index=pd.to_datetime(funding["source_ts_ms"], unit="ms", utc=True),
    ).sort_index()
    oi_s = pd.Series(
        open_interest["open_interest"].astype(float).values,
        index=pd.to_datetime(open_interest["source_ts_ms"], unit="ms", utc=True),
    ).sort_index()
    break_f = compute_structural_break_features(base["close"], funding_s, oi_s)

    basis_s = pd.Series(
        basis["basis_bps"].astype(float).values,
        index=pd.to_datetime(basis["source_ts_ms"], unit="ms", utc=True),
    ).sort_index().reindex(base.index, method="ffill")

    all_f = pd.concat([range_f, vol_f, of_f, cross_f, break_f], axis=1)
    all_f["basis_bps"] = basis_s

    # explicit timestamp alignment fields
    all_f["source_ts_ms"] = (all_f.index.view("int64") // 1_000_000).astype("int64")
    all_f["effective_ts_ms"] = all_f["source_ts_ms"]

    return all_f
