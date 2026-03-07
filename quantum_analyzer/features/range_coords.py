from __future__ import annotations

import numpy as np
import pandas as pd


def _range_position(close: pd.Series, low: pd.Series, high: pd.Series) -> pd.Series:
    width = (high - low).replace(0.0, np.nan)
    pos = (close - low) / width
    return pos.clip(0.0, 1.0)


def compute_range_features(df: pd.DataFrame) -> pd.DataFrame:
    """Range geometry and trend geometry features.

    Expects columns: close, high, low, volume.
    Uses rolling windows with current/past bars only (no lookahead).
    """
    out = pd.DataFrame(index=df.index)
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    low24 = low.rolling(24, min_periods=24).min()
    high24 = high.rolling(24, min_periods=24).max()
    low7d = low.rolling(24 * 7, min_periods=24 * 7).min()
    high7d = high.rolling(24 * 7, min_periods=24 * 7).max()
    low30d = low.rolling(24 * 30, min_periods=24 * 30).min()
    high30d = high.rolling(24 * 30, min_periods=24 * 30).max()

    out["micro_range_pos_24h"] = _range_position(close, low24, high24)
    out["meso_range_pos_7d"] = _range_position(close, low7d, high7d)
    out["macro_range_pos_30d"] = _range_position(close, low30d, high30d)

    range_width = (high24 - low24) / close.replace(0.0, np.nan)
    out["range_width"] = range_width
    out["compression_ratio"] = range_width / (
        (high7d - low7d) / close.replace(0.0, np.nan)
    )

    # multi-horizon slopes
    out["slope_1h"] = close.pct_change(1)
    out["slope_4h"] = close.pct_change(4)
    out["slope_24h"] = close.pct_change(24)
    out["slope_7d"] = close.pct_change(24 * 7)

    # curvature (2nd difference over normalized close)
    out["curvature_4h"] = close.diff(1).diff(1) / close.replace(0.0, np.nan)
    out["curvature_24h"] = close.diff(4).diff(4) / close.replace(0.0, np.nan)

    # mean-reversion tension: distance from 24h rolling mean normalized by rolling std
    mean_24 = close.rolling(24, min_periods=24).mean()
    std_24 = close.rolling(24, min_periods=24).std()
    out["mean_reversion_tension"] = (close - mean_24) / std_24.replace(0.0, np.nan)

    return out
