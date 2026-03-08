from __future__ import annotations

import numpy as np
import pandas as pd

from .cross_asset import compute_cross_asset_features
from .orderflow import compute_orderflow_features
from .range_coords import compute_range_features
from .structural_breaks import compute_structural_break_features
from .volatility import compute_volatility_features


def _coverage_ratio(ts_series: pd.Series | None, index: pd.DatetimeIndex) -> float:
    if ts_series is None or ts_series.empty or index.empty:
        return 0.0
    ts = pd.to_datetime(ts_series, errors="coerce", utc=True).dropna().sort_values()
    if ts.empty:
        return 0.0
    start, end = index.min(), index.max()
    in_range = ts[(ts >= start) & (ts <= end)]
    if in_range.empty:
        return 0.0
    span = max((end - start).total_seconds(), 1.0)
    covered = max((in_range.max() - in_range.min()).total_seconds(), 0.0)
    return float(min(1.0, covered / span))


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

    Fail closed: if historical coverage for liquidity/OI/basis is insufficient,
    disable those feature columns instead of silently fabricating history.
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

    funding_s = pd.Series(dtype=float)
    if funding is not None and not funding.empty:
        funding_s = pd.Series(
            funding["funding_rate"].astype(float).values,
            index=pd.to_datetime(funding["source_ts_ms"], unit="ms", utc=True),
        ).sort_index()

    oi_s = pd.Series(dtype=float)
    if open_interest is not None and not open_interest.empty:
        oi_s = pd.Series(
            open_interest["open_interest"].astype(float).values,
            index=pd.to_datetime(open_interest["source_ts_ms"], unit="ms", utc=True),
        ).sort_index()

    break_f = compute_structural_break_features(base["close"], funding_s, oi_s)

    basis_s = pd.Series(np.nan, index=base.index)
    if basis is not None and not basis.empty:
        basis_s = pd.Series(
            basis["basis_bps"].astype(float).values,
            index=pd.to_datetime(basis["source_ts_ms"], unit="ms", utc=True),
        ).sort_index().reindex(base.index, method="ffill")

    # coverage checks
    cov_agg = _coverage_ratio(agg_trades["trade_time_ms"] if agg_trades is not None and not agg_trades.empty else None, base.index)
    cov_book = _coverage_ratio(book_ticker["source_ts_ms"] if book_ticker is not None and not book_ticker.empty else None, base.index)
    cov_oi = _coverage_ratio(open_interest["source_ts_ms"] if open_interest is not None and not open_interest.empty else None, base.index)
    cov_basis = _coverage_ratio(basis["source_ts_ms"] if basis is not None and not basis.empty else None, base.index)

    all_f = pd.concat([range_f, vol_f, of_f, cross_f, break_f], axis=1)
    all_f["basis_bps"] = basis_s

    # disable historical microstructure/liquidity features if coverage is weak
    if cov_agg < 0.7:
        all_f["aggtrade_imbalance"] = np.nan
    if cov_book < 0.7:
        for c in ["orderbook_imbalance", "spread_bps", "depth_usd_10bps"]:
            all_f[c] = np.nan
    if cov_oi < 0.7:
        all_f["oi_zscore"] = np.nan
    if cov_basis < 0.7:
        all_f["basis_bps"] = np.nan

    all_f["coverage_agg_trades"] = cov_agg
    all_f["coverage_book_ticker"] = cov_book
    all_f["coverage_open_interest"] = cov_oi
    all_f["coverage_basis"] = cov_basis
    all_f["historical_liquidity_ok"] = bool(cov_agg >= 0.7 and cov_book >= 0.7)
    all_f["historical_derivatives_ok"] = bool(cov_oi >= 0.7 and cov_basis >= 0.7)

    # provenance-aware timestamp fields (market-derived)
    index_ts_ms = pd.Series((all_f.index.view("int64") // 1_000_000).astype("int64"), index=all_f.index)

    def _source_series(df_in: pd.DataFrame | None, col: str, unit: str = "ms") -> pd.Series:
        if df_in is None or df_in.empty or col not in df_in.columns:
            return pd.Series(index=all_f.index, dtype="float64")
        src_idx = pd.to_datetime(df_in[col], unit=unit, utc=True, errors="coerce")
        src = pd.Series(df_in[col].astype("float64").values, index=src_idx).sort_index()
        src = src[~src.index.isna()]
        return src.reindex(all_f.index, method="ffill")

    sol_src = index_ts_ms.astype("float64")
    btc_src = _source_series(btc_klines, "open_time_ms")
    agg_src = _source_series(agg_trades, "trade_time_ms")
    book_src = _source_series(book_ticker, "source_ts_ms")
    fund_src = _source_series(funding, "source_ts_ms")
    oi_src = _source_series(open_interest, "source_ts_ms")
    basis_src = _source_series(basis, "source_ts_ms")

    # source timestamp = primary bar source (SOL kline event time)
    all_f["source_ts_ms"] = index_ts_ms.astype("int64")

    # effective timestamp = earliest available upstream event used for this row
    src_mat = pd.concat([sol_src, btc_src, agg_src, book_src, fund_src, oi_src, basis_src], axis=1)
    src_mat = src_mat.fillna(src_mat.iloc[:, 0], axis=0)
    eff = src_mat.min(axis=1)
    all_f["effective_ts_ms"] = eff.astype("int64")

    # explicit per-source provenance columns
    all_f["source_ts_sol_ms"] = sol_src.astype("int64")
    all_f["source_ts_btc_ms"] = btc_src.fillna(sol_src).astype("int64")
    all_f["source_ts_agg_ms"] = agg_src.fillna(sol_src).astype("int64")
    all_f["source_ts_book_ms"] = book_src.fillna(sol_src).astype("int64")
    all_f["source_ts_funding_ms"] = fund_src.fillna(sol_src).astype("int64")
    all_f["source_ts_oi_ms"] = oi_src.fillna(sol_src).astype("int64")
    all_f["source_ts_basis_ms"] = basis_src.fillna(sol_src).astype("int64")

    return all_f
