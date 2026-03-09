from __future__ import annotations

import numpy as np
import pandas as pd


def compute_orderflow_features(
    agg_trades: pd.DataFrame,
    book_ticker: pd.DataFrame,
    kline_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    """Build order-flow and liquidity/impact features aligned to kline timestamps.

    agg_trades columns: trade_time_ms, qty, price, is_buyer_maker
    book_ticker columns: source_ts_ms, bid_price, bid_qty, ask_price, ask_qty
    """
    out = pd.DataFrame(index=kline_index)

    if agg_trades is None or agg_trades.empty:
        out["aggtrade_imbalance"] = np.nan
    else:
        at = agg_trades.copy()
        at["ts"] = pd.to_datetime(at["trade_time_ms"], unit="ms", utc=True)
        at = at.set_index("ts").sort_index()
        if at.index.has_duplicates:
            at = at.groupby(level=0, sort=True).agg({
                "qty": "sum",
                "price": "last",
                "is_buyer_maker": "last",
            })
        at["signed_qty"] = np.where(at["is_buyer_maker"].astype(bool), -at["qty"], at["qty"])

        one_h_qty = at["qty"].rolling("1h").sum()
        one_h_signed = at["signed_qty"].rolling("1h").sum()
        imbalance = one_h_signed / one_h_qty.replace(0.0, np.nan)
        imbalance = imbalance.reindex(kline_index, method="ffill")
        out["aggtrade_imbalance"] = imbalance

    if book_ticker is None or book_ticker.empty:
        out["orderbook_imbalance"] = np.nan
        out["spread_bps"] = np.nan
        out["depth_usd_10bps"] = np.nan
        return out

    bt = book_ticker.copy()
    bt["ts"] = pd.to_datetime(bt["source_ts_ms"], unit="ms", utc=True)
    bt = bt.set_index("ts").sort_index()
    if bt.index.has_duplicates:
        bt = bt.groupby(level=0, sort=True).last()

    bid = bt["bid_price"].astype(float)
    ask = bt["ask_price"].astype(float)
    bid_qty = bt["bid_qty"].astype(float)
    ask_qty = bt["ask_qty"].astype(float)
    mid = (bid + ask) / 2.0

    out["orderbook_imbalance"] = ((bid_qty - ask_qty) / (bid_qty + ask_qty).replace(0.0, np.nan)).reindex(kline_index, method="ffill")
    out["spread_bps"] = (((ask - bid) / mid.replace(0.0, np.nan)) * 10_000).reindex(kline_index, method="ffill")

    # Approx depth near touch as proxy for 10bps depth (limited by available snapshot fields)
    out["depth_usd_10bps"] = ((bid_qty * bid) + (ask_qty * ask)).reindex(kline_index, method="ffill")

    return out
