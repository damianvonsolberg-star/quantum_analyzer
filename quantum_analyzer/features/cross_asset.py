from __future__ import annotations

import numpy as np
import pandas as pd


def compute_cross_asset_features(
    sol_close: pd.Series,
    btc_close: pd.Series,
) -> pd.DataFrame:
    out = pd.DataFrame(index=sol_close.index)
    sol = sol_close.astype(float)
    btc = btc_close.astype(float).reindex(sol.index).ffill()

    sol_ret_24 = sol.pct_change(24)
    btc_ret_24 = btc.pct_change(24)

    out["sol_btc_rel_strength"] = sol_ret_24 - btc_ret_24

    btc_log_ret = np.log(btc / btc.shift(1))
    btc_mom = btc.pct_change(24)
    btc_vol = btc_log_ret.rolling(24, min_periods=24).std() * np.sqrt(24)
    out["btc_regime_score"] = (btc_mom / btc_vol.replace(0.0, np.nan)).clip(-5, 5)

    return out
