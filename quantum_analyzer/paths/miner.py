from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

from .archetypes import PathTemplate, medoid_signature
from .payoff_surfaces import summarize_action_surface


@dataclass
class MinerConfig:
    window_bars: int = 24 * 3
    sweep_days: int = 30
    n_clusters: int = 8
    min_support: int = 20
    purge_bars: int = 24
    embargo_bars: int = 24


def _window_signature(close: np.ndarray, rv: np.ndarray) -> np.ndarray:
    # normalize by level and realized volatility
    c0 = close[0] if close[0] != 0 else 1.0
    level_norm = close / c0 - 1.0
    vol_norm = level_norm / (np.nanmean(rv) + 1e-9)
    return vol_norm


def build_signatures(df: pd.DataFrame, cfg: MinerConfig) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    close = df["close"].astype(float).values
    rv = df.get("realized_vol_24h", pd.Series(np.ones(len(df)), index=df.index)).astype(float).values
    idx = np.arange(len(df))

    X: list[np.ndarray] = []
    end_pos: list[int] = []
    for end in range(cfg.window_bars, len(df) - 72):
        st = end - cfg.window_bars
        sig = _window_signature(close[st:end], rv[st:end])
        X.append(sig)
        end_pos.append(end)
    return np.array(X), np.array(end_pos), idx


def _anchored_split(n: int, train_frac: float = 0.7) -> tuple[np.ndarray, np.ndarray]:
    cut = int(n * train_frac)
    tr = np.arange(0, cut)
    te = np.arange(cut, n)
    return tr, te


def _apply_purge_embargo(train_idx: np.ndarray, test_idx: np.ndarray, cfg: MinerConfig) -> np.ndarray:
    if len(test_idx) == 0:
        return train_idx
    lo = max(0, test_idx.min() - cfg.purge_bars)
    hi = test_idx.max() + cfg.embargo_bars
    keep = train_idx[(train_idx < lo) | (train_idx > hi)]
    return keep


def mine_path_templates(df: pd.DataFrame, cfg: MinerConfig) -> list[PathTemplate]:
    X, end_pos, _ = build_signatures(df, cfg)
    if len(X) < max(cfg.min_support, cfg.n_clusters * 3):
        return []

    train_idx, test_idx = _anchored_split(len(X), train_frac=0.7)
    train_idx = _apply_purge_embargo(train_idx, test_idx, cfg)
    if len(train_idx) < cfg.n_clusters:
        return []

    km = KMeans(n_clusters=cfg.n_clusters, n_init=10, random_state=42)
    km.fit(X[train_idx])
    labels = km.predict(X)

    close = df["close"].astype(float)
    ret36 = close.shift(-36) / close - 1.0

    templates: list[PathTemplate] = []
    n_total = len(labels)
    for cid in range(cfg.n_clusters):
        idx_all = np.where(labels == cid)[0]
        if len(idx_all) < cfg.min_support:
            continue

        # map signature indices to df rows where future payoff is read
        df_rows = end_pos[idx_all]
        r = ret36.iloc[df_rows]
        surfaces = summarize_action_surface(r)

        # choose best action by in-sample expectancy
        best = max(surfaces.values(), key=lambda x: x.expectancy)

        # oos stability: same action expectancy on test subset
        test_mask = np.isin(idx_all, test_idx)
        if test_mask.any():
            df_rows_te = end_pos[idx_all[test_mask]]
            r_te = ret36.iloc[df_rows_te]
            oos_exp = summarize_action_surface(r_te)[best.action].expectancy
        else:
            oos_exp = 0.0

        robustness = float(1.0 if np.sign(best.expectancy) == np.sign(oos_exp) else 0.0)
        support = len(idx_all) / n_total
        sig = medoid_signature(X, labels, cid)

        templates.append(
            PathTemplate(
                template_id=f"cluster_{cid}_{best.action}",
                cluster_id=cid,
                sample_count=len(idx_all),
                action=best.action,
                expectancy=best.expectancy,
                pf_proxy=best.pf_proxy,
                robustness=robustness,
                support=support,
                oos_stability=float(oos_exp),
                archetype_signature=sig.tolist(),
                meta={
                    "ci_low": best.ci_low,
                    "ci_high": best.ci_high,
                    "purge_bars": cfg.purge_bars,
                    "embargo_bars": cfg.embargo_bars,
                },
            )
        )

    # reject unstable templates
    templates = [t for t in templates if t.robustness >= 1.0]
    return sorted(templates, key=lambda t: (t.expectancy, t.pf_proxy), reverse=True)
