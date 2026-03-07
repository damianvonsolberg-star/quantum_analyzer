from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class WalkForwardConfig:
    train_bars: int
    test_bars: int
    purge_bars: int = 0
    embargo_bars: int = 0


def purged_walkforward_splits(n: int, cfg: WalkForwardConfig) -> list[tuple[np.ndarray, np.ndarray]]:
    splits: list[tuple[np.ndarray, np.ndarray]] = []
    start = 0
    while True:
        tr_start = start
        tr_end = tr_start + cfg.train_bars
        te_start = tr_end
        te_end = te_start + cfg.test_bars
        if te_end > n:
            break

        train_idx = np.arange(tr_start, tr_end)
        test_idx = np.arange(te_start, te_end)

        # purge + embargo around test segment
        lo = max(0, te_start - cfg.purge_bars)
        hi = min(n, te_end + cfg.embargo_bars)
        keep = train_idx[(train_idx < lo) | (train_idx >= hi)]
        if len(keep) > 0 and len(test_idx) > 0:
            splits.append((keep, test_idx))
        start += cfg.test_bars

    return splits
