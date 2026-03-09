from __future__ import annotations

from pathlib import Path
import pandas as pd


def _dataset_root(data_root: str | Path, dataset: str, market: str, symbol: str, timeframe: str | None = None) -> Path:
    p = Path(data_root) / dataset / f"market={market}" / f"symbol={symbol}"
    if timeframe:
        p = p / f"timeframe={timeframe}"
    return p


def load_dataset_frame(
    data_root: str | Path,
    dataset: str,
    market: str,
    symbol: str,
    timeframe: str | None = None,
) -> pd.DataFrame:
    root = _dataset_root(data_root, dataset, market, symbol, timeframe)
    if not root.exists():
        return pd.DataFrame()
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    parts = []
    for f in files:
        try:
            parts.append(pd.read_parquet(f))
        except Exception:
            continue
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    return df


def resolve_context_symbol(data_root: str | Path, preferred: list[str]) -> str | None:
    for sym in preferred:
        root = _dataset_root(data_root, "klines", "spot", sym, "1h")
        if root.exists() and any(root.rglob("*.parquet")):
            return sym
    return None
