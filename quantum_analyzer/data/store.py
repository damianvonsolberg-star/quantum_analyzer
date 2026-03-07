from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def _date_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def partition_path(
    root: str | Path,
    dataset: str,
    market: str,
    symbol: str,
    date: str,
    timeframe: str | None = None,
) -> Path:
    p = Path(root) / dataset / f"market={market}" / f"symbol={symbol}"
    if timeframe:
        p = p / f"timeframe={timeframe}"
    return p / f"date={date}"


def write_partitioned_parquet(
    rows: list[dict],
    root: str | Path,
    dataset: str,
    market: str,
    symbol: str,
    timeframe: str | None = None,
    ts_field: str = "source_ts_ms",
) -> list[Path]:
    if not rows:
        return []

    # Group rows by date derived from source event timestamps.
    buckets: dict[str, list[dict]] = {}
    for row in rows:
        ts_ms = int(row[ts_field])
        day = _date_from_ms(ts_ms)
        buckets.setdefault(day, []).append(row)

    written: list[Path] = []
    for day, day_rows in buckets.items():
        out_dir = partition_path(
            root=root,
            dataset=dataset,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            date=day,
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "part-00000.parquet"

        # Lazy import to keep module import clean when pyarrow is unavailable.
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        table = pa.Table.from_pylist(day_rows)
        pq.write_table(table, out_file)
        written.append(out_file)

    return written


def iter_parquet_files(root: str | Path) -> Iterable[Path]:
    p = Path(root)
    if not p.exists():
        return []
    return p.rglob("*.parquet")
