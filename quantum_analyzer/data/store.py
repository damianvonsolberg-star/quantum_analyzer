from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4


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


def _rows_checksum(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _safe_part_name() -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"part-{now}-{uuid4().hex[:8]}.parquet"


def _update_manifest(
    out_dir: Path,
    *,
    dataset: str,
    market: str,
    symbol: str,
    timeframe: str | None,
    date: str,
    run_mode: str,
    rows: list[dict[str, Any]],
    file_path: Path,
) -> dict[str, Any]:
    manifest_path = out_dir / "_manifest.json"
    existing: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}

    entries = existing.get("entries", []) if isinstance(existing.get("entries"), list) else []
    checksum = _rows_checksum(rows)
    row_count = len(rows)

    entry = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "file": file_path.name,
        "rows": row_count,
        "checksum_sha256": checksum,
        "run_mode": run_mode,
        "watermark": {
            "min_source_ts_ms": min(int(r.get("source_ts_ms", 0) or 0) for r in rows) if rows else None,
            "max_source_ts_ms": max(int(r.get("source_ts_ms", 0) or 0) for r in rows) if rows else None,
        },
    }
    entries.append(entry)

    manifest = {
        "dataset": dataset,
        "market": market,
        "symbol": symbol,
        "timeframe": timeframe,
        "date": date,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "entries": entries,
        "totals": {
            "files": len(entries),
            "rows": int(sum(int(e.get("rows", 0) or 0) for e in entries)),
        },
        "latest_watermark": entry["watermark"],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return entry


def write_partitioned_parquet(
    rows: list[dict],
    root: str | Path,
    dataset: str,
    market: str,
    symbol: str,
    timeframe: str | None = None,
    ts_field: str = "source_ts_ms",
    *,
    overwrite: bool = False,
) -> list[dict[str, Any]]:
    """Append-safe partition writer.

    - default is append mode (unique part files)
    - overwrite mode is explicit
    - writes per-partition manifest with watermark/checksum
    """
    if not rows:
        return []

    # Group rows by date derived from source event timestamps.
    buckets: dict[str, list[dict]] = {}
    for row in rows:
        ts_ms = int(row[ts_field])
        day = _date_from_ms(ts_ms)
        buckets.setdefault(day, []).append(row)

    written: list[dict[str, Any]] = []
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
        out_file = (out_dir / "part-00000.parquet") if overwrite else (out_dir / _safe_part_name())

        # Lazy import to keep module import clean when pyarrow is unavailable.
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        table = pa.Table.from_pylist(day_rows)
        pq.write_table(table, out_file)

        entry = _update_manifest(
            out_dir,
            dataset=dataset,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            date=day,
            run_mode=("overwrite" if overwrite else "append"),
            rows=day_rows,
            file_path=out_file,
        )

        written.append(
            {
                "path": str(out_file),
                "rows": len(day_rows),
                "checksum_sha256": entry["checksum_sha256"],
                "watermark": entry["watermark"],
            }
        )

    return written


def iter_parquet_files(root: str | Path) -> Iterable[Path]:
    p = Path(root)
    if not p.exists():
        return []
    return p.rglob("*.parquet")
