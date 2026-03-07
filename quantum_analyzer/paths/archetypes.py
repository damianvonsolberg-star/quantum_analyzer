from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class PathTemplate:
    template_id: str
    cluster_id: int
    sample_count: int
    action: str
    expectancy: float
    pf_proxy: float
    robustness: float
    support: float
    oos_stability: float
    archetype_signature: list[float]
    meta: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def templates_to_frame(templates: list[PathTemplate]) -> pd.DataFrame:
    rows = [t.to_dict() for t in templates]
    return pd.DataFrame(rows)


def save_templates_json(templates: list[PathTemplate], path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump([t.to_dict() for t in templates], f, indent=2)


def save_templates_parquet(templates: list[PathTemplate], path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df = templates_to_frame(templates)
    if not df.empty:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        table = pa.Table.from_pandas(df)
        pq.write_table(table, p)
    else:
        # write empty schema-compatible parquet
        pd.DataFrame(
            columns=[
                "template_id",
                "cluster_id",
                "sample_count",
                "action",
                "expectancy",
                "pf_proxy",
                "robustness",
                "support",
                "oos_stability",
                "archetype_signature",
                "meta",
            ]
        ).to_parquet(p, index=False)


def medoid_signature(X: np.ndarray, labels: np.ndarray, cluster_id: int) -> np.ndarray:
    idx = np.where(labels == cluster_id)[0]
    if len(idx) == 0:
        return np.array([])
    block = X[idx]
    centroid = block.mean(axis=0)
    d = np.linalg.norm(block - centroid, axis=1)
    return block[np.argmin(d)]
