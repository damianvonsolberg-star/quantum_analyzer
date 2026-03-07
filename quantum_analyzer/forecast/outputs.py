from __future__ import annotations

import json
from pathlib import Path

from quantum_analyzer.contracts import ForecastBundle


def forecast_to_json(bundle: ForecastBundle) -> str:
    return bundle.to_json()


def save_forecast_json(bundle: ForecastBundle, path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(bundle.to_dict(), f, indent=2, default=str)
