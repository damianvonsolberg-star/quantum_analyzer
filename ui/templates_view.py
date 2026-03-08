from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class TemplateRow:
    template_id: str
    label: str
    description: str
    sample_count: int
    expectancy: float
    pf_proxy: float
    robustness: float
    preferred_action: str
    preferred_horizon: str
    similarity: float
    family: str


def _pick(row: pd.Series, keys: list[str], default: Any = None) -> Any:
    for k in keys:
        if k in row.index and pd.notna(row[k]):
            return row[k]
    return default


def templates_to_view_df(templates_df: pd.DataFrame) -> pd.DataFrame:
    if templates_df is None or templates_df.empty:
        return pd.DataFrame()

    rows: list[TemplateRow] = []
    for _, r in templates_df.iterrows():
        tid = str(_pick(r, ["template_id", "id", "name"], "unknown"))
        label = str(_pick(r, ["label", "name", "template_name"], tid))
        desc = str(_pick(r, ["description", "desc", "narrative"], f"Template {label}"))
        sample_count = int(_pick(r, ["sample_count", "support", "n_samples"], 0) or 0)
        expectancy = float(_pick(r, ["expectancy", "edge", "mean_pnl"], 0.0) or 0.0)
        pf_proxy = float(_pick(r, ["pf_proxy", "profit_factor", "pf"], 0.0) or 0.0)
        robustness = float(_pick(r, ["oos_robustness", "robustness", "robustness_score"], 0.0) or 0.0)
        preferred_action = str(_pick(r, ["preferred_action", "action_bias", "action"], "HOLD"))
        preferred_horizon = str(_pick(r, ["preferred_horizon", "horizon", "best_horizon"], "h12"))
        similarity = float(_pick(r, ["similarity", "match_score", "score"], 0.0) or 0.0)
        family = str(_pick(r, ["family", "archetype", "template_family"], "unknown"))

        rows.append(
            TemplateRow(
                template_id=tid,
                label=label,
                description=desc,
                sample_count=sample_count,
                expectancy=expectancy,
                pf_proxy=pf_proxy,
                robustness=robustness,
                preferred_action=preferred_action,
                preferred_horizon=preferred_horizon,
                similarity=similarity,
                family=family,
            )
        )

    out = pd.DataFrame([x.__dict__ for x in rows])
    out = out.sort_values(["similarity", "robustness", "expectancy"], ascending=False).reset_index(drop=True)
    return out


def apply_template_filters(
    df: pd.DataFrame,
    action: str = "ALL",
    horizon: str = "ALL",
    robustness_threshold: float = 0.0,
    min_sample_count: int = 0,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if action != "ALL":
        out = out[out["preferred_action"].astype(str) == action]
    if horizon != "ALL":
        out = out[out["preferred_horizon"].astype(str) == horizon]
    out = out[out["robustness"] >= float(robustness_threshold)]
    out = out[out["sample_count"] >= int(min_sample_count)]
    return out.reset_index(drop=True)


def build_why_now(features: dict[str, Any] | None, templates_df: pd.DataFrame) -> dict[str, str]:
    f = features or {}
    micro = f.get("micro_range_position", f.get("range_pos_micro", "n/a"))
    meso = f.get("meso_range_position", f.get("range_pos_meso", "n/a"))
    macro = f.get("macro_range_position", f.get("range_pos_macro", "n/a"))
    vol = f.get("vol_state", f.get("volatility_state", "n/a"))
    cross = f.get("cross_asset_context", f.get("cross_asset", "n/a"))

    dominant = "unknown"
    if templates_df is not None and not templates_df.empty and "family" in templates_df.columns:
        dominant = str(templates_df.iloc[0]["family"])

    return {
        "micro_range_position": str(micro),
        "meso_range_position": str(meso),
        "macro_range_position": str(macro),
        "current_volatility_state": str(vol),
        "cross_asset_context": str(cross),
        "dominant_template_family": dominant,
    }
