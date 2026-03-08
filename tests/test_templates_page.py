from __future__ import annotations

import pandas as pd

from ui.templates_view import apply_template_filters, build_why_now, templates_to_view_df


def test_templates_to_view_df_mapping_and_sort():
    raw = pd.DataFrame(
        [
            {
                "template_id": "T1",
                "label": "Trend Up",
                "description": "Momentum continuation",
                "support": 50,
                "expectancy": 0.12,
                "pf": 1.4,
                "robustness": 0.72,
                "preferred_action": "BUY",
                "preferred_horizon": "h36",
                "similarity": 0.85,
                "family": "trend",
            },
            {
                "template_id": "T2",
                "label": "Chop",
                "support": 30,
                "expectancy": 0.02,
                "pf": 1.05,
                "robustness": 0.55,
                "preferred_action": "HOLD",
                "preferred_horizon": "h12",
                "similarity": 0.40,
                "family": "mean_revert",
            },
        ]
    )
    df = templates_to_view_df(raw)
    assert len(df) == 2
    assert df.iloc[0]["template_id"] == "T1"
    assert "pf_proxy" in df.columns


def test_template_filters():
    df = pd.DataFrame(
        [
            {"template_id": "T1", "preferred_action": "BUY", "preferred_horizon": "h36", "robustness": 0.8, "sample_count": 40},
            {"template_id": "T2", "preferred_action": "HOLD", "preferred_horizon": "h12", "robustness": 0.4, "sample_count": 10},
        ]
    )
    out = apply_template_filters(df, action="BUY", horizon="h36", robustness_threshold=0.7, min_sample_count=20)
    assert len(out) == 1
    assert out.iloc[0]["template_id"] == "T1"


def test_why_now_builder():
    feats = {
        "micro_range_position": "upper",
        "meso_range_position": "mid",
        "macro_range_position": "lower",
        "vol_state": "elevated",
        "cross_asset_context": "BTC supportive",
    }
    tdf = pd.DataFrame([{"family": "trend"}])
    why = build_why_now(feats, tdf)
    assert why["dominant_template_family"] == "trend"
    assert why["current_volatility_state"] == "elevated"
