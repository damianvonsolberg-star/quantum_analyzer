from __future__ import annotations

from quantum_analyzer.features.registry import DEFAULT_FEATURE_REGISTRY


FEATURE_SUBSETS: dict[str, list[str]] = {
    "geom_core": ["micro_range_pos_24h", "meso_range_pos_7d"],
    "geom_vol": ["micro_range_pos_24h", "meso_range_pos_7d", "realized_vol_24h"],
    "geom_vol_cross": ["micro_range_pos_24h", "meso_range_pos_7d", "realized_vol_24h", "btc_return_1h"],
    "geom_vol_flow": ["micro_range_pos_24h", "meso_range_pos_7d", "realized_vol_24h", "aggtrade_imbalance", "orderbook_imbalance"],
    "full_stack": list(DEFAULT_FEATURE_REGISTRY.keys()),
}


def resolve_feature_subset(name: str, registry: dict[str, object] | None = None) -> list[str]:
    reg = registry or DEFAULT_FEATURE_REGISTRY
    if name not in FEATURE_SUBSETS:
        raise ValueError(f"Unknown feature subset: {name}")
    cols = FEATURE_SUBSETS[name]
    missing = [c for c in cols if c not in reg]
    if missing:
        raise ValueError(f"Subset {name} has unknown features: {missing}")
    return cols
