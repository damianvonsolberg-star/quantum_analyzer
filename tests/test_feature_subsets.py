from __future__ import annotations

import pytest

from quantum_analyzer.features.subsets import resolve_feature_subset


def test_feature_subset_resolves_from_registry():
    cols = resolve_feature_subset("geom_vol")
    assert "realized_vol_24h" in cols


def test_unknown_subset_fails_closed():
    with pytest.raises(ValueError):
        resolve_feature_subset("does_not_exist")
