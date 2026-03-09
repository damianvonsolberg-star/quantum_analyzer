from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json


@dataclass(frozen=True)
class FeatureDef:
    name: str
    family: str
    version: str
    dependencies: tuple[str, ...]
    lookahead_policy: str
    source_requirements: tuple[str, ...]


DEFAULT_FEATURE_REGISTRY: dict[str, FeatureDef] = {
    "micro_range_pos_24h": FeatureDef("micro_range_pos_24h", "range", "1.0.0", ("close",), "no_lookahead", ("sol_klines",)),
    "meso_range_pos_7d": FeatureDef("meso_range_pos_7d", "range", "1.0.0", ("close",), "no_lookahead", ("sol_klines",)),
    "realized_vol_24h": FeatureDef("realized_vol_24h", "vol", "1.0.0", ("close",), "no_lookahead", ("sol_klines",)),
    "aggtrade_imbalance": FeatureDef("aggtrade_imbalance", "flow", "1.0.0", (), "no_lookahead", ("agg_trades",)),
    "orderbook_imbalance": FeatureDef("orderbook_imbalance", "flow", "1.0.0", (), "no_lookahead", ("book_ticker",)),
    "btc_return_1h": FeatureDef("btc_return_1h", "cross", "1.0.0", ("btc_close",), "no_lookahead", ("btc_klines",)),
    "basis_bps": FeatureDef("basis_bps", "derivatives", "1.0.0", (), "no_lookahead", ("basis",)),
    "oi_zscore": FeatureDef("oi_zscore", "derivatives", "1.0.0", (), "no_lookahead", ("open_interest",)),
    "hour_sin": FeatureDef("hour_sin", "time_structure", "1.0.0", (), "no_lookahead", ("clock",)),
    "dow": FeatureDef("dow", "time_structure", "1.0.0", (), "no_lookahead", ("clock",)),
}


def feature_versions(registry: dict[str, FeatureDef] | None = None) -> dict[str, str]:
    r = registry or DEFAULT_FEATURE_REGISTRY
    return {k: v.version for k, v in r.items()}


def registry_version_hash(registry: dict[str, FeatureDef] | None = None) -> str:
    import hashlib
    import json

    versions = feature_versions(registry)
    raw = json.dumps(dict(sorted(versions.items())), sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]
