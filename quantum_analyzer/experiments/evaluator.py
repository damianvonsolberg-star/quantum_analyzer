from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from quantum_analyzer.backtest.engine import BacktestConfig, run_backtest
from quantum_analyzer.discovery.dsl import eval_genome_score, score_to_actions
from quantum_analyzer.backtest.walkforward import WalkForwardConfig
from quantum_analyzer.strategies import (
    BreakoutContinuationStrategy,
    CandidateStrategy,
    EnsembleStrategy,
    InterpretableMLBaselineStrategy,
    MeanReversionStrategy,
    RegimeSwitchStrategy,
    TrendFollowingStrategy,
)


FAMILY_MAP = {
    "trend": TrendFollowingStrategy,
    "mean_reversion": MeanReversionStrategy,
    "breakout": BreakoutContinuationStrategy,
    "regime_switch": RegimeSwitchStrategy,
    "ml_baseline": InterpretableMLBaselineStrategy,
}


@dataclass
class CandidateResult:
    candidate_id: str
    family: str
    summary: dict[str, Any]
    diagnostics: dict[str, Any]
    artifact_dir: str | None = None


class DiscoveryGenomeStrategy(CandidateStrategy):
    def __init__(self, *args, genome: dict[str, Any], **kwargs):
        super().__init__(*args, **kwargs)
        self.genome = genome

    def generate_scores(self, features: pd.DataFrame) -> pd.Series:
        return eval_genome_score(self.genome, features)

    def propose_actions(self, features: pd.DataFrame) -> pd.Series:
        return score_to_actions(self.generate_scores(features), self.genome.get("rules", {}))


def build_candidate(candidate_id: str, family: str, params: dict[str, Any], feature_subset: str, horizon: int, regime_filter: str) -> CandidateStrategy:
    if family == "discovery_genome":
        return DiscoveryGenomeStrategy(candidate_id=candidate_id, family=family, params=params, feature_subset=feature_subset, horizon=horizon, regime_filter=regime_filter, genome=params.get("genome", {}))
    if family == "ensemble":
        members = [
            TrendFollowingStrategy(candidate_id=f"{candidate_id}:trend", family="trend", params=params, feature_subset=feature_subset, horizon=horizon, regime_filter=regime_filter),
            MeanReversionStrategy(candidate_id=f"{candidate_id}:mr", family="mean_reversion", params=params, feature_subset=feature_subset, horizon=horizon, regime_filter=regime_filter),
            BreakoutContinuationStrategy(candidate_id=f"{candidate_id}:bo", family="breakout", params=params, feature_subset=feature_subset, horizon=horizon, regime_filter=regime_filter),
        ]
        return EnsembleStrategy(candidate_id=candidate_id, family=family, params=params, feature_subset=feature_subset, horizon=horizon, regime_filter=regime_filter, members=members)
    cls = FAMILY_MAP.get(family, TrendFollowingStrategy)
    return cls(candidate_id=candidate_id, family=family, params=params, feature_subset=feature_subset, horizon=horizon, regime_filter=regime_filter)


def evaluate_candidate(
    *,
    features: pd.DataFrame,
    close: pd.Series,
    candidate: CandidateStrategy,
    walkforward: WalkForwardConfig,
    backtest: BacktestConfig,
    out_dir: str | None = None,
) -> CandidateResult:
    r = run_backtest(
        features=features,
        close=close,
        templates=[],
        wf_cfg=walkforward,
        bt_cfg=backtest,
        out_dir=out_dir,
        candidate_strategy=candidate,
    )
    return CandidateResult(
        candidate_id=candidate.candidate_id,
        family=candidate.family,
        summary=r.summary,
        diagnostics=r.diagnostics.to_dict(),
        artifact_dir=out_dir,
    )
