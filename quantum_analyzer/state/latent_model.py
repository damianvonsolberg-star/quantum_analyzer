from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
import pickle
from typing import Any

import numpy as np
import pandas as pd
from sklearn.mixture import GaussianMixture

from quantum_analyzer.contracts import StateBelief

STATE_NAMES = [
    "range_low_absorption",
    "range_mid_drift",
    "range_high_reject",
    "squeeze",
    "breakout_up",
    "breakdown_down",
    "trend_up",
    "trend_down",
    "capitulation",
    "stabilization",
]


@dataclass
class LatentModelArtifacts:
    model_type: str
    state_names: list[str]
    feature_columns: list[str]
    transition_matrix: np.ndarray
    payload: dict[str, Any]


class LatentStateModel(ABC):
    @abstractmethod
    def fit(self, X: pd.DataFrame) -> "LatentStateModel":
        raise NotImplementedError

    @abstractmethod
    def predict_proba(self, X: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def transition_matrix(self) -> np.ndarray:
        raise NotImplementedError

    @abstractmethod
    def save(self, path: str | Path) -> None:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def load(cls, path: str | Path) -> "LatentStateModel":
        raise NotImplementedError


class GaussianHMMBaseline(LatentStateModel):
    """Pragmatic baseline with Gaussian emissions + Markov transition estimate.

    Uses GaussianMixture for emission probabilities and estimates transition matrix
    from most-likely state path. Keeps caller contract compatible with future HSMM.
    """

    def __init__(self, n_states: int = 10, random_state: int = 7):
        self.n_states = n_states
        self.random_state = random_state
        self.state_names = STATE_NAMES[:n_states]
        self.feature_columns: list[str] = []
        self._gmm = GaussianMixture(
            n_components=n_states,
            covariance_type="full",
            random_state=random_state,
            max_iter=400,
        )
        self._transition = np.full((n_states, n_states), 1.0 / n_states)

    @staticmethod
    def _prep(X: pd.DataFrame) -> pd.DataFrame:
        Z = X.copy().replace([np.inf, -np.inf], np.nan)
        Z = Z.fillna(method="ffill").fillna(method="bfill").fillna(0.0)
        return Z.astype(float)

    def fit(self, X: pd.DataFrame) -> "GaussianHMMBaseline":
        Z = self._prep(X)
        self.feature_columns = list(Z.columns)
        self._gmm.fit(Z.values)
        post = self._gmm.predict_proba(Z.values)
        path = np.argmax(post, axis=1)

        trans = np.ones((self.n_states, self.n_states))  # Laplace smoothing
        for i in range(1, len(path)):
            trans[path[i - 1], path[i]] += 1
        trans = trans / trans.sum(axis=1, keepdims=True)
        self._transition = trans
        return self

    def predict_proba(self, X: pd.DataFrame) -> pd.DataFrame:
        if not self.feature_columns:
            raise RuntimeError("Model not fitted")
        Z = self._prep(X[self.feature_columns])
        proba = self._gmm.predict_proba(Z.values)
        return pd.DataFrame(proba, index=Z.index, columns=self.state_names)

    def transition_matrix(self) -> np.ndarray:
        return self._transition.copy()

    def predict_state_beliefs(self, X: pd.DataFrame, symbol: str) -> list[StateBelief]:
        probs = self.predict_proba(X)
        out: list[StateBelief] = []
        k = len(probs.columns)
        logk = np.log(max(k, 2))
        for ts, row in probs.iterrows():
            p = row.values.astype(float)
            p = np.clip(p, 1e-12, 1.0)
            p = p / p.sum()
            entropy = float(-(p * np.log(p)).sum() / logk)
            confidence = float(p.max())
            out.append(
                StateBelief(
                    ts=ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                    symbol=symbol,
                    regime_probabilities={c: float(v) for c, v in row.items()},
                    entropy=entropy,
                    confidence=confidence,
                )
            )
        return out

    def save(self, path: str | Path) -> None:
        payload = {
            "n_states": self.n_states,
            "random_state": self.random_state,
            "state_names": self.state_names,
            "feature_columns": self.feature_columns,
            "transition": self._transition,
            "gmm": self._gmm,
        }
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(payload, f)

    @classmethod
    def load(cls, path: str | Path) -> "GaussianHMMBaseline":
        with open(path, "rb") as f:
            payload = pickle.load(f)
        obj = cls(n_states=payload["n_states"], random_state=payload["random_state"])
        obj.state_names = payload["state_names"]
        obj.feature_columns = payload["feature_columns"]
        obj._transition = payload["transition"]
        obj._gmm = payload["gmm"]
        return obj
