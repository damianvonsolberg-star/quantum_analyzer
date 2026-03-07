from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pickle

import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression


@dataclass
class ProbCalibrator:
    method: str = "isotonic"  # isotonic | beta
    _iso: IsotonicRegression | None = None
    _beta: LogisticRegression | None = None

    def fit(self, p_raw: np.ndarray, y_true: np.ndarray) -> "ProbCalibrator":
        p = np.asarray(p_raw, dtype=float).clip(1e-6, 1 - 1e-6)
        y = np.asarray(y_true, dtype=int)

        if self.method == "isotonic":
            iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
            iso.fit(p, y)
            self._iso = iso
        elif self.method == "beta":
            # beta calibration proxy: logistic regression on logit(p)
            x = np.log(p / (1 - p)).reshape(-1, 1)
            lr = LogisticRegression(max_iter=200)
            lr.fit(x, y)
            self._beta = lr
        else:
            raise ValueError(f"Unknown method {self.method}")
        return self

    def predict(self, p_raw: np.ndarray) -> np.ndarray:
        p = np.asarray(p_raw, dtype=float).clip(1e-6, 1 - 1e-6)
        if self.method == "isotonic":
            if self._iso is None:
                raise RuntimeError("Calibrator not fitted")
            return np.asarray(self._iso.predict(p), dtype=float)
        if self._beta is None:
            raise RuntimeError("Calibrator not fitted")
        x = np.log(p / (1 - p)).reshape(-1, 1)
        return self._beta.predict_proba(x)[:, 1]

    def calibration_score(self, p_raw: np.ndarray, y_true: np.ndarray) -> float:
        p_cal = self.predict(p_raw)
        y = np.asarray(y_true, dtype=float)
        brier = np.mean((p_cal - y) ** 2)
        return float(max(0.0, 1.0 - brier))

    def save(self, path: str | Path) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, path: str | Path) -> "ProbCalibrator":
        with open(path, "rb") as f:
            obj = pickle.load(f)
        if not isinstance(obj, ProbCalibrator):
            raise TypeError("Invalid calibrator artifact")
        return obj
