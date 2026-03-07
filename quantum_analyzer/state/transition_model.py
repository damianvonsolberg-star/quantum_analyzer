from __future__ import annotations

import numpy as np


def normalize_transition_matrix(matrix: np.ndarray) -> np.ndarray:
    m = np.asarray(matrix, dtype=float)
    if m.ndim != 2 or m.shape[0] != m.shape[1]:
        raise ValueError("transition matrix must be square")
    row_sums = m.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0.0] = 1.0
    return m / row_sums


def stationary_distribution(matrix: np.ndarray, n_iter: int = 1000) -> np.ndarray:
    m = normalize_transition_matrix(matrix)
    n = m.shape[0]
    p = np.full(n, 1.0 / n)
    for _ in range(n_iter):
        p = p @ m
    s = p.sum()
    return p / s if s > 0 else np.full(n, 1.0 / n)
