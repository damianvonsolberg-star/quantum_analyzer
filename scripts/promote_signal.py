#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantum_analyzer.experiments.promotion import promote_from_leaderboard


def main() -> int:
    ap = argparse.ArgumentParser(description="Promote explorer output to current signal")
    ap.add_argument("--explorer-root", default="artifacts/explorer")
    ap.add_argument("--out-root", default="artifacts/promoted")
    ap.add_argument("--min-score", type=float, default=0.25)
    ap.add_argument("--governance-status", default="OK", choices=["OK", "WATCH", "HALT"])
    args = ap.parse_args()

    out = promote_from_leaderboard(
        args.explorer_root,
        args.out_root,
        min_score=args.min_score,
        governance_status=args.governance_status,
    )
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
