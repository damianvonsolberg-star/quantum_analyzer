#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.research_ops import run_research_cycle


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/research/solusdc_research.json")
    ap.add_argument("--discovery-config", default="config/discovery/discovery_daily.json")
    ap.add_argument("--interval-seconds", type=int, default=900)
    ap.add_argument("--runs", type=int, default=0, help="0 = run continuously")
    args = ap.parse_args()

    i = 0
    while True:
        run_research_cycle(config=args.config, discovery_config=args.discovery_config)
        i += 1
        if args.runs > 0 and i >= args.runs:
            break
        time.sleep(max(1, args.interval_seconds))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
