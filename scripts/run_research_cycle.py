#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
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
    args = ap.parse_args()

    ok, status = run_research_cycle(config=args.config, discovery_config=args.discovery_config)
    print(json.dumps(status, indent=2, default=str))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
