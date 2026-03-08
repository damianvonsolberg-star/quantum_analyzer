#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess


def main() -> int:
    ap = argparse.ArgumentParser(description="Schedule-friendly explorer + promotion runner")
    ap.add_argument("--preset", default="daily", choices=["fast", "daily", "full"])
    ap.add_argument("--explorer-root", default="artifacts/explorer")
    ap.add_argument("--governance-status", default="OK", choices=["OK", "WATCH", "HALT"])
    args = ap.parse_args()

    r1 = subprocess.run(
        ["python3", "scripts/run_explorer.py", "--preset", args.preset, "--artifacts-root", args.explorer_root],
        capture_output=True,
        text=True,
    )
    if r1.returncode != 0:
        print(r1.stderr or r1.stdout)
        return r1.returncode

    r2 = subprocess.run(
        [
            "python3",
            "scripts/promote_signal.py",
            "--explorer-root",
            args.explorer_root,
            "--out-root",
            "artifacts/promoted",
            "--governance-status",
            args.governance_status,
        ],
        capture_output=True,
        text=True,
    )
    if r2.returncode != 0:
        print(r2.stderr or r2.stdout)
        return r2.returncode

    print("schedule_explorer: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
