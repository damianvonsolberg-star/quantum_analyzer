from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass
class UIConfig:
    default_artifacts_dir: str = str(Path("artifacts").resolve())
    default_wallet: str = os.getenv("BENCHMARK_WALLET", "")
    default_rpc: str = os.getenv("SOL_RPC_URL", "https://api.mainnet-beta.solana.com")
    default_refresh_seconds: int = 30
