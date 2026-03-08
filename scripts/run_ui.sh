#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env" ]]; then
  # shellcheck disable=SC1091
  source .env
fi

export ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT_DIR/artifacts}"
export SOL_RPC_URL="${SOL_RPC_URL:-https://api.mainnet-beta.solana.com}"
export BENCHMARK_WALLET="${BENCHMARK_WALLET:-}"
export UI_DATA_DIR="${UI_DATA_DIR:-$ROOT_DIR/ui_data}"

mkdir -p "$UI_DATA_DIR"

echo "[quantum_analyzer UI]"
echo "ARTIFACT_DIR=$ARTIFACT_DIR"
echo "SOL_RPC_URL=$SOL_RPC_URL"
echo "BENCHMARK_WALLET=${BENCHMARK_WALLET:-<empty>}"
echo "UI_DATA_DIR=$UI_DATA_DIR"

if [[ ! -f "$ARTIFACT_DIR/artifact_bundle.json" ]]; then
  if [[ -f "$ROOT_DIR/ui/fixtures/artifact_bundle.json" ]]; then
    echo "[warn] No artifact_bundle.json in ARTIFACT_DIR. Falling back to ui/fixtures."
    export ARTIFACT_DIR="$ROOT_DIR/ui/fixtures"
  else
    echo "[warn] No artifact_bundle.json in ARTIFACT_DIR. UI will still start with fallback placeholders."
  fi
fi

if command -v streamlit >/dev/null 2>&1; then
  exec streamlit run ui/app.py
else
  exec python3 -m streamlit run ui/app.py
fi
