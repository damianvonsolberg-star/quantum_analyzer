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

RESEARCH_INTERVAL_SECONDS="${RESEARCH_INTERVAL_SECONDS:-300}"
AUTO_RESEARCH_CYCLE="${AUTO_RESEARCH_CYCLE:-1}"

mask_wallet() {
  local w="$1"
  if [[ -z "$w" ]]; then
    echo "<empty>"
  elif [[ ${#w} -le 10 ]]; then
    echo "***"
  else
    echo "${w:0:4}...${w: -4}"
  fi
}

mask_rpc() {
  local u="$1"
  if [[ -z "$u" ]]; then
    echo "<empty>"
    return
  fi
  # mask common api-key query values
  local m
  m="$(echo "$u" | sed -E 's/(api-key=)[^&]+/\1***MASKED***/g; s/(apikey=)[^&]+/\1***MASKED***/g')"
  echo "$m"
}

echo "[quantum_analyzer UI]"
echo "ARTIFACT_DIR=$ARTIFACT_DIR"
echo "SOL_RPC_URL=$(mask_rpc "$SOL_RPC_URL")"
echo "BENCHMARK_WALLET=$(mask_wallet "$BENCHMARK_WALLET")"
echo "UI_DATA_DIR=$UI_DATA_DIR"
echo "AUTO_RESEARCH_CYCLE=$AUTO_RESEARCH_CYCLE"
echo "RESEARCH_INTERVAL_SECONDS=$RESEARCH_INTERVAL_SECONDS"

if [[ ! -f "$ARTIFACT_DIR/artifact_bundle.json" ]]; then
  echo "[warn] No artifact_bundle.json in ARTIFACT_DIR. UI will start without fixture fallback (production-safe)."
fi

if [[ "$AUTO_RESEARCH_CYCLE" == "1" ]]; then
  PID_FILE="$UI_DATA_DIR/research_cycle.pid"
  LOG_FILE="$UI_DATA_DIR/research_cycle.log"

  running=0
  if [[ -f "$PID_FILE" ]]; then
    old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$old_pid" ]] && kill -0 "$old_pid" >/dev/null 2>&1; then
      running=1
      echo "[info] Research cycle scheduler already running (pid=$old_pid)"
    else
      rm -f "$PID_FILE"
    fi
  fi

  if [[ "$running" == "0" ]]; then
    echo "[info] Starting background research cycle scheduler..."
    nohup python3 scripts/schedule_research_cycle.py \
      --config "${RESEARCH_CONFIG:-config/research/solusdc_research.json}" \
      --discovery-config "${DISCOVERY_CONFIG:-config/discovery/discovery_daily.json}" \
      --interval-seconds "$RESEARCH_INTERVAL_SECONDS" \
      --runs 999999 \
      >>"$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "[info] Research cycle scheduler started (pid=$(cat "$PID_FILE"))"
  fi
fi

if command -v streamlit >/dev/null 2>&1; then
  exec streamlit run ui/app.py
else
  exec python3 -m streamlit run ui/app.py
fi
