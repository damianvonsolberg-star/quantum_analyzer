# Quantum Analyzer Operator UI

A Streamlit operator UI for **advisory-only** workflows.

## ⚠ Advisory-only safety

This UI **does not**:
- place trades
- sign transactions
- use private keys
- call exchange execution APIs
- retrain models at runtime

It only reads artifacts + market/wallet data and provides operator guidance.

---

## Quick start (one command)

```bash
make ui
```

Equivalent:
```bash
./scripts/run_ui.sh
```

Both start:
```bash
streamlit run ui/app.py
```

---

## Production-safe launch expectations

- Runtime logs mask sensitive values (wallet/RPC API key query params).
- Missing artifacts should degrade safely with explicit warnings (not crash).
- UI remains advisory-only; no execution path is enabled.
- Use non-root container runtime for pilot deployments (`Dockerfile.ui`).
- Health endpoint is monitored in container mode.

---

## Required environment variables

Copy and edit:
```bash
cp .env.example .env
```

Required/expected:
- `ARTIFACT_DIR` – folder with exported artifacts
- `SOL_RPC_URL` – Helius or standard Solana RPC URL
- `BENCHMARK_WALLET` – wallet address for read-only balances

Also used:
- `UI_DATA_DIR` – persistent local storage for manual journal (`journal.sqlite`)

---

## Artifact file expectations

Expected under `ARTIFACT_DIR`:

Required for full UI:
- `artifact_bundle.json`
- `summary.json`
- `equity_curve.csv`
- `actions.csv`

Optional:
- `templates.json` or `templates.parquet`
- `doctor_report.json`
- `rolling_metrics.csv` or `diagnostics_rolling.csv`

### Fallback behavior

If artifacts are missing, pages still load and show warning banners/placeholders like:
- “Missing required artifacts …”
- “not yet implemented”
- empty-state tables/charts

UI should not crash on partial diagnostics.

---

## Pages

- **Live Advice**: traffic-light recommendation + wallet-aligned advisory deltas
- **Backtest**: KPIs, equity/drawdown charts, action table, downloads
- **Templates**: matched archetypes and “Why now?” explanation
- **Drift & Governance**: OK/WATCH/HALT trust status and operator response
- **Journal**: manual fills tracker (sqlite), realized/unrealized PnL, reconciliation
- **Explorer**: one-click preset scan, leaderboard, promoted signal bundle, research cycle status
- **Discovery**: surviving/rejected discovered signals, novelty/complexity, genealogy summary

---

## Validation commands

UI-focused tests:
```bash
make test-ui
```

Explorer tests:
```bash
make test-explorer
```

Research cycle tests:
```bash
make test-research
```

Run explorer presets:
```bash
make explorer-fast
make explorer-daily
```

Full project tests:
```bash
make test
```

Optional shell lint:
```bash
make lint-shell
```

---

## Manual smoke test checklist (5 min)

1. Start UI:
   ```bash
   make ui
   ```
2. In sidebar, set `ARTIFACT_DIR`, wallet, and RPC URL.
3. Verify pages open without errors.
4. Live Advice:
   - click refresh wallet/price
   - confirm freshness badges (artifact + live refresh) render
   - confirm NAV, allocation, and advisory deltas render.
   - confirm target semantics line clearly shows generic model target vs spot actionable target and scope (whole wallet vs sleeve).
5. Backtest:
   - confirm KPI cards + equity/drawdown charts.
6. Templates:
   - confirm top matches + filters + Why now section.
7. Drift:
   - confirm status card and operator response visible.
8. Journal:
   - add one BUY fill and one SELL fill
   - verify net qty / avg entry / PnL updates.
9. Explorer:
   - run FAST preset from Explorer page
   - verify run manifest and leaderboard render
   - run promotion and verify current promoted signal appears

---

## Container (optional)

Build:
```bash
docker build -f Dockerfile.ui -t quantum-analyzer-ui .
```

Run:
```bash
docker run --rm -p 8501:8501 \
  -e ARTIFACT_DIR=/app/artifacts \
  -e SOL_RPC_URL="https://api.mainnet-beta.solana.com" \
  -e BENCHMARK_WALLET="" \
  quantum-analyzer-ui
```

Notes:
- container runs as non-root `appuser`
- healthcheck probes `/_stcore/health`

---

## UI screenshots (placeholders)

Add your screenshots here after local run:
- `docs/ui-live-advice.png`
- `docs/ui-backtest.png`
- `docs/ui-templates.png`
- `docs/ui-drift.png`
- `docs/ui-journal.png`
