# Quantum Analyzer Operator UI

A Streamlit operator UI for **advisory-only** workflows.

## ⚠ Advisory-only safety

This UI **does not**:
- place trades
- sign transactions
- use private keys
- call exchange execution APIs

It only reads artifacts + market/wallet data and provides operator guidance.

---

## Quick start

### Option A: one command
```bash
make ui
```

### Option B: script
```bash
./scripts/run_ui.sh
```

Both start:
```bash
streamlit run ui/app.py
```

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

---

## Manual smoke test (5 min)

1. Start UI:
   ```bash
   make ui
   ```
2. In sidebar, set `ARTIFACT_DIR`, wallet, and RPC URL.
3. Verify pages open without errors.
4. Live Advice:
   - click refresh wallet/price
   - confirm NAV, allocation, and advisory deltas render.
5. Backtest:
   - confirm KPI cards + equity/drawdown charts.
6. Templates:
   - confirm top matches + filters + Why now section.
7. Drift:
   - confirm status card and operator response visible.
8. Journal:
   - add one BUY fill and one SELL fill
   - verify net qty / avg entry / PnL updates.

---

## UI screenshots (placeholders)

Add your screenshots here after local run:
- `docs/ui-live-advice.png`
- `docs/ui-backtest.png`
- `docs/ui-templates.png`
- `docs/ui-drift.png`
- `docs/ui-journal.png`
