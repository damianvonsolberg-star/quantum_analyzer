# quantum_analyzer

Fresh Python 3.11 research core for a new market analyzer stack.

## Goals

- No browser/localStorage architecture.
- Shared importable core for both backtest and live advisory.
- Contract-first design using dataclasses.

## Package tree

- `quantum_analyzer/data/`
- `quantum_analyzer/features/`
- `quantum_analyzer/state/`
- `quantum_analyzer/paths/`
- `quantum_analyzer/forecast/`
- `quantum_analyzer/policy/`
- `quantum_analyzer/backtest/`
- `quantum_analyzer/monitoring/`
- `quantum_analyzer/live/`
- `quantum_analyzer/config/`

Core contracts are in `quantum_analyzer/contracts.py`:

- `FeatureSnapshot`
- `StateBelief`
- `HorizonDistribution`
- `ForecastBundle`
- `ActionProposal`

## Quick start

```bash
cd quantum_analyzer
python3.11 -m pip install -e .
pytest
```
