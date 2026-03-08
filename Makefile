SHELL := /bin/bash

.PHONY: ui ui-install ui-doctor test-ui test lint-shell explorer-fast explorer-daily explorer-full test-explorer promote-signal test-signals schedule-explorer

ui-install:
	python3 -m pip install -r requirements-ui.txt

ui:
	bash ./scripts/run_ui.sh

ui-doctor:
	python3 scripts/qa_doctor.py --artifacts "$${ARTIFACT_DIR:-./artifacts}"

test-ui:
	python3 -m pytest tests/test_ui_adapters.py tests/test_wallet.py tests/test_live_advice.py tests/test_backtest_page.py tests/test_templates_page.py tests/test_drift_page.py tests/test_journal.py -q

test-explorer:
	python3 -m pytest tests/test_experiment_specs.py tests/test_experiment_runner.py tests/test_score_engine.py tests/test_registry.py -q

test-signals:
	python3 -m pytest tests/test_signal_selector.py tests/test_promotion.py -q

explorer-fast:
	python3 scripts/run_explorer.py --preset fast --artifacts-root "$${EXPLORER_ARTIFACTS_ROOT:-./artifacts/explorer}"

explorer-daily:
	python3 scripts/run_explorer.py --preset daily --artifacts-root "$${EXPLORER_ARTIFACTS_ROOT:-./artifacts/explorer}"

explorer-full:
	python3 scripts/run_explorer.py --preset full --artifacts-root "$${EXPLORER_ARTIFACTS_ROOT:-./artifacts/explorer}"

promote-signal:
	python3 scripts/promote_signal.py --explorer-root "$${EXPLORER_ARTIFACTS_ROOT:-./artifacts/explorer}" --out-root "$${PROMOTED_ARTIFACTS_ROOT:-./artifacts/promoted}"

schedule-explorer:
	python3 scripts/schedule_explorer.py --preset "$${EXPLORER_PRESET:-daily}" --explorer-root "$${EXPLORER_ARTIFACTS_ROOT:-./artifacts/explorer}" --governance-status "$${GOVERNANCE_STATUS:-OK}"

# Full project test suite

test:
	python3 -m pytest -q

# Optional shell lint when shellcheck exists
lint-shell:
	@if command -v shellcheck >/dev/null 2>&1; then \
		shellcheck scripts/run_ui.sh; \
		echo "shellcheck passed"; \
	else \
		echo "shellcheck not installed; skipping"; \
	fi
