SHELL := /bin/bash

.PHONY: ui ui-install ui-doctor test-ui test lint-shell

ui-install:
	python3 -m pip install -r requirements-ui.txt

ui:
	bash ./scripts/run_ui.sh

ui-doctor:
	python3 scripts/qa_doctor.py --artifacts "$${ARTIFACT_DIR:-./artifacts}"

test-ui:
	python3 -m pytest tests/test_ui_adapters.py tests/test_wallet.py tests/test_live_advice.py tests/test_backtest_page.py tests/test_templates_page.py tests/test_drift_page.py tests/test_journal.py -q

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
