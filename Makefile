.PHONY: run-app run-worker test lint format format-check type-check dependency-audit secret-scan security check

UV ?= uv

run-app:
	$(UV) run vuzol-app

run-worker:
	$(UV) run vuzol-worker

test:
	$(UV) run pytest

lint:
	$(UV) run ruff check .

format:
	$(UV) run ruff format .

format-check:
	$(UV) run ruff format --check .

type-check:
	$(UV) run mypy

dependency-audit:
	$(UV) run pip-audit

secret-scan:
	$(UV) run detect-secrets-hook $$(git ls-files -co --exclude-standard)

security: dependency-audit secret-scan

check: lint format-check type-check test security
