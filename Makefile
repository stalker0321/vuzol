.PHONY: run-app run-worker run-telegram test test-rootless test-rootless-proxy test-postgres lint format format-check type-check dependency-audit secret-scan security check mvp-check db-up db-down db-migrate db-current

UV ?= uv
LOCAL_DATABASE_DSN ?= postgresql+psycopg://vuzol:vuzol-local-only@127.0.0.1:5432/vuzol# pragma: allowlist secret
LOCAL_TEST_DATABASE_DSN ?= postgresql://vuzol:vuzol-local-only@127.0.0.1:5432/vuzol_test# pragma: allowlist secret

run-app:
	$(UV) run vuzol-app

run-worker:
	$(UV) run vuzol-worker

run-telegram:
	$(UV) run vuzol-telegram

test:
	@if command -v vuzol-offline-test >/dev/null 2>&1; then \
		vuzol-offline-test; \
	else \
		$(UV) run pytest; \
	fi

test-rootless:
	@test -n "$(VUZOL_ROOTLESS_DOCKER_SOCKET)" || (echo "VUZOL_ROOTLESS_DOCKER_SOCKET is required" >&2; exit 2)
	$(UV) run pytest tests/integration/execution/test_proxy_networks.py --no-cov

test-rootless-proxy:
	@test -n "$(VUZOL_ROOTLESS_DOCKER_SOCKET)" || (echo "VUZOL_ROOTLESS_DOCKER_SOCKET is required" >&2; exit 2)
	@test -n "$(VUZOL_PROXY_IMAGE)" || (echo "VUZOL_PROXY_IMAGE is required" >&2; exit 2)
	@test -n "$(VUZOL_PROXY_RUNTIME_ROOT)" || (echo "VUZOL_PROXY_RUNTIME_ROOT is required" >&2; exit 2)
	$(UV) run pytest tests/integration/execution/test_proxy_service.py --no-cov

test-postgres: db-up db-migrate
	VUZOL_DATABASE_DSN_REFERENCE=env:VUZOL_DATABASE_DSN VUZOL_DATABASE_DSN="$(subst postgresql://,postgresql+psycopg://,$(LOCAL_TEST_DATABASE_DSN))" $(UV) run alembic upgrade head
	VUZOL_TEST_DATABASE_DSN="$(LOCAL_TEST_DATABASE_DSN)" $(UV) run pytest -m postgresql --no-cov

lint:
	$(UV) run ruff check .

format:
	$(UV) run ruff format .

format-check:
	$(UV) run ruff format --check .

type-check:
	$(UV) run mypy

dependency-audit:
	@if command -v vuzol-offline-dependency-audit >/dev/null 2>&1; then \
		vuzol-offline-dependency-audit; \
	else \
		$(UV) run pip-audit; \
	fi

secret-scan:
	$(UV) run detect-secrets-hook $$(git ls-files -co --exclude-standard)

security: dependency-audit secret-scan

check: lint format-check type-check test security

mvp-check:
	uv run python deploy/mvp/check.py --expected-sha "$$(git rev-parse HEAD)"

db-up:
	docker compose up -d postgres

db-down:
	docker compose down

db-migrate:
	VUZOL_DATABASE_DSN_REFERENCE=env:VUZOL_DATABASE_DSN VUZOL_DATABASE_DSN="$(LOCAL_DATABASE_DSN)" $(UV) run alembic upgrade head

db-current:
	VUZOL_DATABASE_DSN_REFERENCE=env:VUZOL_DATABASE_DSN VUZOL_DATABASE_DSN="$(LOCAL_DATABASE_DSN)" $(UV) run alembic current
