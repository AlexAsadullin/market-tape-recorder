.PHONY: setup lint format typecheck test test-live reproduce report report-smoke record

UV ?= uv

setup:
	$(UV) venv --python 3.13 --allow-existing .venv
	$(UV) sync --frozen

lint:
	$(UV) run ruff check src tests experiments
	$(UV) run ruff format --check src tests experiments

format:
	$(UV) run ruff format src tests experiments
	$(UV) run ruff check --fix src tests experiments

typecheck:
	$(UV) run mypy

test:
	$(UV) run pytest --cov --cov-report=term

test-live:
	$(UV) run pytest -m live

reproduce:
	$(UV) run python -m tape_recorder.demo

report:
	$(UV) sync --frozen --group bench
	$(UV) run --group bench python -m experiments

report-smoke:
	$(UV) run --group bench python -m experiments --smoke

record:
	$(UV) run tape record
