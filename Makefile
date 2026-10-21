.PHONY: setup lint format typecheck test test-live reproduce report record

UV ?= uv

setup:
	$(UV) venv --python 3.13 --allow-existing .venv
	$(UV) sync --frozen

lint:
	$(UV) run ruff check src tests
	$(UV) run ruff format --check src tests

format:
	$(UV) run ruff format src tests
	$(UV) run ruff check --fix src tests

typecheck:
	$(UV) run mypy

test:
	$(UV) run pytest --cov --cov-report=term

test-live:
	$(UV) run pytest -m live

reproduce:
	$(UV) run python -m tape_recorder.demo

report:
	$(UV) run python -m tape_recorder.experiments

record:
	$(UV) run tape record
