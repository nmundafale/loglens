.PHONY: install test lint format clean

install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check .
	mypy src

format:
	ruff format .

clean:
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf .mypy_cache
	rm -rf build
	rm -rf dist
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
