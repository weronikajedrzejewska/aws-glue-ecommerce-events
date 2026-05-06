.PHONY: install-dev lint test smoke-test

install-dev:
	python -m pip install -r requirements-dev.txt

lint:
	ruff check .

test:
	pytest

smoke-test:
	pytest tests/integration/test_local_pipeline_smoke.py
