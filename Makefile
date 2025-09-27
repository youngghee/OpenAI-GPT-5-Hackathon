.PHONY: format lint test ci

format:
	black src tests

lint:
	ruff check src tests

test:
	pytest --maxfail=1 --disable-warnings

ci: format lint test
