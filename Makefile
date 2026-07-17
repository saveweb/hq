.PHONY: check fmt test test-go test-python test-postgres

check: test
	go vet ./...

fmt:
	find . -path './.git' -prune -o -path './sdk/python/.venv' -prune -o -name '*.go' -type f -print | xargs -r gofmt -w
	uv run --project sdk/python ruff format sdk/python
	uv run --project sdk/python ruff check --fix sdk/python

test: test-go test-python

test-go:
	go test ./...

test-python:
	uv run --project sdk/python pytest

test-postgres:
	./scripts/test-postgres.sh
