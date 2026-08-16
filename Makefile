.PHONY: check deploy fmt test test-go test-python test-shell test-postgres test-e2e

deploy:
	./scripts/deploy.sh

check: test
	go vet ./...

fmt:
	find . -path './.git' -prune -o -path './sdk/python/.venv' -prune -o -name '*.go' -type f -print | xargs -r gofmt -w
	uv run --project sdk/python ruff format sdk/python
	uv run --project sdk/python ruff check --fix sdk/python

test: test-go test-python test-shell

test-go:
	go test ./...

test-python:
	uv run --project sdk/python python -m pytest

test-shell:
	bash -n scripts/*.sh internal/trackerweb/*.sh

test-postgres:
	./scripts/test-postgres.sh

test-e2e:
	./scripts/test-e2e.sh
