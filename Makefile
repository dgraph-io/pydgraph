# Add local bin directories to PATH
export PATH := $(HOME)/.local/bin:$(HOME)/.cargo/bin:$(PATH)

# Source venv if it exists and isn't already active
PROJECT_VENV := $(CURDIR)/.venv
ACTIVATE := $(wildcard .venv/bin/activate)
ifneq ($(VIRTUAL_ENV),$(PROJECT_VENV))
  ifdef ACTIVATE
    RUN := . .venv/bin/activate &&
  else
    RUN :=
  endif
else
  RUN :=
endif

.PHONY: help setup sync deps deps-uv deps-ruff test check protogen

.DEFAULT_GOAL := help

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: deps ## Setup project (install tools and sync dependencies)
	$(RUN) uv sync --group dev --extra dev
	@if [ ! -f .git/hooks/pre-commit ] || ! grep -q "pre-commit" .git/hooks/pre-commit 2>/dev/null; then \
		echo "Installing pre-commit hooks..."; \
		$(RUN) pre-commit install; \
	fi

sync: ## Sync project virtual environment dependencies
	$(RUN) uv sync --group dev --extra dev

check: ## Run pre-commit hooks on all files
	$(RUN) pre-commit run --all-files

protogen: ## Regenerate protobuf files (requires Python 3.13+)
	$(RUN) uv run python scripts/protogen.py

test: ## Run tests
	$(RUN) uv run pytest

deps: deps-uv deps-ruff ## Install tool dependencies (uv, ruff)

deps-uv:
	@(command -v uv >/dev/null 2>&1 && command -v uvx >/dev/null 2>&1) || { \
		echo "uv/uvx not found, installing..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
	}

deps-ruff:
	@command -v ruff >/dev/null 2>&1 || { \
		echo "ruff not found, installing..."; \
		curl -LsSf https://astral.sh/ruff/install.sh | sh; \
	}
