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

.PHONY: help setup sync deps deps-uv deps-ruff deps-ty deps-docker test check protogen

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

test: deps-docker ## Run tests
	bash scripts/local-test.sh

deps: deps-uv deps-ruff deps-ty deps-docker ## Install tool dependencies (uv, ruff, ty, docker)

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

deps-ty:
	@command -v ty >/dev/null 2>&1 || { \
		echo "ty not found, installing..."; \
		curl -LsSf https://astral.sh/ty/install.sh | sh; \
	}

deps-docker: ## Check and install Docker if needed (requires Docker 20.10.0+)
	@if ! command -v docker >/dev/null 2>&1; then \
		echo "Docker not found, installing..."; \
		if [ "$$(uname)" = "Darwin" ]; then \
			if ! command -v brew >/dev/null 2>&1; then \
				echo "Homebrew not found. Please install Homebrew first: https://brew.sh"; \
				exit 1; \
			fi; \
			brew install --cask docker; \
			echo "Docker installed. Please start Docker Desktop and run 'make deps-docker' again."; \
			exit 1; \
		elif [ "$$(uname)" = "Linux" ]; then \
			sudo apt-get update; \
			sudo apt-get install -y docker.io docker-compose-plugin; \
			sudo systemctl start docker; \
			sudo systemctl enable docker; \
			sudo usermod -aG docker $$USER; \
			echo "Docker installed. Please log out and back in for group changes to take effect."; \
		else \
			echo "Unsupported OS. Please install Docker manually."; \
			exit 1; \
		fi; \
	fi
	@if ! docker compose version >/dev/null 2>&1; then \
		echo "Error: 'docker compose' command not available. Please ensure Docker Compose v2 is installed."; \
		exit 1; \
	fi
	@DOCKER_VERSION=$$(docker version --format '{{.Server.Version}}' 2>/dev/null); \
	if [ -z "$$DOCKER_VERSION" ]; then \
		echo "Error: Docker daemon is not running. Please start Docker Desktop."; \
		exit 1; \
	fi; \
	DOCKER_VERSION_SHORT=$$(echo $$DOCKER_VERSION | cut -d. -f1,2); \
	REQUIRED_VERSION="20.10"; \
	if [ "$$(printf '%s\n%s\n' "$$REQUIRED_VERSION" "$$DOCKER_VERSION_SHORT" | sort -V | head -n1)" != "$$REQUIRED_VERSION" ]; then \
		echo "Error: Docker version $$DOCKER_VERSION is less than required version $$REQUIRED_VERSION"; \
		exit 1; \
	fi; \
	echo "Docker version check passed: $$DOCKER_VERSION"
