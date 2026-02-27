# Add local bin directories to PATH
SHELL := /bin/bash
export PATH := $(HOME)/.local/bin:$(HOME)/.cargo/bin:$(PATH)

# Export test configuration variables so they're available to child processes
# Usage: make test STRESS_TEST_MODE=moderate PYTEST_ARGS="-v"
#        make test LOG=info   (adds --log-cli-level=INFO to default PYTEST_ARGS)
export STRESS_TEST_MODE
export DGRAPH_IMAGE_TAG

# When LOG is set (e.g., LOG=info), inject --log-cli-level into pytest flags.
# Works with both the default PYTEST_ARGS and explicit overrides:
#   make test LOG=info                      → -v --benchmark-disable --log-cli-level=INFO
#   make benchmark LOG=warning              → --benchmark-only ... --log-cli-level=WARNING
#   make test PYTEST_ARGS="-x" LOG=debug    → -x --log-cli-level=DEBUG
PYTEST_ARGS ?= -v --benchmark-disable
ifdef LOG
  LOG_FLAG := --log-cli-level=$(shell echo '$(LOG)' | tr '[:lower:]' '[:upper:]')
  PYTEST_ARGS += $(LOG_FLAG)
endif
export LOG
export PYTEST_ARGS

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

.PHONY: help setup sync deps deps-uv deps-trunk deps-docker test benchmark check protogen clean build publish

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo ""
	@echo "Environment Variables:"
	@echo "  INSTALL_MISSING_TOOLS=true    Enable automatic installation of missing tools (default: disabled)"
	@echo "  LOG=<level>                   Add --log-cli-level to pytest (e.g., LOG=info, LOG=debug)"
	@echo "                                Works with both 'test' and 'benchmark' targets"
	@echo "  STRESS_TEST_MODE=<mode>       Stress test preset: quick (default), moderate, full"
	@echo "  PYTEST_ARGS=\"...\"             Override default pytest flags (default: -v --benchmark-disable)"
	@echo "                                Note: overrides LOG when set explicitly. 'benchmark' sets its own"
	@echo "                                PYTEST_ARGS internally but still honours LOG"
	@echo "  DGRAPH_IMAGE_TAG=<tag>        Override the Dgraph Docker image tag (default: latest)"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""

setup: deps ## Setup project (install tools and sync dependencies)
	@$(MAKE) sync

sync: ## Sets up and syncs project virtual environment.
	$(RUN) uv sync --group dev --extra dev

check: ## Run code quality checks on all files
	trunk check --all --no-fix

protogen: ## Regenerate protobuf files (requires Python 3.13+)
	$(RUN) uv run python scripts/protogen.py

clean: ## Cleans build artifacts
	rm -rf build/
	mkdir -p build

build: deps-uv sync protogen ## Builds release package
	$(RUN) uv build

test: deps-uv sync ## Run tests (use PYTEST_ARGS to pass options, e.g., make test PYTEST_ARGS="-v tests/test_connect.py")
	bash scripts/local-test.sh $(PYTEST_ARGS)

benchmark: ## Run benchmarks (measures per-operation latency with pytest-benchmark)
	@# Outputs (all .gitignored):
	@#   benchmark-results.json        Phase 1 results (pytest-benchmark JSON)
	@#   benchmark-histogram.svg       Phase 1 latency histogram
	@#   stress-benchmark-results.json Phase 2 results (pytest-benchmark JSON)
	@#
	@# Phase 1: Per-operation latency benchmarks against a clean database.
	@# Runs targeted benchmark tests (test_benchmark_*.py) which measure individual
	@# operations (query, mutation, upsert, etc.) in isolation.  Each test creates a
	@# fresh schema via drop_all, so these MUST run on their own Dgraph cluster —
	@# the rapid schema churn destabilises the alpha for any tests that follow.
	@echo "═══ Phase 1: Per-operation latency benchmarks ═══"
	$(MAKE) test PYTEST_ARGS="--benchmark-only --benchmark-json=benchmark-results.json --benchmark-histogram=benchmark-histogram -v $(LOG_FLAG) tests/test_benchmark_async.py tests/test_benchmark_sync.py"
	@# Phase 2: Stress-test benchmarks under sustained concurrent load.
	@# Runs stress tests (test_stress_*.py) with the 1-million-movie dataset loaded.
	@# Uses a separate Dgraph cluster (via a second 'make test' invocation) so the
	@# alpha starts fresh after Phase 1's drop_all churn.
	@# benchmark.pedantic(rounds=1) in each stress test prevents pytest-benchmark
	@# from compounding iterations — the stress_config["rounds"] inner loop
	@# (controlled by STRESS_TEST_MODE) handles repetition instead.
	@echo "═══ Phase 2: Stress-test benchmarks (moderate load, 1M movies) ═══"
	$(MAKE) test STRESS_TEST_MODE=moderate PYTEST_ARGS="--benchmark-only --benchmark-json=stress-benchmark-results.json -v $(LOG_FLAG) tests/test_stress_async.py tests/test_stress_sync.py"

publish: clean build  ## Publish a new release to PyPi (requires UV_PUBLISH_USERNAME and UV_PUBLISH_PASSWORD to be set)
	$(RUN) uv publish

deps: deps-uv deps-trunk deps-docker ## Check/install tool dependencies (set INSTALL_MISSING_TOOLS=true to auto-install)

deps-uv: ## Check for uv/uvx installation (installs if INSTALL_MISSING_TOOLS=true)
	@(command -v uv >/dev/null 2>&1 && command -v uvx >/dev/null 2>&1) || { \
		if [ "$(INSTALL_MISSING_TOOLS)" = "true" ]; then \
			echo "uv/uvx not found, installing..."; \
			curl -LsSf https://astral.sh/uv/install.sh | sh; \
		else \
			echo "Error: uv is not installed."; \
			echo ""; \
			echo "To install uv:"; \
			echo ""; \
			if [ "$$(uname)" = "Darwin" ]; then \
				echo "  macOS:"; \
				echo "    brew install uv"; \
				echo "    # or"; \
				echo "    curl -LsSf https://astral.sh/uv/install.sh | sh"; \
			elif [ "$$(uname)" = "Linux" ]; then \
				echo "  Linux:"; \
				echo "    curl -LsSf https://astral.sh/uv/install.sh | sh"; \
				echo "    # or via pip"; \
				echo "    pip install uv"; \
			else \
				echo "  Windows:"; \
				echo "    powershell -ExecutionPolicy ByPass -c \"irm https://astral.sh/uv/install.ps1 | iex\""; \
				echo "    # or via pip"; \
				echo "    pip install uv"; \
			fi; \
			echo ""; \
			echo "Or run: INSTALL_MISSING_TOOLS=true make setup"; \
			exit 1; \
		fi; \
	}

deps-trunk: ## Check for trunk installation (installs if INSTALL_MISSING_TOOLS=true)
	@command -v trunk >/dev/null 2>&1 || { \
		if [ "$(INSTALL_MISSING_TOOLS)" = "true" ]; then \
			echo "trunk not found, installing..."; \
			TMPFILE=$$(mktemp); \
			curl -fsSL https://get.trunk.io -o "$$TMPFILE"; \
			bash "$$TMPFILE" -y; \
			rm "$$TMPFILE"; \
		else \
			echo "Error: trunk is not installed."; \
			echo ""; \
			echo "To install trunk:"; \
			echo ""; \
			if [ "$$(uname)" = "Darwin" ] || [ "$$(uname)" = "Linux" ]; then \
				echo "  macOS/Linux:"; \
				echo "    curl -fsSL https://get.trunk.io | bash"; \
				echo "    # or via npm"; \
				echo "    npm install -g @trunk/launcher"; \
			else \
				echo "  Windows:"; \
				echo "    Visit: https://docs.trunk.io/check/usage#windows"; \
			fi; \
			echo ""; \
			echo "Or run: INSTALL_MISSING_TOOLS=true make setup"; \
			exit 1; \
		fi; \
	}

deps-docker: ## Check for Docker installation (installs if INSTALL_MISSING_TOOLS=true, requires 20.10.0+)
	@if ! command -v docker >/dev/null 2>&1; then \
		if [ "$(INSTALL_MISSING_TOOLS)" = "true" ]; then \
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
		else \
			echo "Error: Docker is not installed."; \
			echo ""; \
			echo "To install Docker:"; \
			echo ""; \
			if [ "$$(uname)" = "Darwin" ]; then \
				echo "  macOS:"; \
				echo "    brew install --cask docker"; \
				echo "    # or download from"; \
				echo "    # https://docs.docker.com/desktop/install/mac-install/"; \
			elif [ "$$(uname)" = "Linux" ]; then \
				echo "  Linux (Debian/Ubuntu):"; \
				echo "    sudo apt-get update"; \
				echo "    sudo apt-get install -y docker.io docker-compose-plugin"; \
				echo "    sudo systemctl start docker"; \
				echo "    sudo systemctl enable docker"; \
				echo "    sudo usermod -aG docker \$$USER"; \
				echo ""; \
				echo "  Linux (Other):"; \
				echo "    https://docs.docker.com/engine/install/"; \
			else \
				echo "  Windows:"; \
				echo "    Download from: https://docs.docker.com/desktop/install/windows-install/"; \
			fi; \
			echo ""; \
			echo "Or run: INSTALL_MISSING_TOOLS=true make setup"; \
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
