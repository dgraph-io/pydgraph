# Add local bin directories to PATH
SHELL := /bin/bash
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

.PHONY: help setup sync deps deps-uv deps-trunk deps-docker test benchmark check protogen clean build publish

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo ""
	@echo "Environment Variables:"
	@echo "  INSTALL_MISSING_TOOLS=true    Enable automatic installation of missing tools (default: disabled)"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""

setup: deps ## Setup project (install tools and sync dependencies)
	@if [ ! -f .git/hooks/pre-commit ] || ! grep -q "pre-commit" .git/hooks/pre-commit 2>/dev/null; then \
		echo "Installing pre-commit hooks..."; \
		uv run pre-commit install; \
	fi
	@$(MAKE) sync

sync: ## Sets up and syncs project virtual environment.
	$(RUN) uv sync --group dev --extra dev

check: ## Run pre-commit hooks on all files
	$(RUN) pre-commit run --all-files

protogen: ## Regenerate protobuf files (requires Python 3.13+)
	$(RUN) uv run python scripts/protogen.py

clean: ## Cleans build artifacts
	rm -rf build/
	mkdir -p build

build: deps-uv sync protogen ## Builds release package
	$(RUN) uv build

test: deps-uv sync ## Run tests (use PYTEST_ARGS to pass options, e.g., make test PYTEST_ARGS="-v tests/test_connect.py")
	bash scripts/local-test.sh $(PYTEST_ARGS)

benchmark: deps-uv sync ## Run benchmarks
	STRESS_TEST_MODE=moderate $(RUN) uv run pytest tests/ \
		--benchmark-only \
		--benchmark-json=benchmark-results.json \
		--benchmark-histogram=benchmark-histogram \
		-v

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
