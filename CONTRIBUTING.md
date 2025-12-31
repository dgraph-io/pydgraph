# Contributing to pydgraph

Thank you for your interest in contributing to pydgraph! We welcome contributions from the
community.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Code Style and Standards](#code-style-and-standards)
- [Testing](#testing)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Code of Conduct](#code-of-conduct)

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```sh
   git clone https://github.com/YOUR-USERNAME/pydgraph.git
   cd pydgraph
   ```
3. Add the upstream repository:
   ```sh
   git remote add upstream https://github.com/dgraph-io/pydgraph.git
   ```

## Development Setup

### Prerequisites

- Python 3.13+ (for development; Python 3.9+ for using the library)
- Docker and Docker Compose (for running tests)
- Git

### Makefile Commands

This project uses a Makefile to simplify common development tasks. To see all available commands:

```sh
make help
```

**Output:**

```
Environment Variables:
  INSTALL_MISSING_DEPS=true    Enable automatic installation of missing tools (default: disabled)

Available targets:
help            Show this help message
setup           Setup project (install tools and sync dependencies)
sync            Sets up and syncs project virtual environment.
check           Run pre-commit hooks on all files
protogen        Regenerate protobuf files (requires Python 3.13+)
clean           Cleans build artifacts
build           Builds release package
test            Run tests
publish         Publish a new release to PyPi (requires UV_PUBLISH_USERNAME and UV_PUBLISH_PASSWORD to be set)
deps            Check/install tool dependencies (set INSTALL_MISSING_DEPS=true to auto-install)
deps-docker     Check and install Docker if needed (requires Docker 20.10.0+)
```

### Setting Up Your Environment

1. Set up the project:

   ```sh
   make setup
   ```

   This will:
   - Check for required tools (uv, trunk, docker)
   - Install pre-commit hooks

   **Note:** To automatically install missing tool dependencies (uv, trunk, docker), you can set
   `INSTALL_MISSING_DEPS` to `true`:

   ```sh
   INSTALL_MISSING_DEPS=true make setup
   ```

   After running `make setup`, you'll need to sync the Python environment and dependencies:

   ```sh
   make sync
   ```

   This will:
   - Set up the correct Python version
   - Create and configure a virtual environment
   - Install all project dependencies

2. Verify your setup:
   ```sh
   make check
   ```

### Syncing Dependencies

After making changes to dependencies or pulling updates:

```sh
make sync
```

This syncs the project virtual environment with the latest dependencies.

### Regenerating Protocol Buffers

If you make changes to `pydgraph/proto/api.proto`, regenerate the source files:

```sh
make protogen
```

Or directly with uv:

```sh
uv run python scripts/protogen.py
```

**Important:** This project uses Python 3.13+ with grpcio-tools 1.66.2+ as the canonical development
environment. The generated proto files include mypy type stubs for better type checking. The script
will verify you have the correct Python and grpcio-tools versions before generating files.

### About grpcio Version Requirements

This project requires grpcio 1.65.0 or higher. Older versions have practical limitations:

- **Compilation failures**: grpcio versions older than ~1.60.0 fail to compile from source on modern
  systems (macOS with recent Xcode, newer Linux distributions) due to C++ compiler compatibility
  issues and outdated build configurations.
- **No pre-built wheels**: PyPI doesn't provide pre-built wheels for very old grpcio versions on
  modern Python versions (3.11+), forcing compilation from source.
- **Build tool incompatibility**: The build process for older grpcio versions uses deprecated
  compiler flags and build patterns that modern toolchains reject.

## Making Changes

1. Create a new branch for your changes:

   ```sh
   git checkout -b your-feature-name
   ```

2. Make your changes following our [Code Style and Standards](#code-style-and-standards)

3. Add tests for your changes (see [Testing](#testing))

4. Ensure all checks pass:

   ```sh
   make check test
   ```

   **Important:** Before opening a pull request, make sure `make check test` succeeds locally.

5. Commit your changes using [Conventional Commits](https://www.conventionalcommits.org/) format:
   ```sh
   git commit -m "feat: add new feature"
   git commit -m "fix: resolve issue with..."
   git commit -m "docs: update README"
   ```

## Code Style and Standards

### Python Code Standards

- Follow PEP 8 style guidelines (enforced by ruff)
- Use type hints for all function signatures
- Add docstrings for public APIs
- Maximum line length: handled by the formatter

### File Headers

Every new Python file must start with SPDX headers followed by proper attribution:

```python
# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
"""
Module description here.
"""

__author__ = "Your Name"
__maintainer__ = "Istari Digital, Inc."
```

**Requirements:**

- **SPDX Headers**: All new Python files must start with the two SPDX comment lines shown above
- `__author__`: Set to your name (the contributor's name)
- `__maintainer__`: Always set to "Istari Digital, Inc."

### Code Quality Tools

This project uses several tools to maintain code quality:

- **ruff**: Linting and code formatting
- **mypy**: Static type checking
- **trunk**: Additional code quality checks
- **pre-commit**: Automated checks on commit

Run all checks:

```sh
make check
```

## Testing

### Running Tests

Run the full test suite:

```sh
make test
```

Run specific tests:

```sh
bash scripts/local-test.sh -v tests/test_connect.py::TestOpen
```

Run a single test:

```sh
bash scripts/local-test.sh -v tests/test_connect.py::TestOpen::test_connection_with_auth
```

### Test Infrastructure

The test script requires Docker and Docker Compose to be installed on your machine.

The script will:

- Automatically bring up a Dgraph cluster
- Connect to randomly selected ports for HTTP and gRPC to prevent interference with clusters running
  on default ports
- Run the tests
- Bring down the cluster after tests complete

For Docker installation instructions, refer to the
[official Docker documentation](https://docs.docker.com/).

### Writing Tests

- Add tests for all new features
- Add regression tests for bug fixes
- Tests should be clear, concise, and well-documented
- Use descriptive test names that explain what is being tested

### Test Requirements

Tests are automatically run in CI/CD against:

- Python versions: 3.9, 3.10, 3.11, 3.12, 3.13, 3.14
- Dgraph latest release
- Dgraph HEAD (main branch)

## Submitting a Pull Request

1. Push your changes to your fork:

   ```sh
   git push origin your-feature-name
   ```

2. Open a pull request on GitHub

3. Fill out the pull request template (see
   [PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md))

### Pull Request Requirements

- **Title**: Must follow [Conventional Commits](https://www.conventionalcommits.org/) format
  - Examples: `feat: add connection pooling`, `fix: resolve memory leak`,
    `docs: update API reference`
- **Description**: Clearly explain what the PR does and why
- **Checklist**: Complete all applicable items in the PR template
- **Tests**: All tests must pass (`make check test` succeeds)
- **Code Quality**: All linting and type checking must pass
- **Documentation**: Update docs if adding/changing public APIs

### Commit Message Guidelines

Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `style:` - Code style changes (formatting, etc.)
- `refactor:` - Code refactoring
- `test:` - Adding or updating tests
- `chore:` - Maintenance tasks
- `ci:` - CI/CD changes

Examples:

```
feat: add async client support
fix: resolve connection timeout issue
docs: update installation instructions
refactor: simplify error handling
test: add integration tests for ACL
```

### Review Process

1. Maintainers will review your PR
2. Address any feedback or requested changes
3. Once approved, a maintainer will merge your PR

## Code of Conduct

This project follows the Contributor Covenant Code of Conduct. Please read our
[Code of Conduct](CODE_OF_CONDUCT.md) to understand the standards we expect from all contributors
and community members.

## Questions or Need Help?

- Open an issue for bugs or feature requests
- Join the [Dgraph Community Slack](https://slack.dgraph.io) for discussions
- Check the [Dgraph documentation](https://dgraph.io/docs)

## Additional Resources

- [README.md](README.md) - Project overview and usage
- [PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) - PR template
- [Conventional Commits](https://www.conventionalcommits.org/) - Commit message format
- [Dgraph Documentation](https://dgraph.io/docs) - Product documentation

Thank you for contributing to pydgraph! 🎉
