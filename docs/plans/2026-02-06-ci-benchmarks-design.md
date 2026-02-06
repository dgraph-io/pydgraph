# CI Benchmarks Design

## Overview

Add benchmark testing to CI workflows to catch performance regressions and provide visibility into
client performance characteristics.

## Goals

1. Run benchmarks on every PR and push to main
2. Run benchmarks on semver tag releases
3. Produce machine-readable (JSON) and visual (SVG histogram) artifacts
4. Minimal impact on CI duration by running benchmarks in parallel with existing test matrix

## Design

### Workflow Structure

**PR/Main CI** (`ci-pydgraph-tests.yml`)

- Existing matrix jobs continue unchanged
- New parallel job: "Benchmarks"
  - Python 3.13, Dgraph Latest (single configuration)
  - `STRESS_TEST_MODE=moderate` (~2-3 minutes)
  - Uploads: `benchmark-results.json`, `benchmark-histogram.svg`

**Semver Tag Workflow** (new `ci-pydgraph-benchmarks.yml`)

- Triggers on tags matching `v*.*.*`
- Single job: "Release Benchmarks"
- `STRESS_TEST_MODE=moderate`
- Artifact name includes version: `benchmark-results-v1.0.0`

### Test Modifications

Wrap stress test operations with `pytest-benchmark` fixture:

```python
def test_concurrent_read_queries(self, ..., benchmark):
    # Setup code stays the same

    def run_queries():
        futures = [executor.submit(run_query) for _ in range(num_ops)]
        wait(futures)
        return len([f for f in futures if not f.exception()])

    success_count = benchmark(run_queries)
    assert success_count == num_ops
```

Key points:

- Add `benchmark` fixture parameter to stress test methods
- Extract core operation into callable for `benchmark()` to measure
- Tests remain runnable without benchmarking (fixture is optional)

### CI Job Configuration

**PR/Main Benchmarks Job:**

```yaml
benchmarks:
  name: Benchmarks
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v5
    - name: Setup python runtime and tooling
      uses: ./.github/actions/setup-python-and-tooling
      with:
        python-version: "3.13"
    - name: Setup project dependencies
      run: INSTALL_MISSING_TOOLS=true make setup
    - name: Sync python virtualenv
      run: make sync
    - name: Run benchmarks
      run: |
        STRESS_TEST_MODE=moderate uv run pytest tests/ \
          --benchmark-only \
          --benchmark-json=benchmark-results.json \
          --benchmark-histogram=benchmark-histogram \
          -v
    - name: Upload benchmark results
      uses: actions/upload-artifact@v4
      with:
        name: benchmark-results
        path: |
          benchmark-results.json
          benchmark-histogram.svg
```

**Semver Tag Workflow** (`ci-pydgraph-benchmarks.yml`):

```yaml
name: ci-pydgraph-benchmarks
on:
  push:
    tags:
      - "v[0-9]+.[0-9]+.[0-9]+*"

permissions:
  contents: read

jobs:
  benchmarks:
    name: Release Benchmarks
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - name: Setup python runtime and tooling
        uses: ./.github/actions/setup-python-and-tooling
        with:
          python-version: "3.13"
      - name: Setup project dependencies
        run: INSTALL_MISSING_TOOLS=true make setup
      - name: Sync python virtualenv
        run: make sync
      - name: Run benchmarks
        run: |
          STRESS_TEST_MODE=moderate uv run pytest tests/ \
            --benchmark-only \
            --benchmark-json=benchmark-results.json \
            --benchmark-histogram=benchmark-histogram \
            -v
      - name: Upload benchmark results
        uses: actions/upload-artifact@v4
        with:
          name: benchmark-results-${{ github.ref_name }}
          path: |
            benchmark-results.json
            benchmark-histogram.svg
```

### Dependencies

Add to `pyproject.toml` test dependencies:

```toml
[project.optional-dependencies]
test = [
    # ... existing deps ...
    "pytest-benchmark>=4.0.0",
    "pygal>=3.0.0",
]
```

### Makefile Target

```makefile
.PHONY: benchmark
benchmark:  ## Run benchmarks
	STRESS_TEST_MODE=moderate $(UV) run pytest tests/ \
		--benchmark-only \
		--benchmark-json=benchmark-results.json \
		--benchmark-histogram=benchmark-histogram \
		-v
```

## Files to Modify

1. `tests/test_stress_sync.py` - Add `benchmark` fixture to stress tests
2. `tests/test_stress_async.py` - Add `benchmark` fixture to async stress tests
3. `.github/workflows/ci-pydgraph-tests.yml` - Add benchmarks job
4. `.github/workflows/ci-pydgraph-benchmarks.yml` - New file for tag workflow
5. `pyproject.toml` - Add pytest-benchmark and pygal dependencies
6. `Makefile` - Add benchmark target

## Artifacts Produced

| Workflow | Artifact Name              | Contents   |
| -------- | -------------------------- | ---------- |
| PR/Main  | `benchmark-results`        | JSON + SVG |
| Tag      | `benchmark-results-v1.0.0` | JSON + SVG |

## Testing the Implementation

1. Run locally: `make benchmark`
2. Verify JSON output: `cat benchmark-results.json | jq .`
3. View histogram: open `benchmark-histogram.svg` in browser
4. Create test PR to verify CI job runs in parallel with matrix
