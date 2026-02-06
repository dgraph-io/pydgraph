# CI Benchmarks Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan
> task-by-task.

**Goal:** Add pytest-benchmark instrumentation to stress tests with CI integration for PRs, main
branch, and semver tag releases.

**Architecture:** Wrap existing stress test operations with `benchmark()` fixture calls. Add
parallel CI job for benchmarking that produces JSON and SVG histogram artifacts. Create separate
workflow for release tag benchmarks.

**Tech Stack:** pytest-benchmark, pygal (SVG histograms), GitHub Actions

---

## Task 1: Add Dependencies

**Files:**

- Modify: `pyproject.toml:47-55` (project.optional-dependencies.dev section)

**Step 1: Add pytest-benchmark and pygal to dev dependencies**

Edit `pyproject.toml` to add the benchmark dependencies:

```toml
[project.optional-dependencies]
dev = [
  "build>=1.2.2.post1",
  "grpcio-tools>=1.66.2",
  "pytest>=8.3.3",
  "pytest-asyncio>=0.23.0",
  "pytest-benchmark>=4.0.0",
  "pygal>=3.0.0",
  "ruff>=0.8.4",
  "ty>=0.0.8",
]
```

**Step 2: Sync dependencies**

Run: `make sync` Expected: Dependencies install successfully, including pytest-benchmark and pygal

**Step 3: Verify pytest-benchmark is available**

Run: `uv run pytest --help | grep benchmark` Expected: Shows benchmark-related options like
`--benchmark-only`, `--benchmark-json`

**Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "deps: add pytest-benchmark and pygal for CI benchmarks"
```

---

## Task 2: Add Makefile Benchmark Target

**Files:**

- Modify: `Makefile:18` (add benchmark to .PHONY)
- Modify: `Makefile` (add new target after test target)

**Step 1: Add benchmark target to Makefile**

Add to `.PHONY` line:

```makefile
.PHONY: help setup sync deps deps-uv deps-trunk deps-docker test benchmark check protogen clean build publish
```

Add new target after `test:` target (around line 56):

```makefile
benchmark: deps-uv sync ## Run benchmarks
	STRESS_TEST_MODE=moderate $(RUN) uv run pytest tests/ \
		--benchmark-only \
		--benchmark-json=benchmark-results.json \
		--benchmark-histogram=benchmark-histogram \
		-v
```

**Step 2: Verify target shows in help**

Run: `make help` Expected: Shows `benchmark       Run benchmarks`

**Step 3: Commit**

```bash
git add Makefile
git commit -m "build: add benchmark Makefile target"
```

---

## Task 3: Add Benchmark Fixture to Sync Stress Tests

**Files:**

- Modify: `tests/test_stress_sync.py`

**Step 1: Import BenchmarkFixture type for annotations**

Add to imports section (after line 24):

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture
```

**Step 2: Update test_concurrent_read_queries to use benchmark**

Modify the test method signature to accept benchmark fixture and wrap the execution:

```python
def test_concurrent_read_queries(
    self,
    sync_client_with_schema: DgraphClient,
    executor: Executor,
    executor_type: str,
    stress_config: dict[str, Any],
    benchmark: BenchmarkFixture,
) -> None:
    """Test many concurrent read-only queries don't cause issues."""
    client = sync_client_with_schema
    num_ops = stress_config["ops"]

    # Insert some test data first
    txn = client.txn()
    for i in range(100):
        txn.mutate(set_obj=generate_person(i))
    txn.commit()

    query = """query {
        people(func: has(name), first: 10) {
            name
            email
            age
        }
    }"""

    if executor_type == "thread":
        results: list[api.Response] = []
        exc_list: list[Exception] = []

        def run_query() -> None:
            try:
                txn = client.txn(read_only=True)
                response = txn.query(query)
                results.append(response)
            except Exception as e:
                exc_list.append(e)

        def run_all_queries() -> int:
            futures = [executor.submit(run_query) for _ in range(num_ops)]
            wait(futures)
            return len(results)

        result_count = benchmark(run_all_queries)

        assert len(exc_list) == 0, f"Got {len(exc_list)} errors: {exc_list[:5]}"
        assert result_count == num_ops
    else:
        def run_all_process_queries() -> int:
            query_futures: list[Future[api.Response | Exception]] = [
                executor.submit(_worker_query, query) for _ in range(num_ops)
            ]
            wait(query_futures)
            results_list = [f.result() for f in query_futures]
            return len([r for r in results_list if not isinstance(r, Exception)])

        result_count = benchmark(run_all_process_queries)

        # Re-run to get actual exception list for assertion
        query_futures: list[Future[api.Response | Exception]] = [
            executor.submit(_worker_query, query) for _ in range(num_ops)
        ]
        wait(query_futures)
        results_list = [f.result() for f in query_futures]
        exc_list = [r for r in results_list if isinstance(r, Exception)]
        assert len(exc_list) == 0, f"Got {len(exc_list)} errors: {exc_list[:5]}"
```

**Step 3: Update test_concurrent_mutations_separate_txns similarly**

Add `benchmark: BenchmarkFixture` parameter and wrap the mutation execution in a callable.

**Step 4: Run tests to verify they still pass**

Run:
`STRESS_TEST_MODE=quick uv run pytest tests/test_stress_sync.py -v -k "test_concurrent_read" --benchmark-disable`
Expected: Tests pass

**Step 5: Run with benchmarking enabled**

Run:
`STRESS_TEST_MODE=quick uv run pytest tests/test_stress_sync.py::TestSyncClientStress::test_concurrent_read_queries -v --benchmark-only`
Expected: Benchmark output with timing statistics

**Step 6: Commit**

```bash
git add tests/test_stress_sync.py
git commit -m "feat(tests): add benchmark fixture to sync stress tests"
```

---

## Task 4: Add Benchmark Fixture to Async Stress Tests

**Files:**

- Modify: `tests/test_stress_async.py`

**Step 1: Import BenchmarkFixture type**

Add TYPE_CHECKING import and BenchmarkFixture:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture
```

**Step 2: Update test_concurrent_async_queries to use benchmark**

Note: pytest-benchmark's `benchmark` fixture works with sync functions. For async tests, wrap the
async execution in a sync function:

```python
@pytest.mark.asyncio
async def test_concurrent_async_queries(
    self,
    async_client_with_schema: AsyncDgraphClient,
    stress_config: dict[str, Any],
    benchmark: BenchmarkFixture,
) -> None:
    """Test many concurrent read-only queries using asyncio.gather."""
    client = async_client_with_schema
    num_ops = stress_config["ops"]

    # Insert some test data first (outside benchmark)
    txn = client.txn()
    for i in range(100):
        await txn.mutate(set_obj=_generate_person(i))
    await txn.commit()

    query = """query {
        people(func: has(name), first: 10) {
            name
            email
            age
        }
    }"""

    async def run_query() -> api.Response:
        txn = client.txn(read_only=True)
        return await txn.query(query)

    # benchmark.pedantic allows async via wrapper
    def run_benchmark() -> list[api.Response]:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(asyncio.gather(*[run_query() for _ in range(num_ops)]))

    results = benchmark(run_benchmark)

    assert len(results) == num_ops
    for result in results:
        assert result.json is not None
```

**Step 3: Update remaining async tests similarly**

Apply the same pattern to other async stress tests.

**Step 4: Run tests to verify**

Run: `STRESS_TEST_MODE=quick uv run pytest tests/test_stress_async.py -v --benchmark-disable`
Expected: Tests pass

**Step 5: Commit**

```bash
git add tests/test_stress_async.py
git commit -m "feat(tests): add benchmark fixture to async stress tests"
```

---

## Task 5: Add Benchmarks Job to PR/Main CI

**Files:**

- Modify: `.github/workflows/ci-pydgraph-tests.yml`

**Step 1: Add benchmarks job after existing jobs**

Add at the end of the jobs section:

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

**Step 2: Verify YAML syntax**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/ci-pydgraph-tests.yml'))"`
Expected: No errors

**Step 3: Commit**

```bash
git add .github/workflows/ci-pydgraph-tests.yml
git commit -m "ci: add benchmarks job to PR/main workflow"
```

---

## Task 6: Create Semver Tag Workflow

**Files:**

- Create: `.github/workflows/ci-pydgraph-benchmarks.yml`

**Step 1: Create the workflow file**

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

**Step 2: Verify YAML syntax**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/ci-pydgraph-benchmarks.yml'))"`
Expected: No errors

**Step 3: Commit**

```bash
git add .github/workflows/ci-pydgraph-benchmarks.yml
git commit -m "ci: add benchmarks workflow for semver tag releases"
```

---

## Task 7: Add Benchmark Artifacts to .gitignore

**Files:**

- Modify: `.gitignore`

**Step 1: Add benchmark output files to .gitignore**

Add at end of file:

```
# Benchmark outputs
benchmark-results.json
benchmark-histogram.svg
```

**Step 2: Commit**

```bash
git add .gitignore
git commit -m "chore: ignore benchmark output files"
```

---

## Task 8: Local Verification

**Step 1: Run full benchmark locally**

Run: `make benchmark` Expected:

- Benchmarks run with timing output
- `benchmark-results.json` created
- `benchmark-histogram.svg` created

**Step 2: Verify JSON output**

Run: `cat benchmark-results.json | python -m json.tool | head -50` Expected: Valid JSON with
benchmark data

**Step 3: Verify SVG output**

Run: `file benchmark-histogram.svg` Expected:
`benchmark-histogram.svg: SVG Scalable Vector Graphics image`

**Step 4: Clean up local artifacts**

Run: `rm -f benchmark-results.json benchmark-histogram.svg`

---

## Task 9: Final Verification and Push

**Step 1: Run all checks**

Run: `make check` Expected: All checks pass

**Step 2: Run tests with benchmark disabled**

Run:
`STRESS_TEST_MODE=quick uv run pytest tests/test_stress_sync.py tests/test_stress_async.py -v --benchmark-disable`
Expected: All tests pass

**Step 3: View git log**

Run: `git log --oneline -10` Expected: See all implementation commits

**Step 4: Push branch for PR**

Run: `git push -u origin feature/ci-benchmarks` Expected: Branch pushed, ready for PR
