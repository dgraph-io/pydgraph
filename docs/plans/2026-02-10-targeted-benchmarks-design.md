# Targeted Benchmark Tests Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan
> task-by-task.

**Goal:** Create isolated benchmarks for individual pydgraph operations to pinpoint performance
regression sources when stress tests regress.

**Architecture:** Two test files (`test_benchmark_sync.py`, `test_benchmark_async.py`) using
pytest-benchmark with real Dgraph compose backend via existing fixtures. Each operation gets its own
benchmark test.

**Tech Stack:** pytest, pytest-benchmark, existing pydgraph fixtures, real Dgraph backend

---

## Task 1: Create Sync Benchmark Test File Structure

**Files:**

- Create: `tests/test_benchmark_sync.py`

**Step 1: Create the test file with imports and class structure**

```python
# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Targeted benchmark tests for sync client operations.

These benchmarks measure individual operations in isolation to help
identify the root cause of performance regressions in stress tests.

Usage:
    # Run all sync benchmarks
    pytest tests/test_benchmark_sync.py -v

    # Compare against previous run
    pytest tests/test_benchmark_sync.py --benchmark-compare
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pydgraph
from pydgraph import DgraphClient, run_transaction
from pydgraph.proto import api_pb2 as api

from .helpers import generate_person

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture
```

**Step 2: Verify the file is created correctly**

Run: `python -c "import tests.test_benchmark_sync"` Expected: No import errors

**Step 3: Commit**

```bash
git add tests/test_benchmark_sync.py
git commit -m "feat(tests): add sync benchmark test file structure"
```

---

## Task 2: Implement Query Benchmarks (Sync)

**Files:**

- Modify: `tests/test_benchmark_sync.py`

**Step 1: Add query benchmark tests**

```python
class TestSyncQueryBenchmarks:
    """Benchmarks for sync query operations."""

    def test_benchmark_query_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark a simple read query."""
        client = sync_client_with_schema

        # Setup: seed data outside benchmark
        txn = client.txn()
        txn.mutate(set_obj=generate_person(0), commit_now=True)

        query = """query {
            people(func: has(name), first: 1) {
                name
                email
                age
            }
        }"""

        def run_query() -> api.Response:
            txn = client.txn(read_only=True)
            return txn.query(query)

        benchmark(run_query)

    def test_benchmark_query_with_vars_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark a query with variables."""
        client = sync_client_with_schema

        # Setup: seed data
        txn = client.txn()
        txn.mutate(set_obj={"name": "BenchmarkUser", "email": "bench@test.com"}, commit_now=True)

        query = """query people($name: string) {
            people(func: eq(name, $name)) {
                name
                email
            }
        }"""

        def run_query() -> api.Response:
            txn = client.txn(read_only=True)
            return txn.query(query, variables={"$name": "BenchmarkUser"})

        benchmark(run_query)

    def test_benchmark_query_best_effort_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark a best-effort read query."""
        client = sync_client_with_schema

        # Setup: seed data
        txn = client.txn()
        txn.mutate(set_obj=generate_person(0), commit_now=True)

        query = "{ people(func: has(name), first: 1) { name } }"

        def run_query() -> api.Response:
            txn = client.txn(read_only=True, best_effort=True)
            return txn.query(query)

        benchmark(run_query)
```

**Step 2: Run the query benchmarks to verify**

Run: `pytest tests/test_benchmark_sync.py::TestSyncQueryBenchmarks -v --benchmark-disable` Expected:
PASS (3 tests)

**Step 3: Commit**

```bash
git add tests/test_benchmark_sync.py
git commit -m "feat(tests): add sync query benchmarks"
```

---

## Task 3: Implement Mutation Benchmarks (Sync)

**Files:**

- Modify: `tests/test_benchmark_sync.py`

**Step 1: Add mutation benchmark tests**

```python
class TestSyncMutationBenchmarks:
    """Benchmarks for sync mutation operations."""

    def test_benchmark_mutation_commit_now_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark mutation with commit_now (single round-trip)."""
        client = sync_client_with_schema
        counter = [0]

        def run_mutation() -> api.Response:
            counter[0] += 1
            txn = client.txn()
            return txn.mutate(set_obj=generate_person(counter[0]), commit_now=True)

        benchmark(run_mutation)

    def test_benchmark_mutation_explicit_commit_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark mutation with explicit commit (two round-trips)."""
        client = sync_client_with_schema
        counter = [0]

        def run_mutation() -> api.TxnContext:
            counter[0] += 1
            txn = client.txn()
            txn.mutate(set_obj=generate_person(counter[0]))
            return txn.commit()

        benchmark(run_mutation)

    def test_benchmark_discard_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark mutation followed by discard (rollback cost)."""
        client = sync_client_with_schema
        counter = [0]

        def run_mutation() -> None:
            counter[0] += 1
            txn = client.txn()
            txn.mutate(set_obj=generate_person(counter[0]))
            txn.discard()

        benchmark(run_mutation)

    def test_benchmark_mutation_nquads_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark N-Quads mutation format."""
        client = sync_client_with_schema
        counter = [0]

        def run_mutation() -> api.Response:
            counter[0] += 1
            txn = client.txn()
            nquads = f'''
                _:person <name> "Person_{counter[0]}" .
                _:person <email> "person{counter[0]}@test.com" .
                _:person <age> "{counter[0] % 80}" .
            '''
            return txn.mutate(set_nquads=nquads, commit_now=True)

        benchmark(run_mutation)

    def test_benchmark_delete_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark delete mutation."""
        client = sync_client_with_schema

        # Pre-create nodes to delete
        uids: list[str] = []
        for i in range(100):
            txn = client.txn()
            resp = txn.mutate(set_obj=generate_person(i), commit_now=True)
            uids.append(next(iter(resp.uids.values())))

        uid_index = [0]

        def run_delete() -> api.Response:
            idx = uid_index[0] % len(uids)
            uid_index[0] += 1
            txn = client.txn()
            return txn.mutate(del_obj={"uid": uids[idx]}, commit_now=True)

        benchmark(run_delete)
```

**Step 2: Run the mutation benchmarks to verify**

Run: `pytest tests/test_benchmark_sync.py::TestSyncMutationBenchmarks -v --benchmark-disable`
Expected: PASS (5 tests)

**Step 3: Commit**

```bash
git add tests/test_benchmark_sync.py
git commit -m "feat(tests): add sync mutation benchmarks"
```

---

## Task 4: Implement Advanced Transaction Benchmarks (Sync)

**Files:**

- Modify: `tests/test_benchmark_sync.py`

**Step 1: Add upsert and batch benchmarks**

```python
class TestSyncTransactionBenchmarks:
    """Benchmarks for advanced sync transaction operations."""

    def test_benchmark_upsert_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark upsert operation (query + conditional mutation)."""
        client = sync_client_with_schema
        counter = [0]

        def run_upsert() -> api.Response:
            counter[0] += 1
            email = f"upsert{counter[0]}@test.com"
            txn = client.txn()
            query = f'{{ u as var(func: eq(email, "{email}")) }}'
            mutation = pydgraph.Mutation(
                set_nquads=f'''
                    uid(u) <email> "{email}" .
                    uid(u) <name> "Upsert_{counter[0]}" .
                '''.encode(),
                cond="@if(eq(len(u), 0))",
            )
            request = api.Request(
                query=query,
                mutations=[mutation],
                commit_now=True,
            )
            return txn.do_request(request)

        benchmark(run_upsert)

    def test_benchmark_batch_mutations_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark multiple mutations in one transaction."""
        client = sync_client_with_schema
        counter = [0]
        batch_size = 10

        def run_batch() -> api.TxnContext:
            txn = client.txn()
            for i in range(batch_size):
                counter[0] += 1
                txn.mutate(set_obj=generate_person(counter[0]))
            return txn.commit()

        benchmark(run_batch)

    def test_benchmark_run_transaction_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark run_transaction helper overhead."""
        client = sync_client_with_schema
        counter = [0]

        def txn_func(txn: pydgraph.Txn) -> str:
            counter[0] += 1
            response = txn.mutate(set_obj=generate_person(counter[0]), commit_now=True)
            return next(iter(response.uids.values()), "")

        def run_with_helper() -> str:
            return run_transaction(client, txn_func)

        benchmark(run_with_helper)
```

**Step 2: Run the transaction benchmarks to verify**

Run: `pytest tests/test_benchmark_sync.py::TestSyncTransactionBenchmarks -v --benchmark-disable`
Expected: PASS (3 tests)

**Step 3: Commit**

```bash
git add tests/test_benchmark_sync.py
git commit -m "feat(tests): add sync transaction benchmarks"
```

---

## Task 5: Implement Client Operation Benchmarks (Sync)

**Files:**

- Modify: `tests/test_benchmark_sync.py`

**Step 1: Add client-level operation benchmarks**

```python
class TestSyncClientBenchmarks:
    """Benchmarks for sync client-level operations."""

    def test_benchmark_check_version_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark check_version (health check)."""
        client = sync_client_with_schema

        def run_check() -> str:
            return client.check_version()

        benchmark(run_check)

    def test_benchmark_alter_schema_sync(
        self,
        sync_client_with_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark schema alter operation."""
        client = sync_client_with_schema
        counter = [0]

        def run_alter() -> api.Payload:
            counter[0] += 1
            # Add a new predicate each time to avoid conflicts
            schema = f"benchmark_pred_{counter[0]}: string @index(exact) ."
            return client.alter(pydgraph.Operation(schema=schema))

        benchmark(run_alter)
```

**Step 2: Run the client benchmarks to verify**

Run: `pytest tests/test_benchmark_sync.py::TestSyncClientBenchmarks -v --benchmark-disable`
Expected: PASS (2 tests)

**Step 3: Commit**

```bash
git add tests/test_benchmark_sync.py
git commit -m "feat(tests): add sync client operation benchmarks"
```

---

## Task 6: Create Async Benchmark Test File

**Files:**

- Create: `tests/test_benchmark_async.py`

**Step 1: Create the async test file with imports**

```python
# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Targeted benchmark tests for async client operations.

These benchmarks measure individual async operations in isolation to help
identify the root cause of performance regressions in stress tests.

Usage:
    # Run all async benchmarks
    pytest tests/test_benchmark_async.py -v

    # Compare against previous run
    pytest tests/test_benchmark_async.py --benchmark-compare
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import pydgraph
from pydgraph import AsyncDgraphClient, run_transaction_async
from pydgraph.proto import api_pb2 as api

from .helpers import generate_person

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture
```

**Step 2: Verify the file is created correctly**

Run: `python -c "import tests.test_benchmark_async"` Expected: No import errors

**Step 3: Commit**

```bash
git add tests/test_benchmark_async.py
git commit -m "feat(tests): add async benchmark test file structure"
```

---

## Task 7: Implement Query Benchmarks (Async)

**Files:**

- Modify: `tests/test_benchmark_async.py`

**Step 1: Add async query benchmark tests**

Note: Async benchmarks use the `async_client_with_schema_for_benchmark` fixture which returns
`(client, loop)` to avoid pytest-asyncio/benchmark conflicts.

```python
class TestAsyncQueryBenchmarks:
    """Benchmarks for async query operations."""

    def test_benchmark_query_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark a simple async read query."""
        client, loop = async_client_with_schema_for_benchmark

        # Setup: seed data outside benchmark
        async def setup() -> None:
            txn = client.txn()
            await txn.mutate(set_obj=generate_person(0), commit_now=True)

        loop.run_until_complete(setup())

        query = """query {
            people(func: has(name), first: 1) {
                name
                email
                age
            }
        }"""

        async def run_query() -> api.Response:
            txn = client.txn(read_only=True)
            return await txn.query(query)

        def benchmark_wrapper() -> api.Response:
            return loop.run_until_complete(run_query())

        benchmark(benchmark_wrapper)

    def test_benchmark_query_with_vars_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark an async query with variables."""
        client, loop = async_client_with_schema_for_benchmark

        # Setup
        async def setup() -> None:
            txn = client.txn()
            await txn.mutate(set_obj={"name": "BenchmarkUser", "email": "bench@test.com"}, commit_now=True)

        loop.run_until_complete(setup())

        query = """query people($name: string) {
            people(func: eq(name, $name)) {
                name
                email
            }
        }"""

        async def run_query() -> api.Response:
            txn = client.txn(read_only=True)
            return await txn.query(query, variables={"$name": "BenchmarkUser"})

        def benchmark_wrapper() -> api.Response:
            return loop.run_until_complete(run_query())

        benchmark(benchmark_wrapper)

    def test_benchmark_query_best_effort_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark a best-effort async read query."""
        client, loop = async_client_with_schema_for_benchmark

        # Setup
        async def setup() -> None:
            txn = client.txn()
            await txn.mutate(set_obj=generate_person(0), commit_now=True)

        loop.run_until_complete(setup())

        query = "{ people(func: has(name), first: 1) { name } }"

        async def run_query() -> api.Response:
            txn = client.txn(read_only=True, best_effort=True)
            return await txn.query(query)

        def benchmark_wrapper() -> api.Response:
            return loop.run_until_complete(run_query())

        benchmark(benchmark_wrapper)
```

**Step 2: Run the async query benchmarks to verify**

Run: `pytest tests/test_benchmark_async.py::TestAsyncQueryBenchmarks -v --benchmark-disable`
Expected: PASS (3 tests)

**Step 3: Commit**

```bash
git add tests/test_benchmark_async.py
git commit -m "feat(tests): add async query benchmarks"
```

---

## Task 8: Implement Mutation Benchmarks (Async)

**Files:**

- Modify: `tests/test_benchmark_async.py`

**Step 1: Add async mutation benchmark tests**

```python
class TestAsyncMutationBenchmarks:
    """Benchmarks for async mutation operations."""

    def test_benchmark_mutation_commit_now_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async mutation with commit_now."""
        client, loop = async_client_with_schema_for_benchmark
        counter = [0]

        async def run_mutation() -> api.Response:
            counter[0] += 1
            txn = client.txn()
            return await txn.mutate(set_obj=generate_person(counter[0]), commit_now=True)

        def benchmark_wrapper() -> api.Response:
            return loop.run_until_complete(run_mutation())

        benchmark(benchmark_wrapper)

    def test_benchmark_mutation_explicit_commit_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async mutation with explicit commit."""
        client, loop = async_client_with_schema_for_benchmark
        counter = [0]

        async def run_mutation() -> api.TxnContext:
            counter[0] += 1
            txn = client.txn()
            await txn.mutate(set_obj=generate_person(counter[0]))
            return await txn.commit()

        def benchmark_wrapper() -> api.TxnContext:
            return loop.run_until_complete(run_mutation())

        benchmark(benchmark_wrapper)

    def test_benchmark_discard_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async mutation followed by discard."""
        client, loop = async_client_with_schema_for_benchmark
        counter = [0]

        async def run_mutation() -> None:
            counter[0] += 1
            txn = client.txn()
            await txn.mutate(set_obj=generate_person(counter[0]))
            await txn.discard()

        def benchmark_wrapper() -> None:
            return loop.run_until_complete(run_mutation())

        benchmark(benchmark_wrapper)

    def test_benchmark_mutation_nquads_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async N-Quads mutation."""
        client, loop = async_client_with_schema_for_benchmark
        counter = [0]

        async def run_mutation() -> api.Response:
            counter[0] += 1
            txn = client.txn()
            nquads = f'''
                _:person <name> "Person_{counter[0]}" .
                _:person <email> "person{counter[0]}@test.com" .
                _:person <age> "{counter[0] % 80}" .
            '''
            return await txn.mutate(set_nquads=nquads, commit_now=True)

        def benchmark_wrapper() -> api.Response:
            return loop.run_until_complete(run_mutation())

        benchmark(benchmark_wrapper)

    def test_benchmark_delete_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async delete mutation."""
        client, loop = async_client_with_schema_for_benchmark

        # Pre-create nodes to delete
        async def setup() -> list[str]:
            uids = []
            for i in range(100):
                txn = client.txn()
                resp = await txn.mutate(set_obj=generate_person(i), commit_now=True)
                uids.append(next(iter(resp.uids.values())))
            return uids

        uids = loop.run_until_complete(setup())
        uid_index = [0]

        async def run_delete() -> api.Response:
            idx = uid_index[0] % len(uids)
            uid_index[0] += 1
            txn = client.txn()
            return await txn.mutate(del_obj={"uid": uids[idx]}, commit_now=True)

        def benchmark_wrapper() -> api.Response:
            return loop.run_until_complete(run_delete())

        benchmark(benchmark_wrapper)
```

**Step 2: Run the async mutation benchmarks to verify**

Run: `pytest tests/test_benchmark_async.py::TestAsyncMutationBenchmarks -v --benchmark-disable`
Expected: PASS (5 tests)

**Step 3: Commit**

```bash
git add tests/test_benchmark_async.py
git commit -m "feat(tests): add async mutation benchmarks"
```

---

## Task 9: Implement Advanced Transaction Benchmarks (Async)

**Files:**

- Modify: `tests/test_benchmark_async.py`

**Step 1: Add async upsert and batch benchmarks**

```python
class TestAsyncTransactionBenchmarks:
    """Benchmarks for advanced async transaction operations."""

    def test_benchmark_upsert_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async upsert operation."""
        client, loop = async_client_with_schema_for_benchmark
        counter = [0]

        async def run_upsert() -> api.Response:
            counter[0] += 1
            email = f"upsert{counter[0]}@test.com"
            txn = client.txn()
            query = f'{{ u as var(func: eq(email, "{email}")) }}'
            mutation = pydgraph.Mutation(
                set_nquads=f'''
                    uid(u) <email> "{email}" .
                    uid(u) <name> "Upsert_{counter[0]}" .
                '''.encode(),
                cond="@if(eq(len(u), 0))",
            )
            request = api.Request(
                query=query,
                mutations=[mutation],
                commit_now=True,
            )
            return await txn.do_request(request)

        def benchmark_wrapper() -> api.Response:
            return loop.run_until_complete(run_upsert())

        benchmark(benchmark_wrapper)

    def test_benchmark_batch_mutations_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark multiple async mutations in one transaction."""
        client, loop = async_client_with_schema_for_benchmark
        counter = [0]
        batch_size = 10

        async def run_batch() -> api.TxnContext:
            txn = client.txn()
            for i in range(batch_size):
                counter[0] += 1
                await txn.mutate(set_obj=generate_person(counter[0]))
            return await txn.commit()

        def benchmark_wrapper() -> api.TxnContext:
            return loop.run_until_complete(run_batch())

        benchmark(benchmark_wrapper)

    def test_benchmark_run_transaction_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark run_transaction_async helper overhead."""
        client, loop = async_client_with_schema_for_benchmark
        counter = [0]

        async def txn_func(txn: pydgraph.AsyncTxn) -> str:
            counter[0] += 1
            response = await txn.mutate(set_obj=generate_person(counter[0]), commit_now=True)
            return next(iter(response.uids.values()), "")

        async def run_with_helper() -> str:
            return await run_transaction_async(client, txn_func)

        def benchmark_wrapper() -> str:
            return loop.run_until_complete(run_with_helper())

        benchmark(benchmark_wrapper)
```

**Step 2: Run the async transaction benchmarks to verify**

Run: `pytest tests/test_benchmark_async.py::TestAsyncTransactionBenchmarks -v --benchmark-disable`
Expected: PASS (3 tests)

**Step 3: Commit**

```bash
git add tests/test_benchmark_async.py
git commit -m "feat(tests): add async transaction benchmarks"
```

---

## Task 10: Implement Client Operation Benchmarks (Async)

**Files:**

- Modify: `tests/test_benchmark_async.py`

**Step 1: Add async client-level operation benchmarks**

```python
class TestAsyncClientBenchmarks:
    """Benchmarks for async client-level operations."""

    def test_benchmark_check_version_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async check_version."""
        client, loop = async_client_with_schema_for_benchmark

        async def run_check() -> str:
            return await client.check_version()

        def benchmark_wrapper() -> str:
            return loop.run_until_complete(run_check())

        benchmark(benchmark_wrapper)

    def test_benchmark_alter_schema_async(
        self,
        async_client_with_schema_for_benchmark: tuple[AsyncDgraphClient, asyncio.AbstractEventLoop],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async schema alter operation."""
        client, loop = async_client_with_schema_for_benchmark
        counter = [0]

        async def run_alter() -> api.Payload:
            counter[0] += 1
            schema = f"benchmark_pred_{counter[0]}: string @index(exact) ."
            return await client.alter(pydgraph.Operation(schema=schema))

        def benchmark_wrapper() -> api.Payload:
            return loop.run_until_complete(run_alter())

        benchmark(benchmark_wrapper)
```

**Step 2: Run the async client benchmarks to verify**

Run: `pytest tests/test_benchmark_async.py::TestAsyncClientBenchmarks -v --benchmark-disable`
Expected: PASS (2 tests)

**Step 3: Commit**

```bash
git add tests/test_benchmark_async.py
git commit -m "feat(tests): add async client operation benchmarks"
```

---

## Task 11: Run Full Benchmark Suite and Verify

**Step 1: Run all sync benchmarks**

Run: `pytest tests/test_benchmark_sync.py -v --benchmark-disable` Expected: PASS (13 tests)

**Step 2: Run all async benchmarks**

Run: `pytest tests/test_benchmark_async.py -v --benchmark-disable` Expected: PASS (13 tests)

**Step 3: Run benchmarks with actual timing**

Run: `pytest tests/test_benchmark_sync.py tests/test_benchmark_async.py -v` Expected: All benchmarks
complete with timing data

**Step 4: Commit final state**

```bash
git add -A
git commit -m "test(benchmarks): complete targeted benchmark test suite"
```

---

## Task 12: Update PR and Generate Benchmark Results

**Step 1: Push changes**

Run: `git push`

**Step 2: Generate benchmark SVG**

Run: `make benchmark` Expected: Benchmark SVG generated

**Step 3: Update PR description with new benchmark results**

Include the new targeted benchmarks in the PR description, noting that these complement the stress
tests by isolating individual operations.

---

## Summary

**Created files:**

- `tests/test_benchmark_sync.py` - 13 sync benchmarks
- `tests/test_benchmark_async.py` - 13 async benchmarks

**Operations covered:** | Category | Operations | |----------|------------| | Query | Simple, with
variables, best-effort | | Mutation | commit_now, explicit commit, discard, N-Quads, delete | |
Transaction | Upsert, batch, run_transaction helper | | Client | check_version, alter schema |

**Total: 26 targeted benchmarks**

**Regression analysis workflow:**

1. Stress test regresses
2. Run targeted benchmarks
3. Compare individual operation times
4. Identify specific operation that degraded
