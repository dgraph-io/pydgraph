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
from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

import pydgraph
from pydgraph import AsyncDgraphClient, AsyncDgraphClientStub, run_transaction_async
from pydgraph.proto import api_pb2 as api

from .helpers import TEST_SERVER_ADDR, generate_movie

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Dedicated event loop for async benchmark tests."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture
def benchmark_client(
    event_loop: asyncio.AbstractEventLoop,
    movies_schema: str,
) -> Generator[AsyncDgraphClient, None, None]:
    """Async client with schema for benchmark tests."""
    loop = event_loop

    async def setup() -> AsyncDgraphClient:
        client_stub = AsyncDgraphClientStub(TEST_SERVER_ADDR)
        client = AsyncDgraphClient(client_stub)
        for _ in range(30):
            try:
                await client.login("groot", "password")
                break
            except Exception as e:
                if "user not found" in str(e):
                    raise
                await asyncio.sleep(0.1)
        await client.alter(pydgraph.Operation(drop_all=True))
        await client.alter(pydgraph.Operation(schema=movies_schema))
        return client

    client = loop.run_until_complete(setup())
    yield client
    loop.run_until_complete(client.close())


# =============================================================================
# Query Benchmarks
# =============================================================================


class TestAsyncQueryBenchmarks:
    """Benchmarks for async query operations."""

    def test_benchmark_query_async(
        self,
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark a simple async read query."""
        loop = event_loop
        client = benchmark_client

        # Setup: seed data outside benchmark
        async def setup() -> None:
            txn = client.txn()
            await txn.mutate(set_obj=generate_movie(0), commit_now=True)

        loop.run_until_complete(setup())

        query = """query {
            people(func: has(name), first: 1) {
                name
                email
                tagline
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
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark an async query with variables."""
        loop = event_loop
        client = benchmark_client

        # Setup
        async def setup() -> None:
            txn = client.txn()
            await txn.mutate(
                set_obj={"name": "BenchmarkUser", "email": "bench@test.com"},
                commit_now=True,
            )

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
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark a best-effort async read query."""
        loop = event_loop
        client = benchmark_client

        # Setup
        async def setup() -> None:
            txn = client.txn()
            await txn.mutate(set_obj=generate_movie(0), commit_now=True)

        loop.run_until_complete(setup())

        query = "{ people(func: has(name), first: 1) { name } }"

        async def run_query() -> api.Response:
            txn = client.txn(read_only=True, best_effort=True)
            return await txn.query(query)

        def benchmark_wrapper() -> api.Response:
            return loop.run_until_complete(run_query())

        benchmark(benchmark_wrapper)


# =============================================================================
# Mutation Benchmarks
# =============================================================================


class TestAsyncMutationBenchmarks:
    """Benchmarks for async mutation operations."""

    def test_benchmark_mutation_commit_now_async(
        self,
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async mutation with commit_now."""
        loop = event_loop
        client = benchmark_client
        counter = [0]

        async def run_mutation() -> api.Response:
            counter[0] += 1
            txn = client.txn()
            return await txn.mutate(set_obj=generate_movie(counter[0]), commit_now=True)

        def benchmark_wrapper() -> api.Response:
            return loop.run_until_complete(run_mutation())

        benchmark(benchmark_wrapper)

    def test_benchmark_mutation_explicit_commit_async(
        self,
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async mutation with explicit commit."""
        loop = event_loop
        client = benchmark_client
        counter = [0]

        async def run_mutation() -> api.TxnContext | None:
            counter[0] += 1
            txn = client.txn()
            await txn.mutate(set_obj=generate_movie(counter[0]))
            return await txn.commit()

        def benchmark_wrapper() -> api.TxnContext | None:
            return loop.run_until_complete(run_mutation())

        benchmark(benchmark_wrapper)

    def test_benchmark_discard_async(
        self,
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async mutation followed by discard."""
        loop = event_loop
        client = benchmark_client
        counter = [0]

        async def run_mutation() -> None:
            counter[0] += 1
            txn = client.txn()
            await txn.mutate(set_obj=generate_movie(counter[0]))
            await txn.discard()

        def benchmark_wrapper() -> None:
            return loop.run_until_complete(run_mutation())

        benchmark(benchmark_wrapper)

    def test_benchmark_mutation_nquads_async(
        self,
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async N-Quads mutation."""
        loop = event_loop
        client = benchmark_client
        counter = [0]

        async def run_mutation() -> api.Response:
            counter[0] += 1
            txn = client.txn()
            nquads = f"""
                _:person <name> "Movie_{counter[0]}" .
                _:person <email> "movie{counter[0]}@test.com" .
                _:person <tagline> "A test movie number {counter[0]}" .
            """
            return await txn.mutate(set_nquads=nquads, commit_now=True)

        def benchmark_wrapper() -> api.Response:
            return loop.run_until_complete(run_mutation())

        benchmark(benchmark_wrapper)

    def test_benchmark_delete_async(
        self,
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async delete mutation."""
        loop = event_loop
        client = benchmark_client

        # Pre-create nodes to delete
        async def setup() -> list[str]:
            uids = []
            for i in range(100):
                txn = client.txn()
                resp = await txn.mutate(set_obj=generate_movie(i), commit_now=True)
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


# =============================================================================
# Transaction Benchmarks
# =============================================================================


class TestAsyncTransactionBenchmarks:
    """Benchmarks for advanced async transaction operations."""

    def test_benchmark_upsert_async(
        self,
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async upsert operation."""
        loop = event_loop
        client = benchmark_client
        counter = [0]

        async def run_upsert() -> api.Response:
            counter[0] += 1
            email = f"upsert{counter[0]}@test.com"
            txn = client.txn()
            query = f'{{ u as var(func: eq(email, "{email}")) }}'
            mutation = pydgraph.Mutation(
                set_nquads=f"""
                    uid(u) <email> "{email}" .
                    uid(u) <name> "Upsert_{counter[0]}" .
                """.encode(),
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
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark multiple async mutations in one transaction."""
        loop = event_loop
        client = benchmark_client
        counter = [0]
        batch_size = 10

        async def run_batch() -> api.TxnContext | None:
            txn = client.txn()
            for _ in range(batch_size):
                counter[0] += 1
                await txn.mutate(set_obj=generate_movie(counter[0]))
            return await txn.commit()

        def benchmark_wrapper() -> api.TxnContext | None:
            return loop.run_until_complete(run_batch())

        benchmark(benchmark_wrapper)

    def test_benchmark_run_transaction_async(
        self,
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark run_transaction_async helper overhead."""
        loop = event_loop
        client = benchmark_client
        counter = [0]

        async def txn_func(txn: pydgraph.AsyncTxn) -> str:
            counter[0] += 1
            response = await txn.mutate(
                set_obj=generate_movie(counter[0]), commit_now=True
            )
            return next(iter(response.uids.values()), "")

        async def run_with_helper() -> str:
            return await run_transaction_async(client, txn_func)

        def benchmark_wrapper() -> str:
            return loop.run_until_complete(run_with_helper())

        benchmark(benchmark_wrapper)


# =============================================================================
# Client Benchmarks
# =============================================================================


class TestAsyncClientBenchmarks:
    """Benchmarks for async client-level operations."""

    def test_benchmark_check_version_async(
        self,
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async check_version."""
        loop = event_loop
        client = benchmark_client

        async def run_check() -> str:
            return await client.check_version()

        def benchmark_wrapper() -> str:
            return loop.run_until_complete(run_check())

        benchmark(benchmark_wrapper)

    def test_benchmark_alter_schema_async(
        self,
        event_loop: asyncio.AbstractEventLoop,
        benchmark_client: AsyncDgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark async schema alter operation."""
        loop = event_loop
        client = benchmark_client
        counter = [0]

        async def run_alter() -> api.Payload:
            counter[0] += 1
            schema = f"benchmark_pred_{counter[0]}: string @index(exact) ."
            return await client.alter(pydgraph.Operation(schema=schema))

        def benchmark_wrapper() -> api.Payload:
            return loop.run_until_complete(run_alter())

        benchmark(benchmark_wrapper)
