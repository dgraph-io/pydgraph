# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Async client stress tests.

These tests stress test the asynchronous pydgraph client using pure asyncio
concurrency patterns (asyncio.gather, asyncio.create_task). No concurrent.futures
mixing - all concurrency is handled by the asyncio event loop.

Usage:
    # Quick mode (default, CI-friendly)
    pytest tests/test_stress_async.py -v

    # Full mode (thorough stress testing)
    STRESS_TEST_MODE=full pytest tests/test_stress_async.py -v
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest

import pydgraph
from pydgraph import (
    AsyncDgraphClient,
    AsyncDgraphClientStub,
    errors,
    retry_async,
    run_transaction_async,
    with_retry_async,
)
from pydgraph.proto import api_pb2 as api

from .helpers import TEST_SERVER_ADDR, generate_movie

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest_benchmark.fixture import BenchmarkFixture

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def benchmark_event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Module-scoped event loop for async stress and benchmark tests."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def stress_client(
    benchmark_event_loop: asyncio.AbstractEventLoop,
    movies_schema: str,
    movies_data_loaded: bool,
) -> Generator[AsyncDgraphClient, None, None]:
    """Module-scoped async client with movies test schema for stress tests."""
    loop = benchmark_event_loop

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
        if not movies_data_loaded:
            await client.alter(pydgraph.Operation(drop_all=True))
            await client.alter(pydgraph.Operation(schema=movies_schema))
        return client

    client = loop.run_until_complete(setup())
    yield client
    loop.run_until_complete(client.close())


# =============================================================================
# Async Client Stress Tests
# =============================================================================


class TestAsyncClientStress:
    """Stress tests for asynchronous Dgraph client using pure asyncio."""

    def test_concurrent_read_queries_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test many concurrent read-only queries using asyncio.gather."""
        loop = benchmark_event_loop
        client = stress_client
        num_ops = stress_config["ops"]
        rounds = stress_config["rounds"]

        query = """query {
            people(func: has(name), first: 10) {
                name
                email
                tagline
            }
        }"""

        # Setup: Insert test data once before benchmarking (using same loop)
        async def setup_data() -> None:
            txn = client.txn()
            for i in range(100):
                await txn.mutate(set_obj=generate_movie(i))
            await txn.commit()

        loop.run_until_complete(setup_data())

        async def run_query() -> api.Response:
            txn = client.txn(read_only=True)
            return await txn.query(query)

        # Wrap async execution in sync function for benchmark (using same loop)
        def run_benchmark() -> list[api.Response | BaseException]:
            all_results: list[api.Response | BaseException] = []
            for _ in range(rounds):
                batch = loop.run_until_complete(
                    asyncio.gather(
                        *[run_query() for _ in range(num_ops)],
                        return_exceptions=True,
                    )
                )
                all_results.extend(batch)
            return all_results

        # Stress tests use pedantic(rounds=1) because the stress workload is
        # already controlled by stress_config["rounds"] inside run_benchmark().
        # Letting pytest-benchmark repeat the whole concurrent batch would
        # compound iterations and overwhelm the Dgraph cluster.
        results = benchmark.pedantic(
            run_benchmark, rounds=1, iterations=1, warmup_rounds=0
        )

        exc_list = [r for r in results if isinstance(r, Exception)]
        assert len(exc_list) == 0, f"Got {len(exc_list)} errors: {exc_list[:5]}"
        assert len(results) == num_ops * rounds

    def test_concurrent_mutations_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test concurrent mutations in separate transactions using asyncio.gather."""
        loop = benchmark_event_loop
        client = stress_client
        num_ops = stress_config["workers"] * 10
        rounds = stress_config["rounds"]
        counter = [0]

        async def run_mutation() -> bool:
            counter[0] += 1
            try:
                txn = client.txn()
                await txn.mutate(set_obj=generate_movie(counter[0]), commit_now=True)
            except errors.AbortedError:
                return False
            else:
                return True

        def run_benchmark() -> list[bool | BaseException]:
            all_results: list[bool | BaseException] = []
            for _ in range(rounds):
                batch = loop.run_until_complete(
                    asyncio.gather(
                        *[run_mutation() for _ in range(num_ops)],
                        return_exceptions=True,
                    )
                )
                all_results.extend(batch)
            return all_results

        results = benchmark.pedantic(
            run_benchmark, rounds=1, iterations=1, warmup_rounds=0
        )

        exc_list = [r for r in results if isinstance(r, Exception)]
        successes = sum(1 for r in results if r is True)

        assert len(exc_list) == 0, f"Unexpected errors: {exc_list[:5]}"
        assert successes > num_ops * rounds * 0.5, f"Too few successes: {successes}/{num_ops * rounds}"

    def test_mixed_workload_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test mix of queries, mutations, commits, and discards concurrently."""
        loop = benchmark_event_loop
        client = stress_client
        num_ops = stress_config["workers"] * 20
        rounds = stress_config["rounds"]
        counter = [0]

        # Setup: Seed some data once before benchmarking (using same loop)
        async def setup_data() -> None:
            txn = client.txn()
            for i in range(50):
                await txn.mutate(set_obj=generate_movie(i))
            await txn.commit()

        loop.run_until_complete(setup_data())

        async def random_operation(op_id: int) -> str:
            counter[0] += 1
            unique_id = counter[0]
            op_type = op_id % 4
            result = "unknown"
            try:
                if op_type == 0:
                    # Read query
                    txn = client.txn(read_only=True)
                    await txn.query("{ q(func: has(name), first: 5) { name } }")
                    result = "query"
                elif op_type == 1:
                    # Mutation with commit_now
                    txn = client.txn()
                    await txn.mutate(set_obj=generate_movie(unique_id), commit_now=True)
                    result = "mutation"
                elif op_type == 2:
                    # Mutation with explicit commit
                    txn = client.txn()
                    await txn.mutate(set_obj=generate_movie(unique_id))
                    await txn.commit()
                    result = "commit"
                else:
                    # Mutation with discard
                    txn = client.txn()
                    await txn.mutate(set_obj=generate_movie(unique_id))
                    await txn.discard()
                    result = "discard"
            except errors.AbortedError:
                return "aborted"
            return result

        def run_benchmark() -> list[str | BaseException]:
            all_results: list[str | BaseException] = []
            for _ in range(rounds):
                batch = loop.run_until_complete(
                    asyncio.gather(
                        *[random_operation(i) for i in range(num_ops)],
                        return_exceptions=True,
                    )
                )
                all_results.extend(batch)
            return all_results

        results = benchmark.pedantic(
            run_benchmark, rounds=1, iterations=1, warmup_rounds=0
        )

        exc_list = [r for r in results if isinstance(r, Exception)]
        assert len(exc_list) == 0, f"Unexpected errors: {exc_list[:5]}"


# =============================================================================
# Async Transaction Stress Tests
# =============================================================================


class TestAsyncTransactionStress:
    """Stress tests for async transaction conflict handling."""

    def test_upsert_conflicts_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test concurrent upserts on the same key detect conflicts properly."""
        loop = benchmark_event_loop
        client = stress_client
        target_email = "async_conflict@test.com"
        num_workers = stress_config["workers"]
        rounds = stress_config["rounds"]

        async def run_upsert(worker_id: int) -> str:
            try:
                txn = client.txn()
                query = f'{{ u as var(func: eq(email, "{target_email}")) }}'
                mutation = pydgraph.Mutation(
                    set_nquads=f"""
                    uid(u) <email> "{target_email}" .
                    uid(u) <name> "AsyncWorker_{worker_id}" .
                    uid(u) <tagline> "AsyncWorker {worker_id} tagline" .
                    """.encode(),
                    cond="@if(eq(len(u), 0))",
                )
                request = api.Request(
                    query=query,
                    mutations=[mutation],
                    commit_now=True,
                )
                await txn.do_request(request)
            except errors.AbortedError:
                return "aborted"
            else:
                return "success"

        def run_benchmark() -> list[str | BaseException]:
            all_results: list[str | BaseException] = []
            for _ in range(rounds):
                batch = loop.run_until_complete(
                    asyncio.gather(
                        *[run_upsert(i) for i in range(num_workers)],
                        return_exceptions=True,
                    )
                )
                all_results.extend(batch)
            return all_results

        results = benchmark.pedantic(
            run_benchmark, rounds=1, iterations=1, warmup_rounds=0
        )

        exc_list = [r for r in results if isinstance(r, Exception)]
        successes = sum(1 for r in results if r == "success")

        assert len(exc_list) == 0, f"Unexpected errors: {exc_list}"
        assert successes >= rounds, "Too few upserts succeeded"

    def test_deadlock_regression_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
        stress_config: dict[str, Any],
    ) -> None:
        """Regression test for PR #293 asyncio.Lock deadlock fix.

        Verifies that when do_request() encounters an error, the transaction
        is properly cleaned up without causing deadlocks due to the non-reentrant
        asyncio.Lock trying to be acquired twice.
        """
        loop = benchmark_event_loop
        client = stress_client
        num_ops = stress_config["ops"]

        async def cause_error() -> None:
            txn = client.txn()
            try:
                # Invalid query syntax causes error
                await txn.query("{ invalid syntax")
            except Exception:
                pass  # Expected

        async def run_all() -> None:
            # If there's a deadlock, wait_for will timeout
            try:
                await asyncio.wait_for(
                    asyncio.gather(*[cause_error() for _ in range(num_ops)]),
                    timeout=30,
                )
            except asyncio.TimeoutError:
                pytest.fail("Deadlock detected - asyncio.Lock not released properly")

        loop.run_until_complete(run_all())

    def test_lock_released_after_mutation_error_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
    ) -> None:
        """Test that lock is released after mutation errors allowing reuse."""
        loop = benchmark_event_loop
        client = stress_client

        async def run_test() -> None:
            # Create a transaction and force an error
            txn = client.txn()
            await txn.mutate(set_obj={"name": "Test"})

            # Force cleanup by discarding
            await txn.discard()

            # Create new transaction and verify it works
            txn2 = client.txn()
            response = await txn2.mutate(set_obj={"name": "Test2"}, commit_now=True)
            assert len(response.uids) == 1

        loop.run_until_complete(run_test())

    def test_context_manager_cleanup_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
    ) -> None:
        """Test that context managers properly clean up even on errors."""
        loop = benchmark_event_loop
        client = stress_client

        async def run_test() -> None:
            async def use_txn_with_error() -> None:
                async with client.txn() as txn:
                    await txn.mutate(set_obj={"name": "ContextTest"})
                    raise ValueError("Intentional error")

            # Should not leave any locks held
            for _ in range(2):
                try:
                    await use_txn_with_error()
                except ValueError:
                    pass  # Expected

            # Verify client still works
            async with client.txn() as txn:
                response = await txn.query(
                    "{ q(func: has(name), first: 1) { name } }"
                )
                assert response is not None

        loop.run_until_complete(run_test())

    def test_rapid_txn_create_discard_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
        stress_config: dict[str, Any],
    ) -> None:
        """Test rapidly creating and discarding transactions."""
        loop = benchmark_event_loop
        client = stress_client
        num_ops = stress_config["ops"]

        async def create_and_discard() -> None:
            txn = client.txn()
            await txn.discard()

        async def run_all() -> None:
            # Pure asyncio concurrency
            results = await asyncio.gather(
                *[create_and_discard() for _ in range(num_ops)],
                return_exceptions=True,
            )
            exc_list = [r for r in results if isinstance(r, Exception)]
            assert len(exc_list) == 0, (
                f"Errors during rapid txn lifecycle: {exc_list[:5]}"
            )

        loop.run_until_complete(run_all())


# =============================================================================
# Async Retry Stress Tests
# =============================================================================


class TestAsyncRetryStress:
    """Stress tests for async retry utilities."""

    def test_retry_under_conflicts_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test retry_async() generator handles conflicts correctly under load."""
        loop = benchmark_event_loop
        client = stress_client
        num_workers = min(stress_config["workers"], 20)
        rounds = stress_config["rounds"]
        reps_per_worker = 2

        async def retry_work(worker_id: int) -> int:
            successes = 0
            for _ in range(reps_per_worker):
                async for attempt in retry_async():
                    with attempt:  # Note: regular 'with', not 'async with'
                        txn = client.txn()
                        await txn.mutate(
                            set_obj=generate_movie(worker_id * 1000 + successes),
                            commit_now=True,
                        )
                        successes += 1
            return successes

        def run_benchmark() -> list[int | BaseException]:
            all_results: list[int | BaseException] = []
            for _ in range(rounds):
                batch = loop.run_until_complete(
                    asyncio.gather(
                        *[retry_work(i) for i in range(num_workers)],
                        return_exceptions=True,
                    )
                )
                all_results.extend(batch)
            return all_results

        results = benchmark.pedantic(
            run_benchmark, rounds=1, iterations=1, warmup_rounds=0
        )

        exc_list = [r for r in results if isinstance(r, Exception)]
        total_successes = sum(r for r in results if isinstance(r, int))

        assert len(exc_list) == 0, f"Errors: {exc_list[:5]}"
        assert total_successes >= num_workers * reps_per_worker * rounds

    def test_with_retry_decorator_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test @with_retry_async decorator handles conflicts correctly."""
        loop = benchmark_event_loop
        client = stress_client
        num_workers = min(stress_config["workers"], 10)
        rounds = stress_config["rounds"]
        counter = [0]

        @with_retry_async()
        async def create_person() -> str:
            counter[0] += 1
            txn = client.txn()
            response = await txn.mutate(
                set_obj=generate_movie(counter[0]),
                commit_now=True,
            )
            return next(iter(response.uids.values()), "")

        def run_benchmark() -> list[str | BaseException]:
            all_results: list[str | BaseException] = []
            for _ in range(rounds):
                batch = loop.run_until_complete(
                    asyncio.gather(
                        *[create_person() for _ in range(num_workers)],
                        return_exceptions=True,
                    )
                )
                all_results.extend(batch)
            return all_results

        results = benchmark.pedantic(
            run_benchmark, rounds=1, iterations=1, warmup_rounds=0
        )

        exc_list = [r for r in results if isinstance(r, Exception)]
        successes = [r for r in results if isinstance(r, str) and r]

        assert len(exc_list) == 0, f"Errors: {exc_list[:5]}"
        assert len(successes) == num_workers * rounds

    def test_run_transaction_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test run_transaction_async() helper handles conflicts correctly."""
        loop = benchmark_event_loop
        client = stress_client
        num_workers = min(stress_config["workers"], 10)
        rounds = stress_config["rounds"]
        counter = [0]

        async def work() -> str:
            counter[0] += 1
            unique_id = counter[0]

            async def txn_func(txn: pydgraph.AsyncTxn) -> str:
                response = await txn.mutate(
                    set_obj={
                        "name": f"AsyncRunTxn_{unique_id}",
                        "tagline": f"AsyncWorker {unique_id} transaction",
                    },
                    commit_now=True,
                )
                return next(iter(response.uids.values()), "")

            return await run_transaction_async(client, txn_func)

        def run_benchmark() -> list[str | BaseException]:
            all_results: list[str | BaseException] = []
            for _ in range(rounds):
                batch = loop.run_until_complete(
                    asyncio.gather(
                        *[work() for _ in range(num_workers)],
                        return_exceptions=True,
                    )
                )
                all_results.extend(batch)
            return all_results

        results = benchmark.pedantic(
            run_benchmark, rounds=1, iterations=1, warmup_rounds=0
        )

        exc_list = [r for r in results if isinstance(r, Exception)]
        successes = [r for r in results if isinstance(r, str) and r]

        assert len(exc_list) == 0, f"Errors: {exc_list[:5]}"
        assert len(successes) == num_workers * rounds


# =============================================================================
# Async Transaction Edge Cases
# =============================================================================


class TestAsyncTransactionEdgeCases:
    """Tests for async transaction edge cases and error handling."""

    def test_double_commit_error_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
    ) -> None:
        """Test that double commit raises appropriate error."""
        loop = benchmark_event_loop
        client = stress_client

        async def run_test() -> None:
            txn = client.txn()
            await txn.mutate(set_obj={"name": "DoubleCommit"})
            await txn.commit()

            with pytest.raises(errors.TransactionError):
                await txn.commit()

        loop.run_until_complete(run_test())

    def test_use_after_commit_error_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
    ) -> None:
        """Test that using transaction after commit raises error."""
        loop = benchmark_event_loop
        client = stress_client

        async def run_test() -> None:
            txn = client.txn()
            await txn.mutate(set_obj={"name": "UseAfterCommit"}, commit_now=True)

            with pytest.raises(errors.TransactionError):
                await txn.query("{ q(func: has(name)) { name } }")

        loop.run_until_complete(run_test())

    def test_read_only_mutation_error_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
    ) -> None:
        """Test that mutations in read-only transaction raise error."""
        loop = benchmark_event_loop
        client = stress_client

        async def run_test() -> None:
            txn = client.txn(read_only=True)

            with pytest.raises(errors.TransactionError):
                await txn.mutate(set_obj={"name": "ReadOnlyMutation"})

        loop.run_until_complete(run_test())

    def test_best_effort_requires_read_only_async(
        self,
        stress_client: AsyncDgraphClient,
    ) -> None:
        """Test that best_effort requires read_only=True."""
        client = stress_client

        with pytest.raises(ValueError):
            client.txn(read_only=False, best_effort=True)

    def test_double_discard_is_safe_async(
        self,
        benchmark_event_loop: asyncio.AbstractEventLoop,
        stress_client: AsyncDgraphClient,
    ) -> None:
        """Test that calling discard twice is safe for async transactions."""
        loop = benchmark_event_loop
        client = stress_client

        async def run_test() -> None:
            txn = client.txn()
            await txn.mutate(set_obj={"name": "AsyncDoubleDiscard"})
            await txn.discard()
            await txn.discard()  # Should not raise

        loop.run_until_complete(run_test())
