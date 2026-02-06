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
import random
from typing import TYPE_CHECKING, Any

import pytest

import pydgraph
from pydgraph import (
    AsyncDgraphClient,
    errors,
    retry_async,
    run_transaction_async,
    with_retry_async,
)
from pydgraph.proto import api_pb2 as api

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


def _generate_person(index: int) -> dict[str, Any]:
    """Generate a person object for testing."""
    return {
        "name": f"Person_{index}_{random.randint(1000, 9999)}",  # noqa: S311
        "email": f"person{index}_{random.randint(1000, 9999)}@test.com",  # noqa: S311
        "age": random.randint(18, 80),  # noqa: S311
        "balance": random.uniform(0, 10000),  # noqa: S311
        "active": random.choice([True, False]),  # noqa: S311
    }


# =============================================================================
# Async Client Stress Tests
# =============================================================================


class TestAsyncClientStress:
    """Stress tests for asynchronous Dgraph client using pure asyncio."""

    def test_concurrent_async_queries(
        self,
        async_client_with_schema_for_benchmark: tuple[
            AsyncDgraphClient, asyncio.AbstractEventLoop
        ],
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test many concurrent read-only queries using asyncio.gather."""
        client, loop = async_client_with_schema_for_benchmark
        num_ops = stress_config["ops"]

        query = """query {
            people(func: has(name), first: 10) {
                name
                email
                age
            }
        }"""

        # Setup: Insert test data once before benchmarking (using same loop)
        async def setup_data() -> None:
            txn = client.txn()
            for i in range(100):
                await txn.mutate(set_obj=_generate_person(i))
            await txn.commit()

        loop.run_until_complete(setup_data())

        async def run_query() -> api.Response:
            txn = client.txn(read_only=True)
            return await txn.query(query)

        # Wrap async execution in sync function for benchmark (using same loop)
        def run_benchmark() -> list[api.Response | BaseException]:
            return loop.run_until_complete(
                asyncio.gather(
                    *[run_query() for _ in range(num_ops)],
                    return_exceptions=True,
                )
            )

        results = benchmark(run_benchmark)

        exc_list = [r for r in results if isinstance(r, Exception)]
        assert len(exc_list) == 0, f"Got {len(exc_list)} errors: {exc_list[:5]}"
        assert len(results) == num_ops

    def test_concurrent_async_mutations(
        self,
        async_client_with_schema_for_benchmark: tuple[
            AsyncDgraphClient, asyncio.AbstractEventLoop
        ],
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test concurrent mutations in separate transactions using asyncio.gather."""
        client, loop = async_client_with_schema_for_benchmark
        num_ops = stress_config["workers"] * 10

        async def run_mutation(index: int) -> bool:
            try:
                txn = client.txn()
                await txn.mutate(set_obj=_generate_person(index), commit_now=True)
                return True
            except errors.AbortedError:
                return False

        def run_benchmark() -> list[bool | BaseException]:
            return loop.run_until_complete(
                asyncio.gather(
                    *[run_mutation(i) for i in range(num_ops)],
                    return_exceptions=True,
                )
            )

        results = benchmark(run_benchmark)

        exc_list = [r for r in results if isinstance(r, Exception)]
        successes = sum(1 for r in results if r is True)

        assert len(exc_list) == 0, f"Unexpected errors: {exc_list[:5]}"
        assert successes > num_ops * 0.5, f"Too few successes: {successes}/{num_ops}"

    def test_mixed_async_workload(
        self,
        async_client_with_schema_for_benchmark: tuple[
            AsyncDgraphClient, asyncio.AbstractEventLoop
        ],
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test mix of queries, mutations, commits, and discards concurrently."""
        client, loop = async_client_with_schema_for_benchmark
        num_ops = stress_config["workers"] * 20

        # Setup: Seed some data once before benchmarking (using same loop)
        async def setup_data() -> None:
            txn = client.txn()
            for i in range(50):
                await txn.mutate(set_obj=_generate_person(i))
            await txn.commit()

        loop.run_until_complete(setup_data())

        async def random_operation(op_id: int) -> str:
            op_type = op_id % 4
            try:
                if op_type == 0:
                    # Read query
                    txn = client.txn(read_only=True)
                    await txn.query("{ q(func: has(name), first: 5) { name } }")
                    return "query"
                if op_type == 1:
                    # Mutation with commit_now
                    txn = client.txn()
                    await txn.mutate(set_obj=_generate_person(op_id), commit_now=True)
                    return "mutation"
                if op_type == 2:
                    # Mutation with explicit commit
                    txn = client.txn()
                    await txn.mutate(set_obj=_generate_person(op_id))
                    await txn.commit()
                    return "commit"
                # Mutation with discard
                txn = client.txn()
                await txn.mutate(set_obj=_generate_person(op_id))
                await txn.discard()
                return "discard"
            except errors.AbortedError:
                return "aborted"

        def run_benchmark() -> list[str | BaseException]:
            return loop.run_until_complete(
                asyncio.gather(
                    *[random_operation(i) for i in range(num_ops)],
                    return_exceptions=True,
                )
            )

        results = benchmark(run_benchmark)

        exc_list = [r for r in results if isinstance(r, Exception)]
        assert len(exc_list) == 0, f"Unexpected errors: {exc_list[:5]}"


# =============================================================================
# Async Transaction Stress Tests
# =============================================================================


class TestAsyncTransactionStress:
    """Stress tests for async transaction conflict handling."""

    def test_async_transaction_conflicts(
        self,
        async_client_with_schema_for_benchmark: tuple[
            AsyncDgraphClient, asyncio.AbstractEventLoop
        ],
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test concurrent upserts on the same key detect conflicts properly."""
        client, loop = async_client_with_schema_for_benchmark
        target_email = "async_conflict@test.com"
        num_workers = stress_config["workers"]

        async def run_upsert(worker_id: int) -> str:
            try:
                txn = client.txn()
                query = f'{{ u as var(func: eq(email, "{target_email}")) }}'
                mutation = pydgraph.Mutation(
                    set_nquads=f"""
                    uid(u) <email> "{target_email}" .
                    uid(u) <name> "AsyncWorker_{worker_id}" .
                    uid(u) <balance> "{worker_id}" .
                    """.encode(),
                    cond="@if(eq(len(u), 0))",
                )
                request = api.Request(
                    query=query,
                    mutations=[mutation],
                    commit_now=True,
                )
                await txn.do_request(request)
                return "success"
            except errors.AbortedError:
                return "aborted"

        def run_benchmark() -> list[str | BaseException]:
            return loop.run_until_complete(
                asyncio.gather(
                    *[run_upsert(i) for i in range(num_workers)],
                    return_exceptions=True,
                )
            )

        results = benchmark(run_benchmark)

        exc_list = [r for r in results if isinstance(r, Exception)]
        successes = sum(1 for r in results if r == "success")

        assert len(exc_list) == 0, f"Unexpected errors: {exc_list}"
        assert successes >= 1, "No upserts succeeded"

    @pytest.mark.asyncio
    async def test_async_deadlock_regression(
        self,
        async_client_with_schema: AsyncDgraphClient,
        stress_config: dict[str, Any],
    ) -> None:
        """Regression test for PR #293 asyncio.Lock deadlock fix.

        Verifies that when do_request() encounters an error, the transaction
        is properly cleaned up without causing deadlocks due to the non-reentrant
        asyncio.Lock trying to be acquired twice.
        """
        client = async_client_with_schema
        num_ops = stress_config["ops"]

        async def cause_error() -> None:
            txn = client.txn()
            try:
                # Invalid query syntax causes error
                await txn.query("{ invalid syntax")
            except Exception:
                pass  # Expected

        # If there's a deadlock, wait_for will timeout
        try:
            await asyncio.wait_for(
                asyncio.gather(*[cause_error() for _ in range(num_ops)]),
                timeout=30,
            )
        except asyncio.TimeoutError:
            pytest.fail("Deadlock detected - asyncio.Lock not released properly")

    @pytest.mark.asyncio
    async def test_lock_released_after_mutation_error(
        self,
        async_client_with_schema: AsyncDgraphClient,
    ) -> None:
        """Test that lock is released after mutation errors allowing reuse."""
        client = async_client_with_schema

        # Create a transaction and force an error
        txn = client.txn()
        await txn.mutate(set_obj={"name": "Test"})

        # Force cleanup by discarding
        await txn.discard()

        # Create new transaction and verify it works
        txn2 = client.txn()
        response = await txn2.mutate(set_obj={"name": "Test2"}, commit_now=True)
        assert len(response.uids) == 1

    @pytest.mark.asyncio
    async def test_context_manager_cleanup(
        self,
        async_client_with_schema: AsyncDgraphClient,
        stress_config: dict[str, Any],
    ) -> None:
        """Test that context managers properly clean up even on errors."""
        client = async_client_with_schema
        iterations = stress_config["iterations"]

        async def use_txn_with_error() -> None:
            async with client.txn() as txn:
                await txn.mutate(set_obj={"name": "ContextTest"})
                raise ValueError("Intentional error")

        # Should not leave any locks held
        for _ in range(iterations):
            try:
                await use_txn_with_error()
            except ValueError:
                pass  # Expected

        # Verify client still works
        async with client.txn() as txn:
            response = await txn.query("{ q(func: has(name), first: 1) { name } }")
            assert response is not None

    @pytest.mark.asyncio
    async def test_rapid_txn_create_discard(
        self,
        async_client_with_schema: AsyncDgraphClient,
        stress_config: dict[str, Any],
    ) -> None:
        """Test rapidly creating and discarding transactions."""
        client = async_client_with_schema
        num_ops = stress_config["ops"]

        async def create_and_discard() -> None:
            txn = client.txn()
            await txn.discard()

        # Pure asyncio concurrency
        results = await asyncio.gather(
            *[create_and_discard() for _ in range(num_ops)],
            return_exceptions=True,
        )

        exc_list = [r for r in results if isinstance(r, Exception)]
        assert len(exc_list) == 0, f"Errors during rapid txn lifecycle: {exc_list[:5]}"


# =============================================================================
# Async Retry Stress Tests
# =============================================================================


class TestAsyncRetryStress:
    """Stress tests for async retry utilities."""

    def test_retry_async_under_conflicts(
        self,
        async_client_with_schema_for_benchmark: tuple[
            AsyncDgraphClient, asyncio.AbstractEventLoop
        ],
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test retry_async() generator handles conflicts correctly under load."""
        client, loop = async_client_with_schema_for_benchmark
        iterations = stress_config["iterations"]
        num_workers = min(stress_config["workers"], 20)

        async def retry_work(worker_id: int) -> int:
            successes = 0
            for _ in range(iterations):
                async for attempt in retry_async():
                    with attempt:  # Note: regular 'with', not 'async with'
                        txn = client.txn()
                        await txn.mutate(
                            set_obj=_generate_person(worker_id * 1000 + successes),
                            commit_now=True,
                        )
                        successes += 1
            return successes

        def run_benchmark() -> list[int | BaseException]:
            return loop.run_until_complete(
                asyncio.gather(
                    *[retry_work(i) for i in range(num_workers)],
                    return_exceptions=True,
                )
            )

        results = benchmark(run_benchmark)

        exc_list = [r for r in results if isinstance(r, Exception)]
        total_successes = sum(r for r in results if isinstance(r, int))

        assert len(exc_list) == 0, f"Errors: {exc_list[:5]}"
        assert total_successes >= num_workers * iterations

    def test_with_retry_async_decorator(
        self,
        async_client_with_schema_for_benchmark: tuple[
            AsyncDgraphClient, asyncio.AbstractEventLoop
        ],
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test @with_retry_async decorator handles conflicts correctly."""
        client, loop = async_client_with_schema_for_benchmark
        num_workers = min(stress_config["workers"], 10)

        @with_retry_async()
        async def create_person(index: int) -> str:
            txn = client.txn()
            response = await txn.mutate(
                set_obj=_generate_person(index),
                commit_now=True,
            )
            return next(iter(response.uids.values()), "")

        def run_benchmark() -> list[str | BaseException]:
            return loop.run_until_complete(
                asyncio.gather(
                    *[create_person(i) for i in range(num_workers)],
                    return_exceptions=True,
                )
            )

        results = benchmark(run_benchmark)

        exc_list = [r for r in results if isinstance(r, Exception)]
        successes = [r for r in results if isinstance(r, str) and r]

        assert len(exc_list) == 0, f"Errors: {exc_list[:5]}"
        assert len(successes) == num_workers

    def test_run_transaction_async(
        self,
        async_client_with_schema_for_benchmark: tuple[
            AsyncDgraphClient, asyncio.AbstractEventLoop
        ],
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test run_transaction_async() helper handles conflicts correctly."""
        client, loop = async_client_with_schema_for_benchmark
        num_workers = min(stress_config["workers"], 10)

        async def work(worker_id: int) -> str:
            async def txn_func(txn: pydgraph.AsyncTxn) -> str:
                response = await txn.mutate(
                    set_obj={
                        "name": f"AsyncRunTxn_{worker_id}",
                        "balance": float(worker_id),
                    },
                    commit_now=True,
                )
                return next(iter(response.uids.values()), "")

            return await run_transaction_async(client, txn_func)

        def run_benchmark() -> list[str | BaseException]:
            return loop.run_until_complete(
                asyncio.gather(
                    *[work(i) for i in range(num_workers)],
                    return_exceptions=True,
                )
            )

        results = benchmark(run_benchmark)

        exc_list = [r for r in results if isinstance(r, Exception)]
        successes = [r for r in results if isinstance(r, str) and r]

        assert len(exc_list) == 0, f"Errors: {exc_list[:5]}"
        assert len(successes) == num_workers


# =============================================================================
# Async Transaction Edge Cases
# =============================================================================


class TestAsyncTransactionEdgeCases:
    """Tests for async transaction edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_double_commit_error(
        self,
        async_client_with_schema: AsyncDgraphClient,
    ) -> None:
        """Test that double commit raises appropriate error."""
        client = async_client_with_schema

        txn = client.txn()
        await txn.mutate(set_obj={"name": "DoubleCommit"})
        await txn.commit()

        with pytest.raises(errors.TransactionError):
            await txn.commit()

    @pytest.mark.asyncio
    async def test_use_after_commit_error(
        self,
        async_client_with_schema: AsyncDgraphClient,
    ) -> None:
        """Test that using transaction after commit raises error."""
        client = async_client_with_schema

        txn = client.txn()
        await txn.mutate(set_obj={"name": "UseAfterCommit"}, commit_now=True)

        with pytest.raises(errors.TransactionError):
            await txn.query("{ q(func: has(name)) { name } }")

    @pytest.mark.asyncio
    async def test_read_only_mutation_error(
        self,
        async_client_with_schema: AsyncDgraphClient,
    ) -> None:
        """Test that mutations in read-only transaction raise error."""
        client = async_client_with_schema

        txn = client.txn(read_only=True)

        with pytest.raises(errors.TransactionError):
            await txn.mutate(set_obj={"name": "ReadOnlyMutation"})

    @pytest.mark.asyncio
    async def test_best_effort_requires_read_only(
        self,
        async_client_with_schema: AsyncDgraphClient,
    ) -> None:
        """Test that best_effort requires read_only=True."""
        client = async_client_with_schema

        with pytest.raises(ValueError):
            client.txn(read_only=False, best_effort=True)

    @pytest.mark.asyncio
    async def test_async_double_discard_is_safe(
        self,
        async_client_with_schema: AsyncDgraphClient,
    ) -> None:
        """Test that calling discard twice is safe for async transactions."""
        client = async_client_with_schema

        txn = client.txn()
        await txn.mutate(set_obj={"name": "AsyncDoubleDiscard"})
        await txn.discard()
        await txn.discard()  # Should not raise
