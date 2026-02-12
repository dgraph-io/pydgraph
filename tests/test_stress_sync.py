# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Sync client stress tests.

These tests stress test the synchronous pydgraph client by running concurrent
queries and mutations using ThreadPoolExecutor.

Usage:
    # Quick mode (default, CI-friendly)
    pytest tests/test_stress_sync.py -v

    # Full mode (thorough stress testing)
    STRESS_TEST_MODE=full pytest tests/test_stress_sync.py -v
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, wait
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture

import pydgraph
from pydgraph import DgraphClient, errors, retry, run_transaction
from pydgraph.proto import api_pb2 as api

from .helpers import generate_movie

# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def stress_client(
    _sync_client_clean: DgraphClient, movies_schema_content: str
) -> DgraphClient:
    """Sync client with movies test schema for stress tests."""
    _sync_client_clean.alter(pydgraph.Operation(schema=movies_schema_content))
    return _sync_client_clean

# =============================================================================
# Sync Client Stress Tests
# =============================================================================

@pytest.mark.usefixtures("movies_data_loaded")
class TestSyncClientStress:
    """Stress tests for synchronous Dgraph client."""

    def test_concurrent_read_queries_sync(
        self,
        stress_client: DgraphClient,
        executor: ThreadPoolExecutor,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test many concurrent read-only queries don't cause issues."""
        client = stress_client
        num_ops = stress_config["ops"]

        # Insert some test data first (outside benchmark)
        txn = client.txn()
        for i in range(100):
            txn.mutate(set_obj=generate_movie(i))
        txn.commit()

        query = """query {
            people(func: has(name), first: 10) {
                name
                email
                tagline
            }
        }"""

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
            # Clear state at start of each benchmark iteration
            results.clear()
            exc_list.clear()
            futures = [executor.submit(run_query) for _ in range(num_ops)]
            wait(futures)
            return len(results)

        result_count = benchmark(run_all_queries)

        assert result_count == num_ops

    def test_concurrent_mutations_sync(
        self,
        stress_client: DgraphClient,
        executor: ThreadPoolExecutor,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test concurrent mutations in separate transactions."""
        client = stress_client
        num_ops = stress_config["workers"] * 10

        success_count = 0
        exc_list: list[Exception] = []

        def run_mutation(index: int) -> None:
            nonlocal success_count
            try:
                txn = client.txn()
                txn.mutate(set_obj=generate_movie(index), commit_now=True)
                success_count += 1
            except errors.AbortedError:
                pass  # Expected conflict
            except Exception as e:
                exc_list.append(e)

        def run_all_mutations() -> int:
            nonlocal success_count
            # Clear state at start of each benchmark iteration
            success_count = 0
            exc_list.clear()
            futures = [executor.submit(run_mutation, i) for i in range(num_ops)]
            wait(futures)
            return success_count

        result_count = benchmark(run_all_mutations)

        # Some AbortedErrors are expected
        assert result_count > num_ops * 0.5

    def test_mixed_workload_sync(
        self,
        stress_client: DgraphClient,
        executor: ThreadPoolExecutor,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test mix of queries, mutations, commits, and discards concurrently."""
        client = stress_client
        num_ops = stress_config["workers"] * 20

        # Setup: Seed some data once before benchmarking
        txn = client.txn()
        for i in range(50):
            txn.mutate(set_obj=generate_movie(i))
        txn.commit()

        results: list[str] = []
        exc_list: list[Exception] = []

        def random_operation(op_id: int) -> None:
            op_type = op_id % 4
            try:
                if op_type == 0:
                    # Read query
                    txn = client.txn(read_only=True)
                    txn.query("{ q(func: has(name), first: 5) { name } }")
                    results.append("query")
                elif op_type == 1:
                    # Mutation with commit_now
                    txn = client.txn()
                    txn.mutate(set_obj=generate_movie(op_id), commit_now=True)
                    results.append("mutation")
                elif op_type == 2:
                    # Mutation with explicit commit
                    txn = client.txn()
                    txn.mutate(set_obj=generate_movie(op_id))
                    txn.commit()
                    results.append("commit")
                else:
                    # Mutation with discard
                    txn = client.txn()
                    txn.mutate(set_obj=generate_movie(op_id))
                    txn.discard()
                    results.append("discard")
            except errors.AbortedError:
                results.append("aborted")
            except Exception as e:
                exc_list.append(e)

        def run_all_operations() -> int:
            # Clear state at start of each benchmark iteration
            results.clear()
            exc_list.clear()
            futures = [executor.submit(random_operation, i) for i in range(num_ops)]
            wait(futures)
            return len(results)

        result_count = benchmark(run_all_operations)

        assert len(exc_list) == 0, f"Unexpected errors: {exc_list[:5]}"
        assert result_count == num_ops

@pytest.mark.usefixtures("movies_data_loaded")
class TestSyncTransactionStress:
    """Stress tests for sync transaction conflict handling."""

    def test_upsert_conflicts_sync(
        self,
        stress_client: DgraphClient,
        executor: ThreadPoolExecutor,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test concurrent upserts on the same key detect conflicts properly."""
        client = stress_client
        target_email = "conflict@test.com"
        num_workers = stress_config["workers"]

        aborted_count = 0
        success_count = 0
        exc_list: list[Exception] = []

        def run_upsert(worker_id: int) -> None:
            nonlocal aborted_count, success_count
            try:
                txn = client.txn()
                query = f'{{ u as var(func: eq(email, "{target_email}")) }}'
                mutation = pydgraph.Mutation(
                    set_nquads=f"""
                    uid(u) <email> "{target_email}" .
                    uid(u) <name> "Worker_{worker_id}" .
                    uid(u) <tagline> "Worker {worker_id} tagline" .
                    """.encode(),
                    cond="@if(eq(len(u), 0))",
                )
                request = api.Request(
                    query=query,
                    mutations=[mutation],
                    commit_now=True,
                )
                txn.do_request(request)
                success_count += 1
            except errors.AbortedError:
                aborted_count += 1
            except Exception as e:
                exc_list.append(e)

        def run_all_upserts() -> int:
            nonlocal aborted_count, success_count
            # Clear state at start of each benchmark iteration
            aborted_count = 0
            success_count = 0
            exc_list.clear()
            futures = [executor.submit(run_upsert, i) for i in range(num_workers)]
            wait(futures)
            return success_count

        result_count = benchmark(run_all_upserts)

        assert result_count >= 1, "No upserts succeeded"

    def test_transaction_isolation_sync(  # noqa: C901
        self,
        stress_client: DgraphClient,
        stress_config: dict[str, Any],
    ) -> None:
        """Test that transactions provide proper isolation."""
        client = stress_client
        workers = min(stress_config["workers"], 20)

        # Insert initial data with a counter stored in tagline
        txn = client.txn()
        response = txn.mutate(
            set_obj={"name": "IsolationTest", "tagline": "counter:100"}, commit_now=True
        )
        uid = next(iter(response.uids.values()))

        results: list[int] = []
        exc_list: list[Exception] = []

        def read_counter() -> None:
            try:
                txn = client.txn(read_only=True)
                query = f'{{ node(func: uid("{uid}")) {{ tagline }} }}'
                response = txn.query(query)
                data = json.loads(response.json)
                if data.get("node"):
                    tagline = data["node"][0]["tagline"]
                    counter = int(tagline.split(":")[1])
                    results.append(counter)
            except Exception as e:
                exc_list.append(e)

        def update_counter(delta: int) -> None:
            try:
                txn = client.txn()
                query = f'{{ node(func: uid("{uid}")) {{ tagline }} }}'
                response = txn.query(query)
                data = json.loads(response.json)
                if data.get("node"):
                    tagline = data["node"][0]["tagline"]
                    current = int(tagline.split(":")[1])
                    txn.mutate(
                        set_obj={"uid": uid, "tagline": f"counter:{current + delta}"},
                        commit_now=True,
                    )
            except errors.AbortedError:
                pass  # Expected
            except Exception as e:
                exc_list.append(e)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for i in range(100):
                if i % 3 == 0:
                    futures.append(executor.submit(update_counter, 1))
                else:
                    futures.append(executor.submit(read_counter))
            wait(futures)

        assert len(exc_list) == 0, f"Unexpected errors: {exc_list}"
        for counter in results:
            assert isinstance(counter, int)
            assert counter >= 100

@pytest.mark.usefixtures("movies_data_loaded")
class TestSyncRetryStress:
    """Stress tests for sync retry utilities."""

    def test_retry_under_conflicts_sync(
        self,
        stress_client: DgraphClient,
        executor: ThreadPoolExecutor,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test retry() generator handles conflicts correctly under load."""
        num_workers = min(stress_config["workers"], 10)

        total_successes = 0
        all_errors: list[str] = []

        def retry_work() -> None:
            nonlocal total_successes
            for attempt in retry():
                with attempt:
                    txn = stress_client.txn()
                    txn.mutate(
                        set_obj=generate_movie(total_successes),
                        commit_now=True,
                    )
                    total_successes += 1

        def run_all_retry_work() -> int:
            nonlocal total_successes
            # Clear state at start of each benchmark iteration
            total_successes = 0
            all_errors.clear()
            futures = [executor.submit(retry_work) for _ in range(num_workers)]
            wait(futures)
            # Check for exceptions
            for f in futures:
                try:
                    f.result()
                except Exception as e:
                    all_errors.append(str(e))
            return total_successes

        result_count = benchmark(run_all_retry_work)

        assert result_count >= num_workers

    def test_run_transaction_sync(
        self,
        stress_client: DgraphClient,
        executor: ThreadPoolExecutor,
        stress_config: dict[str, Any],
        benchmark: BenchmarkFixture,
    ) -> None:
        """Test run_transaction() helper handles conflicts correctly."""
        num_workers = min(stress_config["workers"], 10)

        results: list[str] = []
        exc_list: list[Exception] = []

        def work(worker_id: int) -> None:
            try:

                def txn_func(txn: pydgraph.Txn) -> str:
                    response = txn.mutate(
                        set_obj={
                            "name": f"RunTxn_{worker_id}",
                            "tagline": f"Worker {worker_id} transaction",
                        },
                        commit_now=True,
                    )
                    return next(iter(response.uids.values()), "")

                uid = run_transaction(stress_client, txn_func)
                results.append(uid)
            except Exception as e:
                exc_list.append(e)

        def run_all_transactions() -> int:
            # Clear state at start of each benchmark iteration
            results.clear()
            exc_list.clear()
            futures = [executor.submit(work, i) for i in range(num_workers)]
            wait(futures)
            return len(results)

        result_count = benchmark(run_all_transactions)

        assert result_count == num_workers

@pytest.mark.usefixtures("movies_data_loaded")
class TestSyncDeadlockPrevention:
    """Tests for deadlock prevention in sync client."""

    def test_no_deadlock_on_error_sync(
        self,
        stress_client: DgraphClient,
        stress_config: dict[str, Any],
    ) -> None:
        """Test that errors don't cause deadlocks."""
        client = stress_client
        workers = min(stress_config["workers"], 20)

        def cause_error() -> None:
            txn = client.txn()
            try:
                txn.query("{ invalid syntax")
            except Exception:
                pass

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(cause_error) for _ in range(100)]
            _, not_done = wait(futures, timeout=30)  # Ignore done set
            assert len(not_done) == 0, "Possible deadlock detected"
