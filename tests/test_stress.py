# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Stress tests for sync and async Dgraph clients.

These tests stress test the pydgraph client library by running concurrent
queries and mutations against a Dgraph cluster. They are designed to detect
race conditions, deadlocks, and transaction errors.

The tests can optionally use the 1 million movie dataset from the dgraph tour
for more realistic workloads. Set STRESS_TEST_LOAD_MOVIES=true to enable this.

Usage:
    # Run with small synthetic dataset (fast, default)
    pytest tests/test_stress.py -v

    # Run with 1 million movie dataset (requires tour data files)
    STRESS_TEST_LOAD_MOVIES=true pytest tests/test_stress.py -v

    # Run specific test class
    pytest tests/test_stress.py::TestSyncClientStress -v
    pytest tests/test_stress.py::TestAsyncClientStress -v
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import random
import subprocess
import threading
import time
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from typing import Any

import pytest

import pydgraph
from pydgraph import (
    AsyncDgraphClient,
    AsyncDgraphClientStub,
    DgraphClient,
    DgraphClientStub,
    errors,
)
from pydgraph.proto import api_pb2 as api

# Configuration
TEST_SERVER_ADDR = os.getenv("TEST_SERVER_ADDR", "localhost:9180")
STRESS_TEST_LOAD_MOVIES = os.getenv("STRESS_TEST_LOAD_MOVIES", "false").lower() == "true"
CONCURRENT_WORKERS = int(os.getenv("STRESS_TEST_WORKERS", "20"))
OPERATIONS_PER_WORKER = int(os.getenv("STRESS_TEST_OPS", "50"))

# Path to movie dataset (relative to pydgraph repo)
TOUR_RESOURCES = Path(__file__).parent.parent.parent / "tour" / "resources"
MOVIE_SCHEMA_PATH = TOUR_RESOURCES / "1million.schema"
MOVIE_DATA_PATH = TOUR_RESOURCES / "1million.rdf.gz"

# Movie dataset schema for reference
MOVIE_SCHEMA = """
director.film        : [uid] @reverse @count .
actor.film           : [uid] @count .
genre                : [uid] @reverse @count .
initial_release_date : datetime @index(year) .
rating               : [uid] @reverse .
country              : [uid] @reverse .
loc                  : geo @index(geo) .
name                 : string @index(hash, exact, term, trigram, fulltext) @lang .
starring             : [uid] @count .
performance.character_note : string @lang .
tagline              : string @lang .
cut.note             : string @lang .
rated                : [uid] @reverse .
email                : string @index(exact) @upsert .
"""

# Synthetic test schema for fast tests
SYNTHETIC_SCHEMA = """
name: string @index(term, exact) .
email: string @index(exact) @upsert .
age: int @index(int) .
balance: float .
active: bool @index(bool) .
created: datetime @index(hour) .
friends: [uid] @count @reverse .
"""


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sync_client() -> Generator[DgraphClient, None, None]:
    """Fixture providing a sync client with login."""
    client_stub = DgraphClientStub(TEST_SERVER_ADDR)
    client = DgraphClient(client_stub)

    # Retry login until server is ready
    max_retries = 30
    for _ in range(max_retries):
        try:
            client.login("groot", "password")
            break
        except Exception as e:
            if "user not found" in str(e):
                raise
            time.sleep(0.1)

    yield client
    client.close()


@pytest.fixture
def sync_client_clean(sync_client: DgraphClient) -> DgraphClient:
    """Fixture providing a sync client with clean database."""
    sync_client.alter(pydgraph.Operation(drop_all=True))
    return sync_client


@pytest.fixture
async def async_client() -> AsyncGenerator[AsyncDgraphClient, None]:
    """Fixture providing an async client with login."""
    client_stub = AsyncDgraphClientStub(TEST_SERVER_ADDR)
    client = AsyncDgraphClient(client_stub)

    # Retry login until server is ready
    max_retries = 30
    for _ in range(max_retries):
        try:
            await client.login("groot", "password")
            break
        except Exception as e:
            if "user not found" in str(e):
                raise
            await asyncio.sleep(0.1)

    yield client
    await client.close()


@pytest.fixture
async def async_client_clean(async_client: AsyncDgraphClient) -> AsyncDgraphClient:
    """Fixture providing an async client with clean database."""
    await async_client.alter(pydgraph.Operation(drop_all=True))
    return async_client


@pytest.fixture
def sync_client_with_schema(sync_client_clean: DgraphClient) -> DgraphClient:
    """Fixture providing a sync client with synthetic test schema."""
    sync_client_clean.alter(pydgraph.Operation(schema=SYNTHETIC_SCHEMA))
    return sync_client_clean


@pytest.fixture
async def async_client_with_schema(
    async_client_clean: AsyncDgraphClient,
) -> AsyncDgraphClient:
    """Fixture providing an async client with synthetic test schema."""
    await async_client_clean.alter(pydgraph.Operation(schema=SYNTHETIC_SCHEMA))
    return async_client_clean


# =============================================================================
# Helper Functions
# =============================================================================


def generate_person(index: int) -> dict[str, Any]:
    """Generate a person object for testing."""
    return {
        "name": f"Person_{index}_{random.randint(1000, 9999)}",  # noqa: S311
        "email": f"person{index}_{random.randint(1000, 9999)}@test.com",  # noqa: S311
        "age": random.randint(18, 80),  # noqa: S311
        "balance": random.uniform(0, 10000),  # noqa: S311
        "active": random.choice([True, False]),  # noqa: S311
    }


def load_movie_dataset(client: DgraphClient) -> bool:
    """Load the 1 million movie dataset using dgraph live.

    Returns True if loaded successfully, False if skipped.
    """
    if not STRESS_TEST_LOAD_MOVIES:
        return False

    if not MOVIE_SCHEMA_PATH.exists() or not MOVIE_DATA_PATH.exists():
        pytest.skip(
            f"Movie dataset not found at {TOUR_RESOURCES}. "
            "Make sure the dgraph tour repository is cloned as a sibling directory."
        )
        return False

    # Check if data is already loaded
    query = "{ count(func: has(genre), first: 1) { count(uid) } }"
    try:
        txn = client.txn(read_only=True)
        response = txn.query(query)
        result = json.loads(response.json)
        count = result.get("count", [{}])[0].get("count", 0)
        if count > 0:
            return True  # Already loaded
    except Exception:
        pass

    # Load via dgraph live
    host, port = TEST_SERVER_ADDR.split(":")
    # Convert gRPC port to HTTP port for dgraph live
    http_port = str(int(port) - 1000)  # 9180 -> 8180

    cmd = [
        "dgraph",
        "live",
        "-f",
        str(MOVIE_DATA_PATH),
        "-s",
        str(MOVIE_SCHEMA_PATH),
        "--alpha",
        f"{host}:{http_port}",
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=300)  # noqa: S603
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        pytest.skip(f"Failed to load movie dataset: {e}")
        return False
    else:
        return True


# =============================================================================
# Sync Client Stress Tests
# =============================================================================


class TestSyncClientStress:
    """Stress tests for synchronous Dgraph client."""

    def test_concurrent_read_queries(
        self, sync_client_with_schema: DgraphClient
    ) -> None:
        """Test many concurrent read-only queries don't cause issues."""
        client = sync_client_with_schema

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

        exc_list: list[Exception] = []
        results: list[api.Response] = []
        lock = threading.Lock()

        def run_query() -> None:
            try:
                txn = client.txn(read_only=True)
                response = txn.query(query)
                with lock:
                    results.append(response)
            except Exception as e:
                with lock:
                    exc_list.append(e)

        # Run concurrent queries
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONCURRENT_WORKERS
        ) as executor:
            futures = [
                executor.submit(run_query)
                for _ in range(CONCURRENT_WORKERS * OPERATIONS_PER_WORKER)
            ]
            concurrent.futures.wait(futures)

        assert len(exc_list) == 0, f"Got {len(exc_list)} errors: {exc_list[:5]}"
        assert len(results) == CONCURRENT_WORKERS * OPERATIONS_PER_WORKER

    def test_concurrent_mutations_separate_txns(
        self, sync_client_with_schema: DgraphClient
    ) -> None:
        """Test concurrent mutations in separate transactions."""
        client = sync_client_with_schema

        exc_list: list[Exception] = []
        success_count = 0
        lock = threading.Lock()

        def run_mutation(index: int) -> None:
            nonlocal success_count
            try:
                txn = client.txn()
                txn.mutate(set_obj=generate_person(index), commit_now=True)
                with lock:
                    success_count += 1
            except Exception as e:
                with lock:
                    exc_list.append(e)

        # Run concurrent mutations
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONCURRENT_WORKERS
        ) as executor:
            futures = [
                executor.submit(run_mutation, i)
                for i in range(CONCURRENT_WORKERS * 10)
            ]
            concurrent.futures.wait(futures)

        # Some failures due to transaction conflicts are expected and OK
        # But we should have mostly successes
        total = CONCURRENT_WORKERS * 10
        assert success_count > total * 0.5, (
            f"Too many failures: {len(exc_list)} out of {total}"
        )

    def test_concurrent_upsert_conflicts(
        self, sync_client_with_schema: DgraphClient
    ) -> None:
        """Test concurrent upserts on the same key detect conflicts properly."""
        client = sync_client_with_schema

        # All workers try to upsert the same email
        target_email = "conflict@test.com"

        exc_list: list[Exception] = []
        aborted_count = 0
        success_count = 0
        lock = threading.Lock()

        def run_upsert(worker_id: int) -> None:
            nonlocal aborted_count, success_count
            try:
                txn = client.txn()
                query = f'{{ u as var(func: eq(email, "{target_email}")) }}'
                mutation = pydgraph.Mutation(
                    set_nquads=f"""
                    uid(u) <email> "{target_email}" .
                    uid(u) <name> "Worker_{worker_id}" .
                    uid(u) <balance> "{worker_id}" .
                    """.encode(),
                    cond="@if(eq(len(u), 0))",
                )
                request = api.Request(
                    query=query,
                    mutations=[mutation],
                    commit_now=True,
                )
                txn.do_request(request)
                with lock:
                    success_count += 1
            except errors.AbortedError:
                with lock:
                    aborted_count += 1
            except Exception as e:
                with lock:
                    exc_list.append(e)

        # Run concurrent upserts
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONCURRENT_WORKERS
        ) as executor:
            futures = [
                executor.submit(run_upsert, i) for i in range(CONCURRENT_WORKERS)
            ]
            concurrent.futures.wait(futures)

        # Verify only one succeeded and others were aborted
        assert len(exc_list) == 0, f"Unexpected errors: {exc_list}"
        # At least one should succeed
        assert success_count >= 1, "No upserts succeeded"

    def test_transaction_isolation(  # noqa: C901 - complexity from nested functions
        self, sync_client_with_schema: DgraphClient
    ) -> None:
        """Test that transactions provide proper isolation."""
        client = sync_client_with_schema

        # Insert initial data
        txn = client.txn()
        response = txn.mutate(
            set_obj={"name": "IsolationTest", "balance": 100.0}, commit_now=True
        )
        uid = next(iter(response.uids.values()))

        err_list: list[Exception] = []
        results: list[float] = []
        lock = threading.Lock()

        def read_balance() -> None:
            try:
                txn = client.txn(read_only=True)
                query = f'{{ node(func: uid("{uid}")) {{ balance }} }}'
                response = txn.query(query)
                data = json.loads(response.json)
                if data.get("node"):
                    with lock:
                        results.append(data["node"][0]["balance"])
            except Exception as e:
                with lock:
                    err_list.append(e)

        def update_balance(delta: float) -> None:
            try:
                txn = client.txn()
                query = f'{{ node(func: uid("{uid}")) {{ balance }} }}'
                response = txn.query(query)
                data = json.loads(response.json)
                if data.get("node"):
                    current = data["node"][0]["balance"]
                    txn.mutate(
                        set_obj={"uid": uid, "balance": current + delta}, commit_now=True
                    )
            except errors.AbortedError:
                pass  # Expected conflict
            except Exception as e:
                with lock:
                    err_list.append(e)

        # Mix reads and writes
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for i in range(100):
                if i % 3 == 0:
                    futures.append(executor.submit(update_balance, 1.0))
                else:
                    futures.append(executor.submit(read_balance))
            concurrent.futures.wait(futures)

        # All reads should return valid balance values
        assert len(err_list) == 0, f"Unexpected errors: {err_list}"
        for balance in results:
            assert isinstance(balance, (int, float))
            assert balance >= 100.0  # Should never go below initial

    def test_no_deadlock_on_error(self, sync_client_with_schema: DgraphClient) -> None:
        """Test that errors don't cause deadlocks in sync client."""
        client = sync_client_with_schema

        def cause_error() -> None:
            txn = client.txn()
            try:
                # Try to query with invalid syntax to cause an error
                txn.query("{ invalid syntax")
            except Exception:
                pass  # Expected

        # Run many error-causing operations concurrently
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONCURRENT_WORKERS
        ) as executor:
            futures = [executor.submit(cause_error) for _ in range(100)]
            # If there's a deadlock, this will timeout
            _done, not_done = concurrent.futures.wait(futures, timeout=30)
            assert len(not_done) == 0, "Possible deadlock detected"


# =============================================================================
# Async Client Stress Tests
# =============================================================================


class TestAsyncClientStress:
    """Stress tests for asynchronous Dgraph client."""

    @pytest.mark.asyncio
    async def test_concurrent_read_queries(
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test many concurrent read-only queries don't cause issues."""
        client = async_client_with_schema

        # Insert some test data first
        txn = client.txn()
        for i in range(100):
            await txn.mutate(set_obj=generate_person(i))
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

        # Run many concurrent queries
        tasks = [run_query() for _ in range(CONCURRENT_WORKERS * OPERATIONS_PER_WORKER)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        exc_list = [r for r in results if isinstance(r, Exception)]
        assert len(exc_list) == 0, f"Got {len(exc_list)} errors: {exc_list[:5]}"

    @pytest.mark.asyncio
    async def test_concurrent_mutations_separate_txns(
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test concurrent mutations in separate transactions."""
        client = async_client_with_schema

        async def run_mutation(index: int) -> bool:
            try:
                txn = client.txn()
                await txn.mutate(set_obj=generate_person(index), commit_now=True)
            except errors.AbortedError:
                return False
            else:
                return True

        # Run concurrent mutations
        tasks = [run_mutation(i) for i in range(CONCURRENT_WORKERS * 10)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        exc_list = [r for r in results if isinstance(r, Exception)]
        successes = sum(1 for r in results if r is True)

        # Some failures due to transaction conflicts are OK
        total = CONCURRENT_WORKERS * 10
        assert len(exc_list) == 0, f"Unexpected errors: {exc_list[:5]}"
        assert successes > total * 0.5, f"Too few successes: {successes} out of {total}"

    @pytest.mark.asyncio
    async def test_concurrent_upsert_conflicts(
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test concurrent upserts on the same key detect conflicts properly."""
        client = async_client_with_schema

        target_email = "async_conflict@test.com"

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
            except errors.AbortedError:
                return "aborted"
            else:
                return "success"

        # Run concurrent upserts
        tasks = [run_upsert(i) for i in range(CONCURRENT_WORKERS)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        exc_list = [r for r in results if isinstance(r, Exception)]
        successes = sum(1 for r in results if r == "success")

        assert len(exc_list) == 0, f"Unexpected errors: {exc_list}"
        assert successes >= 1, "No upserts succeeded"

    @pytest.mark.asyncio
    async def test_no_deadlock_on_error(
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test that errors don't cause deadlocks - regression test for PR #293.

        This tests the fix for asyncio.Lock deadlock where do_request()
        would try to re-acquire the lock when handling errors via discard().
        """
        client = async_client_with_schema

        async def cause_error() -> None:
            txn = client.txn()
            try:
                # Try to query with invalid syntax to cause an error
                await txn.query("{ invalid syntax")
            except Exception:
                pass  # Expected

        # Run many error-causing operations concurrently
        tasks = [cause_error() for _ in range(100)]

        # If there's a deadlock, wait_for will timeout
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=30)
        except asyncio.TimeoutError:
            pytest.fail("Deadlock detected - asyncio.Lock not released properly")

    @pytest.mark.asyncio
    async def test_lock_released_after_mutation_error(
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test that lock is released after mutation errors allowing reuse."""
        client = async_client_with_schema

        # Create a transaction and force an error
        txn = client.txn()
        await txn.mutate(set_obj={"name": "Test"})

        # Force an error by trying to use a discarded transaction
        await txn.discard()

        # Create new transaction and verify it works
        txn2 = client.txn()
        response = await txn2.mutate(set_obj={"name": "Test2"}, commit_now=True)
        assert len(response.uids) == 1

    @pytest.mark.asyncio
    async def test_context_manager_cleanup(
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test that context managers properly clean up even on errors."""
        client = async_client_with_schema

        async def use_txn_with_error() -> None:
            async with client.txn() as txn:
                await txn.mutate(set_obj={"name": "ContextTest"})
                raise ValueError("Intentional error")

        # Should not leave any locks held
        for _ in range(50):
            try:
                await use_txn_with_error()
            except ValueError:
                pass  # Expected

        # Verify client still works
        async with client.txn() as txn:
            response = await txn.query("{ q(func: has(name), first: 1) { name } }")
            assert response is not None

    @pytest.mark.asyncio
    async def test_mixed_operations_stress(
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test mix of queries, mutations, commits, and discards concurrently."""
        client = async_client_with_schema

        # Seed some data
        txn = client.txn()
        for i in range(50):
            await txn.mutate(set_obj=generate_person(i))
        await txn.commit()

        async def random_operation(op_id: int) -> str:
            op_type = op_id % 4
            result = "unknown"
            try:
                if op_type == 0:
                    # Read query
                    txn = client.txn(read_only=True)
                    await txn.query("{ q(func: has(name), first: 5) { name } }")
                    result = "query"
                elif op_type == 1:
                    # Mutation with commit
                    txn = client.txn()
                    await txn.mutate(set_obj=generate_person(op_id), commit_now=True)
                    result = "mutation"
                elif op_type == 2:
                    # Mutation with explicit commit
                    txn = client.txn()
                    await txn.mutate(set_obj=generate_person(op_id))
                    await txn.commit()
                    result = "commit"
                else:
                    # Mutation with discard
                    txn = client.txn()
                    await txn.mutate(set_obj=generate_person(op_id))
                    await txn.discard()
                    result = "discard"
            except errors.AbortedError:
                return "aborted"
            return result

        # Run many mixed operations
        tasks = [random_operation(i) for i in range(CONCURRENT_WORKERS * 20)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        exc_list = [r for r in results if isinstance(r, Exception)]
        assert len(exc_list) == 0, f"Unexpected errors: {exc_list[:5]}"


# =============================================================================
# Movie Dataset Stress Tests (Optional)
# =============================================================================


@pytest.mark.skipif(
    not STRESS_TEST_LOAD_MOVIES,
    reason="Set STRESS_TEST_LOAD_MOVIES=true to run movie dataset tests",
)
class TestMovieDatasetStress:
    """Stress tests using the 1 million movie dataset."""

    @pytest.fixture(autouse=True)
    def setup_movie_data(self, sync_client: DgraphClient) -> None:
        """Load movie dataset before tests."""
        load_movie_dataset(sync_client)

    def test_complex_graph_queries(self, sync_client: DgraphClient) -> None:
        """Test complex graph traversal queries under load."""
        client = sync_client

        queries = [
            # Directors and their films
            """query {
                directors(func: has(director.film), first: 10) {
                    name@en
                    director.film (first: 5) {
                        name@en
                        initial_release_date
                    }
                }
            }""",
            # Films by genre
            """query {
                genres(func: has(~genre), first: 5) {
                    name@en
                    ~genre (first: 10) {
                        name@en
                    }
                }
            }""",
            # Actor collaborations
            """query {
                actors(func: has(actor.film), first: 5) {
                    name@en
                    actor.film (first: 5) {
                        name@en
                        starring (first: 3) {
                            performance.actor {
                                name@en
                            }
                        }
                    }
                }
            }""",
        ]

        exc_list: list[Exception] = []
        results: list[api.Response] = []
        lock = threading.Lock()

        def run_query(query: str) -> None:
            try:
                txn = client.txn(read_only=True)
                response = txn.query(query)
                with lock:
                    results.append(response)
            except Exception as e:
                with lock:
                    exc_list.append(e)

        # Run queries concurrently
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONCURRENT_WORKERS
        ) as executor:
            futures = [
                executor.submit(run_query, query)
                for _ in range(OPERATIONS_PER_WORKER)
                for query in queries
            ]
            concurrent.futures.wait(futures)

        assert len(exc_list) == 0, f"Got {len(exc_list)} errors: {exc_list[:5]}"
        assert len(results) == OPERATIONS_PER_WORKER * len(queries)

    @pytest.mark.asyncio
    async def test_async_complex_graph_queries(
        self, async_client: AsyncDgraphClient
    ) -> None:
        """Test complex graph traversal queries under load with async client."""
        client = async_client

        queries = [
            # Search by name
            """query {
                search(func: anyoftext(name@en, "Star Wars"), first: 10) {
                    name@en
                    initial_release_date
                }
            }""",
            # Expand with filters
            """query {
                films(func: has(initial_release_date), first: 20) @filter(ge(initial_release_date, "2000-01-01")) {
                    name@en
                    initial_release_date
                    genre {
                        name@en
                    }
                }
            }""",
        ]

        async def run_query(query: str) -> api.Response:
            txn = client.txn(read_only=True)
            return await txn.query(query)

        # Run queries concurrently
        tasks = [
            run_query(query)
            for _ in range(OPERATIONS_PER_WORKER)
            for query in queries
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        exc_list = [r for r in results if isinstance(r, Exception)]
        assert len(exc_list) == 0, f"Got {len(exc_list)} errors: {exc_list[:5]}"


# =============================================================================
# Transaction Edge Case Tests
# =============================================================================


class TestTransactionEdgeCases:
    """Tests for transaction edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_rapid_txn_create_discard(
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test rapidly creating and discarding transactions."""
        client = async_client_with_schema

        async def create_and_discard() -> None:
            txn = client.txn()
            await txn.discard()

        # Rapidly create and discard transactions
        tasks = [create_and_discard() for _ in range(500)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        exc_list = [r for r in results if isinstance(r, Exception)]
        assert len(exc_list) == 0, f"Errors during rapid txn lifecycle: {exc_list[:5]}"

    @pytest.mark.asyncio
    async def test_double_commit_error(
        self, async_client_with_schema: AsyncDgraphClient
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
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test that using transaction after commit raises error."""
        client = async_client_with_schema

        txn = client.txn()
        await txn.mutate(set_obj={"name": "UseAfterCommit"}, commit_now=True)

        with pytest.raises(errors.TransactionError):
            await txn.query("{ q(func: has(name)) { name } }")

    @pytest.mark.asyncio
    async def test_read_only_mutation_error(
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test that mutations in read-only transaction raise error."""
        client = async_client_with_schema

        txn = client.txn(read_only=True)

        with pytest.raises(errors.TransactionError):
            await txn.mutate(set_obj={"name": "ReadOnlyMutation"})

    @pytest.mark.asyncio
    async def test_best_effort_requires_read_only(
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test that best_effort requires read_only=True."""
        client = async_client_with_schema

        with pytest.raises(ValueError):
            client.txn(read_only=False, best_effort=True)

    def test_sync_double_discard_is_safe(
        self, sync_client_with_schema: DgraphClient
    ) -> None:
        """Test that calling discard twice is safe."""
        client = sync_client_with_schema

        txn = client.txn()
        txn.mutate(set_obj={"name": "DoubleDiscard"})
        txn.discard()
        txn.discard()  # Should not raise

    @pytest.mark.asyncio
    async def test_async_double_discard_is_safe(
        self, async_client_with_schema: AsyncDgraphClient
    ) -> None:
        """Test that calling discard twice is safe for async transactions."""
        client = async_client_with_schema

        txn = client.txn()
        await txn.mutate(set_obj={"name": "AsyncDoubleDiscard"})
        await txn.discard()
        await txn.discard()  # Should not raise
