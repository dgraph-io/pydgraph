# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Sync client stress tests.

These tests stress test the synchronous pydgraph client by running concurrent
queries and mutations using both ThreadPoolExecutor and ProcessPoolExecutor.
Each test runs twice - once with threads and once with processes - to catch
different classes of bugs (race conditions vs pickling issues).

Usage:
    # Quick mode (default, CI-friendly)
    pytest tests/test_stress_sync.py -v

    # Full mode (thorough stress testing)
    STRESS_TEST_MODE=full pytest tests/test_stress_sync.py -v
"""

from __future__ import annotations

import json
import time
from concurrent.futures import Executor, ThreadPoolExecutor, wait
from typing import Any

import pytest

import pydgraph
from pydgraph import DgraphClient, DgraphClientStub, errors, retry, run_transaction
from pydgraph.proto import api_pb2 as api

from .helpers import SYNTHETIC_SCHEMA, TEST_SERVER_ADDR, generate_person

# =============================================================================
# Module-level worker functions for ProcessPoolExecutor compatibility
# =============================================================================
# ProcessPoolExecutor requires picklable functions, so these must be defined
# at module level rather than inside test methods.


def _create_client() -> DgraphClient:
    """Create a new client connection for worker processes."""
    client_stub = DgraphClientStub(TEST_SERVER_ADDR)
    client = DgraphClient(client_stub)
    # Retry login
    for _ in range(30):
        try:
            client.login("groot", "password")
            break
        except Exception as e:
            if "user not found" in str(e):
                raise
            time.sleep(0.1)
    return client


def _worker_query(query: str) -> api.Response | Exception:
    """Worker function for running queries in separate processes."""
    try:
        client = _create_client()
        try:
            txn = client.txn(read_only=True)
            return txn.query(query)
        finally:
            client.close()
    except Exception as e:
        return e


def _worker_mutation(index: int) -> tuple[bool, str]:
    """Worker function for running mutations in separate processes."""
    try:
        client = _create_client()
        try:
            txn = client.txn()
            txn.mutate(set_obj=generate_person(index), commit_now=True)
            return (True, "")
        except errors.AbortedError:
            return (False, "aborted")
        except Exception as e:
            return (False, str(e))
        finally:
            client.close()
    except Exception as e:
        return (False, str(e))


def _worker_upsert(worker_id: int, target_email: str) -> tuple[str, str]:
    """Worker function for upsert operations in separate processes."""
    try:
        client = _create_client()
        try:
            # Set schema first (needed for fresh process connections)
            client.alter(pydgraph.Operation(schema=SYNTHETIC_SCHEMA))
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
            return ("success", "")
        except errors.AbortedError:
            return ("aborted", "")
        except errors.RetriableError:
            return ("retriable", "")
        except Exception as e:
            return ("error", str(e))
        finally:
            client.close()
    except Exception as e:
        return ("error", str(e))


def _worker_retry_mutation(iterations: int) -> tuple[int, int, list[str]]:
    """Worker function for testing retry under conflicts."""
    successes = 0
    aborts = 0
    exc_list: list[str] = []
    try:
        client = _create_client()
        try:
            for attempt in retry():
                with attempt:
                    txn = client.txn()
                    txn.mutate(
                        set_obj={"name": f"RetryTest_{iterations}", "balance": 100.0},
                        commit_now=True,
                    )
                    successes += 1
            # Only do a few iterations per worker
            for _ in range(min(iterations, 5)):
                for attempt in retry():
                    with attempt:
                        txn = client.txn()
                        txn.mutate(
                            set_obj=generate_person(iterations),
                            commit_now=True,
                        )
                        successes += 1
        except errors.AbortedError:
            aborts += 1
        except Exception as e:
            exc_list.append(str(e))
        finally:
            client.close()
    except Exception as e:
        exc_list.append(str(e))
    return (successes, aborts, exc_list)


def _worker_run_transaction(worker_id: int) -> tuple[str, str]:
    """Worker function for testing run_transaction helper."""
    try:
        client = _create_client()
        try:

            def txn_func(txn: pydgraph.Txn) -> str:
                response = txn.mutate(
                    set_obj={"name": f"RunTxn_{worker_id}", "balance": float(worker_id)},
                    commit_now=True,
                )
                return next(iter(response.uids.values()), "")

            uid = run_transaction(client, txn_func)
            return ("success", uid)
        except errors.AbortedError:
            return ("aborted", "")
        except Exception as e:
            return ("error", str(e))
        finally:
            client.close()
    except Exception as e:
        return ("error", str(e))


# =============================================================================
# Sync Client Stress Tests
# =============================================================================


class TestSyncClientStress:
    """Stress tests for synchronous Dgraph client."""

    def test_concurrent_read_queries(
        self,
        sync_client_with_schema: DgraphClient,
        executor: Executor,
        executor_type: str,
        stress_config: dict[str, Any],
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
            # ThreadPoolExecutor can share the client
            results: list[api.Response] = []
            exc_list: list[Exception] = []

            def run_query() -> None:
                try:
                    txn = client.txn(read_only=True)
                    response = txn.query(query)
                    results.append(response)
                except Exception as e:
                    exc_list.append(e)

            futures = [executor.submit(run_query) for _ in range(num_ops)]
            wait(futures)

            assert len(exc_list) == 0, f"Got {len(exc_list)} errors: {exc_list[:5]}"
            assert len(results) == num_ops
        else:
            # ProcessPoolExecutor needs module-level function
            futures = [executor.submit(_worker_query, query) for _ in range(num_ops)]
            wait(futures)

            results_list = [f.result() for f in futures]
            exc_list = [r for r in results_list if isinstance(r, Exception)]
            assert len(exc_list) == 0, f"Got {len(exc_list)} errors: {exc_list[:5]}"

    def test_concurrent_mutations_separate_txns(
        self,
        sync_client_with_schema: DgraphClient,
        executor: Executor,
        executor_type: str,
        stress_config: dict[str, Any],
    ) -> None:
        """Test concurrent mutations in separate transactions."""
        client = sync_client_with_schema
        num_ops = stress_config["workers"] * 10

        if executor_type == "thread":
            success_count = 0
            exc_list: list[Exception] = []

            def run_mutation(index: int) -> None:
                nonlocal success_count
                try:
                    txn = client.txn()
                    txn.mutate(set_obj=generate_person(index), commit_now=True)
                    success_count += 1
                except errors.AbortedError:
                    pass  # Expected conflict
                except Exception as e:
                    exc_list.append(e)

            futures = [executor.submit(run_mutation, i) for i in range(num_ops)]
            wait(futures)

            # Some AbortedErrors are expected
            assert len(exc_list) == 0, f"Unexpected errors: {exc_list[:5]}"
            assert success_count > num_ops * 0.5
        else:
            # ProcessPoolExecutor
            futures = [executor.submit(_worker_mutation, i) for i in range(num_ops)]
            wait(futures)

            results = [f.result() for f in futures]
            successes = sum(1 for ok, _ in results if ok)
            errors_list = [msg for ok, msg in results if not ok and msg != "aborted"]

            assert len(errors_list) == 0, f"Unexpected errors: {errors_list[:5]}"
            assert successes > num_ops * 0.5


class TestSyncTransactionStress:
    """Stress tests for sync transaction conflict handling."""

    def test_concurrent_upsert_conflicts(
        self,
        sync_client_with_schema: DgraphClient,
        executor: Executor,
        executor_type: str,
        stress_config: dict[str, Any],
    ) -> None:
        """Test concurrent upserts on the same key detect conflicts properly."""
        client = sync_client_with_schema
        target_email = f"conflict_{executor_type}@test.com"
        num_workers = stress_config["workers"]

        if executor_type == "thread":
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
                    success_count += 1
                except errors.AbortedError:
                    aborted_count += 1
                except Exception as e:
                    exc_list.append(e)

            futures = [executor.submit(run_upsert, i) for i in range(num_workers)]
            wait(futures)

            assert len(exc_list) == 0, f"Unexpected errors: {exc_list}"
            assert success_count >= 1, "No upserts succeeded"
        else:
            # ProcessPoolExecutor
            futures = [
                executor.submit(_worker_upsert, i, target_email)
                for i in range(num_workers)
            ]
            wait(futures)

            results = [f.result() for f in futures]
            successes = sum(1 for status, _ in results if status == "success")
            errors_list = [msg for status, msg in results if status == "error"]

            assert len(errors_list) == 0, f"Unexpected errors: {errors_list}"
            assert successes >= 1, "No upserts succeeded"

    def test_transaction_isolation(
        self,
        sync_client_with_schema: DgraphClient,
        executor_type: str,
        stress_config: dict[str, Any],
    ) -> None:
        """Test that transactions provide proper isolation.

        Note: This test only runs with ThreadPoolExecutor since it requires
        shared state tracking across threads.
        """
        if executor_type == "process":
            pytest.skip("Transaction isolation test requires shared state (threads only)")

        client = sync_client_with_schema
        workers = min(stress_config["workers"], 20)

        # Insert initial data
        txn = client.txn()
        response = txn.mutate(
            set_obj={"name": "IsolationTest", "balance": 100.0}, commit_now=True
        )
        uid = next(iter(response.uids.values()))

        results: list[float] = []
        exc_list: list[Exception] = []

        def read_balance() -> None:
            try:
                txn = client.txn(read_only=True)
                query = f'{{ node(func: uid("{uid}")) {{ balance }} }}'
                response = txn.query(query)
                data = json.loads(response.json)
                if data.get("node"):
                    results.append(data["node"][0]["balance"])
            except Exception as e:
                exc_list.append(e)

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
                pass  # Expected
            except Exception as e:
                exc_list.append(e)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for i in range(100):
                if i % 3 == 0:
                    futures.append(executor.submit(update_balance, 1.0))
                else:
                    futures.append(executor.submit(read_balance))
            wait(futures)

        assert len(exc_list) == 0, f"Unexpected errors: {exc_list}"
        for balance in results:
            assert isinstance(balance, (int, float))
            assert balance >= 100.0


class TestSyncRetryStress:
    """Stress tests for sync retry utilities."""

    def test_retry_under_conflicts(
        self,
        sync_client_with_schema: DgraphClient,
        executor: Executor,
        executor_type: str,
        stress_config: dict[str, Any],
    ) -> None:
        """Test retry() generator handles conflicts correctly under load."""
        iterations = stress_config["iterations"]
        num_workers = min(stress_config["workers"], 10)

        if executor_type == "thread":
            total_successes = 0
            total_aborts = 0
            all_errors: list[str] = []

            def retry_work() -> None:
                nonlocal total_successes, total_aborts
                for attempt in retry():
                    with attempt:
                        txn = sync_client_with_schema.txn()
                        txn.mutate(
                            set_obj=generate_person(iterations),
                            commit_now=True,
                        )
                        total_successes += 1

            futures = [executor.submit(retry_work) for _ in range(num_workers)]
            wait(futures)

            # Check for exceptions
            for f in futures:
                try:
                    f.result()
                except Exception as e:
                    all_errors.append(str(e))

            assert len(all_errors) == 0, f"Errors: {all_errors[:5]}"
            assert total_successes >= num_workers
        else:
            # ProcessPoolExecutor
            futures = [
                executor.submit(_worker_retry_mutation, iterations)
                for _ in range(num_workers)
            ]
            wait(futures)

            total_successes = 0
            all_errors: list[str] = []
            for f in futures:
                successes, _, errs = f.result()  # Ignore aborts count
                total_successes += successes
                all_errors.extend(errs)

            assert len(all_errors) == 0, f"Errors: {all_errors[:5]}"
            assert total_successes > 0

    def test_run_transaction_conflicts(
        self,
        sync_client_with_schema: DgraphClient,
        executor: Executor,
        executor_type: str,
        stress_config: dict[str, Any],
    ) -> None:
        """Test run_transaction() helper handles conflicts correctly."""
        num_workers = min(stress_config["workers"], 10)

        if executor_type == "thread":
            results: list[str] = []
            exc_list: list[Exception] = []

            def work(worker_id: int) -> None:
                try:

                    def txn_func(txn: pydgraph.Txn) -> str:
                        response = txn.mutate(
                            set_obj={
                                "name": f"RunTxn_{worker_id}",
                                "balance": float(worker_id),
                            },
                            commit_now=True,
                        )
                        return next(iter(response.uids.values()), "")

                    uid = run_transaction(sync_client_with_schema, txn_func)
                    results.append(uid)
                except Exception as e:
                    exc_list.append(e)

            futures = [executor.submit(work, i) for i in range(num_workers)]
            wait(futures)

            assert len(exc_list) == 0, f"Errors: {exc_list[:5]}"
            assert len(results) == num_workers
        else:
            # ProcessPoolExecutor
            futures = [
                executor.submit(_worker_run_transaction, i) for i in range(num_workers)
            ]
            wait(futures)

            results_data = [f.result() for f in futures]
            successes = sum(1 for status, _ in results_data if status == "success")
            errors_list = [msg for status, msg in results_data if status == "error"]

            assert len(errors_list) == 0, f"Errors: {errors_list[:5]}"
            assert successes > 0


class TestSyncDeadlockPrevention:
    """Tests for deadlock prevention in sync client."""

    def test_no_deadlock_on_error(
        self,
        sync_client_with_schema: DgraphClient,
        executor_type: str,
        stress_config: dict[str, Any],
    ) -> None:
        """Test that errors don't cause deadlocks.

        Note: Only runs with ThreadPoolExecutor since it requires shared client.
        """
        if executor_type == "process":
            pytest.skip("Deadlock test requires shared client (threads only)")

        client = sync_client_with_schema
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
