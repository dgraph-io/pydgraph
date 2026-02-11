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

from typing import TYPE_CHECKING

import pydgraph
from pydgraph import DgraphClient, run_transaction
from pydgraph.proto import api_pb2 as api

from .helpers import generate_movie

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


# =============================================================================
# Query Benchmarks
# =============================================================================


class TestSyncQueryBenchmarks:
    """Benchmarks for sync query operations."""

    def test_benchmark_query_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark a simple read query."""
        client = sync_client_with_movies_schema

        # Setup: seed data outside benchmark
        txn = client.txn()
        txn.mutate(set_obj=generate_movie(0), commit_now=True)

        query = """query {
            people(func: has(name), first: 1) {
                name
                email
                tagline
            }
        }"""

        def run_query() -> api.Response:
            txn = client.txn(read_only=True)
            return txn.query(query)

        benchmark(run_query)

    def test_benchmark_query_with_vars_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark a query with variables."""
        client = sync_client_with_movies_schema

        # Setup: seed data
        txn = client.txn()
        txn.mutate(
            set_obj={"name": "BenchmarkUser", "email": "bench@test.com"},
            commit_now=True,
        )

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
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark a best-effort read query."""
        client = sync_client_with_movies_schema

        # Setup: seed data
        txn = client.txn()
        txn.mutate(set_obj=generate_movie(0), commit_now=True)

        query = "{ people(func: has(name), first: 1) { name } }"

        def run_query() -> api.Response:
            txn = client.txn(read_only=True, best_effort=True)
            return txn.query(query)

        benchmark(run_query)


# =============================================================================
# Mutation Benchmarks
# =============================================================================


class TestSyncMutationBenchmarks:
    """Benchmarks for sync mutation operations."""

    def test_benchmark_mutation_commit_now_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark mutation with commit_now (single round-trip)."""
        client = sync_client_with_movies_schema
        counter = [0]

        def run_mutation() -> api.Response:
            counter[0] += 1
            txn = client.txn()
            return txn.mutate(set_obj=generate_movie(counter[0]), commit_now=True)

        benchmark(run_mutation)

    def test_benchmark_mutation_explicit_commit_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark mutation with explicit commit (two round-trips)."""
        client = sync_client_with_movies_schema
        counter = [0]

        def run_mutation() -> api.TxnContext | None:
            counter[0] += 1
            txn = client.txn()
            txn.mutate(set_obj=generate_movie(counter[0]))
            return txn.commit()

        benchmark(run_mutation)

    def test_benchmark_discard_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark mutation followed by discard (rollback cost)."""
        client = sync_client_with_movies_schema
        counter = [0]

        def run_mutation() -> None:
            counter[0] += 1
            txn = client.txn()
            txn.mutate(set_obj=generate_movie(counter[0]))
            txn.discard()

        benchmark(run_mutation)

    def test_benchmark_mutation_nquads_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark N-Quads mutation format."""
        client = sync_client_with_movies_schema
        counter = [0]

        def run_mutation() -> api.Response:
            counter[0] += 1
            txn = client.txn()
            nquads = f"""
                _:person <name> "Movie_{counter[0]}" .
                _:person <email> "movie{counter[0]}@test.com" .
                _:person <tagline> "A test movie number {counter[0]}" .
            """
            return txn.mutate(set_nquads=nquads, commit_now=True)

        benchmark(run_mutation)

    def test_benchmark_delete_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark delete mutation."""
        client = sync_client_with_movies_schema

        # Pre-create nodes to delete
        uids: list[str] = []
        for i in range(100):
            txn = client.txn()
            resp = txn.mutate(set_obj=generate_movie(i), commit_now=True)
            uids.append(next(iter(resp.uids.values())))

        uid_index = [0]

        def run_delete() -> api.Response:
            idx = uid_index[0] % len(uids)
            uid_index[0] += 1
            txn = client.txn()
            return txn.mutate(del_obj={"uid": uids[idx]}, commit_now=True)

        benchmark(run_delete)


# =============================================================================
# Transaction Benchmarks
# =============================================================================


class TestSyncTransactionBenchmarks:
    """Benchmarks for advanced sync transaction operations."""

    def test_benchmark_upsert_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark upsert operation (query + conditional mutation)."""
        client = sync_client_with_movies_schema
        counter = [0]

        def run_upsert() -> api.Response:
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
            return txn.do_request(request)

        benchmark(run_upsert)

    def test_benchmark_batch_mutations_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark multiple mutations in one transaction."""
        client = sync_client_with_movies_schema
        counter = [0]
        batch_size = 10

        def run_batch() -> api.TxnContext | None:
            txn = client.txn()
            for _ in range(batch_size):
                counter[0] += 1
                txn.mutate(set_obj=generate_movie(counter[0]))
            return txn.commit()

        benchmark(run_batch)

    def test_benchmark_run_transaction_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark run_transaction helper overhead."""
        client = sync_client_with_movies_schema
        counter = [0]

        def txn_func(txn: pydgraph.Txn) -> str:
            counter[0] += 1
            response = txn.mutate(set_obj=generate_movie(counter[0]), commit_now=True)
            return next(iter(response.uids.values()), "")

        def run_with_helper() -> str:
            return run_transaction(client, txn_func)

        benchmark(run_with_helper)


# =============================================================================
# Client Benchmarks
# =============================================================================


class TestSyncClientBenchmarks:
    """Benchmarks for sync client-level operations."""

    def test_benchmark_check_version_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark check_version (health check)."""
        client = sync_client_with_movies_schema

        def run_check() -> str:
            return client.check_version()

        benchmark(run_check)

    def test_benchmark_alter_schema_sync(
        self,
        sync_client_with_movies_schema: DgraphClient,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmark schema alter operation."""
        client = sync_client_with_movies_schema
        counter = [0]

        def run_alter() -> api.Payload:
            counter[0] += 1
            # Add a new predicate each time to avoid conflicts
            schema = f"benchmark_pred_{counter[0]}: string @index(exact) ."
            return client.alter(pydgraph.Operation(schema=schema))

        benchmark(run_alter)
