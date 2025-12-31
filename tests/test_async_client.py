# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for async client."""

import asyncio
import json
import os
from collections.abc import AsyncGenerator

import pytest

import pydgraph
from pydgraph import AsyncDgraphClient, AsyncDgraphClientStub, async_open
from pydgraph.proto import api_pb2 as api

# Get test server address from environment
TEST_SERVER_ADDR = os.getenv("TEST_SERVER_ADDR", "localhost:9180")


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
                # User not found means auth is working but user doesn't exist yet
                # This shouldn't happen with groot, so treat as error
                raise
            # Server might not be ready, wait and retry
            await asyncio.sleep(0.1)

    yield client
    await client.close()


@pytest.fixture
async def async_client_clean(async_client: AsyncDgraphClient) -> AsyncDgraphClient:
    """Fixture providing an async client with clean database."""
    await async_client.alter(pydgraph.Operation(drop_all=True))
    return async_client


class TestAsyncClient:
    """Test suite for async client basic operations."""

    @pytest.mark.asyncio
    async def test_check_version(self, async_client: AsyncDgraphClient) -> None:
        """Test async version check."""
        version = await async_client.check_version()
        assert version is not None
        assert isinstance(version, str)

    @pytest.mark.asyncio
    async def test_alter_schema(self, async_client: AsyncDgraphClient) -> None:
        """Test async alter operation."""
        # Drop all first
        await async_client.alter(pydgraph.Operation(drop_all=True))

        # Set schema
        response = await async_client.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )
        assert response is not None

    @pytest.mark.asyncio
    async def test_mutation_and_query(self, async_client_clean: AsyncDgraphClient) -> None:
        """Test async mutation and query operations."""
        # Set schema
        await async_client_clean.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Mutation with commit_now
        txn = async_client_clean.txn()
        mutation = pydgraph.Mutation(commit_now=True)
        response = await txn.mutate(
            mutation=mutation, set_nquads='<_:alice> <name> "Alice" .'
        )
        assert len(response.uids) == 1

        # Query
        query = """query {
            me(func: anyofterms(name, "Alice")) {
                name
            }
        }"""

        txn = async_client_clean.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result["me"]) == 1
        assert result["me"][0]["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_mutation_with_json(self, async_client_clean: AsyncDgraphClient) -> None:
        """Test mutation with JSON object."""
        await async_client_clean.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Mutation with set_obj
        txn = async_client_clean.txn()
        response = await txn.mutate(set_obj={"name": "Bob"}, commit_now=True)
        assert len(response.uids) == 1

        # Get the UID
        uid = None
        for val in response.uids.values():
            uid = val
            break

        # Query by UID
        query = f"""{{
            me(func: uid("{uid}")) {{
                name
            }}
        }}"""

        txn = async_client_clean.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result["me"]) == 1
        assert result["me"][0]["name"] == "Bob"

    @pytest.mark.asyncio
    async def test_transaction_commit(self, async_client_clean: AsyncDgraphClient) -> None:
        """Test explicit transaction commit."""
        await async_client_clean.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Mutation without commit_now
        txn = async_client_clean.txn()
        response = await txn.mutate(set_obj={"name": "Charlie"})
        assert len(response.uids) == 1

        # Explicit commit
        await txn.commit()

        # Query to verify
        query = """query {
            me(func: anyofterms(name, "Charlie")) {
                name
            }
        }"""

        txn = async_client_clean.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result["me"]) == 1
        assert result["me"][0]["name"] == "Charlie"

    @pytest.mark.asyncio
    async def test_transaction_discard(self, async_client_clean: AsyncDgraphClient) -> None:
        """Test transaction discard."""
        await async_client_clean.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Mutation without commit
        txn = async_client_clean.txn()
        await txn.mutate(set_obj={"name": "David"})

        # Discard
        await txn.discard()

        # Query to verify data was not committed
        query = """query {
            me(func: anyofterms(name, "David")) {
                name
            }
        }"""

        txn = async_client_clean.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result.get("me", [])) == 0

    @pytest.mark.asyncio
    async def test_read_only_transaction(self, async_client_clean: AsyncDgraphClient) -> None:
        """Test read-only transactions."""
        await async_client_clean.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Add some data
        txn = async_client_clean.txn()
        await txn.mutate(set_obj={"name": "Eve"}, commit_now=True)

        # Read-only query
        query = """query {
            me(func: anyofterms(name, "Eve")) {
                name
            }
        }"""

        txn = async_client_clean.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result["me"]) == 1
        assert result["me"][0]["name"] == "Eve"

        # Try to mutate in read-only txn (should fail)
        with pytest.raises(pydgraph.errors.TransactionError):
            await txn.mutate(set_obj={"name": "Frank"})

    @pytest.mark.asyncio
    async def test_query_with_variables(self, async_client_clean: AsyncDgraphClient) -> None:
        """Test query with variables."""
        await async_client_clean.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Add data
        txn = async_client_clean.txn()
        await txn.mutate(set_obj={"name": "Grace"}, commit_now=True)

        # Query with variables
        query = """query search($name: string) {
            me(func: anyofterms(name, $name)) {
                name
            }
        }"""

        txn = async_client_clean.txn(read_only=True)
        response = await txn.query(query, variables={"$name": "Grace"})
        result = json.loads(response.json)
        assert len(result["me"]) == 1
        assert result["me"][0]["name"] == "Grace"


class TestAsyncContextManager:
    """Test suite for async context managers."""

    @pytest.mark.asyncio
    async def test_client_context_manager(self) -> None:
        """Test async client context manager."""
        async with await async_open(
            f"dgraph://groot:password@{TEST_SERVER_ADDR}"
        ) as client:
            version = await client.check_version()
            assert version is not None
        # Client should be closed automatically

    @pytest.mark.asyncio
    async def test_transaction_context_manager(self, async_client_clean: AsyncDgraphClient) -> None:
        """Test async transaction context manager."""
        await async_client_clean.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Use transaction as context manager
        async with async_client_clean.txn() as txn:
            await txn.mutate(set_obj={"name": "Henry"}, commit_now=True)
        # Transaction should be discarded automatically

        # Verify data was committed (commit_now=True)
        query = """query {
            me(func: anyofterms(name, "Henry")) {
                name
            }
        }"""

        async with async_client_clean.txn(read_only=True) as txn:
            response = await txn.query(query)
            result = json.loads(response.json)
            assert len(result["me"]) == 1
            assert result["me"][0]["name"] == "Henry"


class TestAsyncConcurrent:
    """Test suite for concurrent async operations."""

    @pytest.mark.asyncio
    async def test_concurrent_queries(self, async_client_clean: AsyncDgraphClient) -> None:
        """Test multiple concurrent queries."""
        await async_client_clean.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Add some data
        txn = async_client_clean.txn()
        await txn.mutate(set_obj={"name": "Concurrent Test"}, commit_now=True)

        # Run multiple queries concurrently
        query = """query {
            me(func: anyofterms(name, "Concurrent")) {
                name
            }
        }"""

        async def run_query() -> api.Response:
            txn = async_client_clean.txn(read_only=True)
            return await txn.query(query)

        # Run 10 queries concurrently
        tasks = [run_query() for _ in range(10)]
        results = await asyncio.gather(*tasks)

        # Verify all queries succeeded
        assert len(results) == 10
        for response in results:
            result = json.loads(response.json)
            assert len(result["me"]) == 1
            assert result["me"][0]["name"] == "Concurrent Test"

    @pytest.mark.asyncio
    async def test_concurrent_mutations(self, async_client_clean: AsyncDgraphClient) -> None:
        """Test multiple concurrent mutations."""
        await async_client_clean.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Run multiple mutations concurrently
        async def run_mutation(name: str) -> api.Response:
            txn = async_client_clean.txn()
            return await txn.mutate(set_obj={"name": name}, commit_now=True)

        # Run 5 mutations concurrently
        names = [f"Person{i}" for i in range(5)]
        tasks = [run_mutation(name) for name in names]
        results = await asyncio.gather(*tasks)

        # Verify all mutations succeeded
        assert len(results) == 5
        for response in results:
            assert len(response.uids) == 1

        # Verify all data was committed
        query = """query {
            me(func: has(name)) {
                name
            }
        }"""

        txn = async_client_clean.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result["me"]) == 5


class TestAsyncConnectionString:
    """Test suite for async_open connection string parsing."""

    @pytest.mark.asyncio
    async def test_simple_connection_string(self) -> None:
        """Test simple connection string without auth."""
        async with await async_open(f"dgraph://{TEST_SERVER_ADDR}") as client:
            version = await client.check_version()
            assert version is not None

    @pytest.mark.asyncio
    async def test_connection_string_with_auth(self) -> None:
        """Test connection string with username and password."""
        async with await async_open(
            f"dgraph://groot:password@{TEST_SERVER_ADDR}"
        ) as client:
            version = await client.check_version()
            assert version is not None

    @pytest.mark.asyncio
    async def test_invalid_connection_string(self) -> None:
        """Test invalid connection string raises error."""
        with pytest.raises(ValueError, match="scheme must be 'dgraph'"):
            await async_open("invalid://localhost:9080")

        with pytest.raises(ValueError, match="port required"):
            await async_open("dgraph://localhost")  # Missing port

        with pytest.raises(ValueError, match="password required when username is provided"):
            await async_open(
                "dgraph://groot@localhost:9080"
            )  # Username without password
