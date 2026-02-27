# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for async client.

Note: async_client fixture is defined in conftest.py
"""

import asyncio
import json
import os

import pytest

import pydgraph
from pydgraph import AsyncDgraphClient, async_open
from pydgraph.proto import api_pb2 as api

# Get test server address from environment (also defined in conftest.py)
TEST_SERVER_ADDR = os.getenv("TEST_SERVER_ADDR", "localhost:9180")


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
    async def test_mutation_and_query(self, async_client: AsyncDgraphClient) -> None:
        """Test async mutation and query operations."""
        # Set schema
        await async_client.alter(pydgraph.Operation(drop_all=True))
        await async_client.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Mutation with commit_now
        txn = async_client.txn()
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

        txn = async_client.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result["me"]) == 1
        assert result["me"][0]["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_mutation_with_json(self, async_client: AsyncDgraphClient) -> None:
        """Test mutation with JSON object."""
        await async_client.alter(pydgraph.Operation(drop_all=True))
        await async_client.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Mutation with set_obj
        txn = async_client.txn()
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

        txn = async_client.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result["me"]) == 1
        assert result["me"][0]["name"] == "Bob"

    @pytest.mark.asyncio
    async def test_transaction_commit(self, async_client: AsyncDgraphClient) -> None:
        """Test explicit transaction commit."""
        await async_client.alter(pydgraph.Operation(drop_all=True))
        await async_client.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Mutation without commit_now
        txn = async_client.txn()
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

        txn = async_client.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result["me"]) == 1
        assert result["me"][0]["name"] == "Charlie"

    @pytest.mark.asyncio
    async def test_transaction_discard(self, async_client: AsyncDgraphClient) -> None:
        """Test transaction discard."""
        await async_client.alter(pydgraph.Operation(drop_all=True))
        await async_client.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Mutation without commit
        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "David"})

        # Discard
        await txn.discard()

        # Query to verify data was not committed
        query = """query {
            me(func: anyofterms(name, "David")) {
                name
            }
        }"""

        txn = async_client.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result.get("me", [])) == 0

    @pytest.mark.asyncio
    async def test_read_only_transaction(self, async_client: AsyncDgraphClient) -> None:
        """Test read-only transactions."""
        await async_client.alter(pydgraph.Operation(drop_all=True))
        await async_client.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Add some data
        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "Eve"}, commit_now=True)

        # Read-only query
        query = """query {
            me(func: anyofterms(name, "Eve")) {
                name
            }
        }"""

        txn = async_client.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result["me"]) == 1
        assert result["me"][0]["name"] == "Eve"

        # Try to mutate in read-only txn (should fail)
        with pytest.raises(pydgraph.errors.TransactionError):
            await txn.mutate(set_obj={"name": "Frank"})

    @pytest.mark.asyncio
    async def test_query_with_variables(self, async_client: AsyncDgraphClient) -> None:
        """Test query with variables."""
        await async_client.alter(pydgraph.Operation(drop_all=True))
        await async_client.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Add data
        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "Grace"}, commit_now=True)

        # Query with variables
        query = """query search($name: string) {
            me(func: anyofterms(name, $name)) {
                name
            }
        }"""

        txn = async_client.txn(read_only=True)
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
    async def test_transaction_context_manager(self, async_client: AsyncDgraphClient) -> None:
        """Test async transaction context manager."""
        await async_client.alter(pydgraph.Operation(drop_all=True))
        await async_client.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Use transaction as context manager
        async with async_client.txn() as txn:
            await txn.mutate(set_obj={"name": "Henry"}, commit_now=True)
        # Transaction should be discarded automatically

        # Verify data was committed (commit_now=True)
        query = """query {
            me(func: anyofterms(name, "Henry")) {
                name
            }
        }"""

        async with async_client.txn(read_only=True) as txn:
            response = await txn.query(query)
            result = json.loads(response.json)
            assert len(result["me"]) == 1
            assert result["me"][0]["name"] == "Henry"


class TestAsyncConcurrent:
    """Test suite for concurrent async operations."""

    @pytest.mark.asyncio
    async def test_concurrent_queries(self, async_client: AsyncDgraphClient) -> None:
        """Test multiple concurrent queries."""
        await async_client.alter(pydgraph.Operation(drop_all=True))
        await async_client.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Add some data
        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "Concurrent Test"}, commit_now=True)

        # Run multiple queries concurrently
        query = """query {
            me(func: anyofterms(name, "Concurrent")) {
                name
            }
        }"""

        async def run_query() -> api.Response:
            txn = async_client.txn(read_only=True)
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
    async def test_concurrent_mutations(self, async_client: AsyncDgraphClient) -> None:
        """Test multiple concurrent mutations."""
        await async_client.alter(pydgraph.Operation(drop_all=True))
        await async_client.alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )

        # Run multiple mutations concurrently
        async def run_mutation(name: str) -> api.Response:
            txn = async_client.txn()
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

        txn = async_client.txn(read_only=True)
        response = await txn.query(query)
        result = json.loads(response.json)
        assert len(result["me"]) == 5


async def _skip_if_below_v25(client: AsyncDgraphClient) -> None:
    """Skip test if Dgraph version is below 25.0.0.

    The v25 API methods (run_dql, allocations, namespaces, convenience methods)
    require Dgraph v25.0.0 or above. This helper checks the server version and
    skips with a clear message if the server is too old.
    """
    try:
        version_str = await client.check_version()
        parts = version_str.lstrip("v").split(".")
        major = int(parts[0])
        if major < 25:
            pytest.skip(f"Dgraph v25+ required, got {version_str}")
    except Exception:
        pytest.skip("Could not determine Dgraph version")


async def _wait_for_namespace_deletion_async(
    client: AsyncDgraphClient,
    namespace_id: int,
    max_retries: int = 5,
    initial_delay: float = 0.1,
) -> None:
    """Wait for namespace deletion to propagate (eventual consistency).

    Args:
        client: Async Dgraph client
        namespace_id: The namespace ID to check for deletion
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds (doubles each retry)
    """
    delay = initial_delay
    for attempt in range(max_retries):
        namespaces = await client.list_namespaces()
        if namespace_id not in namespaces:
            return
        if attempt < max_retries - 1:
            await asyncio.sleep(delay)
            delay *= 2

    namespaces = await client.list_namespaces()
    assert namespace_id not in namespaces, (
        f"Namespace {namespace_id} still exists after {max_retries} retries"
    )


class TestAsyncConvenienceMethods:
    """Test suite for async convenience methods (drop_all, drop_data, etc.)."""

    @pytest.mark.asyncio
    async def test_drop_all(self, async_client: AsyncDgraphClient) -> None:
        """Test async drop_all drops all data and schema."""
        await _skip_if_below_v25(async_client)

        # Set schema and add data
        await async_client.drop_all()
        await async_client.set_schema("name: string @index(term) .")
        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "DropAllTest"}, commit_now=True)

        # Drop all
        response = await async_client.drop_all()
        assert response is not None

        # Verify data is gone by re-applying schema and querying
        await async_client.set_schema("name: string @index(term) .")
        txn = async_client.txn(read_only=True)
        result = await txn.query('{ me(func: anyofterms(name, "DropAllTest")) { name } }')
        data = json.loads(result.json)
        assert len(data.get("me", [])) == 0

    @pytest.mark.asyncio
    async def test_drop_data(self, async_client: AsyncDgraphClient) -> None:
        """Test async drop_data drops data but preserves schema."""
        await _skip_if_below_v25(async_client)

        await async_client.drop_all()
        await async_client.set_schema("name: string @index(term) .")

        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "DropDataTest"}, commit_now=True)

        # Drop data only
        response = await async_client.drop_data()
        assert response is not None

        # Verify data is gone but schema still works
        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "AfterDropData"}, commit_now=True)

        txn = async_client.txn(read_only=True)
        result = await txn.query('{ me(func: anyofterms(name, "AfterDropData")) { name } }')
        data = json.loads(result.json)
        assert len(data["me"]) == 1

    @pytest.mark.asyncio
    async def test_drop_predicate(self, async_client: AsyncDgraphClient) -> None:
        """Test async drop_predicate drops a specific predicate."""
        await _skip_if_below_v25(async_client)

        await async_client.drop_all()
        await async_client.set_schema("name: string @index(term) .\nage: int .")

        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "PredicateTest", "age": 30}, commit_now=True)

        # Drop the age predicate
        await async_client.drop_predicate("age")

        # Verify name still exists but age is gone
        txn = async_client.txn(read_only=True)
        result = await txn.query(
            '{ me(func: anyofterms(name, "PredicateTest")) { name age } }'
        )
        data = json.loads(result.json)
        assert len(data["me"]) == 1
        assert data["me"][0]["name"] == "PredicateTest"
        assert "age" not in data["me"][0]

    @pytest.mark.asyncio
    async def test_drop_predicate_empty_raises(self, async_client: AsyncDgraphClient) -> None:
        """Test that drop_predicate with empty string raises ValueError."""
        with pytest.raises(ValueError, match="predicate cannot be empty"):
            await async_client.drop_predicate("")

    @pytest.mark.asyncio
    async def test_drop_type(self, async_client: AsyncDgraphClient) -> None:
        """Test async drop_type drops a type definition."""
        await _skip_if_below_v25(async_client)

        await async_client.drop_all()
        schema = """
            name: string @index(term) .
            type Person {
                name
            }
        """
        await async_client.set_schema(schema)

        # Drop the Person type
        response = await async_client.drop_type("Person")
        assert response is not None

    @pytest.mark.asyncio
    async def test_drop_type_empty_raises(self, async_client: AsyncDgraphClient) -> None:
        """Test that drop_type with empty string raises ValueError."""
        with pytest.raises(ValueError, match="type_name cannot be empty"):
            await async_client.drop_type("")

    @pytest.mark.asyncio
    async def test_set_schema(self, async_client: AsyncDgraphClient) -> None:
        """Test async set_schema sets the DQL schema."""
        await _skip_if_below_v25(async_client)

        await async_client.drop_all()
        response = await async_client.set_schema("name: string @index(term) .")
        assert response is not None

        # Verify schema was set by inserting and querying data
        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "SchemaTest"}, commit_now=True)

        txn = async_client.txn(read_only=True)
        result = await txn.query('{ me(func: anyofterms(name, "SchemaTest")) { name } }')
        data = json.loads(result.json)
        assert len(data["me"]) == 1

    @pytest.mark.asyncio
    async def test_set_schema_empty_raises(self, async_client: AsyncDgraphClient) -> None:
        """Test that set_schema with empty string raises ValueError."""
        with pytest.raises(ValueError, match="schema cannot be empty"):
            await async_client.set_schema("")


class TestAsyncDQL:
    """Test suite for async run_dql and run_dql_with_vars."""

    @pytest.mark.asyncio
    async def test_run_dql_query(self, async_client: AsyncDgraphClient) -> None:
        """Test async run_dql with a query."""
        await _skip_if_below_v25(async_client)

        await async_client.drop_all()
        await async_client.set_schema("name: string @index(term) .")

        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "DQLTest"}, commit_now=True)

        response = await async_client.run_dql(
            '{ me(func: anyofterms(name, "DQLTest")) { name } }',
            read_only=True,
        )
        assert response is not None
        data = json.loads(response.json)
        assert len(data["me"]) == 1
        assert data["me"][0]["name"] == "DQLTest"

    @pytest.mark.asyncio
    async def test_run_dql_mutation(self, async_client: AsyncDgraphClient) -> None:
        """Test async run_dql with a mutation."""
        await _skip_if_below_v25(async_client)

        await async_client.drop_all()
        await async_client.set_schema("name: string @index(term) .")

        # Run a mutation via DQL (bare set block, no 'mutation' wrapper)
        mutation_dql = """
            {
                set {
                    _:new <name> "DQLMutationTest" .
                }
            }
        """
        response = await async_client.run_dql(mutation_dql)
        assert response is not None

        # Verify data was inserted
        query_response = await async_client.run_dql(
            '{ me(func: anyofterms(name, "DQLMutationTest")) { name } }',
            read_only=True,
        )
        data = json.loads(query_response.json)
        assert len(data["me"]) == 1

    @pytest.mark.asyncio
    async def test_run_dql_with_vars(self, async_client: AsyncDgraphClient) -> None:
        """Test async run_dql_with_vars with query variables."""
        await _skip_if_below_v25(async_client)

        await async_client.drop_all()
        await async_client.set_schema("name: string @index(term) .")

        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "VarsTest"}, commit_now=True)

        response = await async_client.run_dql_with_vars(
            'query search($name: string) { me(func: anyofterms(name, $name)) { name } }',
            vars={"$name": "VarsTest"},
            read_only=True,
        )
        assert response is not None
        data = json.loads(response.json)
        assert len(data["me"]) == 1
        assert data["me"][0]["name"] == "VarsTest"

    @pytest.mark.asyncio
    async def test_run_dql_with_vars_none_raises(
        self, async_client: AsyncDgraphClient
    ) -> None:
        """Test that run_dql_with_vars with None vars raises ValueError."""
        with pytest.raises(ValueError, match="vars parameter is required"):
            await async_client.run_dql_with_vars("{ me(func: has(name)) { name } }", vars=None)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_run_dql_resp_format(self, async_client: AsyncDgraphClient) -> None:
        """Test async run_dql with different response formats."""
        await _skip_if_below_v25(async_client)

        await async_client.drop_all()
        await async_client.set_schema("name: string @index(term) .")

        txn = async_client.txn()
        await txn.mutate(set_obj={"name": "FormatTest"}, commit_now=True)

        # Test JSON format (default)
        response = await async_client.run_dql(
            '{ me(func: anyofterms(name, "FormatTest")) { name } }',
            read_only=True,
            resp_format="JSON",
        )
        assert response is not None
        data = json.loads(response.json)
        assert len(data["me"]) == 1

    @pytest.mark.asyncio
    async def test_run_dql_invalid_format_raises(
        self, async_client: AsyncDgraphClient
    ) -> None:
        """Test that invalid resp_format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid resp_format"):
            await async_client.run_dql("{ me(func: has(name)) { name } }", resp_format="XML")


class TestAsyncAllocations:
    """Test suite for async allocation methods."""

    @pytest.mark.asyncio
    async def test_allocate_uids(self, async_client: AsyncDgraphClient) -> None:
        """Test async allocate_uids returns valid range."""
        await _skip_if_below_v25(async_client)

        how_many = 100
        start, end = await async_client.allocate_uids(how_many)

        assert isinstance(start, int)
        assert isinstance(end, int)
        assert start > 0
        assert end > start
        assert end - start == how_many

        # Second allocation should be non-overlapping
        start2, _end2 = await async_client.allocate_uids(how_many)
        assert start2 >= end

    @pytest.mark.asyncio
    async def test_allocate_timestamps(self, async_client: AsyncDgraphClient) -> None:
        """Test async allocate_timestamps returns valid range."""
        await _skip_if_below_v25(async_client)

        how_many = 50
        start, end = await async_client.allocate_timestamps(how_many)

        assert isinstance(start, int)
        assert isinstance(end, int)
        assert start > 0
        assert end > start
        assert end - start == how_many

    @pytest.mark.asyncio
    async def test_allocate_namespaces(self, async_client: AsyncDgraphClient) -> None:
        """Test async allocate_namespaces returns valid range."""
        await _skip_if_below_v25(async_client)

        how_many = 10
        start, end = await async_client.allocate_namespaces(how_many)

        assert isinstance(start, int)
        assert isinstance(end, int)
        assert start > 0
        assert end > start
        assert end - start == how_many

    @pytest.mark.asyncio
    async def test_allocate_zero_raises(self, async_client: AsyncDgraphClient) -> None:
        """Test allocating zero items raises ValueError."""
        with pytest.raises(ValueError, match="how_many must be greater than 0"):
            await async_client.allocate_uids(0)

        with pytest.raises(ValueError, match="how_many must be greater than 0"):
            await async_client.allocate_timestamps(-1)

    @pytest.mark.asyncio
    async def test_allocate_with_timeout(self, async_client: AsyncDgraphClient) -> None:
        """Test allocation methods work with timeout parameter."""
        await _skip_if_below_v25(async_client)

        start, end = await async_client.allocate_uids(10, timeout=30)
        assert end - start == 10


class TestAsyncNamespaces:
    """Test suite for async namespace management methods."""

    @pytest.mark.asyncio
    async def test_create_namespace(self, async_client: AsyncDgraphClient) -> None:
        """Test async create_namespace returns valid namespace ID."""
        await _skip_if_below_v25(async_client)

        namespace_id = await async_client.create_namespace()
        assert isinstance(namespace_id, int)
        assert namespace_id > 0

        # Creating another namespace gives a different ID
        namespace_id2 = await async_client.create_namespace()
        assert namespace_id2 > 0
        assert namespace_id != namespace_id2

    @pytest.mark.asyncio
    async def test_list_namespaces(self, async_client: AsyncDgraphClient) -> None:
        """Test async list_namespaces returns a dictionary."""
        await _skip_if_below_v25(async_client)

        namespace_id = await async_client.create_namespace()

        namespaces = await async_client.list_namespaces()
        assert isinstance(namespaces, dict)
        assert namespace_id in namespaces

    @pytest.mark.asyncio
    async def test_drop_namespace(self, async_client: AsyncDgraphClient) -> None:
        """Test async drop_namespace removes a namespace."""
        await _skip_if_below_v25(async_client)

        namespace_id = await async_client.create_namespace()

        # Verify it exists
        namespaces = await async_client.list_namespaces()
        assert namespace_id in namespaces

        # Drop it
        if namespace_id != 0:
            await async_client.drop_namespace(namespace_id)
            await _wait_for_namespace_deletion_async(async_client, namespace_id)

    @pytest.mark.asyncio
    async def test_cannot_drop_namespace_zero(self, async_client: AsyncDgraphClient) -> None:
        """Test that namespace 0 cannot be dropped."""
        await _skip_if_below_v25(async_client)

        import grpc

        with pytest.raises(grpc.RpcError) as cm:
            await async_client.drop_namespace(0)
        assert "cannot be deleted" in str(cm.value)


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
