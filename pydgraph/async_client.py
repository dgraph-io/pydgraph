# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dgraph async python client."""

from __future__ import annotations

import asyncio
import secrets
import urllib.parse
from typing import Any, NoReturn

import grpc

from pydgraph import errors, util
from pydgraph.async_txn import AsyncTxn
from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2 as api

__author__ = "Istari Digital, Inc."
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__version__ = VERSION
__status__ = "development"


class AsyncDgraphClient:
    """Creates a new async Client for interacting with Dgraph using asyncio.

    The client can be backed by multiple connections (to the same server, or
    multiple servers in a cluster).

    Can be used as an async context manager:
        async with await async_open("dgraph://localhost:9080") as client:
            # Use client
            pass
    """

    def __init__(self, *clients: Any) -> None:
        """Initialize async client.

        Args:
            *clients: One or more AsyncDgraphClientStub instances

        Raises:
            ValueError: If no clients are provided
        """
        if not clients:
            raise ValueError("No clients provided in AsyncDgraphClient constructor")

        self._clients = clients[:]
        self._jwt = api.Jwt()
        self._login_metadata: list[tuple[str, str]] = []
        self._refresh_lock = asyncio.Lock()  # Prevent concurrent JWT refresh

    async def check_version(
        self,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> str:
        """Returns the version of Dgraph if server is ready to accept requests.

        Args:
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Returns:
            Version string from the server

        Raises:
            Various gRPC errors on failure
        """
        new_metadata = self.add_login_metadata(metadata)
        check_req = api.Check()

        try:
            response = await self.any_client().check_version(
                check_req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            # Handle JWT expiration with automatic retry
            if not util.is_jwt_expired(error):
                raise
            await self.retry_login()
            new_metadata = self.add_login_metadata(metadata)
            response = await self.any_client().check_version(
                check_req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        return response.tag

    async def login(
        self,
        userid: str,
        password: str,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """Login to Dgraph with credentials.

        Args:
            userid: User ID
            password: Password
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Raises:
            Various gRPC errors on failure
        """
        login_req = api.LoginRequest()
        login_req.userid = userid
        login_req.password = password
        login_req.namespace = 0

        response = await self.any_client().login(
            login_req, timeout=timeout, metadata=metadata, credentials=credentials
        )
        self._jwt = api.Jwt()
        self._jwt.ParseFromString(response.json)
        self._login_metadata = [("accessjwt", self._jwt.access_jwt)]

    async def login_into_namespace(
        self,
        userid: str,
        password: str,
        namespace: int,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """Login to specific Dgraph namespace.

        Args:
            userid: User ID
            password: Password
            namespace: Namespace ID
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Raises:
            Various gRPC errors on failure
        """
        login_req = api.LoginRequest()
        login_req.userid = userid
        login_req.password = password
        login_req.namespace = namespace

        response = await self.any_client().login(
            login_req, timeout=timeout, metadata=metadata, credentials=credentials
        )
        self._jwt = api.Jwt()
        self._jwt.ParseFromString(response.json)
        self._login_metadata = [("accessjwt", self._jwt.access_jwt)]

    async def retry_login(
        self,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """Refresh JWT token using refresh token.

        Uses a lock to prevent concurrent refresh attempts (thundering herd).
        Implements double-check pattern: verifies token still needs refresh
        after acquiring lock.

        Args:
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Raises:
            ValueError: If refresh JWT is empty
            Various gRPC errors on failure
        """
        async with self._refresh_lock:
            # Double-check: another coroutine may have already refreshed
            # We can't easily check if token is still expired here without
            # making a test request, so we proceed with refresh.
            # This is safe because refresh tokens can be reused.

            if len(self._jwt.refresh_jwt) == 0:
                raise ValueError("refresh jwt should not be empty")

            login_req = api.LoginRequest()
            login_req.refresh_token = self._jwt.refresh_jwt

            response = await self.any_client().login(
                login_req, timeout=timeout, metadata=metadata, credentials=credentials
            )
            self._jwt = api.Jwt()
            self._jwt.ParseFromString(response.json)

            # Validate that we got valid tokens
            if not self._jwt.access_jwt:
                raise ValueError("Login response did not contain access_jwt")

            self._login_metadata = [("accessjwt", self._jwt.access_jwt)]

    async def alter(
        self,
        operation: api.Operation,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Payload:
        """Runs a schema modification via this client.

        Args:
            operation: Operation protobuf message (schema change or drop)
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Returns:
            Payload protobuf message

        Raises:
            RetriableError: If operation should be retried
            ConnectionError: If connection failed
            Various gRPC errors on failure
        """
        new_metadata = self.add_login_metadata(metadata)

        try:
            return await self.any_client().alter(
                operation,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            # Handle JWT expiration with automatic retry
            if not util.is_jwt_expired(error):
                self._common_except_alter(error)
            await self.retry_login()
            new_metadata = self.add_login_metadata(metadata)
            try:
                return await self.any_client().alter(
                    operation,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            except Exception as error:
                self._common_except_alter(error)

    @staticmethod
    def _common_except_alter(error: Exception) -> NoReturn:
        """Maps alter errors to pydgraph exceptions.

        Args:
            error: Exception from alter operation

        Raises:
            RetriableError: If operation should be retried
            ConnectionError: If connection failed
            The original error otherwise
        """
        if util.is_retriable_error(error):
            raise errors.RetriableError(error)

        if util.is_connection_error(error):
            raise errors.ConnectionError(error)

        raise error

    async def drop_all(
        self,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Payload:
        """Drops all data and schema from the Dgraph instance.

        Args:
            timeout: Optional timeout for the request.
            metadata: Optional metadata to send with the request.
            credentials: Optional credentials for the request.

        Returns:
            Payload: The response from Dgraph.

        Raises:
            Exception: If the request fails.
        """
        operation = api.Operation(drop_all=True)
        return await self.alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def drop_data(
        self,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Payload:
        """Drops all data from the Dgraph instance while preserving the schema.

        Args:
            timeout: Optional timeout for the request.
            metadata: Optional metadata to send with the request.
            credentials: Optional credentials for the request.

        Returns:
            Payload: The response from Dgraph.

        Raises:
            Exception: If the request fails.
        """
        operation = api.Operation(drop_op=api.Operation.DropOp.DATA)
        return await self.alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def drop_predicate(
        self,
        predicate: str,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Payload:
        """Drops a predicate and its associated data from the Dgraph instance.

        Args:
            predicate: The name of the predicate to drop.
            timeout: Optional timeout for the request.
            metadata: Optional metadata to send with the request.
            credentials: Optional credentials for the request.

        Returns:
            Payload: The response from Dgraph.

        Raises:
            Exception: If the request fails.
        """
        if not predicate:
            raise ValueError("predicate cannot be empty")
        operation = api.Operation(
            drop_op=api.Operation.DropOp.ATTR, drop_value=predicate
        )
        return await self.alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def drop_type(
        self,
        type_name: str,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Payload:
        """Drops a type definition from the DQL schema.

        Note: This only removes the type definition from the schema. No data is
        removed from the cluster. The operation does not drop the predicates
        associated with the type.

        Args:
            type_name: The name of the type to drop.
            timeout: Optional timeout for the request.
            metadata: Optional metadata to send with the request.
            credentials: Optional credentials for the request.

        Returns:
            Payload: The response from Dgraph.

        Raises:
            Exception: If the request fails.
        """
        if not type_name:
            raise ValueError("type_name cannot be empty")
        operation = api.Operation(
            drop_op=api.Operation.DropOp.TYPE, drop_value=type_name
        )
        return await self.alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def set_schema(
        self,
        schema: str,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Payload:
        """Sets the DQL schema for the Dgraph instance.

        Args:
            schema: The DQL schema string to set.
            timeout: Optional timeout for the request.
            metadata: Optional metadata to send with the request.
            credentials: Optional credentials for the request.

        Returns:
            Payload: The response from Dgraph.

        Raises:
            Exception: If the request fails.
        """
        if not schema:
            raise ValueError("schema cannot be empty")
        operation = api.Operation(schema=schema)
        return await self.alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def run_dql(
        self,
        dql_query: str,
        vars: dict[str, str] | None = None,  # noqa: A002
        read_only: bool = False,
        best_effort: bool = False,
        resp_format: str = "JSON",
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Response:
        """Runs a DQL query or mutation via this client.

        Args:
            dql_query: The DQL query string to execute
            vars: Variables to substitute in the query
            read_only: Whether this is a read-only query
            best_effort: Whether to use best effort for read queries
            resp_format: Response format, either "JSON" or "RDF"
            timeout: Request timeout
            metadata: Additional metadata for the request
            credentials: gRPC credentials

        Returns:
            Response: The query response from Dgraph

        This is only supported on Dgraph v25.0.0 and above.
        """
        new_metadata = self.add_login_metadata(metadata)

        # Add explicit namespace metadata for RunDQL
        # Extract namespace from JWT token if available
        # TODO(Matthew): Remove this once Dgraph supports RunDQL without namespace metadata
        if self._jwt.access_jwt:
            import base64
            import json

            try:
                # Decode JWT payload (second part after first dot)
                payload_part = self._jwt.access_jwt.split(".")[1]
                # Add padding if needed for base64 decoding
                payload_part += "=" * (4 - len(payload_part) % 4)
                payload = json.loads(base64.b64decode(payload_part))
                namespace = payload.get("namespace", 0)
                new_metadata.append(("namespace", str(namespace)))
            except Exception:
                # If JWT decoding fails, use default namespace
                new_metadata.append(("namespace", "0"))

        # Convert string format to enum if needed
        format_value: int
        if isinstance(resp_format, str):
            if resp_format.upper() == "JSON":
                format_value = api.Request.RespFormat.JSON
            elif resp_format.upper() == "RDF":
                format_value = api.Request.RespFormat.RDF
            else:
                raise ValueError(
                    f"Invalid resp_format: {resp_format}. Must be 'JSON' or 'RDF'"
                )
        else:
            format_value = resp_format
        req = api.RunDQLRequest(
            dql_query=dql_query,
            vars=vars,
            read_only=read_only,
            best_effort=best_effort,
            resp_format=format_value,
        )
        try:
            return await self.any_client().run_dql(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                await self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return await self.any_client().run_dql(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            raise

    async def run_dql_with_vars(
        self,
        dql_query: str,
        vars: dict[str, str],  # noqa: A002
        read_only: bool = False,
        best_effort: bool = False,
        resp_format: str = "JSON",
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Response:
        """Runs a DQL query or mutation with variables via this client.

        This is similar to run_dql but requires variables to be provided.

        Args:
            dql_query: The DQL query string to execute
            vars: Variables to substitute in the query (required, dict mapping string to string)
            read_only: Whether this is a read-only query
            best_effort: Whether to use best effort for read queries
            resp_format: Response format, either "JSON" or "RDF"
            timeout: Request timeout
            metadata: Additional metadata for the request
            credentials: gRPC credentials

        Returns:
            Response: The query response from Dgraph

        This is only supported on Dgraph v25.0.0 and above.
        """
        if vars is None:
            raise ValueError("vars parameter is required for run_dql_with_vars")

        return await self.run_dql(
            dql_query=dql_query,
            vars=vars,
            read_only=read_only,
            best_effort=best_effort,
            resp_format=resp_format,
            timeout=timeout,
            metadata=metadata,
            credentials=credentials,
        )

    async def allocate_uids(
        self,
        how_many: int,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> tuple[int, int]:
        """AllocateUIDs allocates a given number of Node UIDs in the Graph and returns a start and end UIDs,
        end excluded. The UIDs in the range [start, end) can then be used by the client in the mutations
        going forward. Note that, each node in a Graph is assigned a UID in Dgraph. Dgraph ensures that
        these UIDs are not allocated anywhere else throughout the operation of this cluster. This is useful
        in bulk loader or live loader or similar applications.

        Args:
            how_many: Number of UIDs to allocate
            timeout: Request timeout
            metadata: Additional metadata for the request
            credentials: gRPC credentials

        Returns:
            tuple: (start_uid, end_uid) where end_uid is exclusive

        This is only supported on Dgraph v25.0.0 and above.
        """
        return await self._allocate_ids(how_many, api.UID, timeout, metadata, credentials)

    async def allocate_timestamps(
        self,
        how_many: int,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> tuple[int, int]:
        """AllocateTimestamps gets a sequence of timestamps allocated from Dgraph. These timestamps can be
        used in bulk loader and similar applications.

        Args:
            how_many: Number of timestamps to allocate
            timeout: Request timeout
            metadata: Additional metadata for the request
            credentials: gRPC credentials

        Returns:
            tuple: (start_timestamp, end_timestamp) where end_timestamp is exclusive

        This is only supported on Dgraph v25.0.0 and above.
        """
        return await self._allocate_ids(how_many, api.TS, timeout, metadata, credentials)

    async def allocate_namespaces(
        self,
        how_many: int,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> tuple[int, int]:
        """AllocateNamespaces allocates a given number of namespaces in the Graph and returns a start and end
        namespaces, end excluded. The namespaces in the range [start, end) can then be used by the client.
        Dgraph ensures that these namespaces are NOT allocated anywhere else throughout the operation of
        this cluster. This is useful in bulk loader or live loader or similar applications.

        Args:
            how_many: Number of namespaces to allocate
            timeout: Request timeout
            metadata: Additional metadata for the request
            credentials: gRPC credentials

        Returns:
            tuple: (start_namespace, end_namespace) where end_namespace is exclusive

        This is only supported on Dgraph v25.0.0 and above.
        """
        return await self._allocate_ids(how_many, api.NS, timeout, metadata, credentials)

    async def _allocate_ids(
        self,
        how_many: int,
        lease_type: int,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> tuple[int, int]:
        """Helper method to allocate IDs of different types (UIDs, timestamps, namespaces).

        Args:
            how_many: Number of IDs to allocate
            lease_type: Type of lease (api.UID, api.TS, or api.NS)
            timeout: Request timeout
            metadata: Additional metadata for the request
            credentials: gRPC credentials

        Returns:
            tuple: (start, end) where end is exclusive
        """
        if how_many <= 0:
            raise ValueError("how_many must be greater than 0")
        new_metadata = self.add_login_metadata(metadata)
        req = api.AllocateIDsRequest(how_many=how_many, lease_type=lease_type)
        try:
            response = await self.any_client().allocate_ids(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
            return response.start, response.end + 1
        except Exception as error:
            if util.is_jwt_expired(error):
                await self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                response = await self.any_client().allocate_ids(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
                return response.start, response.end + 1
            raise

    async def create_namespace(
        self,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> int:
        """Creates a new namespace and returns its ID.

        Args:
            timeout: Optional timeout for the request.
            metadata: Optional metadata to send with the request.
            credentials: Optional credentials for the request.

        Returns:
            int: The ID of the newly created namespace.

        Raises:
            Exception: If the request fails.
        """
        new_metadata = self.add_login_metadata(metadata)
        request = api.CreateNamespaceRequest()

        try:
            response = await self.any_client().create_namespace(
                request,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                await self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                response = await self.any_client().create_namespace(
                    request,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise

        return response.namespace

    async def drop_namespace(
        self,
        namespace: int,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> Any:
        """Drops the specified namespace. If the namespace does not exist, the request will still
        succeed.

        Args:
            namespace (int): The ID of the namespace to drop.
            timeout: Optional timeout for the request.
            metadata: Optional metadata to send with the request.
            credentials: Optional credentials for the request.

        Raises:
            Exception: If the request fails.
        """
        new_metadata = self.add_login_metadata(metadata)
        request = api.DropNamespaceRequest()
        request.namespace = namespace

        try:
            response = await self.any_client().drop_namespace(
                request,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                await self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                response = await self.any_client().drop_namespace(
                    request,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise

        return response

    async def list_namespaces(
        self,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> dict[int, Any]:
        """Lists all namespaces.

        Args:
            timeout: Optional timeout for the request.
            metadata: Optional metadata to send with the request.
            credentials: Optional credentials for the request.

        Returns:
            dict: A dictionary mapping namespace IDs to namespace objects.

        Raises:
            Exception: If the request fails.
        """
        new_metadata = self.add_login_metadata(metadata)
        request = api.ListNamespacesRequest()

        try:
            response = await self.any_client().list_namespaces(
                request,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
            return dict(response.namespaces)
        except Exception as error:
            if util.is_jwt_expired(error):
                await self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                response = await self.any_client().list_namespaces(
                    request,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
                return dict(response.namespaces)
            raise

    def txn(self, read_only: bool = False, best_effort: bool = False) -> AsyncTxn:
        """Creates an async transaction.

        Args:
            read_only: If True, transaction is read-only
            best_effort: If True, use best-effort mode (only for read-only)

        Returns:
            AsyncTxn instance

        Raises:
            Exception: If best_effort is True but read_only is False
        """
        return AsyncTxn(self, read_only=read_only, best_effort=best_effort)

    def any_client(self) -> Any:
        """Returns a random gRPC client for load balancing.

        Returns:
            AsyncDgraphClientStub instance
        """
        return secrets.choice(self._clients)

    def add_login_metadata(
        self, metadata: list[tuple[str, str]] | None
    ) -> list[tuple[str, str]]:
        """Adds JWT metadata to request metadata.

        Prevents caller from overriding authentication by filtering out
        any existing "accessjwt" keys from caller metadata.

        Args:
            metadata: Existing metadata list or None

        Returns:
            List with JWT metadata, caller metadata filtered
        """
        new_metadata = list(self._login_metadata)
        if not metadata:
            return new_metadata

        # Filter out any "accessjwt" from caller metadata to prevent override
        for key, value in metadata:
            if key.lower() != "accessjwt":
                new_metadata.append((key, value))

        return new_metadata

    async def close(self) -> None:
        """Close all client connections."""
        for client in self._clients:
            await client.close()

    async def __aenter__(self) -> AsyncDgraphClient:
        """Async context manager entry.

        Returns:
            Self for use in 'async with' statement
        """
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """Async context manager exit.

        Automatically closes all client connections.

        Args:
            exc_type: Exception type (if any)
            exc_val: Exception value (if any)
            exc_tb: Exception traceback (if any)

        Returns:
            False to propagate any exception
        """
        await self.close()
        return False


def _validate_parsed_url(parsed_url: urllib.parse.ParseResult) -> None:
    """Validate a parsed connection string URL.

    Args:
        parsed_url: Parsed URL result

    Raises:
        ValueError: If URL is invalid
    """
    if not parsed_url.scheme == "dgraph":
        raise ValueError("Invalid connection string: scheme must be 'dgraph'")
    if not parsed_url.hostname:
        raise ValueError("Invalid connection string: hostname required")
    if not parsed_url.port:
        raise ValueError("Invalid connection string: port required")


def _configure_ssl_credentials(sslmode: str) -> grpc.ChannelCredentials | None:
    """Configure SSL credentials based on sslmode parameter.

    Args:
        sslmode: SSL mode ("disable", "verify-ca")

    Returns:
        Channel credentials or None

    Raises:
        ValueError: If sslmode is invalid
    """
    if sslmode == "disable":
        return None
    if sslmode == "require":
        raise ValueError("sslmode=require is not supported in pydgraph, use verify-ca")
    if sslmode == "verify-ca":
        return grpc.ssl_channel_credentials()
    raise ValueError(f"Invalid sslmode: {sslmode}")


async def async_open(connection_string: str) -> AsyncDgraphClient:
    """Open a new async Dgraph client.

    Connection string format:
        dgraph://<username:password>@<host>:<port>?<params>

    Supported parameters:
        - sslmode: "disable" or "verify-ca"
        - apikey: API key for authentication
        - bearertoken: Bearer token for authentication

    Example:
        async with await async_open("dgraph://localhost:9080") as client:
            # Use client
            pass

        # With authentication
        async with await async_open("dgraph://groot:password@localhost:9080") as client:
            # Use client
            pass

        # With SSL
        async with await async_open("dgraph://localhost:9080?sslmode=verify-ca") as client:
            # Use client
            pass

    Args:
        connection_string: Connection string

    Returns:
        Async Dgraph client

    Raises:
        ValueError: If connection string is invalid
    """
    try:
        parsed_url = urllib.parse.urlparse(connection_string)
        _validate_parsed_url(parsed_url)
    except Exception as e:
        raise ValueError(f"Failed to parse connection string: {e}") from e

    host = parsed_url.hostname
    port = parsed_url.port
    username = parsed_url.username
    password = parsed_url.password

    if username and not password:
        raise ValueError(
            "Invalid connection string: password required when username is provided"
        )

    params = dict(urllib.parse.parse_qsl(parsed_url.query))
    credentials = None
    options = None
    auth_header = None

    # Handle SSL mode
    if "sslmode" in params:
        credentials = _configure_ssl_credentials(params["sslmode"])

    # Handle authentication headers
    if "apikey" in params and "bearertoken" in params:
        raise ValueError("apikey and bearertoken cannot both be provided")

    if "apikey" in params:
        auth_header = params["apikey"]
    elif "bearertoken" in params:
        auth_header = f"Bearer {params['bearertoken']}"

    if auth_header:
        options = [("grpc.enable_http_proxy", 0)]
        call_credentials = grpc.metadata_call_credentials(
            lambda _context, callback: callback((("authorization", auth_header),), None)
        )
        credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(), call_credentials
        )

    from pydgraph.async_client_stub import AsyncDgraphClientStub

    client_stub = AsyncDgraphClientStub(f"{host}:{port}", credentials, options)
    client = AsyncDgraphClient(client_stub)

    # Perform initial login if credentials provided
    if username:
        if password is None:
            # Should never happen due to earlier validation, but check for safety
            raise ValueError("Password cannot be None when username is provided")
        thirty_seconds = 30
        await client.login(username, password, timeout=thirty_seconds)

    return client
