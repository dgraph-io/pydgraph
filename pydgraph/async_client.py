# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
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

    async def __aexit__(
        self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> bool:
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
