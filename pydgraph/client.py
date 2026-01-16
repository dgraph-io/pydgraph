# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dgraph python client."""
from __future__ import annotations

import contextlib
import secrets
import urllib.parse
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import grpc

from pydgraph import errors, txn, util
from pydgraph.client_stub import DgraphClientStub
from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2 as api

if TYPE_CHECKING:
    from pydgraph.txn import Txn

__author__ = "Mohit Ranka <mohitranka@gmail.com>"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__version__ = VERSION
__status__ = "development"


class DgraphClient:
    """Creates a new Client for interacting with Dgraph.

    The client can be backed by multiple connections (to the same server, or
    multiple servers in a cluster).
    """

    def __init__(self, *clients: DgraphClientStub) -> None:
        if not clients:
            raise ValueError("No clients provided in DgraphClient constructor")

        self._clients = clients[:]
        self._jwt = api.Jwt()
        self._login_metadata: list[tuple[str, str]] = []

    def check_version(
        self,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> str:
        """Returns the version of Dgraph if the server is ready to accept requests."""

        new_metadata = self.add_login_metadata(metadata)
        check_req = api.Check()

        try:
            response = self.any_client().check_version(
                check_req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                response = self.any_client().check_version(
                    check_req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise

        return response.tag

    def login(
        self,
        userid: str,
        password: str,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """Attempts a login via this client."""

        return self.login_into_namespace(
            userid,
            password,
            0,
            timeout=timeout,
            metadata=metadata,
            credentials=credentials,
        )

    def login_into_namespace(
        self,
        userid: str,
        password: str,
        namespace: int,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """Attempts a login into a namespace via this client."""

        login_req = api.LoginRequest()
        login_req.userid = userid
        login_req.password = password
        login_req.namespace = namespace

        response = self.any_client().login(
            login_req, timeout=timeout, metadata=metadata, credentials=credentials
        )
        self._jwt = api.Jwt()
        self._jwt.ParseFromString(response.json)
        self._login_metadata = [("accessjwt", self._jwt.access_jwt)]

    def retry_login(
        self,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """Attempts a retry login via this client."""

        if len(self._jwt.refresh_jwt) == 0:
            raise ValueError("refresh jwt should not be empty")

        login_req = api.LoginRequest()
        login_req.refresh_token = self._jwt.refresh_jwt

        response = self.any_client().login(
            login_req, timeout=timeout, metadata=metadata, credentials=credentials
        )
        self._jwt = api.Jwt()
        self._jwt.ParseFromString(response.json)
        self._login_metadata = [("accessjwt", self._jwt.access_jwt)]

    def alter(
        self,
        operation: api.Operation,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Payload:
        """Runs a modification via this client."""

        new_metadata = self.add_login_metadata(metadata)

        try:
            return self.any_client().alter(
                operation,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                try:
                    return self.any_client().alter(
                        operation,
                        timeout=timeout,
                        metadata=new_metadata,
                        credentials=credentials,
                    )
                except Exception as retry_error:
                    self._common_except_alter(retry_error)
                    raise  # This should never be reached due to _common_except_alter raising
            else:
                self._common_except_alter(error)
                raise  # This should never be reached due to _common_except_alter raising

    @staticmethod
    def _common_except_alter(error: Exception) -> None:
        if util.is_retriable_error(error):
            raise errors.RetriableError(error)

        if util.is_connection_error(error):
            raise errors.ConnectionError(error)

        raise error

    def async_alter(
        self,
        operation: api.Operation,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> grpc.Future:
        """The async version of alter."""

        new_metadata = self.add_login_metadata(metadata)
        return self.any_client().async_alter(
            operation, timeout=timeout, metadata=new_metadata, credentials=credentials
        )

    @staticmethod
    def handle_alter_future(future: grpc.Future) -> api.Payload:
        try:
            return future.result()
        except Exception as error:
            DgraphClient._common_except_alter(error)
            raise  # This should never be reached due to _common_except_alter raising

    def drop_all(
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
        return self.alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def drop_data(
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
        return self.alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def drop_predicate(
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
        return self.alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def drop_type(
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
        return self.alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def set_schema(
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
        return self.alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def txn(
        self, read_only: bool = False, best_effort: bool = False, **commit_kwargs: Any
    ) -> Txn:
        """Creates a transaction."""
        return txn.Txn(
            self, read_only=read_only, best_effort=best_effort, **commit_kwargs
        )

    def run_dql(
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
        """
        Runs a DQL query or mutation via this client.

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
            return self.any_client().run_dql(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return self.any_client().run_dql(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            raise

    def run_dql_with_vars(
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
        """
        Runs a DQL query or mutation with variables via this client.

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

        return self.run_dql(
            dql_query=dql_query,
            vars=vars,
            read_only=read_only,
            best_effort=best_effort,
            resp_format=resp_format,
            timeout=timeout,
            metadata=metadata,
            credentials=credentials,
        )

    def allocate_uids(
        self,
        how_many: int,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> tuple[int, int]:
        """
        AllocateUIDs allocates a given number of Node UIDs in the Graph and returns a start and end UIDs,
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
        return self._allocate_ids(how_many, api.UID, timeout, metadata, credentials)

    def allocate_timestamps(
        self,
        how_many: int,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> tuple[int, int]:
        """
        AllocateTimestamps gets a sequence of timestamps allocated from Dgraph. These timestamps can be
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
        return self._allocate_ids(how_many, api.TS, timeout, metadata, credentials)

    def allocate_namespaces(
        self,
        how_many: int,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> tuple[int, int]:
        """
        AllocateNamespaces allocates a given number of namespaces in the Graph and returns a start and end
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
        return self._allocate_ids(how_many, api.NS, timeout, metadata, credentials)

    def _allocate_ids(
        self,
        how_many: int,
        lease_type: int,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> tuple[int, int]:
        """
        Helper method to allocate IDs of different types (UIDs, timestamps, namespaces).

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
            response = self.any_client().allocate_ids(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
            return response.start, response.end + 1
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                response = self.any_client().allocate_ids(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
                return response.start, response.end + 1
            raise

    def create_namespace(
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
            response = self.any_client().create_namespace(
                request,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                response = self.any_client().create_namespace(
                    request,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise

        return response.namespace

    def drop_namespace(
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
            response = self.any_client().drop_namespace(
                request,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                response = self.any_client().drop_namespace(
                    request,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise

        return response

    def list_namespaces(
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
            response = self.any_client().list_namespaces(
                request,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
            return dict(response.namespaces)
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                response = self.any_client().list_namespaces(
                    request,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
                return dict(response.namespaces)
            raise

    def any_client(self) -> DgraphClientStub:
        """Returns a random gRPC client so that requests are distributed evenly among them."""

        return secrets.choice(self._clients)

    def add_login_metadata(
        self, metadata: list[tuple[str, str]] | None
    ) -> list[tuple[str, str]]:
        new_metadata = list(self._login_metadata)
        if not metadata:
            return new_metadata
        new_metadata.extend(metadata)
        return new_metadata

    def close(self) -> None:
        for client in self._clients:
            client.close()

    @contextlib.contextmanager
    def begin(
        self,
        read_only: bool = False,
        best_effort: bool = False,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> Iterator[Txn]:
        """Start a managed transaction.

        Note
        ----
        Only use this function in ``with-as`` blocks.
        """
        tx = self.txn(read_only=read_only, best_effort=best_effort)
        try:
            yield tx
            if not read_only and not tx._finished:
                tx.commit(timeout=timeout, metadata=metadata, credentials=credentials)
        finally:
            tx.discard()


def open(connection_string: str) -> DgraphClient:  # noqa: A001, C901
    """Open a new Dgraph client. Use client.close() to close the client.

    Args:
        connection_string: A connection string in the format of "dgraph://<username:password>@<host>:<port>?<params>"

    Returns:
        A new Dgraph client.

    Raises:
        ValueError: If the connection string is invalid.
    """

    try:
        parsed_url = urllib.parse.urlparse(connection_string)
    except Exception as e:
        raise ValueError(f"Failed to parse connection string: {e}") from e

    if not parsed_url.scheme == "dgraph":
        raise ValueError("Invalid connection string: scheme must be 'dgraph'")
    if not parsed_url.hostname:
        raise ValueError("Invalid connection string: hostname required")
    if not parsed_url.port:
        raise ValueError("Invalid connection string: port required")

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
    namespace = 0

    if "sslmode" in params:
        sslmode = params["sslmode"]
        if sslmode == "disable":
            credentials = None
        elif sslmode == "require":
            raise ValueError(
                "sslmode=require is not supported in pydgraph, use verify-ca"
            )
        elif sslmode == "verify-ca":
            credentials = grpc.ssl_channel_credentials()
        else:
            raise ValueError(f"Invalid sslmode: {sslmode}")

    if "apikey" in params and "bearertoken" in params:
        raise ValueError("apikey and bearertoken cannot both be provided")

    if "apikey" in params:
        auth_header = params["apikey"]
    elif "bearertoken" in params:
        auth_header = f"Bearer {params['bearertoken']}"

    if "namespace" in params:
        try:
            namespace = int(params["namespace"])
        except ValueError as e:
            raise TypeError(
                f"namespace must be an integer, got '{params['namespace']}'"
            ) from e

        if namespace < 0:
            raise ValueError(f"namespace must be >= 0, got {namespace}")
        if not username:
            raise ValueError("username/password required when namespace is provided")

    if auth_header:
        options = [("grpc.enable_http_proxy", 0)]
        call_credentials = grpc.metadata_call_credentials(
            lambda _context, callback: callback((("authorization", auth_header),), None)
        )
        credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(), call_credentials
        )

    client_stub = DgraphClientStub(f"{host}:{port}", credentials, options)
    client = DgraphClient(client_stub)

    if username:
        thirty_seconds = 30
        if password is None:
            raise ValueError("password is required when username is provided")
        client.login_into_namespace(
            username, password, namespace, timeout=thirty_seconds
        )

    return client
