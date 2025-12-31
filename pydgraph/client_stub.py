# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Stub for RPC request."""
from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse

import grpc

from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2 as api
from pydgraph.proto import api_pb2_grpc as api_grpc

__author__ = "Garvit Pahal"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__version__ = VERSION
__status__ = "development"


class DgraphClientStub:
    """Stub for the Dgraph grpc client."""

    def __init__(
        self,
        addr: str = "localhost:9080",
        credentials: grpc.ChannelCredentials | None = None,
        options: list[tuple[str, Any]] | None = None,
    ) -> None:
        if credentials is None:
            self.channel = grpc.insecure_channel(addr, options)
        else:
            self.channel = grpc.secure_channel(addr, credentials, options)

        self.stub = api_grpc.DgraphStub(self.channel)

    def __enter__(self) -> DgraphClientStub:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
        if exc_type is not None:
            raise exc_val

    def login(
        self,
        login_req: api.LoginRequest,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Response:
        return self.stub.Login(
            login_req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def alter(
        self,
        operation: api.Operation,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Payload:
        """Runs alter operation."""
        return self.stub.Alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def async_alter(
        self,
        operation: api.Operation,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> grpc.Future:
        """Async version of alter."""
        return self.stub.Alter.future(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def query(
        self,
        req: api.Request,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Response:
        """Runs query or mutate operation."""
        return self.stub.Query(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def async_query(
        self,
        req: api.Request,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> grpc.Future:
        """Async version of query."""
        return self.stub.Query.future(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def commit_or_abort(
        self,
        ctx: api.TxnContext,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.TxnContext:
        """Runs commit or abort operation."""
        return self.stub.CommitOrAbort(
            ctx, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def check_version(
        self,
        check: api.Check,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Version:
        """Returns the version of the Dgraph instance."""
        return self.stub.CheckVersion(
            check, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def run_dql(
        self,
        req: api.RunDQLRequest,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Response:
        return self.stub.RunDQL(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def allocate_ids(
        self,
        req: api.AllocateIDsRequest,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.AllocateIDsResponse:
        """Allocates IDs (UIDs, timestamps, or namespaces)."""
        return self.stub.AllocateIDs(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def create_namespace(
        self,
        req: api.CreateNamespaceRequest,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.CreateNamespaceResponse:
        """Creates a new namespace."""
        return self.stub.CreateNamespace(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def drop_namespace(
        self,
        req: api.DropNamespaceRequest,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> Any:
        """Drops a namespace."""
        return self.stub.DropNamespace(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def list_namespaces(
        self,
        req: api.ListNamespacesRequest,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.ListNamespacesResponse:
        """Lists all namespaces."""
        return self.stub.ListNamespaces(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def close(self) -> None:
        """Deletes channel and stub."""
        with contextlib.suppress(Exception):
            self.channel.close()
        del self.channel
        del self.stub

    @staticmethod
    def parse_host(cloud_endpoint: str) -> str:
        """Converts any cloud endpoint to grpc endpoint"""
        host = cloud_endpoint
        if cloud_endpoint.startswith("http"):  # catch http:// and https://
            host = urlparse(cloud_endpoint).netloc
        host = host.split(":", 1)[0]  # remove port if any
        if ".grpc." not in host:
            url_parts = host.split(".", 1)
            host = url_parts[0] + ".grpc." + url_parts[1]
        return host

    # accepts grpc endpoint as copied in cloud console as well as graphql endpoint
    # Usage:
    @staticmethod
    def from_cloud(
        cloud_endpoint: str,
        api_key: str,
        options: list[tuple[str, Any]] | None = None,
    ) -> DgraphClientStub:
        """Returns Dgraph Client stub for the Dgraph Cloud endpoint"""
        host = DgraphClientStub.parse_host(cloud_endpoint)
        creds = grpc.ssl_channel_credentials()
        call_credentials = grpc.metadata_call_credentials(
            lambda _context, callback: callback((("authorization", api_key),), None)
        )
        composite_credentials = grpc.composite_channel_credentials(
            creds, call_credentials
        )
        if options is None:
            options = [("grpc.enable_http_proxy", 0)]
        else:
            options.append(("grpc.enable_http_proxy", 0))
        client_stub = DgraphClientStub(
            "{host}:{port}".format(host=host, port="443"),
            composite_credentials,
            options=options,
        )
        return client_stub


@contextlib.contextmanager
def client_stub(
    addr: str = "localhost:9080", **kwargs: Any
) -> Iterator[DgraphClientStub]:
    """Create a managed DgraphClientStub instance.

    Parameters
    ----------
    addr : str, optional
    credentials : ChannelCredentials, optional
    options: List[Dict]
        An optional list of key-value pairs (``channel_arguments``
        in gRPC Core runtime) to configure the channel.

    Note
    ----
    Only use this function in ``with-as`` blocks.
    """
    stub = DgraphClientStub(addr=addr, **kwargs)
    try:
        yield stub
    finally:
        stub.close()
