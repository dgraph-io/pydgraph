# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Async stub for RPC requests using grpc.aio."""

import grpc
import grpc.aio

from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2_grpc as api_grpc

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

__author__ = "Hypermode Inc."
__maintainer__ = "Hypermode Inc. <hello@hypermode.com>"
__version__ = VERSION
__status__ = "development"


class AsyncDgraphClientStub:
    """Async stub for the Dgraph grpc client using grpc.aio."""

    def __init__(self, addr="localhost:9080", credentials=None, options=None):
        """Initialize async client stub.

        Args:
            addr: Address of Dgraph server (default: localhost:9080)
            credentials: gRPC credentials for secure channel (None for insecure)
            options: gRPC channel options
        """
        if credentials is None:
            self.channel = grpc.aio.insecure_channel(addr, options)
        else:
            self.channel = grpc.aio.secure_channel(addr, credentials, options)

        self.stub = api_grpc.DgraphStub(self.channel)

    async def login(self, login_req, timeout=None, metadata=None, credentials=None):
        """Async login operation.

        Args:
            login_req: LoginRequest protobuf message
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Returns:
            Response protobuf message with JWT token
        """
        return await self.stub.Login(
            login_req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def alter(self, operation, timeout=None, metadata=None, credentials=None):
        """Async alter operation for schema changes.

        Args:
            operation: Operation protobuf message
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Returns:
            Payload protobuf message
        """
        return await self.stub.Alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def query(self, req, timeout=None, metadata=None, credentials=None):
        """Async query or mutate operation.

        Args:
            req: Request protobuf message
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Returns:
            Response protobuf message
        """
        return await self.stub.Query(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def commit_or_abort(self, ctx, timeout=None, metadata=None, credentials=None):
        """Async commit or abort operation.

        Args:
            ctx: TxnContext protobuf message
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Returns:
            TxnContext protobuf message
        """
        return await self.stub.CommitOrAbort(
            ctx, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def check_version(self, check, timeout=None, metadata=None, credentials=None):
        """Async version check operation.

        Args:
            check: Check protobuf message
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Returns:
            Version protobuf message
        """
        return await self.stub.CheckVersion(
            check, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def close(self):
        """Close the async channel gracefully."""
        try:
            await self.channel.close()
        except Exception:
            pass

    @staticmethod
    def parse_host(cloud_endpoint):
        """Converts any cloud endpoint to grpc endpoint.

        Args:
            cloud_endpoint: Cloud endpoint URL or hostname

        Returns:
            Parsed hostname with .grpc. inserted if needed
        """
        host = cloud_endpoint
        if cloud_endpoint.startswith("http"):  # catch http:// and https://
            host = urlparse(cloud_endpoint).netloc
        host = host.split(":", 1)[0]  # remove port if any
        if ".grpc." not in host:
            url_parts = host.split(".", 1)
            host = url_parts[0] + ".grpc." + url_parts[1]
        return host

    @staticmethod
    def from_cloud(cloud_endpoint, api_key, options=None):
        """Returns async Dgraph Client stub for Dgraph Cloud endpoint.

        Usage:
            import pydgraph
            client_stub = pydgraph.AsyncDgraphClientStub.from_cloud("cloud_endpoint", "api-key")
            client = pydgraph.AsyncDgraphClient(client_stub)

        Args:
            cloud_endpoint: Dgraph Cloud endpoint (can be grpc or graphql endpoint)
            api_key: API key for authentication
            options: gRPC channel options

        Returns:
            AsyncDgraphClientStub instance configured for Dgraph Cloud
        """
        host = AsyncDgraphClientStub.parse_host(cloud_endpoint)
        creds = grpc.ssl_channel_credentials()
        call_credentials = grpc.metadata_call_credentials(
            lambda context, callback: callback((("authorization", api_key),), None)
        )
        composite_credentials = grpc.composite_channel_credentials(
            creds, call_credentials
        )
        if options is None:
            options = [("grpc.enable_http_proxy", 0)]
        else:
            options.append(("grpc.enable_http_proxy", 0))
        client_stub = AsyncDgraphClientStub(
            "{host}:{port}".format(host=host, port="443"),
            composite_credentials,
            options=options,
        )
        return client_stub
