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

        Inserts .grpc. subdomain for Dgraph Cloud endpoints when appropriate.
        Handles IPv6, single-label hosts, and various URL formats.

        Args:
            cloud_endpoint: Cloud endpoint URL or hostname

        Returns:
            Parsed hostname with .grpc. inserted if needed

        Raises:
            ValueError: If endpoint cannot be parsed
        """
        # Normalize to have scheme for consistent parsing
        endpoint = cloud_endpoint
        if "://" not in endpoint:
            endpoint = f"//{endpoint}"

        # Parse URL to extract hostname (handles IPv6, ports, etc.)
        try:
            parsed = urlparse(endpoint)
            host = parsed.hostname if parsed.hostname else cloud_endpoint
        except Exception:
            # Fallback for malformed URLs
            host = cloud_endpoint

        # Remove any port that wasn't caught by hostname parsing
        if ":" in host and "[" not in host:  # Not IPv6
            host = host.split(":", 1)[0]

        # Only insert .grpc. if:
        # 1. Not already present
        # 2. Host has at least 2 labels (e.g., "example.com" but not "localhost")
        # 3. Not an IP address
        if ".grpc." not in host and "." in host:
            # Check it's not an IPv4 address
            labels = host.split(".")
            if len(labels) >= 2 and not all(label.isdigit() for label in labels):
                host = f'{labels[0]}.grpc.{".".join(labels[1:])}'

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

        # Create new list to avoid mutating caller's options
        opts = list(options) if options is not None else []

        # Add http proxy setting if not already present
        if not any(k == "grpc.enable_http_proxy" for k, _ in opts):
            opts.append(("grpc.enable_http_proxy", 0))

        client_stub = AsyncDgraphClientStub(
            "{host}:{port}".format(host=host, port="443"),
            composite_credentials,
            options=opts,
        )
        return client_stub
