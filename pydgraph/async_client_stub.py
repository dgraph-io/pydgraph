# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Async stub for RPC requests using grpc.aio."""

from __future__ import annotations

import contextlib
import warnings
from typing import Any
from urllib.parse import urlparse

import grpc
import grpc.aio

from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2 as api
from pydgraph.proto import api_pb2_grpc as api_grpc

__author__ = "Istari Digital, Inc."
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__version__ = VERSION
__status__ = "development"


class AsyncDgraphClientStub:
    """Async stub for the Dgraph grpc client using grpc.aio."""

    def __init__(
        self,
        addr: str = "localhost:9080",
        credentials: grpc.ChannelCredentials | None = None,
        options: list[tuple[str, Any]] | None = None,
    ) -> None:
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

    async def login(
        self,
        login_req: api.LoginRequest,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Response:
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

    async def alter(
        self,
        operation: api.Operation,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Payload:
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

    async def query(
        self,
        req: api.Request,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Response:
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

    async def commit_or_abort(
        self,
        ctx: api.TxnContext,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.TxnContext:
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

    async def check_version(
        self,
        check: api.Check,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Version:
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

    @staticmethod
    def parse_host(cloud_endpoint: str) -> str:
        """Converts any cloud endpoint to grpc endpoint.

        Handles IPv6, single-label hosts, and various URL formats.

        .. deprecated:: 25.1.0
            Dgraph Cloud service has been discontinued. This method will be
            removed in version 26.0.0. Use the standard AsyncDgraphClientStub
            constructor with grpc.ssl_channel_credentials() instead.
        """
        warnings.warn(
            "parse_host() is deprecated as Dgraph Cloud has been discontinued. "
            "This method will be removed in version 26.0.0. "
            "Use the standard AsyncDgraphClientStub constructor with SSL credentials instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        endpoint = cloud_endpoint
        if "://" not in endpoint:
            endpoint = f"//{endpoint}"

        try:
            parsed = urlparse(endpoint)
            host = parsed.hostname if parsed.hostname else cloud_endpoint
        except Exception:
            host = cloud_endpoint

        if ":" in host and "[" not in host:
            host = host.split(":", 1)[0]

        if ".grpc." not in host and "." in host:
            labels = host.split(".")
            if len(labels) >= 2 and not all(label.isdigit() for label in labels):
                host = f"{labels[0]}.grpc.{'.'.join(labels[1:])}"

        return host

    @staticmethod
    def from_cloud(
        cloud_endpoint: str,
        api_key: str,
        options: list[tuple[str, Any]] | None = None,
    ) -> AsyncDgraphClientStub:
        """Returns async Dgraph Client stub for Dgraph Cloud endpoint.

        .. deprecated:: 25.1.0
            Dgraph Cloud service has been discontinued. This method will be
            removed in version 26.0.0. Use the standard AsyncDgraphClientStub
            constructor with grpc.ssl_channel_credentials() instead.

        Example migration:
            Old: stub = AsyncDgraphClientStub.from_cloud(endpoint, api_key)

            New:
                import grpc
                creds = grpc.ssl_channel_credentials()
                call_creds = grpc.metadata_call_credentials(
                    lambda _c, cb: cb((("authorization", api_key),), None)
                )
                composite = grpc.composite_channel_credentials(creds, call_creds)
                stub = AsyncDgraphClientStub(endpoint, composite)
        """
        warnings.warn(
            "from_cloud() is deprecated as Dgraph Cloud has been discontinued. "
            "This method will be removed in version 26.0.0. "
            "Use the standard AsyncDgraphClientStub constructor with SSL credentials. "
            "See the docstring for migration examples.",
            DeprecationWarning,
            stacklevel=2,
        )

        host = AsyncDgraphClientStub.parse_host(cloud_endpoint)
        creds = grpc.ssl_channel_credentials()
        call_credentials = grpc.metadata_call_credentials(
            lambda _context, callback: callback((("authorization", api_key),), None)
        )
        composite_credentials = grpc.composite_channel_credentials(
            creds, call_credentials
        )

        opts = list(options) if options is not None else []
        if not any(k == "grpc.enable_http_proxy" for k, _ in opts):
            opts.append(("grpc.enable_http_proxy", 0))

        return AsyncDgraphClientStub(
            f"{host}:443",
            composite_credentials,
            options=opts,
        )

    async def close(self) -> None:
        """Close the async channel gracefully."""
        with contextlib.suppress(Exception):
            await self.channel.close()
