# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Async stub for RPC requests using grpc.aio."""

from __future__ import annotations

import contextlib
from typing import Any

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

    async def close(self) -> None:
        """Close the async channel gracefully."""
        with contextlib.suppress(Exception):
            await self.channel.close()
