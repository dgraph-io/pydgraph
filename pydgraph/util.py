# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Various utility functions."""

from __future__ import annotations

from typing import Any

import grpc
import grpc.aio

from pydgraph.meta import VERSION

__author__ = "Shailesh Kochhar <shailesh.kochhar@gmail.com>"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__version__ = VERSION
__status__ = "development"


def is_string(string: Any) -> bool:
    """Checks if argument is a string."""
    return isinstance(string, str)


def is_jwt_expired(exception: Exception) -> bool:
    """Returns true if the JWT token in the exception is expired."""
    return "Token is expired" in str(exception)


def is_aborted_error(error: Exception) -> bool:
    """Returns true if the error is due to an aborted transaction."""
    # Check for both sync and async gRPC error types
    if isinstance(
        error,
        (grpc._channel._Rendezvous, grpc._channel._InactiveRpcError, grpc.aio.AioRpcError),
    ):
        status_code = error.code()
        if (
            status_code == grpc.StatusCode.ABORTED
            or status_code == grpc.StatusCode.FAILED_PRECONDITION
        ):
            return True
    return False


def is_retriable_error(error: Exception) -> bool:
    """Returns true if the error is retriable (e.g server is not ready yet)."""
    msg = str(error)
    return "Please retry" in msg or "opIndexing is already running" in msg


def is_connection_error(error: Exception) -> bool:
    """Returns true if the error is caused connection issues."""
    msg = str(error)
    return "Unhealthy connection" in msg or "No connection exists" in msg
