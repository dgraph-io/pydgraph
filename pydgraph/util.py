# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Various utility functions."""

import sys

import grpc
import grpc.aio

from pydgraph.meta import VERSION

__author__ = "Shailesh Kochhar <shailesh.kochhar@gmail.com>"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__version__ = VERSION
__status__ = "development"


def is_string(string):
    """Checks if argument is a string. Compatible with Python 2 and 3."""
    if sys.version_info[0] < 3:
        return isinstance(string, basestring)

    return isinstance(string, str)


def is_jwt_expired(exception):
    return "Token is expired" in str(exception)


def is_aborted_error(error):
    """Returns true if the error is due to an aborted transaction."""
    # Check for both sync and async gRPC error types
    if (
        isinstance(error, grpc._channel._Rendezvous)
        or isinstance(error, grpc._channel._InactiveRpcError)
        or isinstance(error, grpc.aio.AioRpcError)
    ):
        status_code = error.code()
        if (
            status_code == grpc.StatusCode.ABORTED
            or status_code == grpc.StatusCode.FAILED_PRECONDITION
        ):
            return True
    return False


def is_retriable_error(error):
    """Returns true if the error is retriable (e.g server is not ready yet)."""
    msg = str(error)
    return "Please retry" in msg or "opIndexing is already running" in msg


def is_connection_error(error):
    """Returns true if the error is caused connection issues."""
    msg = str(error)
    return "Unhealthy connection" in msg or "No connection exists" in msg
