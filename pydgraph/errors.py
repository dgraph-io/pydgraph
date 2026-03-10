# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Errors thrown by the Dgraph client."""

from __future__ import annotations

from pydgraph.meta import VERSION

__author__ = "Garvit Pahal"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__version__ = VERSION
__status__ = "development"


class AbortedError(Exception):
    """Error thrown by aborted transactions."""

    def __init__(
        self, message: str = "Transaction has been aborted. Please retry"
    ) -> None:
        super().__init__(message)


class RetriableError(Exception):
    """Error thrown when the error return by Dgraph indicates the op should be retried."""

    def __init__(self, exception: Exception) -> None:
        self.exception = exception

    def __str__(self) -> str:
        return str(self.exception)


class ConnectionError(Exception):  # noqa: A001
    """Error thrown when the error return when the client has trouble connecting to Dgraph."""

    def __init__(self, exception: Exception) -> None:
        self.exception = exception

    def __str__(self) -> str:
        return str(self.exception)


class TransactionError(Exception):
    """Error thrown when the transaction is invalid (e.g trying to mutate in read-only mode)."""

    def __init__(self, msg: str) -> None:
        super().__init__(msg)
