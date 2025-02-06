# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Errors thrown by the Dgraph client."""

from pydgraph.meta import VERSION

__author__ = "Garvit Pahal"
__maintainer__ = "Hypermode Inc. <hello@hypermode.com>"
__version__ = VERSION
__status__ = "development"


class AbortedError(Exception):
    """Error thrown by aborted transactions."""

    def __init__(self):
        super(AbortedError, self).__init__("Transaction has been aborted. Please retry")


class RetriableError(Exception):
    """Error thrown when the error return by Dgraph indicates the op should be retried."""

    def __init__(self, exception):
        self.exception = exception

    def __str__(self):
        return str(self.exception)


class ConnectionError(Exception):
    """Error thrown when the error return when the client has trouble connecting to Dgraph."""

    def __init__(self, exception):
        self.exception = exception

    def __str__(self):
        return str(self.exception)


class TransactionError(Exception):
    """Error thrown when the transaction is invalid (e.g trying to mutate in read-only mode)."""

    def __init__(self, msg):
        super(TransactionError, self).__init__(msg)
