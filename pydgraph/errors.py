# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Errors thrown by the Dgraph client."""

from __future__ import annotations

import enum

from pydgraph.meta import VERSION

__author__ = "Garvit Pahal"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__version__ = VERSION
__status__ = "development"


class AbortReason(enum.Enum):
    """The category of a transaction abort, as reported by the Dgraph server.

    The server encodes the reason as a ``"<code>: <detail>"`` prefix on the gRPC
    ABORTED status message; :func:`parse_abort_reason` maps that prefix to one of
    these values.

    - ``CONFLICT`` — a write-write conflict with another concurrent transaction;
      retrying with a fresh transaction is the expected response.
    - ``PREDICATE_MOVE`` — a predicate is being moved between groups and commits on
      it are temporarily blocked; back off and retry once the move completes.
    - ``STALE_STARTTS`` — the transaction's start timestamp predates the current Zero
      leader (a leader change); retry with a fresh transaction.
    - ``UNKNOWN`` — no reason was reported. Returned for aborts from older servers
      that do not yet categorize the reason, so callers degrade gracefully.
    """

    CONFLICT = "conflict"
    PREDICATE_MOVE = "predicate-move"
    STALE_STARTTS = "stale-startts"
    UNKNOWN = "unknown"


def parse_abort_reason(message: str | None) -> AbortReason:
    """Parses the abort category from a server abort message.

    The reason is the ``"<code>: <detail>"`` prefix; matching is case-insensitive and
    tolerant of surrounding whitespace. A message with no recognized prefix (e.g. from
    a pre-feature server) returns :attr:`AbortReason.UNKNOWN`.
    """
    if not message:
        return AbortReason.UNKNOWN
    code = message.split(":", 1)[0].strip().lower()
    for reason in (
        AbortReason.CONFLICT,
        AbortReason.PREDICATE_MOVE,
        AbortReason.STALE_STARTTS,
    ):
        if code == reason.value:
            return reason
    return AbortReason.UNKNOWN


class AbortedError(Exception):
    """Error thrown by aborted transactions.

    The parsed abort category is available as :attr:`reason`; the full server message
    remains available via ``str(error)``.
    """

    def __init__(
        self,
        message: str = "Transaction has been aborted. Please retry",
        reason: AbortReason | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason if reason is not None else parse_abort_reason(message)


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
