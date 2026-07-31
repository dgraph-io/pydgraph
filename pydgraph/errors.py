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
      retrying with a fresh transaction is the expected response. The server cannot
      say *which* key collided: conflict keys are one-way fingerprints by the time
      they are compared, so the culprit may be the data key written directly, or an
      index or count key derived from it. On an ``@upsert`` predicate the uid is
      excluded from the comparison, so any two transactions writing the same value
      conflict.
    - ``PREDICATE_MOVE`` — a predicate is being moved between groups and commits on
      it are blocked, or it finished moving while the transaction was open; back off
      and retry once the move completes.
    - ``STALE_STARTTS`` — the transaction's start timestamp is older than the oldest
      timestamp the server can still validate against. That happens after a Zero
      leader change, and also when Zero trims its conflict map at a snapshot, which
      is not a leader change at all. Retry with a fresh transaction.
    - ``UNKNOWN`` — no category was reported. This covers aborts from older servers,
      which do not categorize at all, and aborts a current server declines to
      categorize because no published category fits — for example a transaction
      already aborted out of band by a schema change or the idle-transaction reaper,
      a cancelled request, or a predicate no group currently serves. The message
      still explains what happened; only the machine-readable category is absent.

    Categories are matched on the message prefix, and an unrecognized prefix degrades
    to ``UNKNOWN``. A newer server may therefore introduce categories this enum does
    not name without breaking this client.
    """

    CONFLICT = "conflict"
    PREDICATE_MOVE = "predicate-move"
    STALE_STARTTS = "stale-startts"
    UNKNOWN = "unknown"


def parse_abort_reason(message: str | None) -> AbortReason:
    """Parses the abort category from a server abort message.

    The reason is the ``"<code>: <detail>"`` prefix; matching is case-insensitive and
    tolerant of surrounding whitespace. Only the *first* colon delimits the category —
    several server messages contain colons of their own in the detail that follows.

    A message with no recognized prefix returns :attr:`AbortReason.UNKNOWN`. That is
    the expected result both for a pre-feature server and for causes a current server
    deliberately leaves uncategorized rather than implying the wrong remedy.
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
