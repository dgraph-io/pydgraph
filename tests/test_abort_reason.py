# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for surfacing the transaction-abort reason on AbortedError.

The Dgraph server encodes the abort category as a ``"<code>: <detail>"`` prefix on the
gRPC ABORTED status message. These tests verify that the prefix is parsed into an
``AbortReason``, that the full message is preserved, and that aborts from a server which
reports no reason degrade gracefully to ``UNKNOWN``.
"""

from __future__ import annotations

import unittest

import pydgraph
from pydgraph import errors

# Verbatim messages the server sends, from the abort-detail constants in
# dgraph/cmd/zero/oracle.go. Kept literal rather than abbreviated so these fixtures stay
# a faithful record of the wire format: several now contain colons of their own after the
# category prefix, which is exactly the case the parser has to get right.
SERVER_CONFLICT = (
    "conflict: Transaction has been aborted. Please retry. Another transaction "
    "committed to one of the same keys. The conflicting key cannot be identified: it "
    "may be the data key written directly, or an index or count key derived from it. "
    "On an @upsert predicate the uid is excluded, so any two transactions writing the "
    "same value conflict"
)
SERVER_STALE_STARTTS = (
    "stale-startts: Transaction start timestamp is older than the oldest timestamp Zero "
    "can still validate (Zero leader change, or its conflict map was trimmed at a "
    "snapshot). Please retry"
)
SERVER_MOVE_IN_FLIGHT = (
    "predicate-move: Commits on predicate name are blocked due to predicate move"
)
SERVER_MOVE_COMPLETED = (
    "predicate-move: Mutation done in group: 1. Predicate name assigned to 2"
)

# Causes the server deliberately leaves uncategorized: it says what happened, but no
# published category fits, so it sends the detail with no prefix rather than implying a
# wrong remedy.
SERVER_PRE_ABORTED = (
    "Transaction has been aborted. Please retry. It was already aborted before this "
    "commit was decided, which happens when a schema update or a drop-predicate cancels "
    "pending transactions on a predicate it touched, or when the server ages out "
    'transactions idle for longer than --limit "txn-abort-after"'
)
SERVER_TABLET_NIL = "Tablet for name is nil"
SERVER_MALFORMED_KEY = "Unable to find group id in 1name"
SERVER_BAD_GROUP_ID = (
    'unable to parse group id from xname: strconv.Atoi: parsing "x": invalid syntax'
)
SERVER_CTX_CANCELLED = "context canceled"


class TestAbortReason(unittest.TestCase):
    # --- The three server-reported categories ---

    def test_conflict_reason(self) -> None:
        err = errors.AbortedError(SERVER_CONFLICT)
        assert err.reason == pydgraph.AbortReason.CONFLICT

    def test_predicate_move_reason(self) -> None:
        err = errors.AbortedError(SERVER_MOVE_IN_FLIGHT)
        assert err.reason == pydgraph.AbortReason.PREDICATE_MOVE

    def test_stale_startts_reason(self) -> None:
        err = errors.AbortedError(SERVER_STALE_STARTTS)
        assert err.reason == pydgraph.AbortReason.STALE_STARTTS

    # --- Only the first colon delimits the category ---

    def test_categories_parse_despite_embedded_colons(self) -> None:
        # The conflict detail contains "cannot be identified: it may be", and the
        # completed-move detail contains "group: 1". Splitting on the wrong colon would
        # misread both.
        assert SERVER_CONFLICT.count(":") > 1, "fixture must have >1 colon to be useful"
        assert SERVER_MOVE_COMPLETED.count(":") > 1

        assert (
            errors.AbortedError(SERVER_CONFLICT).reason == pydgraph.AbortReason.CONFLICT
        )
        assert (
            errors.AbortedError(SERVER_MOVE_COMPLETED).reason
            == pydgraph.AbortReason.PREDICATE_MOVE
        )

    # --- Full message preserved alongside the parsed reason ---

    def test_full_message_preserved(self) -> None:
        err = errors.AbortedError(SERVER_CONFLICT)
        # The complete human-readable message survives, including the detail explaining
        # which kinds of key could have collided.
        assert str(err) == SERVER_CONFLICT

    # --- Graceful degradation when no category is reported ---

    def test_legacy_message_degrades_to_unknown(self) -> None:
        # The default message (what pre-feature servers emit) has no category prefix.
        err = errors.AbortedError()
        assert err.reason == pydgraph.AbortReason.UNKNOWN

    def test_unrecognized_prefix_degrades_to_unknown(self) -> None:
        err = errors.AbortedError("something-else: not a known category")
        assert err.reason == pydgraph.AbortReason.UNKNOWN

    def test_uncategorized_server_causes_degrade_to_unknown(self) -> None:
        # A current server also sends aborts with no category, for causes no published
        # category fits. The message still explains what happened; only the
        # machine-readable code is absent. These are the real messages, not invented.
        for message in (
            SERVER_PRE_ABORTED,
            SERVER_TABLET_NIL,
            SERVER_MALFORMED_KEY,
            SERVER_BAD_GROUP_ID,
            SERVER_CTX_CANCELLED,
        ):
            err = errors.AbortedError(message)
            assert err.reason == pydgraph.AbortReason.UNKNOWN, message
            assert str(err) == message, "the explanation must survive intact"

    def test_uncategorized_lookalikes_are_not_misparsed(self) -> None:
        # The out-of-band abort opens with the exact sentence a pre-feature server sent
        # for every abort, and the malformed-group-id message carries a colon of its own.
        # Neither may be mistaken for a category: a false CONFLICT would tell a caller to
        # retry something that cannot succeed.
        assert SERVER_PRE_ABORTED.startswith(
            "Transaction has been aborted. Please retry"
        )
        assert (
            errors.AbortedError(SERVER_PRE_ABORTED).reason
            == pydgraph.AbortReason.UNKNOWN
        )

        assert ":" in SERVER_BAD_GROUP_ID
        assert (
            errors.AbortedError(SERVER_BAD_GROUP_ID).reason
            == pydgraph.AbortReason.UNKNOWN
        )

    # --- Parsing robustness ---

    def test_reason_is_case_insensitive_and_trimmed(self) -> None:
        assert (
            errors.AbortedError("CONFLICT: x").reason == pydgraph.AbortReason.CONFLICT
        )
        assert (
            errors.AbortedError("  predicate-move : y").reason
            == pydgraph.AbortReason.PREDICATE_MOVE
        )

    def test_reason_without_detail_still_parses(self) -> None:
        assert errors.AbortedError("conflict").reason == pydgraph.AbortReason.CONFLICT

    def test_parse_abort_reason_none_is_unknown(self) -> None:
        assert errors.parse_abort_reason(None) == pydgraph.AbortReason.UNKNOWN
        assert errors.parse_abort_reason("") == pydgraph.AbortReason.UNKNOWN

    # --- Explicit reason overrides parsing ---

    def test_explicit_reason_overrides_message(self) -> None:
        err = errors.AbortedError(
            "opaque message", reason=pydgraph.AbortReason.STALE_STARTTS
        )
        assert err.reason == pydgraph.AbortReason.STALE_STARTTS


if __name__ == "__main__":
    unittest.main()
