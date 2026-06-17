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


class TestAbortReason(unittest.TestCase):
    # --- The three server-reported categories ---

    def test_conflict_reason(self) -> None:
        err = errors.AbortedError("conflict: Transaction has been aborted. Please retry")
        assert err.reason == pydgraph.AbortReason.CONFLICT

    def test_predicate_move_reason(self) -> None:
        err = errors.AbortedError(
            "predicate-move: Commits on predicate name are blocked due to predicate move"
        )
        assert err.reason == pydgraph.AbortReason.PREDICATE_MOVE

    def test_stale_startts_reason(self) -> None:
        err = errors.AbortedError(
            "stale-startts: Transaction has been aborted due to a leader change. Please retry"
        )
        assert err.reason == pydgraph.AbortReason.STALE_STARTTS

    # --- Full message preserved alongside the parsed reason ---

    def test_full_message_preserved(self) -> None:
        desc = "conflict: Transaction has been aborted. Please retry"
        err = errors.AbortedError(desc)
        assert str(err) == desc

    # --- Graceful degradation against an older server (no reason prefix) ---

    def test_legacy_message_degrades_to_unknown(self) -> None:
        # The default message (what older servers emit) has no category prefix.
        err = errors.AbortedError()
        assert err.reason == pydgraph.AbortReason.UNKNOWN

    def test_unrecognized_prefix_degrades_to_unknown(self) -> None:
        err = errors.AbortedError("something-else: not a known category")
        assert err.reason == pydgraph.AbortReason.UNKNOWN

    # --- Parsing robustness ---

    def test_reason_is_case_insensitive_and_trimmed(self) -> None:
        assert errors.AbortedError("CONFLICT: x").reason == pydgraph.AbortReason.CONFLICT
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
