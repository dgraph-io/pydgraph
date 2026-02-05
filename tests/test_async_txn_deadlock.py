# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for async transaction deadlock fix.

Verifies that when do_request() encounters an error while holding
asyncio.Lock, the transaction is properly discarded via _locked_discard()
without deadlocking. This is a regression test for the bug where discard()
tried to re-acquire the non-reentrant asyncio.Lock from within do_request().

See: https://github.com/dgraph-io/pydgraph/pull/293
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from pydgraph.async_txn import AsyncTxn
from pydgraph.proto import api_pb2 as api


def _make_txn() -> tuple[AsyncTxn, AsyncMock]:
    """Create an AsyncTxn with a fully mocked client and stub.

    Returns:
        Tuple of (transaction, mock_stub) where mock_stub is the
        underlying gRPC client stub with async methods.
    """
    mock_stub = AsyncMock()
    mock_client = MagicMock()
    mock_client.any_client.return_value = mock_stub
    mock_client.add_login_metadata.return_value = []
    return AsyncTxn(mock_client), mock_stub


def _request_with_mutation() -> api.Request:
    """Create a Request with a mutation so discard will issue an RPC."""
    mutation = api.Mutation(set_json=b'{"name": "test"}')
    request = api.Request()
    request.mutations.append(mutation)
    return request


class TestAsyncTxnDeadlockFix:
    """Regression tests for asyncio.Lock deadlock in do_request error handling."""

    @pytest.mark.asyncio
    async def test_do_request_error_does_not_deadlock(self) -> None:
        """do_request must not deadlock when the gRPC query raises an error.

        The deadlock scenario (before the fix):
        1. do_request() acquires self._lock
        2. _dc.query() raises an error
        3. Error handler called self.discard() which tried to re-acquire self._lock
        4. asyncio.Lock is not reentrant → deadlock

        The fix uses _locked_discard() which skips lock acquisition.
        """
        txn, mock_stub = _make_txn()
        mock_stub.query.side_effect = RuntimeError("schema error: missing index")
        mock_stub.commit_or_abort.return_value = api.TxnContext()

        request = _request_with_mutation()

        # A deadlock would cause wait_for to raise asyncio.TimeoutError
        with pytest.raises(RuntimeError, match="schema error"):
            await asyncio.wait_for(txn.do_request(request), timeout=2.0)

        # Verify the discard RPC was called (transaction had mutations)
        mock_stub.commit_or_abort.assert_called_once()
        assert txn._finished is True

    @pytest.mark.asyncio
    async def test_do_request_error_without_mutations_skips_discard_rpc(self) -> None:
        """When there are no mutations, internal discard skips the RPC."""
        txn, mock_stub = _make_txn()
        mock_stub.query.side_effect = RuntimeError("some query error")

        request = api.Request(query="{ me(func: uid(0x1)) { name } }")

        with pytest.raises(RuntimeError, match="some query error"):
            await asyncio.wait_for(txn.do_request(request), timeout=2.0)

        # No mutations means _prepare_discard returns False → no RPC
        mock_stub.commit_or_abort.assert_not_called()

    @pytest.mark.asyncio
    async def test_locked_discard_asserts_lock_is_held(self) -> None:
        """_locked_discard() raises RuntimeError if lock is not held."""
        txn, _ = _make_txn()

        with pytest.raises(RuntimeError, match="must only be called while holding"):
            await txn._locked_discard()

    @pytest.mark.asyncio
    async def test_public_discard_acquires_lock_correctly(self) -> None:
        """Public discard() acquires the lock then delegates to _locked_discard."""
        txn, mock_stub = _make_txn()
        mock_stub.commit_or_abort.return_value = api.TxnContext()
        txn._mutated = True  # Simulate a prior mutation

        await asyncio.wait_for(txn.discard(), timeout=2.0)

        assert txn._finished is True
        mock_stub.commit_or_abort.assert_called_once()

    @pytest.mark.asyncio
    async def test_lock_released_after_do_request_error(self) -> None:
        """The lock must be released after do_request raises, allowing reuse."""
        txn, mock_stub = _make_txn()
        mock_stub.query.side_effect = RuntimeError("test error")

        request = api.Request(query="{ q(func: uid(0x1)) { name } }")

        with pytest.raises(RuntimeError):
            await txn.do_request(request)

        # If the lock weren't released, this would hang
        acquired = await asyncio.wait_for(txn._lock.acquire(), timeout=1.0)
        assert acquired
        txn._lock.release()

    @pytest.mark.asyncio
    async def test_original_error_raised_when_discard_also_fails(self) -> None:
        """When both query and discard fail, the original query error is raised."""
        txn, mock_stub = _make_txn()
        mock_stub.query.side_effect = RuntimeError("schema error: missing index")
        mock_stub.commit_or_abort.side_effect = RuntimeError("network unavailable")

        request = _request_with_mutation()

        # The original query error must surface, not the discard error
        with pytest.raises(RuntimeError, match="schema error"):
            await asyncio.wait_for(txn.do_request(request), timeout=2.0)
